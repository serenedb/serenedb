////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "catalog/store/store.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include <algorithm>
#include <duckdb/common/exception/binder_exception.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <exception>
#include <filesystem>
#include <initializer_list>
#include <utility>

#include "basics/assert.h"
#include "basics/duckdb_engine.h"
#include "basics/exceptions.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "catalog/database.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/schema.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kStoreAlias = "__sdb_store";
constexpr std::string_view kStoreFile = "store.db";
constexpr std::string_view kCatalogTable = R"("__sdb_store".main.sdb_catalog)";
constexpr std::string_view kSequenceTable = R"("__sdb_store".main.sdb_seq)";

void ExecOrFatal(duckdb::Connection& conn, const std::string& sql) {
  auto res = conn.Query(sql);
  if (res->HasError()) {
    SDB_FATAL(STARTUP, "catalog store: '", sql, "' failed: ", res->GetError());
  }
}

duckdb::unique_ptr<duckdb::PreparedStatement> PrepareOrFatal(
  duckdb::Connection& conn, std::string_view sql) {
  auto stmt = conn.Prepare(std::string{sql});
  if (stmt->HasError()) {
    SDB_FATAL(STARTUP, "catalog store: preparing '", sql,
              "' failed: ", stmt->GetError());
  }
  return stmt;
}

Result Exec(duckdb::PreparedStatement& stmt,
            duckdb::vector<duckdb::Value> values) {
  auto res = stmt.Execute(values, /*allow_stream_result=*/false);
  if (res->HasError()) {
    return {ERROR_INTERNAL, res->GetError()};
  }
  return {};
}

duckdb::Value IdValue(ObjectId id) { return duckdb::Value::UBIGINT(id.id()); }

duckdb::Value TypeValue(ObjectType type) {
  return duckdb::Value::UTINYINT(static_cast<uint8_t>(type));
}

duckdb::Value DefValue(std::string_view def) {
  return duckdb::Value::BLOB(duckdb::const_data_ptr_cast(def.data()),
                             def.size());
}

template<typename Fn>
Result RunInTransaction(duckdb::Connection& conn, Fn&& fn) {
  try {
    conn.BeginTransaction();
  } catch (const std::exception& e) {
    return {ERROR_INTERNAL, e.what()};
  }
  Result r = fn();
  try {
    if (r.ok()) {
      conn.Commit();
    } else {
      conn.Rollback();
    }
  } catch (const std::exception& e) {
    if (r.ok()) {
      return {ERROR_INTERNAL, e.what()};
    }
  }
  return r;
}

}  // namespace

void CatalogStore::WriteContext::PutDefinition(ObjectId parent_id,
                                               ObjectType type, ObjectId id,
                                               std::string_view def) {
  _entries.push_back({.op = Op::kPutDefinition,
                      .key = {parent_id, type, id},
                      .def = std::string{def}});
}

void CatalogStore::WriteContext::PutSequence(ObjectId sequence_id,
                                             uint64_t value) {
  _entries.push_back({.op = Op::kPutSequence,
                      .key = {.id = sequence_id},
                      .sequence_value = value});
}

void CatalogStore::WriteContext::DropDefinition(ObjectId parent_id,
                                                ObjectType type, ObjectId id) {
  _entries.push_back({.op = Op::kDropDefinition, .key = {parent_id, type, id}});
}

void CatalogStore::WriteContext::DropSequence(ObjectId sequence_id) {
  _entries.push_back({.op = Op::kDropSequence, .key = {.id = sequence_id}});
}

void CatalogStore::WriteContext::WriteTombstone(ObjectId parent_id,
                                                ObjectId id) {
  PutDefinition(parent_id, ObjectType::Tombstone, id, {});
}

CatalogStore::CatalogStore() {
  SDB_ASSERT(gInstance == nullptr);
  gInstance = this;
}

CatalogStore::~CatalogStore() { gInstance = nullptr; }

void CatalogStore::Initialize(std::string_view database_directory) {
  namespace fs = std::filesystem;
  const auto dir =
    fs::path{database_directory} / StaticStrings::kCatalogStoreRoot;
  std::error_code ec;
  fs::create_directories(dir, ec);
  if (ec) {
    SDB_FATAL(STARTUP, "catalog store: cannot create directory '", dir.string(),
              "': ", ec.message());
  }
  const auto file = (dir / kStoreFile).string();

  {
    absl::MutexLock lock{&_mutex};
    absl::MutexLock seq_lock{&_seq_mutex};
    _conn = DuckDBEngine::Instance().CreateConnection();
    _seq_conn = DuckDBEngine::Instance().CreateConnection();

    ExecOrFatal(
      *_conn, absl::StrCat("ATTACH '", absl::StrReplaceAll(file, {{"'", "''"}}),
                           "' AS \"", kStoreAlias, "\""));
    ExecOrFatal(*_conn,
                absl::StrCat("CREATE TABLE IF NOT EXISTS ", kCatalogTable,
                             " (parent_id UBIGINT, type UTINYINT, id UBIGINT, "
                             "def BLOB)"));
    ExecOrFatal(*_conn,
                absl::StrCat("CREATE TABLE IF NOT EXISTS ", kSequenceTable,
                             " (id UBIGINT, counter UBIGINT)"));

    _delete_definition = PrepareOrFatal(
      *_conn, absl::StrCat("DELETE FROM ", kCatalogTable,
                           " WHERE parent_id = $1 AND type = $2 AND id = $3"));
    _insert_definition = PrepareOrFatal(
      *_conn,
      absl::StrCat("INSERT INTO ", kCatalogTable, " VALUES ($1, $2, $3, $4)"));
    _delete_by_parent_type = PrepareOrFatal(
      *_conn, absl::StrCat("DELETE FROM ", kCatalogTable,
                           " WHERE parent_id = $1 AND type = $2"));
    _delete_by_parent = PrepareOrFatal(
      *_conn,
      absl::StrCat("DELETE FROM ", kCatalogTable, " WHERE parent_id = $1"));
    _select_definitions = PrepareOrFatal(
      *_conn, absl::StrCat("SELECT id, def FROM ", kCatalogTable,
                           " WHERE parent_id = $1 AND type = $2 ORDER BY id"));
    _delete_sequence_batch = PrepareOrFatal(
      *_conn, absl::StrCat("DELETE FROM ", kSequenceTable, " WHERE id = $1"));
    _insert_sequence_batch = PrepareOrFatal(
      *_conn, absl::StrCat("INSERT INTO ", kSequenceTable, " VALUES ($1, $2)"));

    _select_sequence = PrepareOrFatal(
      *_seq_conn,
      absl::StrCat("SELECT counter FROM ", kSequenceTable, " WHERE id = $1"));
    _delete_sequence = PrepareOrFatal(
      *_seq_conn,
      absl::StrCat("DELETE FROM ", kSequenceTable, " WHERE id = $1"));
    _insert_sequence = PrepareOrFatal(
      *_seq_conn,
      absl::StrCat("INSERT INTO ", kSequenceTable, " VALUES ($1, $2)"));
  }

  EnsureSystemDatabase();
}

void CatalogStore::Shutdown() {
  {
    absl::MutexLock lock{&_mutex};
    absl::MutexLock seq_lock{&_seq_mutex};
    _delete_definition.reset();
    _insert_definition.reset();
    _delete_by_parent_type.reset();
    _delete_by_parent.reset();
    _select_definitions.reset();
    _delete_sequence_batch.reset();
    _insert_sequence_batch.reset();
    _select_sequence.reset();
    _delete_sequence.reset();
    _insert_sequence.reset();
    _conn.reset();
    _seq_conn.reset();
  }
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto res = conn->Query(absl::StrCat("DETACH \"", kStoreAlias, "\""));
  if (res->HasError()) {
    SDB_WARN(STARTUP, "catalog store: detach failed: ", res->GetError());
  }
}

Result CatalogStore::ExecuteEntries(std::vector<WriteContext::Entry>& entries) {
  absl::MutexLock lock{&_mutex};
  return RunInTransaction(*_conn, [&]() -> Result {
    for (const auto& entry : entries) {
      switch (entry.op) {
        case WriteContext::Op::kPutDefinition: {
          if (auto r = Exec(*_delete_definition,
                            {IdValue(entry.key.parent_id),
                             TypeValue(entry.key.type), IdValue(entry.key.id)});
              r.fail()) {
            return r;
          }
          if (auto r =
                Exec(*_insert_definition,
                     {IdValue(entry.key.parent_id), TypeValue(entry.key.type),
                      IdValue(entry.key.id), DefValue(entry.def)});
              r.fail()) {
            return r;
          }
          break;
        }
        case WriteContext::Op::kDropDefinition: {
          if (auto r = Exec(*_delete_definition,
                            {IdValue(entry.key.parent_id),
                             TypeValue(entry.key.type), IdValue(entry.key.id)});
              r.fail()) {
            return r;
          }
          break;
        }
        case WriteContext::Op::kPutSequence: {
          if (auto r = Exec(*_delete_sequence_batch, {IdValue(entry.key.id)});
              r.fail()) {
            return r;
          }
          if (auto r = Exec(*_insert_sequence_batch,
                            {IdValue(entry.key.id),
                             duckdb::Value::UBIGINT(entry.sequence_value)});
              r.fail()) {
            return r;
          }
          break;
        }
        case WriteContext::Op::kDropSequence: {
          if (auto r = Exec(*_delete_sequence_batch, {IdValue(entry.key.id)});
              r.fail()) {
            return r;
          }
          break;
        }
      }
    }
    return {};
  });
}

Result CatalogStore::CreateDefinition(ObjectId parent_id, ObjectType type,
                                      ObjectId id, std::string_view def) {
  WriteContext ctx;
  ctx.PutDefinition(parent_id, type, id, def);
  return ExecuteEntries(ctx._entries);
}

Result CatalogStore::Write(absl::FunctionRef<void(WriteContext&)> fill) {
  WriteContext ctx;
  fill(ctx);
  return ExecuteEntries(ctx._entries);
}

Result CatalogStore::DropDefinition(ObjectId parent_id, ObjectType type,
                                    ObjectId id) {
  WriteContext ctx;
  ctx.DropDefinition(parent_id, type, id);
  return ExecuteEntries(ctx._entries);
}

Result CatalogStore::DropSequence(ObjectId sequence_id) {
  WriteContext ctx;
  ctx.DropSequence(sequence_id);
  return ExecuteEntries(ctx._entries);
}

Result CatalogStore::DropEntry(ObjectId parent_id, ObjectType type) {
  absl::MutexLock lock{&_mutex};
  return RunInTransaction(*_conn, [&]() -> Result {
    return Exec(*_delete_by_parent_type, {IdValue(parent_id), TypeValue(type)});
  });
}

Result CatalogStore::DropEntry(ObjectId parent_id) {
  absl::MutexLock lock{&_mutex};
  return RunInTransaction(*_conn, [&]() -> Result {
    return Exec(*_delete_by_parent, {IdValue(parent_id)});
  });
}

Result CatalogStore::WriteTombstone(ObjectId parent_id, ObjectId id) {
  return CreateDefinition(parent_id, ObjectType::Tombstone, id, {});
}

Result CatalogStore::VisitDefinitions(
  ObjectId parent_id, ObjectType type,
  absl::FunctionRef<Result(Key, std::string_view)> visitor) {
  duckdb::unique_ptr<duckdb::QueryResult> res;
  {
    absl::MutexLock lock{&_mutex};
    duckdb::vector<duckdb::Value> params{IdValue(parent_id), TypeValue(type)};
    res = _select_definitions->Execute(params, /*allow_stream_result=*/false);
  }
  if (res->HasError()) {
    return {ERROR_INTERNAL, res->GetError()};
  }
  while (auto chunk = res->Fetch()) {
    chunk->Flatten();
    const auto count = chunk->size();
    const auto* ids = duckdb::FlatVector::GetData<uint64_t>(chunk->data[0]);
    const auto* defs =
      duckdb::FlatVector::GetData<duckdb::string_t>(chunk->data[1]);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      if (auto r =
            visitor(Key{parent_id, type, ObjectId{ids[i]}},
                    std::string_view{defs[i].GetData(), defs[i].GetSize()});
          r.fail()) {
        return r;
      }
    }
  }
  return {};
}

void CatalogStore::BootConsumeCatalog(duckdb::DataChunk& input) {
  SDB_ENSURE(_boot_loading.load(std::memory_order_acquire), ERROR_FORBIDDEN,
             "sdb_init_catalog is internal to server startup");
  duckdb::UnifiedVectorFormat parents;
  duckdb::UnifiedVectorFormat types;
  duckdb::UnifiedVectorFormat ids;
  duckdb::UnifiedVectorFormat defs;
  input.data[0].ToUnifiedFormat(parents);
  input.data[1].ToUnifiedFormat(types);
  input.data[2].ToUnifiedFormat(ids);
  input.data[3].ToUnifiedFormat(defs);
  const auto* parent_data =
    duckdb::UnifiedVectorFormat::GetData<uint64_t>(parents);
  const auto* type_data = duckdb::UnifiedVectorFormat::GetData<uint8_t>(types);
  const auto* id_data = duckdb::UnifiedVectorFormat::GetData<uint64_t>(ids);
  const auto* def_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(defs);
  const auto count = input.size();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto parent = parent_data[parents.sel->get_index(i)];
    const auto type = type_data[types.sel->get_index(i)];
    const auto id = id_data[ids.sel->get_index(i)];
    const auto& def = def_data[defs.sel->get_index(i)];
    _boot_defs[{parent, type}].push_back(
      {.id = ObjectId{id}, .def = std::string{def.GetData(), def.GetSize()}});
  }
}

void CatalogStore::BootConsumeSequences(duckdb::DataChunk& input) {
  SDB_ENSURE(_boot_loading.load(std::memory_order_acquire), ERROR_FORBIDDEN,
             "sdb_init_sequences is internal to server startup");
  duckdb::UnifiedVectorFormat ids;
  duckdb::UnifiedVectorFormat counters;
  input.data[0].ToUnifiedFormat(ids);
  input.data[1].ToUnifiedFormat(counters);
  const auto* id_data = duckdb::UnifiedVectorFormat::GetData<uint64_t>(ids);
  const auto* counter_data =
    duckdb::UnifiedVectorFormat::GetData<uint64_t>(counters);
  const auto count = input.size();
  for (duckdb::idx_t i = 0; i < count; ++i) {
    _boot_sequences[id_data[ids.sel->get_index(i)]] =
      counter_data[counters.sel->get_index(i)];
  }
}

Result CatalogStore::LoadBootState() {
  _boot_defs.clear();
  _boot_sequences.clear();
  _boot_loading.store(true, std::memory_order_release);
  Result r;
  {
    absl::MutexLock lock{&_mutex};
    auto res = _conn->Query(
      absl::StrCat("SELECT * FROM sdb_init_catalog((SELECT parent_id, type, "
                   "id, def FROM ",
                   kCatalogTable, "))"));
    if (res->HasError()) {
      r = Result{ERROR_INTERNAL, res->GetError()};
    } else {
      res = _conn->Query(absl::StrCat(
        "SELECT * FROM sdb_init_sequences((SELECT id, counter FROM ",
        kSequenceTable, "))"));
      if (res->HasError()) {
        r = Result{ERROR_INTERNAL, res->GetError()};
      }
    }
  }
  _boot_loading.store(false, std::memory_order_release);
  if (r.fail()) {
    ReleaseBootState();
    return r;
  }
  for (auto& [key, defs] : _boot_defs) {
    std::sort(defs.begin(), defs.end(),
              [](const BootDef& lhs, const BootDef& rhs) {
                return lhs.id.id() < rhs.id.id();
              });
  }
  return {};
}

Result CatalogStore::VisitBoot(
  ObjectId parent_id, ObjectType type,
  absl::FunctionRef<Result(Key, std::string_view)> visitor) const {
  const auto it = _boot_defs.find({parent_id.id(), static_cast<uint8_t>(type)});
  if (it == _boot_defs.end()) {
    return {};
  }
  for (const auto& def : it->second) {
    if (auto r = visitor(Key{parent_id, type, def.id}, def.def); r.fail()) {
      return r;
    }
  }
  return {};
}

bool CatalogStore::TryGetBootSequenceValue(ObjectId sequence_id,
                                           uint64_t& value) const {
  const auto it = _boot_sequences.find(sequence_id.id());
  if (it == _boot_sequences.end()) {
    return false;
  }
  value = it->second;
  return true;
}

void CatalogStore::ReleaseBootState() {
  _boot_defs.clear();
  _boot_sequences.clear();
}

uint64_t CatalogStore::BootDefsLoaded() const {
  uint64_t total = 0;
  for (const auto& [key, defs] : _boot_defs) {
    total += defs.size();
  }
  return total;
}

uint64_t CatalogStore::BootSequencesLoaded() const {
  return _boot_sequences.size();
}

Result CatalogStore::PutSequenceValue(ObjectId sequence_id, uint64_t value) {
  absl::MutexLock lock{&_seq_mutex};
  return RunInTransaction(*_seq_conn, [&]() -> Result {
    if (auto r = Exec(*_delete_sequence, {IdValue(sequence_id)}); r.fail()) {
      return r;
    }
    return Exec(*_insert_sequence,
                {IdValue(sequence_id), duckdb::Value::UBIGINT(value)});
  });
}

Result CatalogStore::GetSequenceValue(ObjectId sequence_id, uint64_t& value) {
  value = 0;
  duckdb::unique_ptr<duckdb::QueryResult> res;
  {
    absl::MutexLock lock{&_seq_mutex};
    duckdb::vector<duckdb::Value> params{IdValue(sequence_id)};
    res = _select_sequence->Execute(params, /*allow_stream_result=*/false);
  }
  if (res->HasError()) {
    return {ERROR_INTERNAL, res->GetError()};
  }
  if (auto chunk = res->Fetch(); chunk && chunk->size() > 0) {
    SDB_ASSERT(chunk->size() == 1);
    chunk->Flatten();
    value = duckdb::FlatVector::GetData<uint64_t>(chunk->data[0])[0];
  }
  return {};
}

void CatalogStore::EnsureSystemDatabase() {
  bool has_system = false;
  std::ignore = VisitDefinitions(id::kInstance, ObjectType::Database,
                                 [&](Key key, std::string_view) -> Result {
                                   has_system = key.id == id::kSystemDB;
                                   return {ERROR_INTERNAL};  // stop iteration
                                 });

  if (has_system) {
    SDB_TRACE(STARTUP, "Found system database");
    return;
  }

  Database database{
    id::kSystemDB,
    DatabaseOptions{std::string{StaticStrings::kDefaultDatabase}}};
  duckdb::MemoryStream stream;
  auto database_bytes = SerializeObject(database, stream);
  auto r = CreateDefinition(id::kInstance, ObjectType::Database, id::kSystemDB,
                            database_bytes);
  if (!r.ok()) {
    SDB_FATAL(STARTUP, "unable to write database marker: ", r.errorMessage());
  }

  const auto schema_id = NextId();
  Schema schema{id::kSystemDB,
                SchemaOptions{.id = schema_id,
                              .name = std::string{StaticStrings::kPublic}}};
  auto schema_bytes = SerializeObject(schema, stream);
  r = CreateDefinition(id::kSystemDB, ObjectType::Schema, schema_id,
                       schema_bytes);
  if (!r.ok()) {
    SDB_FATAL(STARTUP, "unable to write schema marker: ", r.errorMessage());
  }
}

CatalogStore& GetCatalogStore() { return CatalogStore::instance(); }

namespace {

void CheckInitInput(const duckdb::vector<duckdb::LogicalType>& types,
                    std::initializer_list<duckdb::LogicalTypeId> expected,
                    std::string_view fn) {
  if (types.size() != expected.size() ||
      !std::equal(expected.begin(), expected.end(), types.begin(),
                  [](duckdb::LogicalTypeId id, const duckdb::LogicalType& t) {
                    return t.id() == id;
                  })) {
    throw duckdb::BinderException(
      absl::StrCat(fn, ": unexpected input table shape"));
  }
}

duckdb::unique_ptr<duckdb::FunctionData> BindInitCatalog(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  CheckInitInput(
    input.input_table_types,
    {duckdb::LogicalTypeId::UBIGINT, duckdb::LogicalTypeId::UTINYINT,
     duckdb::LogicalTypeId::UBIGINT, duckdb::LogicalTypeId::BLOB},
    "sdb_init_catalog");
  return_types.emplace_back(duckdb::LogicalType::UBIGINT);
  names.emplace_back("loaded");
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::unique_ptr<duckdb::FunctionData> BindInitSequences(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  CheckInitInput(
    input.input_table_types,
    {duckdb::LogicalTypeId::UBIGINT, duckdb::LogicalTypeId::UBIGINT},
    "sdb_init_sequences");
  return_types.emplace_back(duckdb::LogicalType::UBIGINT);
  names.emplace_back("loaded");
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitBootGlobal(
  duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
  // Default MaxThreads() == 1 caps the pipeline at one thread: the boot
  // maps are built lock-free.
  return duckdb::make_uniq<duckdb::GlobalTableFunctionState>();
}

duckdb::OperatorResultType InitCatalogExec(duckdb::ExecutionContext&,
                                           duckdb::TableFunctionInput&,
                                           duckdb::DataChunk& input,
                                           duckdb::DataChunk& output) {
  CatalogStore::instance().BootConsumeCatalog(input);
  output.SetCardinality(0);
  return duckdb::OperatorResultType::NEED_MORE_INPUT;
}

duckdb::OperatorResultType InitSequencesExec(duckdb::ExecutionContext&,
                                             duckdb::TableFunctionInput&,
                                             duckdb::DataChunk& input,
                                             duckdb::DataChunk& output) {
  CatalogStore::instance().BootConsumeSequences(input);
  output.SetCardinality(0);
  return duckdb::OperatorResultType::NEED_MORE_INPUT;
}

duckdb::OperatorFinalizeResultType InitCatalogFinal(duckdb::ExecutionContext&,
                                                    duckdb::TableFunctionInput&,
                                                    duckdb::DataChunk& output) {
  output.SetCardinality(1);
  output.SetValue(
    0, 0, duckdb::Value::UBIGINT(CatalogStore::instance().BootDefsLoaded()));
  return duckdb::OperatorFinalizeResultType::FINISHED;
}

duckdb::OperatorFinalizeResultType InitSequencesFinal(
  duckdb::ExecutionContext&, duckdb::TableFunctionInput&,
  duckdb::DataChunk& output) {
  output.SetCardinality(1);
  output.SetValue(
    0, 0,
    duckdb::Value::UBIGINT(CatalogStore::instance().BootSequencesLoaded()));
  return duckdb::OperatorFinalizeResultType::FINISHED;
}

}  // namespace

void RegisterCatalogStoreFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  {
    duckdb::TableFunction fn{"sdb_init_catalog",
                             {duckdb::LogicalType::TABLE},
                             nullptr,
                             BindInitCatalog};
    fn.init_global = InitBootGlobal;
    fn.in_out_function = InitCatalogExec;
    fn.in_out_function_final = InitCatalogFinal;
    loader.RegisterFunction(fn);
  }
  {
    duckdb::TableFunction fn{"sdb_init_sequences",
                             {duckdb::LogicalType::TABLE},
                             nullptr,
                             BindInitSequences};
    fn.init_global = InitBootGlobal;
    fn.in_out_function = InitSequencesExec;
    fn.in_out_function_final = InitSequencesFinal;
    loader.RegisterFunction(fn);
  }
}

}  // namespace sdb::catalog
