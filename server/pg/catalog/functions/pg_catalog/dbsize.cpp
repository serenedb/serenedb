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

#include <absl/algorithm/container.h>
#include <absl/functional/function_ref.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/types/cast_helpers.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/storage/database_size.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/index_entry.hpp>
#include <duckdb/storage/table_io_manager.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/common.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_store_index.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/oids.h"
#include "pg/catalog/utils/lookup.h"
#include "pg/connection_context.h"
#include "pg/types/logical.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::pg {
namespace {

struct RelationStorageSize {
  int64_t bytes = 0;
  int64_t persistent_blocks = 0;
};

RelationStorageSize StoreTableDataSize(duckdb::ClientContext& context,
                                       duckdb::TableCatalogEntry& table) {
  RelationStorageSize result;
  if (!table.IsDuckTable()) {
    return result;
  }
  auto& storage = table.GetStorage();
  duckdb::QueryContext query_context(context);
  irs::containers::FlatHashSet<duckdb::block_id_t> blocks;
  int64_t transient_bytes = 0;
  for (const auto& info : storage.GetColumnSegmentInfo(query_context)) {
    if (info.persistent) {
      blocks.insert(info.block_id);
      blocks.insert(info.additional_blocks.begin(),
                    info.additional_blocks.end());
    } else {
      transient_bytes += static_cast<int64_t>(info.segment_size);
    }
  }
  const auto block_size =
    storage.GetTableIOManager().GetBlockManagerForRowData().GetBlockSize();
  result.persistent_blocks = static_cast<int64_t>(blocks.size());
  result.bytes = result.persistent_blocks * static_cast<int64_t>(block_size) +
                 transient_bytes;
  return result;
}

int64_t BoundIndexBytes(duckdb::ClientContext& context,
                        duckdb::DataTableInfo& info, const auto& match) {
  info.BindIndexes(context);
  int64_t total = 0;
  for (auto entry : info.GetIndexes().IndexEntries()) {
    if (entry->GetBindState() == duckdb::IndexBindState::BOUND) {
      const auto index = entry->GetReadHandle<duckdb::BoundIndex>();
      if (match(index)) {
        total += static_cast<int64_t>(index->GetAllocationSize());
      }
    }
  }
  return total;
}

int64_t StoreTableIndexBytes(duckdb::ClientContext& context,
                             duckdb::TableCatalogEntry& table) {
  if (!table.IsDuckTable()) {
    return 0;
  }
  return BoundIndexBytes(context, *table.GetStorage().GetDataTableInfo(),
                         [](const auto&) { return true; });
}

int64_t SearchTableBytes(const catalog::SearchTableEntry& table) {
  // Introspection must not open a store as a side effect: report only what
  // is already open.
  auto reader = table.Storage()->GetDirectoryReader();
  if (!reader) {
    return 0;
  }
  int64_t total = 0;
  for (const auto& segment : reader.Meta().index_meta.segments) {
    total += static_cast<int64_t>(segment.meta.byte_size);
  }
  return total;
}

void VisitEntries(duckdb::ClientContext& context, duckdb::Catalog& catalog,
                  duckdb::CatalogType type, std::string_view only_schema,
                  absl::FunctionRef<void(duckdb::CatalogEntry&)> callback) {
  std::vector<duckdb::reference<duckdb::CatalogEntry>> entries;
  for (auto& schema : catalog.GetSchemas(context)) {
    if (!only_schema.empty() && schema.get().name != only_schema) {
      continue;
    }
    schema.get().Scan(context, type, [&](duckdb::CatalogEntry& entry) {
      entries.emplace_back(entry);
    });
  }
  for (auto& entry : entries) {
    callback(entry.get());
  }
}

int64_t RelationDataBytes(duckdb::ClientContext& context,
                          duckdb::TableCatalogEntry& table) {
  if (table.GetLogStorage()) {
    return SearchTableBytes(table.Cast<catalog::SearchTableEntry>());
  }
  return StoreTableDataSize(context, table).bytes;
}

int64_t IndexEntryBytes(duckdb::ClientContext& context,
                        const duckdb::DuckIndexEntry& index) {
  if (connector::IsInvertedIndex(index)) {
    const auto& storage = index.Cast<catalog::InvertedIndexEntry>().Storage();
    if (!storage) {
      return 0;
    }
    return static_cast<int64_t>(storage->GetStats().indexSize);
  }
  // A secondary index is an ART on the store table, so its size is the
  // allocation the store table reports for it.
  return BoundIndexBytes(
    context, index.GetDataTableInfo(),
    [&](const auto& bound) { return bound->GetIndexName() == index.name; });
}

int64_t KeyIndexBytes(duckdb::ClientContext& context,
                      duckdb::TableCatalogEntry& table,
                      const duckdb::UniqueConstraint& key) {
  if (!table.IsDuckTable()) {
    return 0;
  }
  const auto& columns = table.GetColumns();
  std::vector<duckdb::column_t> ids;
  for (const auto logical : key.GetLogicalIndexes(columns)) {
    ids.emplace_back(columns.LogicalToPhysical(logical).index);
  }
  return BoundIndexBytes(
    context, *table.GetStorage().GetDataTableInfo(), [&](const auto& index) {
      return index->GetConstraintType() == key.GetIndexConstraintType() &&
             absl::c_equal(index->GetColumnIds(), ids);
    });
}

int64_t TableIndexesTotalBytes(duckdb::ClientContext& context,
                               duckdb::TableCatalogEntry& table) {
  int64_t total = StoreTableIndexBytes(context, table);
  table.ParentSchema(context).Scan(
    context, duckdb::CatalogType::INDEX_ENTRY,
    [&](duckdb::CatalogEntry& entry) {
      auto& index = entry.Cast<duckdb::DuckIndexEntry>();
      if (index.table_oid == table.oid && connector::IsInvertedIndex(index)) {
        total += IndexEntryBytes(context, index);
      }
    });
  return total;
}

duckdb::DatabaseSize DatabaseStorageSize(duckdb::ClientContext& context,
                                         duckdb::Catalog& catalog,
                                         std::string_view only_schema) {
  duckdb::DatabaseSize result;
  auto& attached = catalog.GetAttached();
  if (attached.HasStorageManager()) {
    result.block_size =
      attached.GetStorageManager().GetDatabaseSize().block_size;
  }
  int64_t bytes = 0;
  int64_t blocks = 0;
  VisitEntries(context, catalog, duckdb::CatalogType::TABLE_ENTRY, only_schema,
               [&](duckdb::CatalogEntry& entry) {
                 if (entry.type != duckdb::CatalogType::TABLE_ENTRY) {
                   return;
                 }
                 auto& table = entry.Cast<duckdb::TableCatalogEntry>();
                 if (table.GetLogStorage()) {
                   bytes +=
                     SearchTableBytes(table.Cast<catalog::SearchTableEntry>());
                   return;
                 }
                 const auto data = StoreTableDataSize(context, table);
                 bytes += data.bytes + StoreTableIndexBytes(context, table);
                 blocks += data.persistent_blocks;
               });
  VisitEntries(context, catalog, duckdb::CatalogType::INDEX_ENTRY, only_schema,
               [&](duckdb::CatalogEntry& entry) {
                 const auto& index = entry.Cast<duckdb::DuckIndexEntry>();
                 if (connector::IsInvertedIndex(index)) {
                   bytes += IndexEntryBytes(context, index);
                 }
               });
  result.bytes = static_cast<duckdb::idx_t>(bytes);
  result.total_blocks = static_cast<duckdb::idx_t>(blocks);
  result.used_blocks = result.total_blocks;
  return result;
}

std::optional<int64_t> RelationBytes(const Session& session, uint64_t oid,
                                     bool data, bool indexes) {
  auto relation = FindRelation(session, oid);
  if (!relation) {
    return std::nullopt;
  }
  auto& context = *session.context;
  if (relation->key) {
    return data ? KeyIndexBytes(
                    context, relation->entry->Cast<duckdb::TableCatalogEntry>(),
                    *relation->key)
                : 0;
  }
  if (!relation->entry) {
    return 0;
  }
  auto& entry = *relation->entry;
  if (entry.type == duckdb::CatalogType::TABLE_ENTRY) {
    auto& table = entry.Cast<duckdb::TableCatalogEntry>();
    return (data ? RelationDataBytes(context, table) : 0) +
           (indexes ? TableIndexesTotalBytes(context, table) : 0);
  }
  if (entry.type == duckdb::CatalogType::INDEX_ENTRY) {
    return data ? IndexEntryBytes(context, entry.Cast<duckdb::DuckIndexEntry>())
                : 0;
  }
  return 0;
}

template<bool Data, bool Indexes>
void RelationBytesFunction(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  const auto oids = args.data[0].Values<int64_t>();
  auto writer = duckdb::FlatVector::Writer<int64_t>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto oid = oids[row];
    const auto bytes =
      oid.IsValid()
        ? RelationBytes(session, static_cast<uint64_t>(oid.GetValue()), Data,
                        Indexes)
        : std::nullopt;
    if (bytes) {
      writer.WriteValue(*bytes);
    } else {
      writer.WriteNull();
    }
  }
}

constexpr std::string_view kForkNames[] = {"main", "fsm", "vm", "init"};

void RelationForkBytesFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  const auto oids = args.data[0].Values<int64_t>();
  const auto forks = args.data[1].Values<duckdb::string_t>();
  auto writer = duckdb::FlatVector::Writer<int64_t>(result, args.size());
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto oid = oids[row];
    const auto fork = forks[row];
    if (!oid.IsValid() || !fork.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto name = connector::AsView(fork.GetValue());
    const auto bytes = RelationBytes(
      session, static_cast<uint64_t>(oid.GetValue()), name == "main", false);
    if (!bytes) {
      writer.WriteNull();
      continue;
    }
    if (!absl::c_linear_search(kForkNames, name)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE), ERR_MSG("invalid fork name"),
        ERR_HINT(
          "Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\"."));
    }
    writer.WriteValue(*bytes);
  }
}

std::string KeyText(duckdb::string_t name) {
  return absl::StrCat("\"", connector::AsView(name), "\"");
}

std::string KeyText(int64_t oid) { return absl::StrCat("with OID ", oid); }

duckdb::optional_ptr<duckdb::CatalogEntry> DatabaseEntry(
  duckdb::ClientContext& context, duckdb::string_t name) {
  return FindDatabase(context, connector::AsView(name));
}

duckdb::optional_ptr<duckdb::CatalogEntry> DatabaseEntry(
  duckdb::ClientContext& context, int64_t oid) {
  return FindDatabaseById(context, static_cast<duckdb::idx_t>(oid));
}

template<typename Key>
void DatabaseSizeFunction(duckdb::DataChunk& args,
                          duckdb::ExpressionState& state,
                          duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::UnaryExecutor::Execute<Key, int64_t>(
    args.data[0], result, args.size(), [&](Key key) -> int64_t {
      const auto entry = DatabaseEntry(context, key);
      duckdb::optional_ptr<duckdb::Catalog> database =
        entry ? duckdb::Catalog::GetCatalogEntry(context, entry->name)
              : nullptr;
      if (!database) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                        ERR_MSG("database ", KeyText(key), " does not exist"));
      }
      return static_cast<int64_t>(
        DatabaseStorageSize(context, *database, {}).bytes);
    });
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SchemaEntry(
  const Session& session, duckdb::string_t name) {
  return session.database->GetSchema(
    *session.transaction, duckdb::Identifier{connector::AsView(name)},
    duckdb::OnEntryNotFound::RETURN_NULL);
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SchemaEntry(
  const Session& session, int64_t oid) {
  return SchemaByOid(session, static_cast<uint64_t>(oid));
}

template<typename Key>
void SchemaSizeFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  duckdb::UnaryExecutor::Execute<Key, int64_t>(
    args.data[0], result, args.size(), [&](Key key) -> int64_t {
      const auto schema =
        session.database ? SchemaEntry(session, key) : nullptr;
      if (!schema) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_SCHEMA),
                        ERR_MSG("schema ", KeyText(key), " does not exist"));
      }
      return static_cast<int64_t>(
        DatabaseStorageSize(*session.context, *session.database,
                            schema->name.GetIdentifierName())
          .bytes);
    });
}

const SystemObject& TablespaceEntry(int64_t oid) {
  if (const auto* tablespace =
        FindSystemObject(kSystemTablespaces, static_cast<duckdb::idx_t>(oid))) {
    return *tablespace;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG("tablespace with OID ", oid, " does not exist"));
}

const SystemObject& TablespaceEntry(duckdb::string_t name) {
  if (const auto* tablespace =
        FindSystemObject(kSystemTablespaces, connector::AsView(name))) {
    return *tablespace;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
    ERR_MSG("tablespace \"", connector::AsView(name), "\" does not exist"));
}

int64_t TablespaceBytes(duckdb::ClientContext& context,
                        const SystemObject& tablespace) {
  if (tablespace.oid != kPgDefaultTablespace) {
    if (const auto closure = auth::SessionClosure(context);
        closure && !closure->Owns(kRootUser)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("permission denied for tablespace ", tablespace.name));
    }
    return static_cast<int64_t>(catalog::ClusterOf(context)
                                  .GetAttached()
                                  .GetStorageManager()
                                  .GetWALSize());
  }
  int64_t total = 0;
  for (const auto& entry : Databases(context)) {
    if (auto database =
          duckdb::Catalog::GetCatalogEntry(context, entry.get().name)) {
      total +=
        static_cast<int64_t>(DatabaseStorageSize(context, *database, {}).bytes);
    }
  }
  return total;
}

template<typename Key>
void TablespaceSizeFunction(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::UnaryExecutor::Execute<Key, int64_t>(
    args.data[0], result, args.size(),
    [&](Key key) { return TablespaceBytes(context, TablespaceEntry(key)); });
}

struct SizeUnit {
  std::string_view name;
  int32_t bits;
};

constexpr SizeUnit kSizeUnits[] = {
  {"bytes", 0}, {"kB", 10}, {"MB", 20}, {"GB", 30},
  {"TB", 40},   {"PB", 50}, {"B", 0},
};

constexpr int64_t kMaxSizeExponent = std::numeric_limits<int32_t>::max() / 2;
constexpr int64_t kMaxNumericWeight = 131071;
constexpr int64_t kMaxNumericScale = 16383;

size_t SkipDigits(std::string_view text, size_t pos) {
  while (pos < text.size() && absl::ascii_isdigit(text[pos])) {
    ++pos;
  }
  return pos;
}

size_t SkipSpaces(std::string_view text, size_t pos) {
  while (pos < text.size() && absl::ascii_isspace(text[pos])) {
    ++pos;
  }
  return pos;
}

bool IsSign(std::string_view text, size_t pos) {
  return pos < text.size() && (text[pos] == '+' || text[pos] == '-');
}

[[noreturn]] void ThrowInvalidSize(std::string_view text) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("invalid size: \"", text, "\""));
}

[[noreturn]] void ThrowNumericOverflow() {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                  ERR_MSG("value overflows numeric format"));
}

struct SizeNumber {
  std::string_view integer;
  std::string_view fraction;
  int64_t exponent;
};

std::optional<uint64_t> RoundedMagnitude(const SizeNumber& number,
                                         int32_t bits) {
  const uint64_t multiplier = uint64_t{1} << bits;
  const auto shift =
    number.exponent - static_cast<int64_t>(number.fraction.size());
  int64_t place = shift;
  uint64_t magnitude = 0;
  uint64_t round = 0;
  bool overflow = false;
  const auto emit = [&](uint64_t digit) {
    if (place == -1) {
      round = digit;
    } else if (place >= 0 && digit != 0) {
      if (place >= duckdb::NumericHelper::CACHED_POWERS_OF_TEN) {
        overflow = true;
      } else {
        magnitude += digit * static_cast<uint64_t>(
                               duckdb::NumericHelper::POWERS_OF_TEN[place]);
      }
    }
    ++place;
  };
  uint64_t carry = 0;
  for (const auto digits : {number.fraction, number.integer}) {
    for (auto it = digits.rbegin(); it != digits.rend(); ++it) {
      const auto product =
        static_cast<uint64_t>(*it - '0') * multiplier + carry;
      emit(product % 10);
      carry = product / 10;
    }
  }
  for (; carry != 0; carry /= 10) {
    emit(carry % 10);
  }
  if (overflow) {
    return std::nullopt;
  }
  return magnitude + (round >= 5 ? 1 : 0);
}

int64_t SizeBytes(std::string_view text) {
  auto pos = SkipSpaces(text, 0);
  const auto start = pos;
  const bool negative = IsSign(text, pos) && text[pos] == '-';
  if (IsSign(text, pos)) {
    ++pos;
  }
  SizeNumber number{};
  const auto integer = pos;
  pos = SkipDigits(text, pos);
  number.integer = text.substr(integer, pos - integer);
  if (pos < text.size() && text[pos] == '.') {
    const auto fraction = ++pos;
    pos = SkipDigits(text, pos);
    number.fraction = text.substr(fraction, pos - fraction);
  }
  if (number.integer.empty() && number.fraction.empty()) {
    ThrowInvalidSize(text);
  }
  if (pos < text.size() && absl::ascii_tolower(text[pos]) == 'e') {
    auto cursor = SkipSpaces(text, pos + 1);
    const bool spaced = cursor != pos + 1;
    const bool negative_exponent = IsSign(text, cursor) && text[cursor] == '-';
    if (IsSign(text, cursor)) {
      ++cursor;
    }
    const auto digits = cursor;
    cursor = SkipDigits(text, cursor);
    if (cursor != digits) {
      if (spaced) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        ERR_MSG("invalid input syntax for type numeric: \"",
                                text.substr(start, cursor - start), "\""));
      }
      for (const auto digit : text.substr(digits, cursor - digits)) {
        number.exponent = number.exponent * 10 + (digit - '0');
        if (number.exponent > kMaxSizeExponent) {
          ThrowNumericOverflow();
        }
      }
      if (negative_exponent) {
        number.exponent = -number.exponent;
      }
      pos = cursor;
    }
  }
  if (static_cast<int64_t>(number.fraction.size()) - number.exponent >
      kMaxNumericScale) {
    ThrowNumericOverflow();
  }
  const auto lead = number.integer.find_first_not_of('0');
  const auto fraction_lead = number.fraction.find_first_not_of('0');
  if (lead != std::string_view::npos ||
      fraction_lead != std::string_view::npos) {
    const auto weight =
      lead != std::string_view::npos
        ? static_cast<int64_t>(number.integer.size() - lead) - 1
        : -static_cast<int64_t>(fraction_lead) - 1;
    if (weight + number.exponent > kMaxNumericWeight) {
      ThrowNumericOverflow();
    }
  }
  int32_t bits = 0;
  if (const auto unit = absl::StripAsciiWhitespace(text.substr(pos));
      !unit.empty()) {
    const auto* found = absl::c_find_if(kSizeUnits, [&](const SizeUnit& size) {
      return absl::EqualsIgnoreCase(size.name, unit);
    });
    if (found == std::end(kSizeUnits)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("invalid size: \"", text, "\""),
        ERR_DETAIL("Invalid size unit: \"", unit, "\"."),
        ERR_HINT("Valid units are \"bytes\", \"B\", \"kB\", \"MB\", \"GB\", "
                 "\"TB\", and \"PB\"."));
    }
    bits = found->bits;
  }
  const auto magnitude = RoundedMagnitude(number, bits);
  const auto limit =
    static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) +
    (negative ? 1 : 0);
  if (!magnitude || *magnitude > limit) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                    ERR_MSG("bigint out of range"));
  }
  return negative ? static_cast<int64_t>(~*magnitude + 1)
                  : static_cast<int64_t>(*magnitude);
}

void SizeBytesFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                       duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(),
    [](duckdb::string_t text) { return SizeBytes(connector::AsView(text)); });
}

}  // namespace

void RegisterDbsizeFunctions(duckdb::ExtensionLoader& loader) {
  const auto regclass = REGCLASS();
  const auto text = duckdb::LogicalType::VARCHAR;
  const auto int8 = duckdb::LogicalType::BIGINT;
  const std::string_view main = DEFAULT_SCHEMA;
  const auto stability = duckdb::FunctionStability::VOLATILE;

  RegisterScalar(loader, main, "pg_relation_size", stability, InitSession,
                 {duckdb::ScalarFunction{
                    {regclass}, int8, RelationBytesFunction<true, false>},
                  duckdb::ScalarFunction{
                    {regclass, text}, int8, RelationForkBytesFunction}});
  RegisterScalar(loader, main, "pg_table_size", stability, InitSession,
                 {duckdb::ScalarFunction{
                   {regclass}, int8, RelationBytesFunction<true, false>}});
  RegisterScalar(loader, main, "pg_total_relation_size", stability, InitSession,
                 {duckdb::ScalarFunction{
                   {regclass}, int8, RelationBytesFunction<true, true>}});
  RegisterScalar(loader, main, "pg_indexes_size", stability, InitSession,
                 {duckdb::ScalarFunction{
                   {regclass}, int8, RelationBytesFunction<false, true>}});
  RegisterScalar(
    loader, main, "pg_database_size", stability, nullptr,
    {duckdb::ScalarFunction{
       {text}, int8, DatabaseSizeFunction<duckdb::string_t>},
     duckdb::ScalarFunction{{int8}, int8, DatabaseSizeFunction<int64_t>}});
  RegisterScalar(
    loader, main, "pg_schema_size", stability, InitSession,
    {duckdb::ScalarFunction{{text}, int8, SchemaSizeFunction<duckdb::string_t>},
     duckdb::ScalarFunction{{int8}, int8, SchemaSizeFunction<int64_t>}});

  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;
  duckdb::ScalarFunction size_bytes{{text}, int8, SizeBytesFunction};
  size_bytes.SetFallible();
  RegisterScalar(loader, pg_catalog, "pg_size_bytes",
                 duckdb::FunctionStability::CONSISTENT, nullptr, {size_bytes});
  duckdb::ScalarFunction tablespace_by_oid{
    {OID()}, int8, TablespaceSizeFunction<int64_t>};
  tablespace_by_oid.SetFallible();
  duckdb::ScalarFunction tablespace_by_name{
    {text}, int8, TablespaceSizeFunction<duckdb::string_t>};
  tablespace_by_name.SetFallible();
  RegisterScalar(loader, pg_catalog, "pg_tablespace_size", stability, nullptr,
                 {tablespace_by_oid, tablespace_by_name});
}

}  // namespace sdb::pg
