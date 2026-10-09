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

#include "replication/functions.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/catalog/catalog_entry/replication_origin_catalog_entry.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_context_state.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/transaction/duck_transaction.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <string>
#include <string_view>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/commands/create_subscription.h"
#include "pg/connection_context.h"
#include "replication/subscription_engine.h"

namespace sdb::replication {
namespace {

using duckdb::idx_t;

constexpr const char* kSessionKey = "sdb_replication_origin_session";
constexpr size_t kMaxOriginName = 512;

void RequireSuperuser(duckdb::ClientContext& context, std::string_view name) {
  auto& conn = connector::GetSereneDBContext(context);
  if (!auth::ClosureFor(&context, conn.GetRoleId())->is_superuser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for function ", name));
  }
}

void ReturnVoid(duckdb::Vector& result) {
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::ConstantVector::SetNull(result, true);
}

class OriginHolders {
 public:
  static OriginHolders& Instance() {
    static OriginHolders holders;
    return holders;
  }

  std::optional<int32_t> Holder(idx_t origin) const {
    absl::MutexLock lock{&_mu};
    const auto it = _holders.find(origin);
    if (it == _holders.end()) {
      return std::nullopt;
    }
    return it->second;
  }

  bool Acquire(idx_t origin, int32_t pid) {
    absl::MutexLock lock{&_mu};
    return _holders.try_emplace(origin, pid).second;
  }

  void Release(idx_t origin, int32_t pid) {
    absl::MutexLock lock{&_mu};
    const auto it = _holders.find(origin);
    if (it != _holders.end() && it->second == pid) {
      _holders.erase(it);
    }
  }

 private:
  mutable absl::Mutex _mu;
  irs::containers::FlatHashMap<idx_t, int32_t> _holders ABSL_GUARDED_BY(_mu);
};

struct OriginSession final : public duckdb::ClientContextState {
  OriginSession(idx_t origin, std::string name, int32_t pid, bool owner)
    : origin{origin}, name{std::move(name)}, pid{pid}, owner{owner} {}
  ~OriginSession() override {
    if (owner) {
      OriginHolders::Instance().Release(origin, pid);
    }
  }

  idx_t origin;
  std::string name;
  int32_t pid;
  bool owner;
};

duckdb::shared_ptr<OriginSession> Session(duckdb::ClientContext& context) {
  return context.registered_state->Get<OriginSession>(kSessionKey);
}

OriginSession& RequireSession(duckdb::ClientContext& context) {
  auto session = Session(context);
  if (!session) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                    ERR_MSG("no replication origin is configured"));
  }
  return *session;
}

struct Origin {
  duckdb::ReplicationLsnEntry& entry;
  duckdb::Catalog& catalog;
  bool subscription;
};

std::optional<idx_t> SubscriptionOriginOid(std::string_view name) {
  if (!name.starts_with("pg_")) {
    return std::nullopt;
  }
  const auto digits = name.substr(3);
  idx_t oid = 0;
  if (digits.empty() || !std::ranges::all_of(digits, absl::ascii_isdigit) ||
      !absl::SimpleAtoi(digits, &oid)) {
    return std::nullopt;
  }
  return oid;
}

std::optional<Origin> FindOrigin(duckdb::ClientContext& context,
                                 std::string_view name) {
  if (const auto oid = SubscriptionOriginOid(name)) {
    for (const auto& database :
         duckdb::DatabaseManager::Get(context).GetDatabases()) {
      auto& catalog = database->GetCatalog();
      if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
        continue;
      }
      auto& duck = catalog.Cast<duckdb::DuckCatalog>();
      const auto transaction = duck.GetCatalogTransaction(context);
      auto entry = duck.GetOidIndex().GetVisible(*oid, transaction.view);
      if (entry && entry->type == duckdb::CatalogType::SUBSCRIPTION_ENTRY) {
        return Origin{entry->Cast<duckdb::ReplicationLsnEntry>(), catalog,
                      true};
      }
    }
    return std::nullopt;
  }
  auto& cluster = catalog::ClusterOf(context);
  auto entry =
    cluster.GetCatalogSet(duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY)
      .GetEntry(cluster.GetCatalogTransaction(context),
                duckdb::Identifier{name});
  if (!entry) {
    return std::nullopt;
  }
  return Origin{entry->Cast<duckdb::ReplicationLsnEntry>(), cluster, false};
}

Origin RequireOrigin(duckdb::ClientContext& context, std::string_view name) {
  auto origin = FindOrigin(context, name);
  if (!origin) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("replication origin \"", name, "\" does not exist"));
  }
  return *origin;
}

bool WorkerActive(const Origin& origin) {
  auto* engine = SubscriptionEngine::gInstance;
  return origin.subscription && engine != nullptr &&
         engine->Running(origin.entry.oid);
}

void RequireIdle(const Origin& origin) {
  if (WorkerActive(origin)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("replication origin with ID ", origin.entry.oid,
                            " is already active for the apply worker of "
                            "subscription \"",
                            origin.entry.name.GetIdentifierName(), "\""));
  }
  if (const auto pid = OriginHolders::Instance().Holder(origin.entry.oid)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                    ERR_MSG("replication origin with ID ", origin.entry.oid,
                            " is already active for PID ", *pid));
  }
}

uint64_t RequireLsn(std::string_view text) {
  const auto lsn = pg::ParseLsn(text);
  if (!lsn) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
      ERR_MSG("invalid input syntax for type pg_lsn: \"", text, "\""));
  }
  return *lsn;
}

duckdb::DuckTransaction& Transaction(duckdb::ClientContext& context,
                                     duckdb::Catalog& catalog) {
  catalog::DeclareModified(catalog.GetCatalogTransaction(context), catalog);
  return duckdb::DuckTransaction::Get(context, catalog);
}

void ValidateNewName(std::string_view name) {
  if (name == "any" || name == "none" || name.starts_with("pg_")) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_RESERVED_NAME),
      ERR_MSG("replication origin name \"", name, "\" is reserved"),
      ERR_DETAIL("Origin names \"any\", \"none\", and names starting "
                 "with \"pg_\" are reserved."));
  }
  if (name.size() > kMaxOriginName) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("replication origin name is too long"),
      ERR_DETAIL("Replication origin names must be no longer than ",
                 kMaxOriginName, " bytes."));
  }
}

void CreateOrigin(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                  duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_create");
  auto names = args.data[0].Values<duckdb::string_t>();
  auto writer = duckdb::FlatVector::Writer<int64_t>(result, args.size());
  for (auto value : names) {
    if (!value.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto name = value.GetValue().GetString();
    ValidateNewName(name);
    if (FindOrigin(context, name)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
                      ERR_MSG("duplicate key value violates unique constraint "
                              "\"pg_replication_origin_roname_index\""),
                      ERR_DETAIL("Key (roname)=(", name, ") already exists."));
    }
    auto& cluster = catalog::ClusterOf(context);
    const auto transaction = cluster.GetCatalogTransaction(context);
    catalog::DeclareModified(transaction, cluster);
    duckdb::CreateReplicationOriginInfo info;
    info.SetName(duckdb::Identifier{name});
    auto created = cluster.CreateReplicationOrigin(transaction, info);
    writer.WriteValue(static_cast<int64_t>(created->oid));
  }
}

void DropOrigin(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_drop");
  for (auto value : args.data[0].Values<duckdb::string_t>()) {
    if (!value.IsValid()) {
      continue;
    }
    const auto name = value.GetValue().GetString();
    const auto origin = RequireOrigin(context, name);
    if (origin.subscription) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                      ERR_MSG("could not drop replication origin with ID ",
                              origin.entry.oid, ", in use by subscription \"",
                              origin.entry.name.GetIdentifierName(), "\""));
    }
    if (const auto pid = OriginHolders::Instance().Holder(origin.entry.oid)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                      ERR_MSG("could not drop replication origin with ID ",
                              origin.entry.oid, ", in use by PID ", *pid));
    }
    auto& cluster = catalog::ClusterOf(context);
    const auto transaction = cluster.GetCatalogTransaction(context);
    catalog::DeclareModified(
      transaction, cluster,
      duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
    duckdb::DropInfo info;
    info.type = duckdb::CatalogType::REPLICATION_ORIGIN_ENTRY;
    info.SetName(duckdb::Identifier{name});
    cluster.DropReplicationOrigin(transaction, info);
  }
  ReturnVoid(result);
}

void OriginOid(duckdb::DataChunk& args, duckdb::ExpressionState& state,
               duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_oid");
  auto names = args.data[0].Values<duckdb::string_t>();
  auto writer = duckdb::FlatVector::Writer<int64_t>(result, args.size());
  for (auto value : names) {
    const auto origin = value.IsValid()
                          ? FindOrigin(context, value.GetValue().GetString())
                          : std::nullopt;
    if (!origin) {
      writer.WriteNull();
      continue;
    }
    writer.WriteValue(static_cast<int64_t>(origin->entry.oid));
  }
}

void WriteLsn(duckdb::VectorWriter<duckdb::string_t>& writer, uint64_t lsn) {
  if (lsn == 0) {
    writer.WriteNull();
    return;
  }
  const auto text = pg::FormatLsn(lsn);
  writer.WriteValue(duckdb::string_t{text});
}

void OriginProgress(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                    duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_progress");
  auto names = args.data[0].Values<duckdb::string_t>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
  for (auto value : names) {
    if (!value.IsValid()) {
      writer.WriteNull();
      continue;
    }
    WriteLsn(
      writer,
      RequireOrigin(context, value.GetValue().GetString()).entry.RemoteLsn());
  }
}

void AdvanceOrigin(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_advance");
  auto names = args.data[0].Values<duckdb::string_t>();
  auto lsns = args.data[1].Values<duckdb::string_t>();
  for (idx_t i = 0; i < args.size(); ++i) {
    if (!names[i].IsValid() || !lsns[i].IsValid()) {
      continue;
    }
    const auto lsn = RequireLsn(lsns[i].GetValue().GetString());
    const auto origin = RequireOrigin(context, names[i].GetValue().GetString());
    RequireIdle(origin);
    Transaction(context, origin.catalog)
      .AssignReplicationLsn(origin.entry, lsn);
  }
  ReturnVoid(result);
}

void SetupSession(duckdb::ClientContext& context, std::string_view name,
                  int32_t acquired_by) {
  if (Session(context)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("cannot setup replication origin when one is already setup"));
  }
  const auto origin = RequireOrigin(context, name);
  const auto id = origin.entry.oid;
  const auto pid = connector::GetSereneDBContext(context).GetBackendPid();
  auto& holders = OriginHolders::Instance();
  if (acquired_by != 0) {
    if (holders.Holder(id) != acquired_by) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_IN_USE),
                      ERR_MSG("could not find replication state slot for "
                              "replication origin with OID ",
                              id, " which was acquired by ", acquired_by));
    }
  } else {
    if (WorkerActive(origin)) {
      RequireIdle(origin);
    }
    if (!holders.Acquire(id, pid)) {
      RequireIdle(origin);
    }
  }
  context.registered_state->Insert(
    kSessionKey, duckdb::make_shared_ptr<OriginSession>(id, std::string{name},
                                                        pid, acquired_by == 0));
}

void SessionSetup(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                  duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_session_setup");
  auto names = args.data[0].Values<duckdb::string_t>();
  for (idx_t i = 0; i < args.size(); ++i) {
    if (!names[i].IsValid()) {
      continue;
    }
    int32_t acquired_by = 0;
    if (args.ColumnCount() > 1) {
      auto pids = args.data[1].Values<int32_t>();
      if (!pids[i].IsValid()) {
        continue;
      }
      acquired_by = pids[i].GetValue();
    }
    SetupSession(context, names[i].GetValue().GetString(), acquired_by);
  }
  ReturnVoid(result);
}

void ForgetXactLsn(duckdb::ClientContext& context,
                   const OriginSession& session) {
  if (!context.transaction.HasActiveTransaction()) {
    return;
  }
  if (const auto origin = FindOrigin(context, session.name)) {
    duckdb::DuckTransaction::Get(context, origin->catalog)
      .ForgetReplicationLsn(origin->entry);
  }
}

void SessionReset(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                  duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_session_reset");
  ForgetXactLsn(context, RequireSession(context));
  context.registered_state->Remove(kSessionKey);
  ReturnVoid(result);
}

void SessionIsSetup(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                    duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_session_is_setup");
  const bool is_setup = Session(context) != nullptr;
  auto writer = duckdb::FlatVector::Writer<bool>(result, args.size());
  for (idx_t i = 0; i < args.size(); ++i) {
    writer.WriteValue(is_setup);
  }
}

void SessionProgress(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_session_progress");
  const auto& session = RequireSession(context);
  const auto origin = FindOrigin(context, session.name);
  auto writer =
    duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
  for (idx_t i = 0; i < args.size(); ++i) {
    WriteLsn(writer, origin ? origin->entry.RemoteLsn() : 0);
  }
}

void XactSetup(duckdb::DataChunk& args, duckdb::ExpressionState& state,
               duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_xact_setup");
  const auto& session = RequireSession(context);
  auto lsns = args.data[0].Values<duckdb::string_t>();
  for (idx_t i = 0; i < args.size(); ++i) {
    if (!lsns[i].IsValid()) {
      continue;
    }
    const auto lsn = RequireLsn(lsns[i].GetValue().GetString());
    const auto origin = RequireOrigin(context, session.name);
    auto& transaction = Transaction(context, origin.catalog);
    transaction.ForgetReplicationLsn(origin.entry);
    transaction.PushReplicationLsn(origin.entry, lsn);
  }
  ReturnVoid(result);
}

void XactReset(duckdb::DataChunk& args, duckdb::ExpressionState& state,
               duckdb::Vector& result) {
  auto& context = state.GetContext();
  RequireSuperuser(context, "pg_replication_origin_xact_reset");
  if (auto session = Session(context)) {
    ForgetXactLsn(context, *session);
  }
  ReturnVoid(result);
}

void ResetSubscriptionStats(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  RequireSuperuser(state.GetContext(), "pg_stat_reset_subscription_stats");
  auto ids = args.data[0].Values<int64_t>();
  for (idx_t i = 0; i < args.size(); ++i) {
    std::optional<idx_t> subscription;
    if (const auto id = ids[i]; id.IsValid()) {
      if (id.GetValue() == 0) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                        ERR_MSG("invalid subscription OID 0"));
      }
      subscription = static_cast<idx_t>(id.GetValue());
    }
    if (auto* engine = SubscriptionEngine::gInstance) {
      engine->ResetStats(subscription);
    }
  }
  ReturnVoid(result);
}

void Register(duckdb::ExtensionLoader& loader, const char* name,
              duckdb::vector<duckdb::LogicalType> args,
              duckdb::LogicalType result, duckdb::scalar_function_t function,
              bool special_nulls = false) {
  duckdb::ScalarFunction scalar{name, std::move(args), std::move(result),
                                function};
  if (special_nulls) {
    scalar.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  }
  scalar.SetVolatile();
  loader.RegisterFunction(scalar);
}

}  // namespace

void RegisterReplicationFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  const auto text = duckdb::LogicalType::VARCHAR;
  const auto boolean = duckdb::LogicalType::BOOLEAN;
  Register(loader, "pg_stat_reset_subscription_stats", {pg::OID()}, pg::VOID(),
           ResetSubscriptionStats, true);
  Register(loader, "pg_replication_origin_create", {text}, pg::OID(),
           CreateOrigin);
  Register(loader, "pg_replication_origin_drop", {text}, pg::VOID(),
           DropOrigin);
  Register(loader, "pg_replication_origin_oid", {text}, pg::OID(), OriginOid);
  Register(loader, "pg_replication_origin_progress", {text, boolean}, text,
           OriginProgress);
  Register(loader, "pg_replication_origin_advance", {text, text}, pg::VOID(),
           AdvanceOrigin);
  Register(loader, "pg_replication_origin_session_setup", {text}, pg::VOID(),
           SessionSetup);
  Register(loader, "pg_replication_origin_session_setup",
           {text, duckdb::LogicalType::INTEGER}, pg::VOID(), SessionSetup);
  Register(loader, "pg_replication_origin_session_reset", {}, pg::VOID(),
           SessionReset);
  Register(loader, "pg_replication_origin_session_is_setup", {}, boolean,
           SessionIsSetup);
  Register(loader, "pg_replication_origin_session_progress", {boolean}, text,
           SessionProgress);
  Register(loader, "pg_replication_origin_xact_setup",
           {text, duckdb::LogicalType::TIMESTAMP_TZ}, pg::VOID(), XactSetup);
  Register(loader, "pg_replication_origin_xact_reset", {}, pg::VOID(),
           XactReset);
}

}  // namespace sdb::replication
