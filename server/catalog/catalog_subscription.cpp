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
#include <absl/cleanup/cleanup.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/alter_subscription_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>
#include <utility>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/subscription.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/sql_utils.h"
#include "pg/types/pg_lsn.h"
#include "replication/conninfo.h"
#include "replication/publisher_session.h"
#include "replication/subscription_engine.h"

namespace sdb::catalog {
namespace {

bool IsSuperuser(ConnectionContext& conn_ctx) {
  return auth::ClosureFor(&conn_ctx.GetClientContext(), conn_ctx.GetRoleId())
    ->is_superuser;
}

bool ParseBool(std::string_view option, const duckdb::Value& value) {
  if (value.type().id() == duckdb::LogicalTypeId::BOOLEAN) {
    return value.GetValue<bool>();
  }
  const auto text = absl::AsciiStrToLower(value.ToString());
  if (text == "true" || text == "on" || text == "yes" || text == "1" ||
      text == "t" || text == "y") {
    return true;
  }
  if (text == "false" || text == "off" || text == "no" || text == "0" ||
      text == "f" || text == "n") {
    return false;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                  ERR_MSG(option, " requires a Boolean value"));
}

struct SubscriptionOptions {
  std::optional<bool> connect;
  std::optional<bool> enabled;
  std::optional<bool> create_slot;
  std::optional<std::string> slot_name;
  std::optional<bool> copy_data;
  std::optional<std::string> synchronous_commit;
  std::optional<bool> refresh;
  std::optional<bool> binary;
  std::optional<bool> disable_on_error;
  std::optional<bool> password_required;
  std::optional<bool> run_as_owner;
  std::optional<std::string> origin;
  std::optional<bool> failover;
  std::optional<std::string> streaming;
  std::optional<uint64_t> lsn;
};

enum class Allowed : uint32_t {
  None = 0,
  Connect = 1 << 0,
  Enabled = 1 << 1,
  CreateSlot = 1 << 2,
  SlotName = 1 << 3,
  CopyData = 1 << 4,
  SynchronousCommit = 1 << 5,
  Refresh = 1 << 6,
  Binary = 1 << 7,
  Streaming = 1 << 8,
  TwoPhase = 1 << 9,
  DisableOnError = 1 << 10,
  PasswordRequired = 1 << 11,
  RunAsOwner = 1 << 12,
  Origin = 1 << 13,
  Failover = 1 << 14,
  Lsn = 1 << 15,
};

constexpr Allowed operator|(Allowed a, Allowed b) {
  return static_cast<Allowed>(static_cast<uint32_t>(a) |
                              static_cast<uint32_t>(b));
}

constexpr bool Has(Allowed set, Allowed option) {
  return (static_cast<uint32_t>(set) & static_cast<uint32_t>(option)) != 0;
}

constexpr Allowed kCreateOptions =
  Allowed::Connect | Allowed::Enabled | Allowed::CreateSlot |
  Allowed::SlotName | Allowed::CopyData | Allowed::SynchronousCommit |
  Allowed::Binary | Allowed::Streaming | Allowed::TwoPhase |
  Allowed::DisableOnError | Allowed::PasswordRequired | Allowed::RunAsOwner |
  Allowed::Origin | Allowed::Failover;

constexpr Allowed kSetOptions =
  Allowed::SlotName | Allowed::SynchronousCommit | Allowed::Binary |
  Allowed::Streaming | Allowed::DisableOnError | Allowed::PasswordRequired |
  Allowed::RunAsOwner | Allowed::Origin | Allowed::Failover;

constexpr Allowed kPublicationOptions = Allowed::Refresh | Allowed::CopyData;

SubscriptionOptions ParseOptions(
  const duckdb::case_insensitive_map_t<duckdb::Value>& options,
  Allowed allowed) {
  SubscriptionOptions result;
  for (const auto& [key, value] : options) {
    const auto name = absl::AsciiStrToLower(key);
    const auto boolean = [&](std::string_view option_name, Allowed option,
                             std::optional<bool>& target) {
      if (name != option_name || !Has(allowed, option)) {
        return false;
      }
      target = ParseBool(name, value);
      return true;
    };
    if (boolean("connect", Allowed::Connect, result.connect) ||
        boolean("enabled", Allowed::Enabled, result.enabled) ||
        boolean("create_slot", Allowed::CreateSlot, result.create_slot) ||
        boolean("copy_data", Allowed::CopyData, result.copy_data) ||
        boolean("refresh", Allowed::Refresh, result.refresh) ||
        boolean("binary", Allowed::Binary, result.binary) ||
        boolean("disable_on_error", Allowed::DisableOnError,
                result.disable_on_error) ||
        boolean("password_required", Allowed::PasswordRequired,
                result.password_required) ||
        boolean("run_as_owner", Allowed::RunAsOwner, result.run_as_owner) ||
        boolean("failover", Allowed::Failover, result.failover)) {
      continue;
    }
    if (name == "slot_name" && Has(allowed, Allowed::SlotName)) {
      auto slot = value.ToString();
      if (absl::EqualsIgnoreCase(slot, "none")) {
        slot.clear();
      }
      result.slot_name = std::move(slot);
      continue;
    }
    if (name == "synchronous_commit" &&
        Has(allowed, Allowed::SynchronousCommit)) {
      auto level = absl::AsciiStrToLower(value.ToString());
      if (level != "off" && level != "local" && level != "remote_write" &&
          level != "remote_apply" && level != "on") {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("invalid value for parameter \"synchronous_commit\": \"",
                  level, "\""));
      }
      result.synchronous_commit = std::move(level);
      continue;
    }
    if (name == "origin" && Has(allowed, Allowed::Origin)) {
      auto origin = absl::AsciiStrToLower(value.ToString());
      if (origin != "any" && origin != "none") {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("unrecognized origin value: \"", value.ToString(), "\""));
      }
      result.origin = std::move(origin);
      continue;
    }
    if (name == "streaming" && Has(allowed, Allowed::Streaming)) {
      if (value.type().id() != duckdb::LogicalTypeId::BOOLEAN &&
          absl::EqualsIgnoreCase(value.ToString(), "parallel")) {
        result.streaming = "parallel";
        continue;
      }
      try {
        result.streaming = ParseBool(name, value) ? "on" : "off";
      } catch (const irs::SqlException&) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG(name, " requires a Boolean value or \"parallel\""));
      }
      continue;
    }
    if (name == "two_phase" && Has(allowed, Allowed::TwoPhase)) {
      if (ParseBool(name, value)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG(name, " = true is not supported"));
      }
      continue;
    }
    if (name == "lsn" && Has(allowed, Allowed::Lsn)) {
      const auto text = value.ToString();
      if (absl::EqualsIgnoreCase(text, "none")) {
        result.lsn = 0;
        continue;
      }
      result.lsn = pg::ParseLsn(text);
      if (!result.lsn) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
          ERR_MSG("invalid input syntax for type pg_lsn: \"", text, "\""));
      }
      continue;
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("unrecognized subscription parameter: \"", name, "\""));
  }
  return result;
}

[[noreturn]] void ThrowPasswordNotRequired() {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
    ERR_MSG("password_required=false is superuser-only"),
    ERR_HINT("Subscriptions with the password_required option set to "
             "false may only be created or modified by the superuser."));
}

void RequireExclusive(bool conflict, std::string_view a, std::string_view b) {
  if (conflict) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG(a, " and ", b, " are mutually exclusive options"));
  }
}

void ApplySetOptions(ConnectionContext& conn_ctx,
                     const SubscriptionOptions& options,
                     duckdb::CreateSubscriptionInfo& info) {
  if (options.slot_name) {
    info.slot_name = *options.slot_name;
  }
  if (options.synchronous_commit) {
    info.synchronous_commit = *options.synchronous_commit;
  }
  if (options.binary) {
    info.binary = *options.binary;
  }
  if (options.disable_on_error) {
    info.disable_on_error = *options.disable_on_error;
  }
  if (options.password_required) {
    if (!*options.password_required && !IsSuperuser(conn_ctx)) {
      ThrowPasswordNotRequired();
    }
    info.password_required = *options.password_required;
  }
  if (options.run_as_owner) {
    info.run_as_owner = *options.run_as_owner;
  }
  if (options.origin) {
    info.origin = *options.origin;
  }
  if (options.failover) {
    info.failover = *options.failover;
  }
  if (options.streaming) {
    info.streaming = *options.streaming;
  }
}

void CheckConnInfo(ConnectionContext& conn_ctx,
                   const duckdb::CreateSubscriptionInfo& info) {
  if (replication::NamesServerFiles(info.conninfo) &&
      !auth::ClosureFor(&conn_ctx.GetClientContext(), conn_ctx.GetRoleId())
         ->Has(duckdb::RoleCapability::ReadServerFiles)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied to use server files in the connection string"),
      ERR_DETAIL("Only roles with privileges of the \"pg_read_server_files\" "
                 "role may use the passfile, service and SSL file options."));
  }
  const auto conninfo = replication::ParseConnInfo(info.conninfo);
  if (info.password_required && conninfo.password.empty() &&
      !IsSuperuser(conn_ctx)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
                    ERR_MSG("password is required"),
                    ERR_DETAIL("Non-superuser subscriptions must set a "
                               "password in the connection string."));
  }
}

std::string PublicationList(const duckdb::vector<std::string>& publications) {
  return absl::StrJoin(publications, ", ",
                       [](std::string* out, const std::string& publication) {
                         out->append(pg::QuoteLiteral(publication));
                       });
}

replication::PublisherResult CallPublisher(
  ConnectionContext& conn_ctx, const duckdb::CreateSubscriptionInfo& info,
  replication::PublisherCall::Body body) {
  auto conninfo = replication::ParseConnInfo(info.conninfo);
  auto& context = conn_ctx.GetClientContext();
  const auto owner = info.permissions.owner;
  if (conninfo.user.empty()) {
    conninfo.user = auth::RolesOf(&context)->NameOf(owner);
  }
  const bool require_password =
    info.password_required && !auth::ClosureFor(&context, owner)->is_superuser;
  return replication::CallPublisher(
    conninfo, info.GetQualifiedName().Name().GetIdentifierName(),
    require_password, std::move(body));
}

[[noreturn]] void ThrowNotConnected(
  std::string_view subscription, const replication::PublisherResult& result) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_CONNECTION_FAILURE),
    ERR_MSG("subscription \"", subscription,
            "\" could not connect to the publisher: ", result.error.errmsg));
}

std::string OtherOriginQuery(const duckdb::CreateSubscriptionInfo& info) {
  auto query = absl::StrCat(
    "SELECT DISTINCT P.pubname AS pubname\n"
    "FROM pg_publication P,\n"
    "     LATERAL pg_get_publication_tables(P.pubname) GPT\n"
    "     JOIN pg_subscription_rel PS ON (GPT.relid = PS.srrelid OR"
    "     GPT.relid IN (SELECT relid FROM pg_partition_ancestors(PS.srrelid) "
    "UNION"
    "                   SELECT relid FROM pg_partition_tree(PS.srrelid))),\n"
    "     pg_class C JOIN pg_namespace N ON (N.oid = C.relnamespace)\n"
    "WHERE C.oid = GPT.relid AND P.pubname IN (",
    PublicationList(info.publications), ")\n");
  for (const auto& relation : info.relations) {
    absl::StrAppend(
      &query, "AND NOT (N.nspname = ", pg::QuoteLiteral(relation.schema),
      " AND C.relname = ", pg::QuoteLiteral(relation.table), ")\n");
  }
  return query;
}

std::vector<duckdb::SubscriptionRelation> FetchRelations(
  ConnectionContext& conn_ctx, const duckdb::CreateSubscriptionInfo& info,
  bool copy_data) {
  std::vector<replication::PublisherRow> publications;
  std::vector<replication::PublisherRow> tables;
  std::vector<replication::PublisherRow> other_origins;
  const bool check_origin = copy_data && info.origin == "none";
  bool listed = false;
  auto result = CallPublisher(
    conn_ctx, info,
    [&](replication::PublisherCall& call) -> yaclib::Task<bool> {
      if (!co_await call.Query(
            absl::StrCat("SELECT t.pubname FROM\n"
                         " pg_catalog.pg_publication t WHERE\n"
                         " t.pubname IN (",
                         PublicationList(info.publications), ")"),
            &publications)) {
        co_return false;
      }
      listed = true;
      if (check_origin &&
          !co_await call.Query(OtherOriginQuery(info), &other_origins)) {
        co_return false;
      }
      const auto list = PublicationList(info.publications);
      if (call.ServerVersion() >= 16) {
        co_return co_await call.Query(
          absl::StrCat("SELECT DISTINCT n.nspname, c.relname, gpt.attrs\n"
                       "  FROM pg_class c\n"
                       "  JOIN pg_namespace n ON n.oid = c.relnamespace\n"
                       "  JOIN (SELECT (pg_get_publication_tables(VARIADIC "
                       "array_agg(pubname::text))).*\n"
                       "          FROM pg_publication WHERE pubname IN (",
                       list, ")) AS gpt ON gpt.relid = c.oid"),
          &tables);
      }
      co_return co_await call.Query(
        absl::StrCat("SELECT DISTINCT t.schemaname, t.tablename",
                     call.ServerVersion() >= 15 ? ", t.attnames" : "",
                     "\n  FROM pg_catalog.pg_publication_tables t\n"
                     " WHERE t.pubname IN (",
                     list, ")"),
        &tables);
    });
  const auto name = info.GetQualifiedName().Name().GetIdentifierName();
  if (!result.connected) {
    ThrowNotConnected(name, result);
  }
  if (!result.ok) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_CONNECTION_FAILURE),
      ERR_MSG(listed ? "could not receive list of replicated tables from the "
                       "publisher: "
                     : "could not receive list of publications from the "
                       "publisher: ",
              result.error.errmsg));
  }
  std::vector<std::string> missing;
  for (const auto& publication : info.publications) {
    if (absl::c_none_of(publications, [&](const auto& row) {
          return !row.empty() && row[0] == publication;
        })) {
      missing.push_back(absl::StrCat("\"", publication, "\""));
    }
  }
  if (!missing.empty()) {
    conn_ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG(missing.size() == 1 ? "publication " : "publications ",
              absl::StrJoin(missing, ", "),
              missing.size() == 1 ? " does not exist on the publisher"
                                  : " do not exist on the publisher")));
  }
  std::vector<std::string> origin_publications;
  for (const auto& row : other_origins) {
    if (!row.empty() && row[0]) {
      origin_publications.push_back(absl::StrCat("\"", *row[0], "\""));
    }
  }
  if (!origin_publications.empty()) {
    const bool one = origin_publications.size() == 1;
    conn_ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("subscription \"", name,
              "\" requested copy_data with origin = NONE but might copy data "
              "that had a different origin"),
      ERR_DETAIL(one ? "The subscription being created subscribes to a "
                       "publication ("
                     : "The subscription being created subscribes to "
                       "publications (",
                 absl::StrJoin(origin_publications, ", "),
                 one ? ") that contains tables that are written to by other "
                       "subscriptions."
                     : ") that contain tables that are written to by other "
                       "subscriptions."),
      ERR_HINT("Verify that initial data copied from the publisher tables did "
               "not come from other origins.")));
  }
  auto& context = conn_ctx.GetClientContext();
  const auto database = duckdb::DatabaseManager::GetDefaultDatabase(context);
  std::vector<duckdb::SubscriptionRelation> relations;
  relations.reserve(tables.size());
  irs::containers::FlatHashSet<std::pair<std::string_view, std::string_view>>
    seen;
  seen.reserve(tables.size());
  for (const auto& row : tables) {
    if (row.size() < 2 || !row[0] || !row[1]) {
      continue;
    }
    if (!seen.emplace(*row[0], *row[1]).second) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("cannot use different column lists for table \"", *row[0], ".",
                *row[1], "\" in different publications"));
    }
    auto table = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
      context,
      duckdb::QualifiedName::FromCatalogSchema(
        database, {duckdb::Identifier{*row[0]}}, duckdb::Identifier{*row[1]}),
      duckdb::OnEntryNotFound::RETURN_NULL);
    if (!table) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_TABLE),
        ERR_MSG("relation \"", *row[0], ".", *row[1], "\" does not exist"));
    }
    relations.push_back(
      {.schema = *row[0],
       .table = *row[1],
       .sync_id = duckdb::DatabaseManager::Get(context).NextOid()});
  }
  return relations;
}

void CreateSlot(ConnectionContext& conn_ctx,
                const duckdb::CreateSubscriptionInfo& info) {
  auto result =
    CallPublisher(conn_ctx, info,
                  [&](replication::PublisherCall& call) -> yaclib::Task<bool> {
                    auto command = absl::StrCat(
                      "CREATE_REPLICATION_SLOT ",
                      pg::QuoteIdentifier(info.slot_name), " LOGICAL pgoutput");
                    if (call.ServerVersion() >= 15) {
                      absl::StrAppend(&command, " (SNAPSHOT 'nothing'",
                                      info.failover ? ", FAILOVER" : "", ")");
                    } else {
                      command.append(" NOEXPORT_SNAPSHOT");
                    }
                    co_return co_await call.Query(command);
                  });
  if (!result.connected) {
    ThrowNotConnected(info.GetQualifiedName().Name().GetIdentifierName(),
                      result);
  }
  if (!result.ok) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("could not create replication slot \"",
                            info.slot_name, "\": ", result.error.errmsg));
  }
  conn_ctx.AddNotice(SQL_ERROR_DATA(
    ERR_CODE(ERRCODE_SUCCESSFUL_COMPLETION),
    ERR_MSG("created replication slot \"", info.slot_name, "\" on publisher")));
}

void DropSlot(ConnectionContext& conn_ctx,
              const duckdb::CreateSubscriptionInfo& info) {
  auto result =
    CallPublisher(conn_ctx, info,
                  [&](replication::PublisherCall& call) -> yaclib::Task<bool> {
                    co_return co_await call.Query(absl::StrCat(
                      "DROP_REPLICATION_SLOT ",
                      pg::QuoteIdentifier(info.slot_name), " WAIT"));
                  });
  if (!result.connected) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_CONNECTION_FAILURE),
      ERR_MSG("could not connect to publisher when attempting to drop "
              "replication slot \"",
              info.slot_name, "\": ", result.error.errmsg),
      ERR_HINT("Use ALTER SUBSCRIPTION ... DISABLE to disable the "
               "subscription, and then use ALTER SUBSCRIPTION ... SET "
               "(slot_name = NONE) to disassociate it from the slot."));
  }
  if (!result.ok) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_CONNECTION_FAILURE),
      ERR_MSG("could not drop replication slot \"", info.slot_name,
              "\" on publisher: ", result.error.errmsg));
  }
  conn_ctx.AddNotice(SQL_ERROR_DATA(
    ERR_CODE(ERRCODE_SUCCESSFUL_COMPLETION),
    ERR_MSG("dropped replication slot \"", info.slot_name, "\" on publisher")));
}

void AlterSlotFailover(ConnectionContext& conn_ctx,
                       const duckdb::CreateSubscriptionInfo& info) {
  auto result = CallPublisher(
    conn_ctx, info,
    [&](replication::PublisherCall& call) -> yaclib::Task<bool> {
      co_return co_await call.Query(absl::StrCat(
        "ALTER_REPLICATION_SLOT ", pg::QuoteIdentifier(info.slot_name),
        " ( FAILOVER ", info.failover ? "true" : "false", " )"));
    });
  if (!result.connected) {
    ThrowNotConnected(info.GetQualifiedName().Name().GetIdentifierName(),
                      result);
  }
  if (!result.ok) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROTOCOL_VIOLATION),
                    ERR_MSG("could not alter replication slot \"",
                            info.slot_name, "\": ", result.error.errmsg));
  }
}

void RefreshRelations(ConnectionContext& conn_ctx,
                      duckdb::CreateSubscriptionInfo& info, bool copy_data) {
  auto fetched = FetchRelations(conn_ctx, info, copy_data);
  irs::containers::FlatHashMap<std::pair<std::string_view, std::string_view>,
                               size_t>
    current;
  current.reserve(info.relations.size());
  for (size_t i = 0; i < info.relations.size(); ++i) {
    current.try_emplace(std::pair{std::string_view{info.relations[i].schema},
                                  std::string_view{info.relations[i].table}},
                        i);
  }
  for (auto& relation : fetched) {
    const auto it = current.find(std::pair{std::string_view{relation.schema},
                                           std::string_view{relation.table}});
    if (it != current.end()) {
      relation = info.relations[it->second];
    } else {
      relation.state = copy_data ? 'i' : 'r';
    }
  }
  info.relations = {std::make_move_iterator(fetched.begin()),
                    std::make_move_iterator(fetched.end())};
}

duckdb::optional_ptr<SubscriptionCatalogEntry> FindSubscription(
  SereneDBCatalog& catalog, duckdb::CatalogTransaction transaction,
  const duckdb::Identifier& name, bool missing_ok) {
  auto entry = catalog.GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
                 .GetEntry(transaction, name);
  if (!entry) {
    if (missing_ok) {
      return nullptr;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("subscription \"", name.GetIdentifierName(),
                            "\" does not exist"));
  }
  return entry->Cast<SubscriptionCatalogEntry>();
}

void SyncAfterCommit(ConnectionContext& conn_ctx,
                     const SereneDBCatalog& catalog, duckdb::idx_t oid,
                     bool restart = false) {
  conn_ctx.DeferToCommit(
    [database = catalog.GetName().GetIdentifierName(), oid, restart] {
      if (auto* engine = replication::SubscriptionEngine::gInstance) {
        engine->Sync(database, oid, restart);
      }
    });
}

duckdb::unique_ptr<duckdb::CreateSubscriptionInfo> DefinitionOf(
  const SubscriptionCatalogEntry& subscription) {
  return duckdb::unique_ptr_cast<duckdb::CreateInfo,
                                 duckdb::CreateSubscriptionInfo>(
    subscription.GetInfo());
}

void CheckPublicationNames(std::span<const std::string> publications) {
  irs::containers::FlatHashSet<std::string_view> seen;
  seen.reserve(publications.size());
  for (const auto& publication : publications) {
    if (!seen.emplace(publication).second) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
        ERR_MSG("publication name \"", publication, "\" used more than once"));
    }
  }
}

}  // namespace

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateSubscription(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateSubscriptionInfo& info) {
  auto& conn_ctx = connector::GetSereneDBContext(transaction.GetContext());
  auto options = ParseOptions(info.options, kCreateOptions);
  info.options.clear();
  CheckPublicationNames(info.publications);

  const bool connect = options.connect.value_or(true);
  if (!connect) {
    RequireExclusive(options.enabled.value_or(false), "connect = false",
                     "enabled = true");
    RequireExclusive(options.create_slot.value_or(false), "connect = false",
                     "create_slot = true");
    RequireExclusive(options.copy_data.value_or(false), "connect = false",
                     "copy_data = true");
    options.enabled = false;
    options.create_slot = false;
    options.copy_data = false;
  }
  if (options.slot_name && options.slot_name->empty()) {
    RequireExclusive(options.enabled.value_or(false), "slot_name = NONE",
                     "enabled = true");
    RequireExclusive(options.create_slot.value_or(false), "slot_name = NONE",
                     "create_slot = true");
    if (options.enabled.value_or(true)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("subscription with slot_name = NONE must also "
                              "set enabled = false"));
    }
    if (options.create_slot.value_or(true)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("subscription with slot_name = NONE must also "
                              "set create_slot = false"));
    }
  }

  const auto& name = info.GetQualifiedName().Name();
  info.slot_name = options.slot_name.value_or(name.GetIdentifierName());
  info.enabled = options.enabled.value_or(true);
  info.create_slot = options.create_slot.value_or(true);
  info.copy_data = options.copy_data.value_or(true);
  info.streaming = "parallel";
  ApplySetOptions(conn_ctx, options, info);
  CheckConnInfo(conn_ctx, info);
  if (info.create_slot) {
    conn_ctx.PreventInTransactionBlock(
      "CREATE SUBSCRIPTION ... WITH (create_slot = true)");
  }

  if (FindSubscription(*this, transaction, name, true)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("subscription \"", name.GetIdentifierName(),
                            "\" already exists"));
  }
  if (connect) {
    auto relations = FetchRelations(conn_ctx, info, info.copy_data);
    for (auto& relation : relations) {
      relation.state = info.copy_data ? 'i' : 'r';
    }
    info.relations = {std::make_move_iterator(relations.begin()),
                      std::make_move_iterator(relations.end())};
  }
  DeclareModified(transaction, *this);
  auto entry = duckdb::DuckCatalog::CreateSubscription(transaction, info);
  if (!entry) {
    return entry;
  }
  if (!connect) {
    conn_ctx.AddNotice(SQL_ERROR_DATA(
      ERR_CODE(ERRCODE_WARNING),
      ERR_MSG("subscription was created, but is not connected"),
      ERR_HINT("To initiate replication, you must manually create the "
               "replication slot, enable the subscription, and refresh the "
               "subscription.")));
  } else if (info.create_slot) {
    CreateSlot(conn_ctx, info);
  }
  SyncAfterCommit(conn_ctx, *this, entry->oid);
  return entry;
}

void SereneDBCatalog::DropSubscription(duckdb::CatalogTransaction transaction,
                                       duckdb::DropInfo& info) {
  auto& conn_ctx = connector::GetSereneDBContext(transaction.GetContext());
  auto subscription = FindSubscription(
    *this, transaction, info.GetQualifiedName().Name(),
    info.if_not_found != duckdb::OnEntryNotFound::THROW_EXCEPTION);
  if (!subscription) {
    return;
  }
  if (!subscription->Config().slot_name.empty()) {
    conn_ctx.PreventInTransactionBlock("DROP SUBSCRIPTION");
  }
  auto definition = DefinitionOf(*subscription);
  const auto oid = subscription->oid;

  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  duckdb::DuckCatalog::DropSubscription(transaction, info);

  SyncAfterCommit(conn_ctx, *this, oid);
  if (definition->slot_name.empty()) {
    return;
  }
  auto* engine = replication::SubscriptionEngine::gInstance;
  if (engine) {
    engine->Stop(oid);
  }
  absl::Cleanup resume = [&] {
    if (engine) {
      engine->Sync(GetName().GetIdentifierName(), oid);
    }
  };
  DropSlot(conn_ctx, *definition);
  std::move(resume).Cancel();
}

void SereneDBCatalog::AlterSubscription(duckdb::CatalogTransaction transaction,
                                        duckdb::AlterInfo& info) {
  const auto& name = info.GetQualifiedName().Name();
  if (info.type != duckdb::AlterType::ALTER_SUBSCRIPTION) {
    DeclareModified(transaction, *this);
    auto& subscriptions =
      GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY);
    if (!subscriptions.AlterEntry(transaction, name, info)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("subscription \"", name.GetIdentifierName(),
                              "\" does not exist"));
    }
    auto* conn_ctx =
      transaction.HasContext()
        ? connector::GetSereneDBContextPtr(transaction.GetContext())
        : nullptr;
    if (conn_ctx && info.type == duckdb::AlterType::ALTER_PERMISSIONS) {
      SyncAfterCommit(*conn_ctx, *this,
                      subscriptions.GetEntry(transaction, name)->oid);
    }
    return;
  }
  auto& conn_ctx = connector::GetSereneDBContext(transaction.GetContext());
  const auto& alter = info.Cast<duckdb::AlterSubscriptionInfo>();
  auto& subscription = *FindSubscription(*this, transaction, name, false);
  const auto oid = subscription.oid;
  auto definition = DefinitionOf(subscription);

  using enum duckdb::AlterSubscriptionType;
  switch (alter.alter_subscription_type) {
    case ENABLE:
    case DISABLE: {
      const bool enable = alter.alter_subscription_type == ENABLE;
      if (enable && definition->slot_name.empty()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG("cannot enable subscription that does not have a slot name"));
      }
      definition->enabled = enable;
      break;
    }
    case SET_CONNECTION:
      definition->conninfo = alter.connection;
      CheckConnInfo(conn_ctx, *definition);
      break;
    case SET_PUBLICATION:
    case ADD_PUBLICATION:
    case DROP_PUBLICATION: {
      const auto options = ParseOptions(alter.options, kPublicationOptions);
      const auto& publications = alter.publications;
      CheckPublicationNames(publications);
      auto& current = definition->publications;
      if (alter.alter_subscription_type == SET_PUBLICATION) {
        current = publications;
      } else if (alter.alter_subscription_type == ADD_PUBLICATION) {
        for (const auto& publication : publications) {
          if (absl::c_linear_search(current, publication)) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                            ERR_MSG("publication \"", publication,
                                    "\" is already in subscription \"",
                                    name.GetIdentifierName(), "\""));
          }
          current.push_back(publication);
        }
      } else {
        for (const auto& publication : publications) {
          auto it = absl::c_find(current, publication);
          if (it == current.end()) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                            ERR_MSG("publication \"", publication,
                                    "\" is not in subscription \"",
                                    name.GetIdentifierName(), "\""));
          }
          current.erase(it);
        }
        if (current.empty()) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("cannot drop all the publications from a subscription"));
        }
      }
      if (options.refresh.value_or(true)) {
        if (!definition->enabled) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_SYNTAX_ERROR),
            ERR_MSG("ALTER SUBSCRIPTION with refresh is not allowed for "
                    "disabled subscriptions"),
            ERR_HINT("Use ALTER SUBSCRIPTION ... SET PUBLICATION ... WITH "
                     "(refresh = false)."));
        }
        conn_ctx.PreventInTransactionBlock("ALTER SUBSCRIPTION with refresh");
        RefreshRelations(conn_ctx, *definition,
                         options.copy_data.value_or(true));
      }
      break;
    }
    case REFRESH_PUBLICATION: {
      const auto options = ParseOptions(alter.options, Allowed::CopyData);
      if (!definition->enabled) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("ALTER SUBSCRIPTION ... REFRESH is not allowed "
                                "for disabled subscriptions"));
      }
      conn_ctx.PreventInTransactionBlock("ALTER SUBSCRIPTION ... REFRESH");
      RefreshRelations(conn_ctx, *definition, options.copy_data.value_or(true));
      break;
    }
    case SKIP: {
      const auto options = ParseOptions(alter.options, Allowed::Lsn);
      if (!options.lsn) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("unrecognized subscription parameter"));
      }
      if (!IsSuperuser(conn_ctx)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        ERR_MSG("must be superuser to skip transaction"));
      }
      if (*options.lsn != 0 && *options.lsn <= subscription.RemoteLsn()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("skip WAL location (LSN ", pg::FormatLsn(*options.lsn),
                  ") must be greater than origin LSN ",
                  pg::FormatLsn(subscription.RemoteLsn())));
      }
      definition->skip_lsn = *options.lsn;
      break;
    }
    case RENAME:
      if (FindSubscription(*this, transaction, alter.new_name, true)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
          ERR_MSG("subscription \"", alter.new_name.GetIdentifierName(),
                  "\" already exists"));
      }
      definition->SetName(alter.new_name);
      break;
    case SET_OPTIONS: {
      const auto options = ParseOptions(alter.options, kSetOptions);
      if (options.slot_name && options.slot_name->empty() &&
          definition->enabled) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          ERR_MSG("cannot set slot_name = NONE for enabled subscription"));
      }
      const bool failover_changed =
        options.failover && *options.failover != definition->failover;
      if (options.failover) {
        if (definition->enabled) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                          ERR_MSG("cannot set option \"failover\" for enabled "
                                  "subscription"));
        }
        if (definition->slot_name.empty()) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                          ERR_MSG("cannot set option \"failover\" for a "
                                  "subscription that does not have a slot "
                                  "name"));
        }
        conn_ctx.PreventInTransactionBlock(
          "ALTER SUBSCRIPTION ... SET (failover)");
      }
      ApplySetOptions(conn_ctx, options, *definition);
      if (failover_changed) {
        AlterSlotFailover(conn_ctx, *definition);
      }
      break;
    }
  }

  duckdb::ReplaceDefinitionInfo replace{std::move(definition)};
  replace.SetQualifiedName(duckdb::QualifiedName(subscription.name));
  Alter(transaction, replace);
  SyncAfterCommit(conn_ctx, *this, oid,
                  alter.alter_subscription_type == REFRESH_PUBLICATION);
}

}  // namespace sdb::catalog
