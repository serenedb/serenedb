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

#include "pg/commands/create_subscription.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_join.h>

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <utility>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/subscription.h"
#include "pg/sql_utils.h"
#include "replication/conninfo.h"
#include "replication/publisher_session.h"
#include "replication/subscription_engine.h"

namespace sdb::pg {
namespace {

catalog::SereneDBCatalog& CatalogOf(ConnectionContext& conn_ctx) {
  auto& context = conn_ctx.GetClientContext();
  return duckdb::Catalog::GetCatalog(
           context, duckdb::DatabaseManager::GetDefaultDatabase(context))
    .Cast<catalog::SereneDBCatalog>();
}

std::string DatabaseOf(ConnectionContext& conn_ctx) {
  return duckdb::DatabaseManager::GetDefaultDatabase(
           conn_ctx.GetClientContext())
    .GetIdentifierName();
}

bool IsSuperuser(ConnectionContext& conn_ctx) {
  return auth::ClosureFor(&conn_ctx.GetClientContext(), conn_ctx.GetRoleId())
    ->is_superuser;
}

void PreventInTransactionBlock(ConnectionContext& conn_ctx,
                               std::string_view statement) {
  if (conn_ctx.InTransactionBlock()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_ACTIVE_SQL_TRANSACTION),
      ERR_MSG(statement, " cannot run inside a transaction block"));
  }
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

SubscriptionOptions ParseOptions(const duckdb::named_parameter_map_t& options,
                                 Allowed allowed) {
  SubscriptionOptions result;
  for (const auto& [key, value] : options) {
    const auto name = absl::AsciiStrToLower(key.GetIdentifierName());
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
      result.lsn = ParseLsn(text);
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
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
        ERR_MSG("password_required=false is superuser-only"),
        ERR_HINT("Subscriptions with the password_required option set to "
                 "false may only be created or modified by the superuser."));
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
      co_return co_await call.Query(
        absl::StrCat("SELECT DISTINCT t.schemaname, t.tablename\n"
                     "  FROM pg_catalog.pg_publication_tables t\n"
                     " WHERE t.pubname IN (",
                     PublicationList(info.publications), ")"),
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
    if (std::ranges::none_of(publications, [&](const auto& row) {
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
  for (const auto& row : tables) {
    if (row.size() < 2 || !row[0] || !row[1]) {
      continue;
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
    relations.push_back({.schema = *row[0], .table = *row[1]});
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
  for (auto& relation : fetched) {
    const auto it = std::ranges::find_if(
      info.relations, [&](const duckdb::SubscriptionRelation& current) {
        return current.schema == relation.schema &&
               current.table == relation.table;
      });
    if (it != info.relations.end()) {
      relation = *it;
    } else {
      relation.state = copy_data ? 'i' : 'r';
    }
  }
  info.relations = {std::make_move_iterator(fetched.begin()),
                    std::make_move_iterator(fetched.end())};
}

catalog::SubscriptionCatalogEntry& RequireSubscription(
  catalog::SereneDBCatalog& catalog, duckdb::CatalogTransaction transaction,
  std::string_view name) {
  auto entry = catalog.GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
                 .GetEntry(transaction, duckdb::Identifier{name});
  if (!entry) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("subscription \"", name, "\" does not exist"));
  }
  return entry->Cast<catalog::SubscriptionCatalogEntry>();
}

void RequireOwner(ConnectionContext& conn_ctx,
                  const catalog::SubscriptionCatalogEntry& subscription) {
  if (!auth::ClosureFor(&conn_ctx.GetClientContext(), conn_ctx.GetRoleId())
         ->Owns(subscription.permissions.owner)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("must be owner of subscription ",
                            subscription.name.GetIdentifierName()));
  }
}

void SyncAfterCommit(ConnectionContext& conn_ctx, duckdb::idx_t oid,
                     bool restart = false) {
  conn_ctx.DeferToCommit([database = DatabaseOf(conn_ctx), oid, restart] {
    if (auto* engine = replication::SubscriptionEngine::gInstance) {
      engine->Sync(database, oid, restart);
    }
  });
}

void Replace(ConnectionContext& conn_ctx, catalog::SereneDBCatalog& catalog,
             const catalog::SubscriptionCatalogEntry& subscription,
             duckdb::unique_ptr<duckdb::CreateSubscriptionInfo> definition) {
  duckdb::ReplaceDefinitionInfo alter{std::move(definition)};
  alter.SetQualifiedName(duckdb::QualifiedName(subscription.name));
  catalog.Alter(catalog.GetCatalogTransaction(conn_ctx.GetClientContext()),
                alter);
}

duckdb::unique_ptr<duckdb::CreateSubscriptionInfo> DefinitionOf(
  const catalog::SubscriptionCatalogEntry& subscription) {
  return duckdb::unique_ptr_cast<duckdb::CreateInfo,
                                 duckdb::CreateSubscriptionInfo>(
    subscription.GetInfo());
}

void CheckPublicationNames(const std::vector<std::string>& publications) {
  for (size_t i = 0; i < publications.size(); ++i) {
    for (size_t j = 0; j < i; ++j) {
      if (publications[i] == publications[j]) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                        ERR_MSG("publication name \"", publications[i],
                                "\" used more than once"));
      }
    }
  }
}

}  // namespace

std::optional<uint64_t> ParseLsn(std::string_view text) {
  const auto slash = text.find('/');
  if (slash == std::string_view::npos || slash == 0 ||
      slash + 1 == text.size() || slash > 8 || text.size() - slash - 1 > 8) {
    return std::nullopt;
  }
  uint32_t high = 0;
  uint32_t low = 0;
  if (!absl::SimpleHexAtoi(text.substr(0, slash), &high) ||
      !absl::SimpleHexAtoi(text.substr(slash + 1), &low)) {
    return std::nullopt;
  }
  return (static_cast<uint64_t>(high) << 32) | low;
}

std::string FormatLsn(uint64_t lsn) {
  return absl::StrFormat("%X/%X", static_cast<uint32_t>(lsn >> 32),
                         static_cast<uint32_t>(lsn));
}

void CreateSubscription(ConnectionContext& conn_ctx, std::string_view name,
                        std::string_view conninfo,
                        std::vector<std::string> publications,
                        const duckdb::named_parameter_map_t& options_map) {
  auto options = ParseOptions(options_map, kCreateOptions);
  CheckPublicationNames(publications);

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

  auto& context = conn_ctx.GetClientContext();
  const auto role = conn_ctx.GetRoleId();
  auto& cluster = catalog::ClusterOf(context);
  auto database =
    cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
      .GetEntry(cluster.GetCatalogTransaction(context),
                duckdb::DatabaseManager::GetDefaultDatabase(context));
  if (database && !auth::ClosureFor(&context, role)
                     ->Can(duckdb::CatalogType::DATABASE_ENTRY,
                           database->permissions, duckdb::AclMode::Create)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("permission denied for database ",
                            database->name.GetIdentifierName()));
  }

  duckdb::CreateSubscriptionInfo info;
  info.SetName(duckdb::Identifier{name});
  info.conninfo.assign(conninfo);
  info.publications = {std::make_move_iterator(publications.begin()),
                       std::make_move_iterator(publications.end())};
  info.slot_name = options.slot_name.value_or(std::string{name});
  info.enabled = options.enabled.value_or(true);
  info.create_slot = options.create_slot.value_or(true);
  info.copy_data = options.copy_data.value_or(true);
  info.streaming = "parallel";
  ApplySetOptions(conn_ctx, options, info);
  CheckConnInfo(conn_ctx, info);
  info.permissions.owner = role;
  if (info.create_slot) {
    PreventInTransactionBlock(conn_ctx,
                              "CREATE SUBSCRIPTION ... WITH (create_slot = "
                              "true)");
  }

  auto& catalog = CatalogOf(conn_ctx);
  if (catalog.GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
        .GetEntry(catalog.GetCatalogTransaction(context),
                  duckdb::Identifier{name})) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("subscription \"", name, "\" already exists"));
  }
  if (connect) {
    auto relations = FetchRelations(conn_ctx, info, info.copy_data);
    for (auto& relation : relations) {
      relation.state = info.copy_data ? 'i' : 'r';
    }
    info.relations = {std::make_move_iterator(relations.begin()),
                      std::make_move_iterator(relations.end())};
  }
  auto entry =
    catalog.CreateSubscription(catalog.GetCatalogTransaction(context), info);
  if (!entry) {
    return;
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
  SyncAfterCommit(conn_ctx, entry->oid);
}

void DropSubscription(ConnectionContext& conn_ctx, std::string_view name,
                      bool missing_ok, bool cascade) {
  auto& context = conn_ctx.GetClientContext();
  auto& catalog = CatalogOf(conn_ctx);
  const auto transaction = catalog.GetCatalogTransaction(context);
  auto entry = catalog.GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
                 .GetEntry(transaction, duckdb::Identifier{name});
  if (!entry) {
    if (missing_ok) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("subscription \"", name, "\" does not exist"));
  }
  auto& subscription = entry->Cast<catalog::SubscriptionCatalogEntry>();
  RequireOwner(conn_ctx, subscription);
  const auto& config = subscription.Config();
  if (!config.slot_name.empty()) {
    PreventInTransactionBlock(conn_ctx, "DROP SUBSCRIPTION");
  }
  auto definition = DefinitionOf(subscription);
  const auto oid = subscription.oid;

  duckdb::DropInfo info;
  info.type = duckdb::CatalogType::SUBSCRIPTION_ENTRY;
  info.SetName(duckdb::Identifier{name});
  info.cascade = cascade;
  info.if_not_found = duckdb::OnEntryNotFound::THROW_EXCEPTION;
  catalog.DropSubscription(transaction, info);

  SyncAfterCommit(conn_ctx, oid);
  if (definition->slot_name.empty()) {
    return;
  }
  auto* engine = replication::SubscriptionEngine::gInstance;
  if (engine != nullptr) {
    engine->Stop(oid);
  }
  try {
    DropSlot(conn_ctx, *definition);
  } catch (...) {
    if (engine != nullptr) {
      engine->Sync(DatabaseOf(conn_ctx), oid);
    }
    throw;
  }
}

void AlterSubscription(ConnectionContext& conn_ctx, std::string_view name,
                       std::string_view action, std::string_view argument,
                       std::vector<std::string> publications,
                       const duckdb::named_parameter_map_t& options_map) {
  auto& context = conn_ctx.GetClientContext();
  auto& catalog = CatalogOf(conn_ctx);
  auto& subscription =
    RequireSubscription(catalog, catalog.GetCatalogTransaction(context), name);
  RequireOwner(conn_ctx, subscription);
  const auto oid = subscription.oid;
  auto definition = DefinitionOf(subscription);

  if (action == "enable" || action == "disable") {
    const bool enable = action == "enable";
    if (enable && definition->slot_name.empty()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("cannot enable subscription that does not have a slot name"));
    }
    definition->enabled = enable;
  } else if (action == "connection") {
    definition->conninfo.assign(argument);
    CheckConnInfo(conn_ctx, *definition);
  } else if (action == "set_publication" || action == "add_publication" ||
             action == "drop_publication") {
    const auto options = ParseOptions(options_map, kPublicationOptions);
    CheckPublicationNames(publications);
    auto& current = definition->publications;
    if (action == "set_publication") {
      current = {publications.begin(), publications.end()};
    } else if (action == "add_publication") {
      for (const auto& publication : publications) {
        if (std::find(current.begin(), current.end(), publication) !=
            current.end()) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
            ERR_MSG("publication \"", publication,
                    "\" is already in subscription \"", name, "\""));
        }
        current.push_back(publication);
      }
    } else {
      for (const auto& publication : publications) {
        auto it = std::find(current.begin(), current.end(), publication);
        if (it == current.end()) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                          ERR_MSG("publication \"", publication,
                                  "\" is not in subscription \"", name, "\""));
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
      PreventInTransactionBlock(conn_ctx, "ALTER SUBSCRIPTION with refresh");
      RefreshRelations(conn_ctx, *definition, options.copy_data.value_or(true));
    }
  } else if (action == "refresh_publication") {
    const auto options = ParseOptions(options_map, Allowed::CopyData);
    if (!definition->enabled) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("ALTER SUBSCRIPTION ... REFRESH is not allowed "
                              "for disabled subscriptions"));
    }
    PreventInTransactionBlock(conn_ctx, "ALTER SUBSCRIPTION ... REFRESH");
    RefreshRelations(conn_ctx, *definition, options.copy_data.value_or(true));
  } else if (action == "skip") {
    const auto options = ParseOptions(options_map, Allowed::Lsn);
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
        ERR_MSG("skip WAL location (LSN ", FormatLsn(*options.lsn),
                ") must be greater than origin LSN ",
                FormatLsn(subscription.RemoteLsn())));
    }
    definition->skip_lsn = *options.lsn;
  } else if (action == "rename") {
    if (catalog.GetCatalogSet(duckdb::CatalogType::SUBSCRIPTION_ENTRY)
          .GetEntry(catalog.GetCatalogTransaction(context),
                    duckdb::Identifier{argument})) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
        ERR_MSG("subscription \"", argument, "\" already exists"));
    }
    definition->SetName(duckdb::Identifier{argument});
  } else if (action == "owner") {
    const auto& roles = *auth::RolesOf(&context);
    std::optional<duckdb::idx_t> owner;
    if (argument == "CURRENT_USER" || argument == "CURRENT_ROLE") {
      owner = conn_ctx.GetRoleId();
    } else if (argument == "SESSION_USER") {
      owner = conn_ctx.GetSessionRoleId();
    } else {
      for (const auto& [id, node] : roles.nodes) {
        if (node.name == argument) {
          owner = id;
          break;
        }
      }
    }
    if (!owner) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role \"", argument, "\" does not exist"));
    }
    auto closure = auth::ClosureFor(&context, conn_ctx.GetRoleId());
    if (*owner != conn_ctx.GetRoleId() && !closure->is_superuser &&
        !closure->CanSet(*owner)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("must be able to SET ROLE \"", argument, "\""));
    }
    duckdb::AlterPermissionsInfo alter{
      duckdb::CatalogType::SUBSCRIPTION_ENTRY,
      duckdb::QualifiedName(subscription.name)};
    alter.new_owner = std::string{argument};
    alter.new_owner_id = *owner;
    catalog.Alter(catalog.GetCatalogTransaction(context), alter);
    SyncAfterCommit(conn_ctx, oid);
    return;
  } else {
    const auto options = ParseOptions(options_map, kSetOptions);
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
      PreventInTransactionBlock(conn_ctx,
                                "ALTER SUBSCRIPTION ... SET (failover)");
    }
    ApplySetOptions(conn_ctx, options, *definition);
    if (failover_changed) {
      AlterSlotFailover(conn_ctx, *definition);
    }
  }

  Replace(conn_ctx, catalog, subscription, std::move(definition));
  SyncAfterCommit(conn_ctx, oid, action == "refresh_publication");
}

}  // namespace sdb::pg
