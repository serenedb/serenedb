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

#include <absl/strings/str_split.h>

#include <duckdb/main/database.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "catalog/subscription.h"
#include "connector/duckdb_client_state.h"
#include "pg/commands/create_subscription.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

void CreateSubscriptionPragma(duckdb::ClientContext& context,
                              const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 1) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("CREATE SUBSCRIPTION requires name"));
  }

  const auto subscription_name = args[0].GetValue<std::string>();

  auto& conn_ctx = GetSereneDBContext(context);

  auto connection_it = params.named_parameters.find("connection");
  if (connection_it == params.named_parameters.end()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("CREATE SUBSCRIPTION requires CONNECTION"));
  }
  auto conninfo = connection_it->second.GetValue<std::string>();

  std::vector<std::string> publications;
  auto publications_it = params.named_parameters.find("publications");
  if (publications_it != params.named_parameters.end()) {
    auto raw = publications_it->second.GetValue<std::string>();
    for (std::string_view pub :
         absl::StrSplit(raw, ',', absl::SkipWhitespace())) {
      publications.emplace_back(pub);
    }
  }
  if (publications.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("CREATE SUBSCRIPTION requires PUBLICATION"));
  }

  catalog::Subscription::Config cfg;
  cfg.conninfo = conninfo;
  cfg.publications = std::move(publications);

  const auto bool_opt = [&](const char* key, bool dflt) {
    auto it = params.named_parameters.find(key);
    return it == params.named_parameters.end() ? dflt
                                               : it->second.GetValue<bool>();
  };
  cfg.binary = bool_opt("binary", false);
  cfg.copy_data = bool_opt("copy_data", true);
  cfg.disable_on_error = bool_opt("disable_on_error", false);
  cfg.create_slot = bool_opt("create_slot", true);
  cfg.password_required = bool_opt("password_required", true);
  cfg.run_as_owner = bool_opt("run_as_owner", false);
  cfg.failover = bool_opt("failover", false);

  auto slot_it = params.named_parameters.find("slot_name");
  if (slot_it != params.named_parameters.end()) {
    cfg.slot_name = slot_it->second.GetValue<std::string>();
  }
  auto origin_it = params.named_parameters.find("origin");
  if (origin_it != params.named_parameters.end()) {
    cfg.origin_name = origin_it->second.GetValue<std::string>();
  }
  auto sync_it = params.named_parameters.find("synchronous_commit");
  if (sync_it != params.named_parameters.end()) {
    cfg.synchronous_commit = sync_it->second.GetValue<std::string>();
  }

  pg::CreateSubscription(conn_ctx, subscription_name, std::move(cfg),
                         bool_opt("connect", true));
}

void DropSubscriptionPragma(duckdb::ClientContext& context,
                            const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 2) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("DROP SUBSCRIPTION requires name and missing_ok"));
  }

  const auto subscription_name = args[0].GetValue<std::string>();
  const auto missing_ok = args[1].GetValue<bool>();

  auto& conn_ctx = GetSereneDBContext(context);
  pg::DropSubscription(conn_ctx, subscription_name, missing_ok);
}

void AlterSubscriptionPragma(duckdb::ClientContext& context,
                             const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("ALTER SUBSCRIPTION requires name"));
  }

  const auto subscription_name = args[0].GetValue<std::string>();
  auto& conn_ctx = GetSereneDBContext(context);
  const auto& np = params.named_parameters;
  const auto has = [&](const char* k) { return np.find(k) != np.end(); };

  // The transform maps each ALTER form to distinct named parameters; exactly
  // one form is present per statement.
  if (has("enabled")) {  // ENABLE / DISABLE
    pg::AlterSubscriptionEnabled(conn_ctx, subscription_name,
                                 np.at("enabled").GetValue<bool>());
    return;
  }
  if (has("connection")) {  // CONNECTION '...'
    pg::AlterSubscriptionConnection(
      conn_ctx, subscription_name, np.at("connection").GetValue<std::string>());
    return;
  }
  if (has("new_name")) {  // RENAME TO ...
    pg::AlterSubscriptionRename(conn_ctx, subscription_name,
                                np.at("new_name").GetValue<std::string>());
    return;
  }
  if (has("owner")) {  // OWNER TO ...
    pg::AlterSubscriptionOwner(conn_ctx, subscription_name,
                               np.at("owner").GetValue<std::string>());
    return;
  }
  if (has("publications")) {  // {SET|ADD|DROP} PUBLICATION ...
    std::vector<std::string> pubs;
    for (std::string_view p :
         absl::StrSplit(np.at("publications").GetValue<std::string>(), ',',
                        absl::SkipWhitespace())) {
      pubs.emplace_back(p);
    }
    const auto mode =
      has("pub_mode") ? np.at("pub_mode").GetValue<std::string>() : "set";
    pg::AlterSubscriptionPublication(conn_ctx, subscription_name, mode,
                                     std::move(pubs));
    return;
  }
  // SET (option = ...): binary / origin / disable_on_error / synchronous_commit
  // / password_required / run_as_owner / failover.
  if (has("binary") || has("origin") || has("disable_on_error") ||
      has("synchronous_commit") || has("password_required") ||
      has("run_as_owner") || has("failover")) {
    pg::AlterSubscriptionOptions opts;
    if (has("binary")) {
      opts.binary = np.at("binary").GetValue<bool>();
    }
    if (has("origin")) {
      opts.origin = np.at("origin").GetValue<std::string>();
    }
    if (has("disable_on_error")) {
      opts.disable_on_error = np.at("disable_on_error").GetValue<bool>();
    }
    if (has("synchronous_commit")) {
      opts.synchronous_commit =
        np.at("synchronous_commit").GetValue<std::string>();
    }
    if (has("password_required")) {
      opts.password_required = np.at("password_required").GetValue<bool>();
    }
    if (has("run_as_owner")) {
      opts.run_as_owner = np.at("run_as_owner").GetValue<bool>();
    }
    if (has("failover")) {
      opts.failover = np.at("failover").GetValue<bool>();
    }
    pg::AlterSubscriptionSet(conn_ctx, subscription_name, opts);
    return;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("ALTER SUBSCRIPTION: unsupported option"));
}

}  // namespace

void RegisterSubscriptionPragma(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  auto create_pragma = duckdb::PragmaFunction::PragmaCall(
    "create_subscription", CreateSubscriptionPragma,
    {duckdb::LogicalType::VARCHAR});

  create_pragma.named_parameters["connection"] = duckdb::LogicalType::VARCHAR;
  create_pragma.named_parameters["publications"] = duckdb::LogicalType::VARCHAR;
  create_pragma.named_parameters["binary"] = duckdb::LogicalType::BOOLEAN;
  create_pragma.named_parameters["slot_name"] = duckdb::LogicalType::VARCHAR;
  create_pragma.named_parameters["origin"] = duckdb::LogicalType::VARCHAR;
  create_pragma.named_parameters["copy_data"] = duckdb::LogicalType::BOOLEAN;
  create_pragma.named_parameters["connect"] = duckdb::LogicalType::BOOLEAN;
  create_pragma.named_parameters["disable_on_error"] =
    duckdb::LogicalType::BOOLEAN;
  create_pragma.named_parameters["create_slot"] = duckdb::LogicalType::BOOLEAN;
  create_pragma.named_parameters["synchronous_commit"] =
    duckdb::LogicalType::VARCHAR;
  create_pragma.named_parameters["password_required"] =
    duckdb::LogicalType::BOOLEAN;
  create_pragma.named_parameters["run_as_owner"] = duckdb::LogicalType::BOOLEAN;
  create_pragma.named_parameters["failover"] = duckdb::LogicalType::BOOLEAN;

  loader.RegisterFunction(create_pragma);

  auto drop_pragma = duckdb::PragmaFunction::PragmaCall(
    "drop_subscription", DropSubscriptionPragma,
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN});
  loader.RegisterFunction(drop_pragma);

  auto alter_pragma = duckdb::PragmaFunction::PragmaCall(
    "alter_subscription", AlterSubscriptionPragma,
    {duckdb::LogicalType::VARCHAR});
  alter_pragma.named_parameters["enabled"] = duckdb::LogicalType::BOOLEAN;
  alter_pragma.named_parameters["binary"] = duckdb::LogicalType::BOOLEAN;
  alter_pragma.named_parameters["origin"] = duckdb::LogicalType::VARCHAR;
  alter_pragma.named_parameters["disable_on_error"] =
    duckdb::LogicalType::BOOLEAN;
  alter_pragma.named_parameters["connection"] = duckdb::LogicalType::VARCHAR;
  alter_pragma.named_parameters["publications"] = duckdb::LogicalType::VARCHAR;
  alter_pragma.named_parameters["pub_mode"] = duckdb::LogicalType::VARCHAR;
  alter_pragma.named_parameters["new_name"] = duckdb::LogicalType::VARCHAR;
  alter_pragma.named_parameters["owner"] = duckdb::LogicalType::VARCHAR;
  alter_pragma.named_parameters["synchronous_commit"] =
    duckdb::LogicalType::VARCHAR;
  alter_pragma.named_parameters["password_required"] =
    duckdb::LogicalType::BOOLEAN;
  alter_pragma.named_parameters["run_as_owner"] = duckdb::LogicalType::BOOLEAN;
  alter_pragma.named_parameters["failover"] = duckdb::LogicalType::BOOLEAN;
  loader.RegisterFunction(alter_pragma);
}

}  // namespace sdb::connector
