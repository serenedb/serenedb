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
#include <string>
#include <utility>
#include <vector>

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

  pg::CreateSubscription(conn_ctx, subscription_name, conninfo,
                         std::move(publications));
}

}  // namespace

void RegisterSubscriptionPragma(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  auto create_pragma = duckdb::PragmaFunction::PragmaCall(
    "create_subscription", CreateSubscriptionPragma,
    {duckdb::LogicalType::VARCHAR});

  create_pragma.named_parameters["connection"] = duckdb::LogicalType::VARCHAR;
  create_pragma.named_parameters["publications"] = duckdb::LogicalType::VARCHAR;

  loader.RegisterFunction(create_pragma);
}

}  // namespace sdb::connector
