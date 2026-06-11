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

namespace sdb::connector {
namespace {

void CreateSubscriptionPragma(duckdb::ClientContext& context,
                              const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  if (args.size() < 1) {
    throw duckdb::InvalidInputException(
      "create_subscription requires at least name");
  }

  auto subscription_name = args[0].GetValue<std::string>();

  auto& conn_ctx = GetSereneDBContext(context);
  auto obj_name =
    pg::ParseObjectName(subscription_name, StaticStrings::kPublic);

  std::string conninfo;
  if (params.named_parameters.count("connection")) {
    conninfo = params.named_parameters.at("connection").GetValue<std::string>();
  }

  std::vector<std::string> publications;
  if (params.named_parameters.count("publications")) {
    auto raw =
      params.named_parameters.at("publications").GetValue<std::string>();
    for (std::string_view pub :
         absl::StrSplit(raw, ',', absl::SkipWhitespace())) {
      publications.emplace_back(pub);
    }
  }

  pg::CreateSubscription(conn_ctx, obj_name.relation, conninfo,
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
