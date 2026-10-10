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

#include "connector/duckdb_subscription.h"

#include <algorithm>
#include <duckdb/common/types/value.hpp>
#include <duckdb/function/function.hpp>
#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <string>
#include <vector>

#include "connector/duckdb_client_state.h"
#include "pg/commands/create_subscription.h"
#include "pg/connection_context.h"

namespace sdb::connector {
namespace {

std::vector<std::string> Publications(const duckdb::vector<duckdb::Value>& args,
                                      size_t first) {
  std::vector<std::string> result;
  result.reserve(args.size() - std::min(first, args.size()));
  for (size_t i = first; i < args.size(); ++i) {
    result.push_back(args[i].GetValue<std::string>());
  }
  return result;
}

void CreateSubscriptionPragma(duckdb::ClientContext& context,
                              const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  pg::CreateSubscription(GetSereneDBContext(context),
                         args[0].GetValue<std::string>(),
                         args[1].GetValue<std::string>(), Publications(args, 2),
                         params.named_parameters);
}

void DropSubscriptionPragma(duckdb::ClientContext& context,
                            const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  pg::DropSubscription(GetSereneDBContext(context),
                       args[0].GetValue<std::string>(),
                       args[1].GetValue<bool>(), args[2].GetValue<bool>());
}

void AlterSubscriptionPragma(duckdb::ClientContext& context,
                             const duckdb::FunctionParameters& params) {
  auto& args = params.values;
  pg::AlterSubscription(
    GetSereneDBContext(context), args[0].GetValue<std::string>(),
    args[1].GetValue<std::string>(), args[2].GetValue<std::string>(),
    Publications(args, 3), params.named_parameters);
}

}  // namespace

void RegisterSubscriptionPragma(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  const auto kVarchar = duckdb::LogicalType::VARCHAR;
  const auto kBoolean = duckdb::LogicalType::BOOLEAN;
  auto create = duckdb::PragmaFunction::PragmaCall(
    "create_subscription", CreateSubscriptionPragma, {kVarchar, kVarchar},
    kVarchar);
  create.GetSignature().AddKwargs("options", duckdb::LogicalType::ANY);
  loader.RegisterFunction(create);

  auto drop = duckdb::PragmaFunction::PragmaCall(
    "drop_subscription", DropSubscriptionPragma,
    {kVarchar, kBoolean, kBoolean});
  loader.RegisterFunction(drop);

  auto alter = duckdb::PragmaFunction::PragmaCall(
    "alter_subscription", AlterSubscriptionPragma,
    {kVarchar, kVarchar, kVarchar}, kVarchar);
  alter.GetSignature().AddKwargs("options", duckdb::LogicalType::ANY);
  loader.RegisterFunction(alter);
}

}  // namespace sdb::connector
