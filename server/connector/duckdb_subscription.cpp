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

#include <duckdb/main/database.hpp>

namespace sdb::connector {
namespace {

void CreateSubscriptionPragma(duckdb::ClientContext& context,
                              const duckdb::FunctionParameters& params) {
  throw duckdb::InvalidInputException(
    "create_subscription is not implemented yet");
}

}  // namespace

void RegisterSubscriptionPragma(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  auto create_pragma = duckdb::PragmaFunction::PragmaCall(
    "create_subscription", CreateSubscriptionPragma,
    {duckdb::LogicalType::VARCHAR});

  loader.RegisterFunction(create_pragma);
}

}  // namespace sdb::connector
