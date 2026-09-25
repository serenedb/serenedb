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

#include "connector/functions/ai/ai.h"

#include <absl/strings/str_cat.h>

#include <duckdb/common/string_util.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/main/secret/secret.hpp>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "connector/functions/ai/common.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kOpenAIKeys[] = {
  "api_key", "base_url", "model", "chat_path", "embeddings_path",
};
constexpr std::string_view kTypeSafeKeys[] = {"api_key", "base_url", "model",
                                              "path"};

duckdb::unique_ptr<duckdb::BaseSecret> CreateSecret(
  duckdb::ClientContext&, duckdb::CreateSecretInput& input) {
  auto scope = input.scope;
  if (scope.empty()) {
    scope.emplace_back(absl::StrCat(
      duckdb::StringUtil::Lower(input.type.GetIdentifierName()), "://"));
  }
  auto secret = duckdb::make_uniq<duckdb::KeyValueSecret>(
    scope, input.type, input.provider, input.name);
  for (const auto& [key, value] : input.options) {
    secret->secret_map[duckdb::Identifier{key}] = value;
  }
  secret->redact_keys = {"api_key"};
  return std::move(secret);
}

void RegisterSecretType(duckdb::ExtensionLoader& loader, std::string_view name,
                        std::span<const std::string_view> keys) {
  duckdb::SecretType type;
  type.name = duckdb::Identifier{name};
  type.deserializer =
    duckdb::KeyValueSecret::Deserialize<duckdb::KeyValueSecret>;
  type.default_provider = "config";
  loader.RegisterSecretType(type);

  duckdb::CreateSecretFunction fn = {
    std::string{name}, "config", CreateSecret, {}};
  for (const auto key : keys) {
    fn.named_parameters[duckdb::Identifier{key}] = duckdb::LogicalType::VARCHAR;
  }
  loader.RegisterFunction(fn);
}

}  // namespace

void RegisterAIFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  RegisterSecretType(loader, ai::kOpenAISecretType, kOpenAIKeys);
  RegisterSecretType(loader, ai::kTypeSafeSecretType, kTypeSafeKeys);

  ai::RegisterEmbeddingFunctions(loader);
  ai::RegisterTextFunctions(loader);
  ai::RegisterJevFunction(loader);
  ai::RegisterAggregateFunctions(loader);
  ai::RegisterAIOptimizer(db);
}

}  // namespace sdb::connector
