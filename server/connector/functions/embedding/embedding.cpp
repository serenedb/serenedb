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

#include "connector/functions/embedding/embedding.h"

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/main/secret/secret.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <string>
#include <utility>

#include "basics/down_cast.h"
#include "connector/functions/embedding/provider.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

using embedding::ProviderConfig;
using embedding::ProviderType;

constexpr const char* kOpenAIType = "openai";

duckdb::unique_ptr<duckdb::BaseSecret> CreateOpenAISecretFromConfig(
  duckdb::ClientContext& /*context*/, duckdb::CreateSecretInput& input) {
  auto scope = input.scope;
  if (scope.empty()) {
    scope.emplace_back("openai://");
  }
  auto secret = duckdb::make_uniq<duckdb::KeyValueSecret>(
    scope, input.type, input.provider, input.name);
  for (const auto& named : input.options) {
    auto key = duckdb::StringUtil::Lower(named.first);
    if (key == "api_key" || key == "base_url" || key == "embeddings_path") {
      secret->secret_map[duckdb::Identifier{key}] = named.second;
    }
  }
  secret->redact_keys = {"api_key"};
  return std::move(secret);
}

void RegisterEmbeddingSecretTypes(duckdb::ExtensionLoader& loader) {
  duckdb::SecretType openai_type;
  openai_type.name = kOpenAIType;
  openai_type.deserializer =
    duckdb::KeyValueSecret::Deserialize<duckdb::KeyValueSecret>;
  openai_type.default_provider = "config";
  loader.RegisterSecretType(openai_type);

  duckdb::CreateSecretFunction openai_fn = {
    kOpenAIType, "config", CreateOpenAISecretFromConfig, {}};
  openai_fn.named_parameters["api_key"] = duckdb::LogicalType::VARCHAR;
  openai_fn.named_parameters["base_url"] = duckdb::LogicalType::VARCHAR;
  openai_fn.named_parameters["embeddings_path"] = duckdb::LogicalType::VARCHAR;
  loader.RegisterFunction(openai_fn);
}

std::string FoldVarchar(duckdb::ClientContext& context,
                        duckdb::Expression& expr, const char* arg_label) {
  if (!expr.IsFoldable()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ai_embed: '", arg_label,
                            "' argument must be a constant expression"));
  }
  auto val = duckdb::ExpressionExecutor::EvaluateScalar(context, expr);
  if (val.IsNull()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ai_embed: '", arg_label, "' argument must not be NULL"));
  }
  return duckdb::StringValue::Get(val);
}

ProviderConfig LoadProviderConfig(duckdb::ClientContext& context,
                                  std::string model,
                                  const std::string& secret_name) {
  auto& secret_manager = duckdb::SecretManager::Get(context);
  auto txn = duckdb::CatalogTransaction::GetSystemCatalogTransaction(context);
  auto entry = secret_manager.GetSecretByName(txn, secret_name);
  if (!entry || !entry->secret) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("ai_embed: secret '", secret_name, "' not found"));
  }
  const auto& kv =
    basics::downCast<const duckdb::KeyValueSecret>(*entry->secret);

  ProviderConfig cfg;
  cfg.type = embedding::ResolveProviderType(
    entry->secret->GetType().GetIdentifierName());
  cfg.model = std::move(model);
  duckdb::Value v;
  if (kv.TryGetValue("api_key", v) && !v.IsNull()) {
    cfg.api_key = v.ToString();
  }
  if (kv.TryGetValue("base_url", v) && !v.IsNull()) {
    cfg.base_url = v.ToString();
  }
  if (kv.TryGetValue("embeddings_path", v) && !v.IsNull()) {
    cfg.embeddings_path = v.ToString();
  }
  return cfg;
}

struct EmbeddingBindData final : public duckdb::FunctionData {
  duckdb::DatabaseInstance* db = nullptr;
  ProviderConfig cfg;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    auto out = duckdb::make_uniq<EmbeddingBindData>();
    out->db = db;
    out->cfg = cfg;
    return out;
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    const auto& o = other.Cast<EmbeddingBindData>();
    return cfg.type == o.cfg.type && cfg.model == o.cfg.model &&
           cfg.base_url == o.cfg.base_url &&
           cfg.embeddings_path == o.cfg.embeddings_path &&
           cfg.api_key == o.cfg.api_key;
  }
};

duckdb::unique_ptr<duckdb::FunctionData> AIEmbedBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto& args = input.GetArguments();
  if (args.size() != 3) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ai_embed expects 3 arguments: (text, model, secret_name)"));
  }
  auto model = FoldVarchar(context, *args[1], "model");
  auto secret_name = FoldVarchar(context, *args[2], "secret_name");

  auto cfg = LoadProviderConfig(context, std::move(model), secret_name);
  embedding::NormalizeProviderConfig(*context.db, cfg);
  auto bind = duckdb::make_uniq<EmbeddingBindData>();
  bind->db = context.db.get();
  bind->cfg = std::move(cfg);
  return bind;
}

void AIEmbedFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  const auto& bind = state.expr.Cast<duckdb::BoundFunctionExpression>()
                       .BindInfo()
                       ->Cast<EmbeddingBindData>();
  const auto count = args.size();

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);

  embedding::EmbedBatch(*bind.db, bind.cfg, args.data[0], count, result);
}

}  // namespace

void RegisterEmbeddingFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  RegisterEmbeddingSecretTypes(loader);

  duckdb::ScalarFunction ai_embed{
    "ai_embed",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT),
    AIEmbedFunction,
    AIEmbedBind,
  };
  loader.RegisterFunction(ai_embed);
}

}  // namespace sdb::connector
