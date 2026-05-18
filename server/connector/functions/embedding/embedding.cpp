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

#include <cstring>
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
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "connector/functions/embedding/provider.h"

namespace sdb::connector {
namespace {

using embedding::EmbeddingProvider;
using embedding::MakeEmbeddingProvider;
using embedding::ProviderConfig;

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
      secret->secret_map[key] = named.second;
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
    throw duckdb::BinderException(
      "ai_embed: '%s' argument must be a constant expression", arg_label);
  }
  auto val = duckdb::ExpressionExecutor::EvaluateScalar(context, expr);
  if (val.IsNull()) {
    throw duckdb::BinderException("ai_embed: '%s' argument must not be NULL",
                                  arg_label);
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
    throw duckdb::BinderException("ai_embed: secret '%s' not found",
                                  secret_name);
  }
  const auto& kv = dynamic_cast<const duckdb::KeyValueSecret&>(*entry->secret);

  ProviderConfig cfg;
  cfg.protocol = entry->secret->GetType();
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
  std::shared_ptr<EmbeddingProvider> provider;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    auto out = duckdb::make_uniq<EmbeddingBindData>();
    out->provider = provider;
    return out;
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    return provider.get() == other.Cast<EmbeddingBindData>().provider.get();
  }
};

duckdb::unique_ptr<duckdb::FunctionData> GenerateEmbeddingBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto& args = input.GetArguments();
  if (args.size() != 3) {
    throw duckdb::BinderException(
      "ai_embed expects 3 arguments: (text, model, secret_name)");
  }
  auto model = FoldVarchar(context, *args[1], "model");
  auto secret_name = FoldVarchar(context, *args[2], "secret_name");

  auto cfg = LoadProviderConfig(context, std::move(model), secret_name);
  auto bind = duckdb::make_uniq<EmbeddingBindData>();
  bind->provider = MakeEmbeddingProvider(*context.db, std::move(cfg));
  return bind;
}

void GenerateEmbeddingFunction(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  const auto& bind = state.expr.Cast<duckdb::BoundFunctionExpression>()
                       .bind_info->Cast<EmbeddingBindData>();
  const auto& provider = *bind.provider;
  const auto count = args.size();

  duckdb::UnifiedVectorFormat text_format;
  args.data[0].ToUnifiedFormat(count, text_format);
  const auto* text_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(text_format);

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);
  auto* list_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

  std::vector<std::string_view> batch_texts;
  std::vector<duckdb::idx_t> batch_rows;
  batch_texts.reserve(count);
  batch_rows.reserve(count);
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto idx = text_format.sel->get_index(i);
    if (!text_format.validity.RowIsValid(idx)) {
      result_validity.SetInvalid(i);
      continue;
    }
    auto text = text_data[idx];
    batch_texts.emplace_back(text.GetData(), text.GetSize());
    batch_rows.push_back(i);
  }

  auto batch = provider.EmbedBatch(batch_texts);

  duckdb::idx_t total_floats = 0;
  for (size_t k = 0; k < batch_rows.size(); k++) {
    auto row = batch_rows[k];
    auto dim = batch[k].size();
    list_entries[row] = {total_floats, dim};
    total_floats += dim;
  }
  for (duckdb::idx_t i = 0; i < count; i++) {
    if (!result_validity.RowIsValid(i)) {
      list_entries[i] = {total_floats, 0};
    }
  }

  duckdb::ListVector::Reserve(result, total_floats);
  duckdb::ListVector::SetListSize(result, total_floats);
  auto& child = duckdb::ListVector::GetEntry(result);
  auto* child_data = duckdb::FlatVector::GetDataMutable<float>(child);
  for (size_t k = 0; k < batch_rows.size(); k++) {
    auto row = batch_rows[k];
    const auto& vec = batch[k];
    std::memcpy(child_data + list_entries[row].offset, vec.data(),
                vec.size() * sizeof(float));
  }
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
    GenerateEmbeddingFunction,
    GenerateEmbeddingBind,
  };
  loader.RegisterFunction(ai_embed);
}

}  // namespace sdb::connector
