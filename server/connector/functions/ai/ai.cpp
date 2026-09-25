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

#include <algorithm>
#include <cmath>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/main/secret/secret.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/functions/ai/common.h"
#include "connector/functions/ai/provider.h"
#include "query/config.h"

namespace sdb::connector {
namespace {

using ai::ProviderConfig;

constexpr std::string_view kOpenAIKeys[] = {
  "api_key", "base_url", "model", "chat_path", "embeddings_path",
};
constexpr std::string_view kTypeSafeKeys[] = {"api_key", "base_url", "model"};

constinit SettingRef gEmbeddingBatch{"sdb_ai_embedding_max_batch_size"};

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

struct EmbeddingBindData final : public ai::AIFunctionData {
  std::string fn;
  ProviderConfig cfg;
  bool similarity = false;

  ai::Endpoint GetEndpoint() const final {
    return {.fn = fn, .url = cfg.url, .api_key = cfg.api_key};
  }

  size_t BatchSize() const final { return cfg.max_batch; }

  void Evaluate(ai::Requester& requester, duckdb::DataChunk& args,
                duckdb::Vector& result) const final;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<EmbeddingBindData>(*this);
  }

  bool Equals(const duckdb::FunctionData& other) const final {
    const auto& o = other.Cast<EmbeddingBindData>();
    return fn == o.fn && cfg == o.cfg && similarity == o.similarity;
  }
};

duckdb::unique_ptr<duckdb::FunctionData> EmbeddingBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto& args = input.GetArguments();
  const auto fn = input.GetBoundFunction().GetName().GetIdentifierName();
  const bool similarity = fn == "ai_similarity";
  const size_t model_index = similarity ? 2 : 1;
  const auto model = ai::FoldString(context, *args[model_index], fn, "model");
  if (!model) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG(fn, ": \"model\" must not be NULL"));
  }
  const auto secret_name =
    ai::FoldString(context, *args[model_index + 1], fn, "secret_name");
  const auto dimensions =
    ai::FoldArgument(context, *args[model_index + 2], fn, "dimensions");
  const auto secret =
    ai::LoadSecret(context, fn, secret_name, ai::kEmbeddingDefaultSecretSetting,
                   ai::kOpenAISecretType);

  auto bind = duckdb::make_uniq<EmbeddingBindData>();
  bind->fn = fn;
  bind->similarity = similarity;
  bind->cfg.model = *model;
  if (dimensions) {
    const auto n = dimensions->GetValue<int32_t>();
    if (n < 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG(fn, ": \"dimensions\" must be a non-negative integer"));
    }
    bind->cfg.dimensions = static_cast<uint32_t>(n);
  }
  bind->cfg.max_batch = gEmbeddingBatch.Int(context);
  ai::NormalizeProviderConfig(bind->cfg, secret);
  return bind;
}

void Similarity(ai::Requester& requester, const EmbeddingBindData& bind,
                duckdb::DataChunk& args, duckdb::Vector& result) {
  const auto count = args.size();
  auto left = args.data[0].Values<duckdb::string_t>();
  auto right = args.data[1].Values<duckdb::string_t>();

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto* out = duckdb::FlatVector::GetDataMutable<double>(result);
  auto& out_validity = duckdb::FlatVector::ValidityMutable(result);
  out_validity.SetAllInvalid(count);

  std::vector<duckdb::idx_t> rows;
  duckdb::Vector texts{duckdb::LogicalType::VARCHAR, 2 * count};
  auto* text_data = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(texts);
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto l = left[i];
    auto r = right[i];
    if (!l.IsValid() || !r.IsValid() || l.GetValue().GetSize() == 0 ||
        r.GetValue().GetSize() == 0) {
      continue;
    }
    text_data[2 * rows.size()] = l.GetValue();
    text_data[2 * rows.size() + 1] = r.GetValue();
    rows.push_back(i);
  }
  if (rows.empty()) {
    return;
  }

  const auto n = 2 * rows.size();
  duckdb::Vector embeddings{
    duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT), n};
  ai::EmbedBatch(requester, bind.cfg, texts, n, embeddings);

  const auto* entries =
    duckdb::FlatVector::GetData<duckdb::list_entry_t>(embeddings);
  const auto& validity = duckdb::FlatVector::Validity(embeddings);
  const auto* data = duckdb::FlatVector::GetData<float>(
    duckdb::ListVector::GetEntry(embeddings));
  for (size_t k = 0; k != rows.size(); ++k) {
    const auto a = 2 * k;
    const auto b = a + 1;
    if (!validity.RowIsValid(a) || !validity.RowIsValid(b)) {
      continue;
    }
    if (entries[a].length != entries[b].length) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
        ERR_MSG(bind.fn, ": embeddings have different dimensions (",
                entries[a].length, " and ", entries[b].length, ")"));
    }
    double dot = 0;
    double norm_a = 0;
    double norm_b = 0;
    for (duckdb::idx_t d = 0; d < entries[a].length; d++) {
      const double x = data[entries[a].offset + d];
      const double y = data[entries[b].offset + d];
      dot += x * y;
      norm_a += x * x;
      norm_b += y * y;
    }
    if (norm_a == 0 || norm_b == 0) {
      continue;
    }
    out[rows[k]] =
      std::clamp(dot / (std::sqrt(norm_a) * std::sqrt(norm_b)), -1.0, 1.0);
    out_validity.SetValid(rows[k]);
  }
}

void EmbeddingBindData::Evaluate(ai::Requester& requester,
                                 duckdb::DataChunk& args,
                                 duckdb::Vector& result) const {
  if (similarity) {
    Similarity(requester, *this, args, result);
  } else {
    ai::EmbedBatch(requester, cfg, args.data[0], args.size(), result);
  }
}

void AddEmbeddingOptions(duckdb::FunctionSignature& signature) {
  signature.AddParameter(duckdb::Identifier{"model"},
                         duckdb::LogicalType::VARCHAR);
  ai::AddOption(signature, "secret_name", duckdb::LogicalType::VARCHAR);
  ai::AddOption(signature, "dimensions", duckdb::LogicalType::INTEGER);
}

}  // namespace

void RegisterAIFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};

  RegisterSecretType(loader, ai::kOpenAISecretType, kOpenAIKeys);
  RegisterSecretType(loader, ai::kTypeSafeSecretType, kTypeSafeKeys);

  auto ai_embed = ai::MakeAIFunction(
    "ai_embed", duckdb::LogicalType::LIST(duckdb::LogicalType::FLOAT),
    EmbeddingBind);
  ai_embed.GetSignature().AddParameter(duckdb::Identifier{"text"},
                                       duckdb::LogicalType::VARCHAR);
  AddEmbeddingOptions(ai_embed.GetSignature());
  loader.RegisterFunction(ai_embed);

  auto ai_similarity = ai::MakeAIFunction(
    "ai_similarity", duckdb::LogicalType::DOUBLE, EmbeddingBind);
  ai_similarity.GetSignature().AddParameter(duckdb::Identifier{"text1"},
                                            duckdb::LogicalType::VARCHAR);
  ai_similarity.GetSignature().AddParameter(duckdb::Identifier{"text2"},
                                            duckdb::LogicalType::VARCHAR);
  AddEmbeddingOptions(ai_similarity.GetSignature());
  loader.RegisterFunction(ai_similarity);

  ai::RegisterTextFunctions(loader);
  ai::RegisterJevFunction(loader);
  ai::RegisterAggregateFunctions(loader);
  ai::RegisterAIOptimizer(db);
}

}  // namespace sdb::connector
