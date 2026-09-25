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

#include "connector/functions/ai/provider_openai.h"

#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <algorithm>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>
#include <string_view>
#include <tuple>
#include <vector>

#include "connector/functions/ai/common.h"

namespace sdb::connector::ai {
namespace {

constexpr std::string_view kEmbeddingsPath = "/v1/embeddings";

using Embeddings = std::vector<std::vector<float>>;

Embeddings ParseEmbeddings(std::string_view body, size_t expected) {
  simdjson::dom::parser parser;
  simdjson::dom::element doc;
  if (parser.parse(body.data(), body.size()).get(doc) != simdjson::SUCCESS) {
    ThrowRowError(absl::StrCat("OpenAI response is not valid JSON: ", body));
  }
  simdjson::dom::array data;
  if (doc["data"].get_array().get(data) != simdjson::SUCCESS) {
    ThrowRowError(absl::StrCat("OpenAI response missing 'data' array: ", body));
  }
  if (data.size() != expected) {
    ThrowRowError(absl::StrCat("OpenAI returned ", data.size(),
                               " embeddings, expected ", expected, ": ", body));
  }

  Embeddings embeddings(expected);
  std::vector<bool> seen(expected);
  size_t position = 0;
  for (auto item : data) {
    uint64_t index = position++;
    std::ignore = item["index"].get_uint64().get(index);
    if (index >= expected || seen[index]) {
      ThrowRowError(absl::StrCat("OpenAI response 'data[", position - 1,
                                 "].index' ", index,
                                 " is out of range or repeated: ", body));
    }
    seen[index] = true;
    simdjson::dom::array values;
    if (item["embedding"].get_array().get(values) != simdjson::SUCCESS) {
      ThrowRowError(absl::StrCat("OpenAI response 'data[", position - 1,
                                 "].embedding' is not an array: ", body));
    }
    auto& embedding = embeddings[index];
    embedding.reserve(values.size());
    for (auto val : values) {
      double d = 0.0;
      if (val.get_double().get(d) != simdjson::SUCCESS) {
        ThrowRowError(
          absl::StrCat("OpenAI embedding contains non-numeric value: ", body));
      }
      embedding.push_back(static_cast<float>(d));
    }
  }
  return embeddings;
}

std::string BuildBody(const ProviderConfig& cfg,
                      std::span<const std::string_view> texts) {
  size_t total = 64 + cfg.model.size();
  for (const auto text : texts) {
    total += text.size() + 4;
  }
  simdjson::builder::string_builder builder(total);
  builder.start_object();
  builder.append_raw("\"model\":");
  builder.escape_and_append_with_quotes(cfg.model);
  if (cfg.dimensions != 0) {
    builder.append_comma();
    builder.append_raw("\"dimensions\":");
    builder.append(cfg.dimensions);
  }
  builder.append_comma();
  builder.append_raw("\"input\":");
  builder.start_array();
  for (size_t i = 0; i != texts.size(); ++i) {
    if (i != 0) {
      builder.append_comma();
    }
    builder.escape_and_append_with_quotes(texts[i]);
  }
  builder.end_array();
  builder.end_object();
  std::string_view body;
  if (builder.view().get(body) != simdjson::SUCCESS) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("failed to build OpenAI embeddings request body"));
  }
  return std::string{body};
}

}  // namespace

void NormalizeOpenAIConfig(ProviderConfig& cfg, const SecretConfig& secret) {
  cfg.url = JoinUrl(
    secret.base_url, kOpenAIDefaultBaseUrl,
    secret.embeddings_path.empty() ? kEmbeddingsPath : secret.embeddings_path);
  cfg.api_key = secret.api_key;
}

void EmbedBatchOpenAI(Requester& requester, const ProviderConfig& cfg,
                      duckdb::Vector& texts, duckdb::idx_t count,
                      duckdb::Vector& result) {
  const auto inputs = CollectInputs(texts, count, true, true);
  const auto& unique = inputs.texts;
  const size_t batch = std::max<size_t>(1, cfg.max_batch);
  std::vector<Embeddings> embeddings((unique.size() + batch - 1) / batch);
  requester.ForEach(embeddings.size(), [&](size_t b) {
    const auto chunk = std::span{unique}.subspan(
      b * batch, std::min(batch, unique.size() - b * batch));
    if (auto body = requester.Post(BuildBody(cfg, chunk))) {
      embeddings[b] = ParseEmbeddings(*body, chunk.size());
    }
  });
  auto embedding = [&](size_t slot) -> const std::vector<float>* {
    if (slot == Inputs::kNone || embeddings[slot / batch].empty()) {
      return nullptr;
    }
    return &embeddings[slot / batch][slot % batch];
  };

  size_t total = 0;
  for (const auto slot : inputs.slots) {
    if (const auto* e = embedding(slot)) {
      total += e->size();
    }
  }
  duckdb::ListVector::Reserve(result, total);
  auto* data = duckdb::FlatVector::GetDataMutable<float>(
    duckdb::ListVector::GetEntry(result));
  auto* entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& validity = duckdb::FlatVector::ValidityMutable(result);
  size_t offset = 0;
  for (duckdb::idx_t i = 0; i < count; i++) {
    const auto* e = embedding(inputs.slots[i]);
    if (e == nullptr) {
      entries[i] = {0, 0};
      validity.SetInvalid(i);
      continue;
    }
    entries[i] = {offset, e->size()};
    validity.SetValid(i);
    std::copy(e->begin(), e->end(), data + offset);
    offset += e->size();
  }
  duckdb::ListVector::SetListSize(result, offset);
}

}  // namespace sdb::connector::ai
