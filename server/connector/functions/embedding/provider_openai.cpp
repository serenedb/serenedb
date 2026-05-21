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

#include "connector/functions/embedding/provider_openai.h"

#include <absl/strings/str_cat.h>
#include <simdjson.h>

#include <array>
#include <duckdb/common/http_util.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension_helper.hpp>
#include <span>
#include <string_view>

#include "basics/assert.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector::embedding {
namespace {

constexpr const std::string_view kDefaultBaseUrl = "https://api.openai.com";
constexpr const std::string_view kEmbeddingsPath = "/v1/embeddings";
constexpr size_t kMaxBatch = 64;

void ParseEmbeddings(std::string_view body, size_t expected,
                     duckdb::Vector& result, duckdb::idx_t& next_row) {
  simdjson::padded_string padded{body};
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("OpenAI response is not valid JSON: ", body));
  }

  simdjson::ondemand::array data;
  if (doc["data"].get_array().get(data) != simdjson::SUCCESS) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("OpenAI response missing 'data' array: ", body));
  }

  SDB_ASSERT(result.GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
  auto* list_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  const auto& validity = duckdb::FlatVector::Validity(result);

  size_t parsed = 0;
  size_t size = duckdb::ListVector::GetListSize(result);
  for (auto item : data) {
    if (parsed >= expected) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                      ERR_MSG("OpenAI returned more embeddings than expected (",
                              expected, "): ", body));
    }
    while (!validity.RowIsValid(next_row)) {
      next_row++;
    }
    simdjson::ondemand::value v;
    if (item.get(v) != simdjson::SUCCESS) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
        ERR_MSG("OpenAI response 'data[", parsed, "]' unreadable: ", body));
    }
    simdjson::ondemand::array values;
    if (v["embedding"].get_array().get(values) != simdjson::SUCCESS) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                      ERR_MSG("OpenAI response 'data[", parsed,
                              "].embedding' is not an array: ", body));
    }
    size_t values_size = values.count_elements();
    values.reset();
    duckdb::ListVector::Reserve(result, size + values_size);
    duckdb::ListVector::SetListSize(result, size + values_size);
    auto& child = duckdb::ListVector::GetEntry(result);
    auto* data_ptr = duckdb::FlatVector::GetDataMutable<float>(child);

    list_entries[next_row] = {size, values_size};

    for (auto val : values) {
      double d = 0.0;
      if (val.get_double().get(d) != simdjson::SUCCESS) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
          ERR_MSG("OpenAI embedding contains non-numeric value: ", body));
      }
      data_ptr[size++] = static_cast<float>(d);
    }
    parsed++;
    next_row++;
  }
  if (parsed != expected) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("OpenAI returned ", parsed,
                            " embeddings, expected ", expected, ": ", body));
  }
}

void EmbedChunk(duckdb::DatabaseInstance& db, const ProviderConfig& cfg,
                const std::string& url,
                std::span<const duckdb::string_t* const> texts,
                duckdb::Vector& result, duckdb::idx_t& next_row) {
  size_t total = 32 + cfg.model.size();
  size_t valid_count = 0;
  for (const auto* s : texts) {
    if (s == nullptr) {
      continue;
    }
    total += s->GetSize() + 4;
    valid_count++;
  }
  simdjson::builder::string_builder builder(total);
  builder.start_object();
  builder.append_raw("\"model\":");
  builder.escape_and_append_with_quotes(cfg.model);
  builder.append_comma();
  builder.append_raw("\"input\":");
  builder.start_array();
  bool first = true;
  for (const auto* s : texts) {
    if (s == nullptr) {
      continue;
    }
    if (!first) {
      builder.append_comma();
    }
    first = false;
    builder.escape_and_append_with_quotes(
      std::string_view{s->GetData(), s->GetSize()});
  }
  builder.end_array();
  builder.end_object();
  std::string_view body;
  if (builder.view().get(body) != simdjson::SUCCESS) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("failed to build OpenAI embeddings request body"));
  }

  auto& http_util = duckdb::HTTPUtil::Get(db);
  auto params = http_util.InitializeParameters(db, url);

  duckdb::HTTPHeaders headers;
  headers.Insert("Content-Type", "application/json");
  if (!cfg.auth_header.empty()) {
    headers.Insert("Authorization", cfg.auth_header);
  }

  duckdb::PostRequestInfo post_request(
    url, headers, *params,
    reinterpret_cast<duckdb::const_data_ptr_t>(body.data()), body.size());
  auto response = http_util.Request(post_request);

  if (!response) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONNECTION_FAILURE),
                    ERR_MSG("OpenAI embeddings request to '", url, "' failed"));
  }
  if (response->HasRequestError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONNECTION_FAILURE),
                    ERR_MSG("OpenAI embeddings request to '", url,
                            "' failed: ", response->GetRequestError()));
  }
  if (!response->Success()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                    ERR_MSG("OpenAI embeddings API returned HTTP ",
                            static_cast<int>(response->status), ": ",
                            post_request.buffer_out));
  }
  ParseEmbeddings(post_request.buffer_out, valid_count, result, next_row);
}

}  // namespace

void NormalizeOpenAIConfig(duckdb::DatabaseInstance& db, ProviderConfig& cfg) {
  if (cfg.base_url.empty()) {
    cfg.base_url = kDefaultBaseUrl;
  }
  while (!cfg.base_url.empty() && cfg.base_url.back() == '/') {
    cfg.base_url.pop_back();
  }
  if (cfg.embeddings_path.empty()) {
    cfg.embeddings_path = kEmbeddingsPath;
  } else if (cfg.embeddings_path.front() != '/') {
    cfg.embeddings_path.insert(cfg.embeddings_path.begin(), '/');
  }
  if (!cfg.api_key.empty() && cfg.auth_header.empty()) {
    cfg.auth_header = absl::StrCat("Bearer ", cfg.api_key);
  }
  duckdb::ExtensionHelper::TryAutoLoadExtension(db, "httpfs");
}

void EmbedBatchOpenAI(duckdb::DatabaseInstance& db, const ProviderConfig& cfg,
                      duckdb::Vector& texts, duckdb::idx_t count,
                      duckdb::Vector& result) {
  duckdb::UnifiedVectorFormat text_format;
  texts.ToUnifiedFormat(count, text_format);
  const auto* text_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(text_format);

  auto* list_entries =
    duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);

  const std::string url = cfg.base_url + cfg.embeddings_path;
  duckdb::idx_t next_row = 0;
  std::array<const duckdb::string_t*, kMaxBatch> batch{};
  size_t batch_n = 0;
  size_t valid_in_batch = 0;
  auto flush = [&]() {
    if (valid_in_batch == 0) {
      batch_n = 0;
      return;
    }
    EmbedChunk(db, cfg, url,
               std::span<const duckdb::string_t* const>{batch.data(), batch_n},
               result, next_row);
    batch_n = 0;
    valid_in_batch = 0;
  };
  for (duckdb::idx_t i = 0; i < count; i++) {
    auto idx = text_format.sel->get_index(i);
    if (!text_format.validity.RowIsValid(idx)) {
      result_validity.SetInvalid(i);
      list_entries[i] = {0, 0};
      batch[batch_n++] = nullptr;
    } else {
      batch[batch_n++] = &text_data[idx];
      valid_in_batch++;
    }
    if (valid_in_batch == kMaxBatch) {
      flush();
    }
  }
  flush();
}

}  // namespace sdb::connector::embedding
