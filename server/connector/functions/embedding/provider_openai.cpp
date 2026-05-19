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

#include <simdjson.h>

#include <algorithm>
#include <cstdio>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/http_util.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension_helper.hpp>
#include <string_view>
#include <utility>

#include "basics/assert.h"

namespace sdb::connector::embedding {
namespace {

constexpr const std::string_view kDefaultBaseUrl = "https://api.openai.com";
constexpr const std::string_view kEmbeddingsPath = "/v1/embeddings";

class OpenAIProvider final : public EmbeddingProvider {
 public:
  OpenAIProvider(duckdb::DatabaseInstance& db, ProviderConfig cfg)
    : _db(db), _cfg(std::move(cfg)) {
    if (_cfg.base_url.empty()) {
      _cfg.base_url = kDefaultBaseUrl;
    }
    while (!_cfg.base_url.empty() && _cfg.base_url.back() == '/') {
      _cfg.base_url.pop_back();
    }
    if (_cfg.embeddings_path.empty()) {
      _cfg.embeddings_path = kEmbeddingsPath;
    } else if (_cfg.embeddings_path.front() != '/') {
      _cfg.embeddings_path.insert(_cfg.embeddings_path.begin(), '/');
    }
    duckdb::ExtensionHelper::TryAutoLoadExtension(_db, "httpfs");
  }

  void EmbedBatch(std::span<std::string_view> texts,
                  duckdb::Vector& result) const final {
    constexpr size_t kMaxBatch = 64;
    duckdb::idx_t next_row = 0;
    for (size_t start = 0; start < texts.size(); start += kMaxBatch) {
      size_t end = std::min(start + kMaxBatch, texts.size());
      EmbedChunk(texts.subspan(start, end - start), result, next_row);
    }
  }

 private:
  void EmbedChunk(std::span<std::string_view> texts, duckdb::Vector& result,
                  duckdb::idx_t& next_row) const {
    size_t total = 32 + _cfg.model.size();
    for (auto t : texts) {
      total += t.size() + 4;
    }
    simdjson::builder::string_builder builder(total);
    builder.start_object();
    builder.append_raw("\"model\":");
    builder.escape_and_append_with_quotes(_cfg.model);
    builder.append_comma();
    builder.append_raw("\"input\":");
    builder.start_array();
    for (size_t i = 0; i < texts.size(); i++) {
      if (i > 0) {
        builder.append_comma();
      }
      builder.escape_and_append_with_quotes(texts[i]);
    }
    builder.end_array();
    builder.end_object();
    std::string_view body;
    if (builder.view().get(body) != simdjson::SUCCESS) {
      throw duckdb::IOException(
        "failed to build OpenAI embeddings request body");
    }

    const std::string url = _cfg.base_url + _cfg.embeddings_path;

    auto& http_util = duckdb::HTTPUtil::Get(_db);
    auto params = http_util.InitializeParameters(_db, url);

    duckdb::HTTPHeaders headers;
    headers.Insert("Content-Type", "application/json");
    if (!_cfg.api_key.empty()) {
      headers.Insert("Authorization", "Bearer " + _cfg.api_key);
    }

    duckdb::PostRequestInfo post_request(
      url, headers, *params,
      reinterpret_cast<duckdb::const_data_ptr_t>(body.data()), body.size());
    auto response = http_util.Request(post_request);

    if (!response) {
      throw duckdb::IOException("OpenAI embeddings request to '%s' failed",
                                url);
    }
    if (response->HasRequestError()) {
      throw duckdb::IOException("OpenAI embeddings request to '%s' failed: %s",
                                url, response->GetRequestError());
    }
    if (!response->Success()) {
      throw duckdb::IOException("OpenAI embeddings API returned HTTP %d: %s",
                                static_cast<int>(response->status),
                                post_request.buffer_out);
    }
    return ParseEmbeddings(post_request.buffer_out, texts.size(), result,
                           next_row);
  }

  static void ParseEmbeddings(std::string_view body, size_t expected,
                              duckdb::Vector& result, duckdb::idx_t& next_row) {
    simdjson::padded_string padded{body};
    simdjson::ondemand::parser parser;
    simdjson::ondemand::document doc;
    if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
      throw duckdb::InvalidInputException(
        "OpenAI response is not valid JSON: %s", body);
    }

    simdjson::ondemand::array data;
    if (doc["data"].get_array().get(data) != simdjson::SUCCESS) {
      throw duckdb::InvalidInputException(
        "OpenAI response missing 'data' array: %s", body);
    }

    SDB_ASSERT(result.GetVectorType() == duckdb::VectorType::FLAT_VECTOR);
    auto* list_entries =
      duckdb::FlatVector::GetDataMutable<duckdb::list_entry_t>(result);
    const auto& validity = duckdb::FlatVector::Validity(result);

    size_t parsed = 0;
    size_t size = duckdb::ListVector::GetListSize(result);
    for (auto item : data) {
      if (parsed >= expected) {
        throw duckdb::InvalidInputException(
          "OpenAI returned more embeddings than expected (%d): %s", expected,
          body);
      }
      while (!validity.RowIsValid(next_row)) {
        next_row++;
      }
      simdjson::ondemand::value v;
      if (item.get(v) != simdjson::SUCCESS) {
        throw duckdb::InvalidInputException(
          "OpenAI response 'data[%d]' unreadable: %s", parsed, body);
      }
      simdjson::ondemand::array values;
      if (v["embedding"].get_array().get(values) != simdjson::SUCCESS) {
        throw duckdb::InvalidInputException(
          "OpenAI response 'data[%d].embedding' is not an array: %s", parsed,
          body);
      }
      size_t values_size = values.count_elements();
      values.reset();
      duckdb::ListVector::Reserve(result, size + values_size);
      duckdb::ListVector::SetListSize(result, size + values_size);
      auto& child = duckdb::ListVector::GetEntry(result);
      auto* data = duckdb::FlatVector::GetDataMutable<float>(child);

      list_entries[next_row] = {size, values_size};

      for (auto val : values) {
        double d = 0.0;
        if (val.get_double().get(d) != simdjson::SUCCESS) {
          throw duckdb::InvalidInputException(
            "OpenAI embedding contains non-numeric value: %s", body);
        }
        data[size++] = static_cast<float>(d);
      }
      parsed++;
      next_row++;
    }
    if (parsed != expected) {
      throw duckdb::InvalidInputException(
        "OpenAI returned %d embeddings, expected %d: %s", parsed, expected,
        body);
    }
  }

  duckdb::DatabaseInstance& _db;
  ProviderConfig _cfg;
};

}  // namespace

std::unique_ptr<EmbeddingProvider> MakeOpenAIProvider(
  duckdb::DatabaseInstance& db, ProviderConfig cfg) {
  return std::make_unique<OpenAIProvider>(db, std::move(cfg));
}

}  // namespace sdb::connector::embedding
