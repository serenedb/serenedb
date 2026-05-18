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
#include <duckdb/main/database.hpp>
#include <duckdb/main/extension_helper.hpp>
#include <string_view>
#include <utility>

namespace sdb::connector::embedding {
namespace {

constexpr const std::string_view kDefaultBaseUrl = "https://api.openai.com";
constexpr const std::string_view kEmbeddingsPath = "/v1/embeddings";

void AppendJsonEscaped(std::string& out, std::string_view s) {
  out.reserve(out.size() + s.size() + 2);
  for (unsigned char c : s) {
    switch (c) {
      case '"':
        out.append("\\\"");
        break;
      case '\\':
        out.append("\\\\");
        break;
      case '\b':
        out.append("\\b");
        break;
      case '\f':
        out.append("\\f");
        break;
      case '\n':
        out.append("\\n");
        break;
      case '\r':
        out.append("\\r");
        break;
      case '\t':
        out.append("\\t");
        break;
      default:
        if (c < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", c);
          out.append(buf);
        } else {
          out.push_back(static_cast<char>(c));
        }
    }
  }
}

class OpenAIProvider final : public EmbeddingProvider {
 public:
  OpenAIProvider(duckdb::DatabaseInstance& db, ProviderConfig cfg)
    : _db(db), _cfg(std::move(cfg)) {
    if (_cfg.api_key.empty()) {
      throw duckdb::InvalidInputException(
        "OpenAI secret is missing required 'api_key'");
    }
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

  std::vector<float> Embed(std::string_view text) const override {
    std::vector<std::string_view> one{text};
    auto out = EmbedBatch(one);
    return std::move(out[0]);
  }

  std::vector<std::vector<float>> EmbedBatch(
    const std::vector<std::string_view>& texts) const override {
    std::vector<std::vector<float>> result;
    result.reserve(texts.size());
    constexpr size_t kMaxBatch = 96;
    for (size_t start = 0; start < texts.size(); start += kMaxBatch) {
      size_t end = std::min(start + kMaxBatch, texts.size());
      auto chunk = EmbedChunk(texts.data() + start, end - start);
      for (auto& v : chunk) {
        result.push_back(std::move(v));
      }
    }
    return result;
  }

 private:
  std::vector<std::vector<float>> EmbedChunk(const std::string_view* texts,
                                             size_t n) const {
    std::string body;
    body.append("{\"model\":\"");
    AppendJsonEscaped(body, _cfg.model);
    body.append("\",\"input\":[");
    for (size_t i = 0; i < n; i++) {
      if (i > 0) {
        body.push_back(',');
      }
      body.push_back('"');
      AppendJsonEscaped(body, texts[i]);
      body.push_back('"');
    }
    body.append("]}");

    const std::string url = _cfg.base_url + _cfg.embeddings_path;

    auto& http_util = duckdb::HTTPUtil::Get(_db);
    auto params = http_util.InitializeParameters(_db, url);

    duckdb::HTTPHeaders headers;
    headers.Insert("Content-Type", "application/json");
    headers.Insert("Authorization", "Bearer " + _cfg.api_key);

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
    return ParseEmbeddings(post_request.buffer_out, n);
  }

  static std::vector<std::vector<float>> ParseEmbeddings(
    const std::string& body, size_t expected) {
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

    std::vector<std::vector<float>> out;
    out.reserve(expected);
    for (auto item : data) {
      simdjson::ondemand::value v;
      if (item.get(v) != simdjson::SUCCESS) {
        throw duckdb::InvalidInputException(
          "OpenAI response 'data[%d]' unreadable: %s", (int)out.size(), body);
      }
      simdjson::ondemand::array values;
      if (v["embedding"].get_array().get(values) != simdjson::SUCCESS) {
        throw duckdb::InvalidInputException(
          "OpenAI response 'data[%d].embedding' is not an array: %s",
          (int)out.size(), body);
      }
      std::vector<float> vec;
      for (auto x : values) {
        double d = 0.0;
        if (x.get_double().get(d) != simdjson::SUCCESS) {
          throw duckdb::InvalidInputException(
            "OpenAI embedding contains non-numeric value: %s", body);
        }
        vec.push_back(static_cast<float>(d));
      }
      if (vec.empty()) {
        throw duckdb::InvalidInputException(
          "OpenAI returned empty embedding vector: %s", body);
      }
      out.push_back(std::move(vec));
    }
    if (out.size() != expected) {
      throw duckdb::InvalidInputException(
        "OpenAI returned %d embeddings, expected %d: %s", (int)out.size(),
        (int)expected, body);
    }
    return out;
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
