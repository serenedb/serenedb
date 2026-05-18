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

#pragma once

#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {

class DatabaseInstance;
}

namespace sdb::connector::embedding {

class EmbeddingProvider {
 public:
  virtual ~EmbeddingProvider() = default;
  virtual std::vector<float> Embed(std::string_view text) const = 0;
  virtual std::vector<std::vector<float>> EmbedBatch(
    const std::vector<std::string_view>& texts) const {
    std::vector<std::vector<float>> out;
    out.reserve(texts.size());
    for (auto t : texts) {
      out.push_back(Embed(t));
    }
    return out;
  }
};

struct ProviderConfig {
  std::string protocol;
  std::string model;
  std::string api_key;
  std::string base_url;
  std::string embeddings_path;
};

std::unique_ptr<EmbeddingProvider> MakeEmbeddingProvider(
  duckdb::DatabaseInstance& db, ProviderConfig cfg);

}  // namespace sdb::connector::embedding
