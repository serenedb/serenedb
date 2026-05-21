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

#include <duckdb/common/types/vector.hpp>
#include <string>
#include <string_view>

namespace duckdb {

class DatabaseInstance;
}

namespace sdb::connector::embedding {

enum class ProviderType {
  OpenAI,
};

struct ProviderConfig {
  ProviderType type = ProviderType::OpenAI;
  std::string model;
  std::string api_key;
  std::string base_url;
  std::string embeddings_path;
  std::string auth_header;
};

ProviderType ResolveProviderType(std::string_view protocol);

void NormalizeProviderConfig(duckdb::DatabaseInstance& db, ProviderConfig& cfg);

void EmbedBatch(duckdb::DatabaseInstance& db, const ProviderConfig& cfg,
                duckdb::Vector& texts, duckdb::idx_t count,
                duckdb::Vector& result);

}  // namespace sdb::connector::embedding
