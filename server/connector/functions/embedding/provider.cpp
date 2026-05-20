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

#include "connector/functions/embedding/provider.h"

#include <duckdb/common/exception.hpp>
#include <utility>

#include "connector/functions/embedding/provider_openai.h"

namespace sdb::connector::embedding {

std::unique_ptr<EmbeddingProvider> MakeEmbeddingProvider(
  duckdb::DatabaseInstance& db, ProviderConfig cfg) {
  if (cfg.protocol == "openai") {
    return MakeOpenAIProvider(db, std::move(cfg));
  }
  throw duckdb::InvalidInputException(
    "Unknown embedding protocol '%s' (supported: openai)", cfg.protocol);
}

}  // namespace sdb::connector::embedding
