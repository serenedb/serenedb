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

#include "connector/functions/ai/provider.h"

#include <iresearch/utils/system_compiler.hpp>

#include "connector/functions/ai/provider_openai.h"

namespace sdb::connector::ai {

void NormalizeProviderConfig(ProviderConfig& cfg, const SecretConfig& secret) {
  switch (cfg.type) {
    case ProviderType::OpenAI:
      NormalizeOpenAIConfig(cfg, secret);
      break;
    default:
      SDB_UNREACHABLE();
  }
}

void EmbedBatch(Requester& requester, const ProviderConfig& cfg,
                duckdb::Vector& texts, duckdb::idx_t count,
                duckdb::Vector& result) {
  switch (cfg.type) {
    case ProviderType::OpenAI:
      EmbedBatchOpenAI(requester, cfg, texts, count, result);
      break;
    default:
      SDB_UNREACHABLE();
  }
}

}  // namespace sdb::connector::ai
