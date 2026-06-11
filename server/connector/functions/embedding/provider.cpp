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

#include <absl/strings/str_cat.h>

#include "connector/functions/embedding/provider_openai.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector::embedding {

ProviderType ResolveProviderType(std::string_view protocol) {
  if (protocol == "openai") {
    return ProviderType::OpenAI;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
    ERR_MSG("Unknown embedding protocol '", protocol, "' (supported: openai)"));
}

void NormalizeProviderConfig(duckdb::DatabaseInstance& db,
                             ProviderConfig& cfg) {
  switch (cfg.type) {
    case ProviderType::OpenAI:
      NormalizeOpenAIConfig(db, cfg);
      break;
    default:
      SDB_UNREACHABLE();
  }
}

void EmbedBatch(duckdb::DatabaseInstance& db, const ProviderConfig& cfg,
                duckdb::Vector& texts, duckdb::idx_t count,
                duckdb::Vector& result) {
  switch (cfg.type) {
    case ProviderType::OpenAI:
      EmbedBatchOpenAI(db, cfg, texts, count, result);
      break;
    default:
      SDB_UNREACHABLE();
  }
}

}  // namespace sdb::connector::embedding
