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

#include <iresearch/analysis/tokenizer_config.hpp>

#include "test_resources.hpp"

namespace tests {

inline bool AnyExpression(const duckdb::Expression& /*expr*/) { return true; }

inline irs::analysis::TokenizerConfig Bound(
  const irs::analysis::TokenizerConfig& cfg,
  const duckdb::LogicalType& input = duckdb::LogicalType::VARCHAR,
  duckdb::ClientContext& client = EngineContext()) {
  irs::analysis::BindContext ctx{
    .client = client,
    .row_alone = AnyExpression,
  };
  return irs::analysis::BindInput(cfg, input, ctx);
}

inline irs::analysis::Tokenizer::ptr MakeBound(
  const irs::analysis::TokenizerConfig& cfg,
  const duckdb::LogicalType& input = duckdb::LogicalType::VARCHAR,
  duckdb::ClientContext& client = EngineContext()) {
  return irs::analysis::CreateTokenizer(Bound(cfg, input, client), Cache());
}

}  // namespace tests
