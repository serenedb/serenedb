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

#include "iresearch/analysis/tokenizer_config.hpp"

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs::analysis {

duckdb::LogicalTypeId OutputOf(const TokenizerConfig& cfg,
                               duckdb::LogicalTypeId input) noexcept {
  return std::visit(
    [&]<typename Options>(const Options& opts) {
      return Options::Owner::Output(opts, input);
    },
    cfg.config);
}

void SetInput(Tokenizer& tokenizer, duckdb::LogicalTypeId input) {
  if (input == duckdb::LogicalTypeId::GEOMETRY) {
    GeoTokenizer::Cast(tokenizer).SetWkbInput(true);
  } else if (input == duckdb::LogicalTypeId::BLOB &&
             tokenizer.type() == irs::Type<WildcardTokenizer>::id()) {
    utils::downCast<WildcardTokenizer>(tokenizer).SetByteInput(true);
  }
}

void ResolveSqlOutputs(TokenizerConfig& cfg, duckdb::ClientContext& ctx,
                       duckdb::SharedObjectCache& cache) {
  std::visit(
    [&]<typename Options>(Options& opts) {
      if constexpr (std::is_same_v<Options, SqlTokenizer::Options>) {
        if (opts.output == duckdb::LogicalTypeId::INVALID) {
          opts.output =
            SqlTokenizer::ResolveOutput(opts.expression, ctx, cache);
        }
      } else if constexpr (std::is_same_v<Options,
                                          PipelineTokenizer::Options> ||
                           std::is_same_v<Options, UnionTokenizer::Options>) {
        for (auto& child : opts.children) {
          SDB_ASSERT(child);
          ResolveSqlOutputs(*child, ctx, cache);
        }
      } else if constexpr (std::is_same_v<Options,
                                          WildcardTokenizer::Options> ||
                           std::is_same_v<Options, ShingleTokenizer::Options>) {
        SDB_ASSERT(opts.base_analyzer);
        ResolveSqlOutputs(*opts.base_analyzer, ctx, cache);
      }
    },
    cfg.config);
}

}  // namespace irs::analysis
