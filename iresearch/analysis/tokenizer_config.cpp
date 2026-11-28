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

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::analysis {

duckdb::LogicalTypeId TextOrBytes(std::string_view stage,
                                  const duckdb::LogicalType& input) {
  const auto id = input.id();
  if (id != duckdb::LogicalTypeId::VARCHAR &&
      id != duckdb::LogicalTypeId::BLOB) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
      ERR_MSG(stage, ": takes text or bytes, not ", input.ToString()));
  }
  return id;
}

void BindNode(TokenizerConfig& node, const duckdb::LogicalType& input,
              BindContext& ctx) {
  node.terms = std::visit(
    [&]<typename Options>(Options& opts) {
      if constexpr (requires { Options::Owner::BindInput(opts, input, ctx); }) {
        return Options::Owner::BindInput(opts, input, ctx);
      } else {
        return TextOrBytes(Options::Owner::type_name(), input);
      }
    },
    node.config);
  node.input = input.id();
}

TokenizerConfig BindInput(const TokenizerConfig& definition,
                          const duckdb::LogicalType& input, BindContext& ctx) {
  auto bound = Clone(definition);
  BindNode(bound, input, ctx);
  return bound;
}

}  // namespace irs::analysis
