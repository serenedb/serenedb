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

#include "tokenizer.hpp"

namespace duckdb {

class ParsedExpression;

}  // namespace duckdb
namespace irs::analysis {

// Tokenizer backed by a DuckDB scalar expression over the pseudo-column
// `input` (VARCHAR), e.g. `lower(regexp_split_to_array(input, '\W+'))`.
// A VARCHAR expression emits one token per value; LIST(VARCHAR) emits the
// list elements (NULL row = value rejected, NULL element = token dropped).
// Expressions are pure over their inputs: only system-catalog functions
// resolve (no user macros/UDFs), no subqueries, no parameters, no volatile
// functions. Construction parses and pre-validates; Bind compiles the
// expression against the leasing caller's context; Unbind drops the
// executor so no context reference survives pool parking.
class SqlTokenizer final : public TypedTokenizer<SqlTokenizer>,
                           private util::Noncopyable {
 public:
  struct Options {
    using Owner = SqlTokenizer;
    std::string expression;
  };

  static constexpr std::string_view type_name() noexcept { return "sql"; }
  static ptr Make(Options opts);

  explicit SqlTokenizer(Options opts);
  ~SqlTokenizer() override;

  void Bind(duckdb::ClientContext& ctx) final;
  void Unbind() noexcept final;

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

  using TypedTokenizer<SqlTokenizer>::Fill;
  void DoFillColumn(std::span<const duckdb::string_t> values,
                    std::span<const doc_id_t> docs, TokenSink& sink,
                    TokenLayout layout);

 private:
  enum class Mode : uint8_t { Scalar, List };

  struct Plan;
  struct Exec;

  template<TokenLayout Layout>
  void FillSlice(std::span<const duckdb::string_t> values,
                 std::span<const doc_id_t> docs, TokenSink& sink);

  std::string _expression;
  std::unique_ptr<duckdb::ParsedExpression> _parsed;
  std::unique_ptr<Plan> _plan;
  std::unique_ptr<Exec> _exec;
  Mode _mode = Mode::Scalar;
};

extern template class TypedTokenizer<SqlTokenizer>;

}  // namespace irs::analysis
