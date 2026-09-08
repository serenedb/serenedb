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

class Expression;
class ParsedExpression;

}
namespace irs::analysis {

class SqlTokenizer final : public Tokenizer, private util::Noncopyable {
 public:
  struct Options {
    using Owner = SqlTokenizer;
    std::string expression;
  };

  static constexpr std::string_view type_name() noexcept { return "sql"; }
  static ptr Make(Options opts);

  explicit SqlTokenizer(Options opts);
  ~SqlTokenizer() override;

  TypeInfo::type_id type() const noexcept final {
    return irs::Type<SqlTokenizer>::id();
  }

  TokenTraits Traits() const noexcept final;

  void Bind(duckdb::ClientContext& ctx) final;
  void Unbind() noexcept final;

  size_t MemoryUsage() const noexcept final;

  using Tokenizer::Fill;

  bool Fill(const duckdb::string_t& value, TokenSink& sink, FillCtx ctx) final;

  void Fill(const duckdb::UnifiedVectorFormat& fmt, uint32_t count,
            doc_id_t first_doc, TokenSink& sink, FillCtx ctx) final;

 private:
  struct Call;

  void BindExpression(duckdb::ClientContext& ctx);

  std::unique_ptr<duckdb::ParsedExpression> _parsed;
  std::unique_ptr<duckdb::Expression> _expr;
  std::unique_ptr<Call> _call;
};

}  // namespace irs::analysis
