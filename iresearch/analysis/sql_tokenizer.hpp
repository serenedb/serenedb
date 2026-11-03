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

#include <absl/synchronization/mutex.h>

#include <duckdb/storage/shared_object_cache.hpp>
#include <memory>
#include <string>
#include <string_view>

#include "tokenizer.hpp"

namespace duckdb {

class Expression;
class ParsedExpression;

}  // namespace duckdb
namespace irs::analysis {

struct SqlCall;

class SqlExpression final : public duckdb::ObjectCacheEntry {
 public:
  static constexpr std::string_view ObjectType() {
    return "sdb_sql_expression";
  }

  static duckdb::shared_ptr<SqlExpression> Get(duckdb::SharedObjectCache& cache,
                                               std::string_view owner,
                                               std::string_view text);

  SqlExpression(std::string_view owner, std::string_view text);
  ~SqlExpression() final;

  std::string GetObjectType() final { return std::string{ObjectType()}; }

  duckdb::optional_idx GetEstimatedCacheMemory() const final { return 0u; }

  const duckdb::Expression& Bind(duckdb::ClientContext& ctx);

 private:
  std::string_view _owner;
  absl::Mutex _mutex;
  std::unique_ptr<duckdb::ParsedExpression> _parsed ABSL_GUARDED_BY(_mutex);
  std::unique_ptr<duckdb::Expression> _expr ABSL_GUARDED_BY(_mutex);
};

class SqlTokenizer final : public Tokenizer, private util::Noncopyable {
 public:
  struct Options {
    using Owner = SqlTokenizer;
    std::string expression;
  };

  static constexpr std::string_view type_name() noexcept { return "sql"; }
  static ptr Make(Options opts, duckdb::SharedObjectCache& cache);

  SqlTokenizer(const Options& opts, duckdb::SharedObjectCache& cache);
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
  duckdb::shared_ptr<SqlExpression> _expression;
  const duckdb::Expression* _expr = nullptr;
  std::unique_ptr<SqlCall> _call;
};

class SqlPredicate final : private util::Noncopyable {
 public:
  SqlPredicate(std::string_view owner, std::string_view expression,
               duckdb::SharedObjectCache& cache);
  ~SqlPredicate();

  void Bind(duckdb::ClientContext& ctx);
  void Unbind() noexcept;

  bool Test(const duckdb::string_t& value);
  bool Apply(const duckdb::string_t* terms, uint32_t count, uint64_t* valid);

  size_t MemoryUsage() const noexcept;

 private:
  std::string_view _owner;
  duckdb::shared_ptr<SqlExpression> _expression;
  std::unique_ptr<SqlCall> _call;
};

}  // namespace irs::analysis
