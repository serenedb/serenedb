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
#include <tuple>

#include "iresearch/analysis/process_tokens.hpp"
#include "iresearch/utils/noncopyable.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class SqlPredicate;

class FilterTokensTokenizer final
  : public TypedTokenizer<FilterTokensTokenizer>,
    public TokenStage,
    private util::Noncopyable {
 public:
  struct Options {
    using Owner = FilterTokensTokenizer;
    size_t min_length{0};
    size_t max_length{0};
    std::string predicate;
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept {
    return "filter_tokens";
  }

  explicit FilterTokensTokenizer(Options opts);
  ~FilterTokensTokenizer() override;

  TokenTraits Traits() const noexcept final {
    return {
      .unique = true, .offsets = true, .stable = true, .keeps_ascii = true};
  }

  BlockTraits WantedBlockTraits() const noexcept final {
    return {.ascii = _by_length};
  }

  void Bind(duckdb::ClientContext& ctx) final;
  void Unbind() noexcept final;

  size_t MemoryUsage() const noexcept final;

  std::tuple<bool> PrepareBatch(BlockTraits traits) const noexcept {
    return {traits.ascii};
  }

  template<TokenLayout Layout, bool KnownAscii>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

  bool ProcessTokens(TokenBatch& batch, BatchCtx& ctx) final;

 private:
  template<bool KnownAscii>
  IRS_FORCE_INLINE bool FitsLength(const duckdb::string_t& term) const noexcept;

  template<bool KnownAscii>
  bool DropByLength(TokenBatch& batch, uint64_t* valid) const noexcept;

  size_t _min;
  size_t _max;
  bool _by_length;
  std::unique_ptr<SqlPredicate> _predicate;
};

}  // namespace irs::analysis
