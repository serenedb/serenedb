////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "iresearch/analysis/text/delim/finders.hpp"
#include "re2/re2.h"
#include "tokenizer.hpp"

namespace irs::analysis {

class PatternTokenizer final : public TypedTokenizer<PatternTokenizer>,
                               private util::Noncopyable {
 public:
  struct Options {
    using Owner = PatternTokenizer;
    std::string pattern;
    // -1 splits on matches, 0 emits whole matches, N>0 emits the N-th group.
    int group = -1;
  };

  static constexpr std::string_view type_name() noexcept { return "pattern"; }
  static ptr Make(Options opts);

  explicit PatternTokenizer(std::string_view pattern, int group = -1);

  // Which fill path a value takes -- fixed at construction
  // (DetectFastSplit), so it resolves once per chunk instead of branching
  // per value.
  enum class Mode : uint8_t {
    OneChar,
    ManyChars,
    Literal,
    LongLiteral,
    Regex,
  };

  auto PrepareBatch(BlockTraits) const { return std::tuple{_mode}; }

  TokenTraits Traits() const noexcept final {
    return {.offsets = true, .stable = true};
  }

  template<TokenLayout Layout, Mode M>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  template<TokenLayout Layout>
  void FillValue(TokenSink& sink, duckdb::string_t value);
  void DetectFastSplit(int num_groups);
  void SetLiteral(bstring&& literal);

  re2::RE2 _pattern;
  int _group;
  std::vector<re2::StringPiece> _matches;
  delim::ManyCharsFinder _chars;
  std::optional<delim::OneStringFinder> _literal;
  std::optional<delim::OneLongStringFinder> _long_literal;
  Mode _mode = Mode::Regex;
};

extern template class TypedTokenizer<PatternTokenizer>;

}  // namespace irs::analysis
