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

#include <array>
#include <string>
#include <tuple>
#include <vector>

#include "re2/re2.h"
#include "tokenizer.hpp"

namespace irs::analysis {

class PatternTokenizer final : public TypedTokenizer<PatternTokenizer>,
                               private util::Noncopyable {
 public:
  struct Options {
    using Owner = PatternTokenizer;

    // RE2 regular expression used for matching or splitting
    // Must be a valid regex
    std::string pattern;

    // Capture group to extract:
    // -1 means "split mode" (emit text between matches),
    //  0 means "whole match",
    //  N>0 means "N-th capturing group"
    int group = -1;
  };

  static constexpr std::string_view type_name() noexcept { return "pattern"; }
  static ptr Make(Options opts);

  explicit PatternTokenizer(std::string_view pattern, int group = -1);
  ~PatternTokenizer() override;

  // Which fill path a value takes -- fixed at construction
  // (DetectFastSplit), so it resolves once per chunk instead of branching
  // per value.
  enum class Mode : uint8_t {
    ByteSet,  // single-byte delimiter set: block-classified scan
    Literal,  // multi-byte fixed delimiter: memchr + memcmp
    Regex,    // general RE2 match/split
  };

  auto PrepareBatch() const { return std::tuple{_mode}; }

  TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  template<TokenLayout Layout, Mode M>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  template<TokenLayout Layout>
  void FillValue(TokenSink& sink, duckdb::string_t value);
  template<TokenLayout Layout>
  void FastSplitValue(TokenSink& sink, duckdb::string_t value);
  template<TokenLayout Layout>
  void FastLiteralSplitValue(TokenSink& sink, duckdb::string_t value);
  void DetectFastSplit();

  bool IsDelimByte(unsigned char c) const noexcept {
    return (_delim_bitmap[c >> 6] >> (c & 63)) & 1;
  }

  re2::RE2 _pattern;  // compiled regex pattern
  int _group;         // which group to extract (-1 for split)

  int _num_groups;  // number of capturing groups in the pattern

  std::vector<re2::StringPiece> _matches;  // buffer for regex matches

  std::array<uint64_t, 4> _delim_bitmap{};
  // byte-set members when the set is small enough for block classification
  std::array<byte_type, 8> _block_delims{};
  uint8_t _nblock = 0;
  std::string _split_literal;  // multi-byte fixed delimiter (split mode)
  Mode _mode = Mode::Regex;
};

extern template class TypedTokenizer<PatternTokenizer>;

}  // namespace irs::analysis
