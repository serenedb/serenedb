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

#include "iresearch/utils/attribute_helper.hpp"
#include "re2/re2.h"
#include "tokenizer.hpp"

namespace re2 {

class RE2;
}

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
  ~PatternTokenizer();

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

  void ForceRegexPath(bool force) noexcept { _force_regex = force; }
  bool FastSplitEligible() const noexcept {
    return _fast_split || !_split_literal.empty();
  }

 private:
  template<TokenLayout Layout>
  void FillValue(TokenEmitter& sink, std::string_view value);
  template<TokenLayout Layout>
  void FastSplitValue(TokenEmitter& sink, std::string_view value);
  template<TokenLayout Layout>
  void FastLiteralSplitValue(TokenEmitter& sink, std::string_view value);
  void DetectFastSplit();

  bool IsDelimByte(unsigned char c) const noexcept {
    return (_delim_bitmap[c >> 6] >> (c & 63)) & 1;
  }

  re2::RE2 _pattern;  // compiled regex pattern
  int _group;         // which group to extract (-1 for split)

  int _num_groups;  // number of capturing groups in the pattern

  std::vector<re2::StringPiece> _matches;  // buffer for regex matches

  std::array<uint64_t, 4> _delim_bitmap{};
  std::string _split_literal;  // multi-byte fixed delimiter (split mode)
  bool _fast_split = false;
  bool _force_regex = false;
};

extern template class TypedTokenizer<PatternTokenizer>;

}  // namespace irs::analysis
