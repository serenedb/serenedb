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

#include "analyzers.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "re2/re2.h"
#include "token_attributes.hpp"

namespace re2 {
class RE2;
}

namespace irs::analysis {

class PatternTokenizer final : public TypedAnalyzer<PatternTokenizer>,
                               private util::Noncopyable {
 public:
  struct Options {
    // RE2 regular expression used for matching or splitting
    // Must be a valid regex
    std::string pattern = "";

    // Capture group to extract:
    // -1 means "split mode" (emit text between matches),
    //  0 means "whole match",
    //  N>0 means "N-th capturing group"
    int group = -1;
  };

  static constexpr std::string_view type_name() noexcept { return "pattern"; }
  static void init();  // for triggering registration in a static build
  static ptr make(std::string_view pattern, int group = -1);

  explicit PatternTokenizer(std::string_view pattern, int group = -1);
  ~PatternTokenizer();

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  bool next() final;
  bool reset(std::string_view data) final;

 private:
  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;

  std::string_view _data;  // buffer to store the entire input string
  re2::RE2 _pattern;       // compiled regex pattern
  int _group;              // which group to extract (-1 for split)

  // State for pattern matching
  size_t _current_pos;  // current position in _data
  bool _exhausted;      // whether we've exhausted the input
  int _num_groups;      // number of capturing groups in the pattern

  std::vector<re2::StringPiece> _matches;  // buffer for regex matches

  attributes _attrs;
};

}  // namespace irs::analysis
