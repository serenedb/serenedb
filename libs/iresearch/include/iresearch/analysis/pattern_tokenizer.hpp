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

#include <string>
#include <string_view>
#include <variant>

#include "iresearch/analysis/text/delim/finders.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class PatternTokenizer : private util::Noncopyable {
 public:
  struct Options {
    using Owner = PatternTokenizer;
    std::string pattern;
    // -1 splits on matches, 0 emits whole matches, N>0 emits the N-th group.
    int group = -1;
  };

  using Split =
    std::variant<std::monostate, delim::OneCharFinder, delim::ManyCharsFinder,
                 delim::ByteRangesFinder, delim::OneStringFinder,
                 delim::OneLongStringFinder>;

  static constexpr std::string_view type_name() noexcept { return "pattern"; }
  static Tokenizer::ptr Make(Options opts);
  static Split Detect(std::string_view pattern, int group = -1);
};

}  // namespace irs::analysis
