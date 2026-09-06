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

#include "basics/noncopyable.hpp"
#include "iresearch/analysis/tokenizer.hpp"

namespace irs {

class KeywordTokenizer : public analysis::TypedTokenizer<KeywordTokenizer>,
                         private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept { return "keyword"; }

  struct Options {
    using Owner = KeywordTokenizer;
  };
  static ptr Make(Options) { return std::make_unique<KeywordTokenizer>(); }

  TokenTraits Traits() const noexcept final {
    return {
      .unique = true,
      .keyword = true,
      .offsets = true,
      .stable = true,
    };
  }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t raw, TokenSink& sink) {
    sink.Emit<Layout>(raw);
    return true;
  }
};

extern template class analysis::TypedTokenizer<KeywordTokenizer>;

}  // namespace irs
