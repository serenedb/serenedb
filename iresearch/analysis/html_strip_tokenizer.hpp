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

#include <string>

#include "iresearch/utils/noncopyable.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class HtmlStripTokenizer final : public TypedTokenizer<HtmlStripTokenizer>,
                                 private util::Noncopyable {
 public:
  struct Options {
    using Owner = HtmlStripTokenizer;
    bool join_inline_tags{false};
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept {
    return "strip_html";
  }

  explicit HtmlStripTokenizer(Options opts) noexcept : _options{opts} {}

  TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  size_t MemoryUsage() const noexcept final { return _word.capacity(); }

  template<TokenLayout Layout>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  Options _options;
  std::string _word;
};

}  // namespace irs::analysis
