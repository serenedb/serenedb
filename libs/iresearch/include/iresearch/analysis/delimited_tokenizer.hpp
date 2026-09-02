////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string>

#include "tokenizer.hpp"

namespace irs::analysis {

// an analyzer capable of breaking up delimited text into tokens as per
// RFC4180 (without starting new records on newlines)
class DelimitedTokenizer final : public TypedTokenizer<DelimitedTokenizer>,
                                 private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept { return "delimiter"; }

  // an empty delimiter splits per symbol with quote handling
  enum class Mode : uint8_t {
    Chars,
    Single,
    Multi,
  };

  struct Options {
    using Owner = DelimitedTokenizer;
    std::string delimiter;
  };
  static ptr Make(Options opts);

  explicit DelimitedTokenizer(std::string_view delimiter);
  TokenTraits Traits() const noexcept final { return {.offsets = true}; }

  auto PrepareBatch(BlockTraits) const { return std::tuple{_mode}; }

  template<TokenLayout Layout, Mode M>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  template<TokenLayout Layout>
  void FastFillValue(TokenSink& sink, const duckdb::string_t& value);
  template<TokenLayout Layout>
  void CharsFillValue(TokenSink& sink, const duckdb::string_t& value);
  template<TokenLayout Layout>
  void QuotedFillValue(TokenSink& sink, const duckdb::string_t& value,
                       size_t from);

  bstring _delim;
  Mode _mode;
};

extern template class TypedTokenizer<DelimitedTokenizer>;

}  // namespace irs::analysis
