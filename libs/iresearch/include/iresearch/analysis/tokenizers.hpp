////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/numeric_utils.hpp"
#include "tokenizer.hpp"

namespace irs {

// Term encodings for typed columns: ingest gathers these directly into
// keyword/constant blocks, no tokenizer involved.
constexpr std::string_view kTrueTerm{"\xFF", 1};
constexpr std::string_view kFalseTerm{"\x00", 1};

constexpr std::string_view BooleanTerm(bool value) noexcept {
  return value ? kTrueTerm : kFalseTerm;
}

// data pointer != nullptr or IRS_ASSERT failure in bytes_hash::insert(...)
constexpr std::string_view kNullTerm{"\x00", 0};

// Basic implementation of token_stream for simple string field.
// it does not tokenize or analyze field, just set attributes based
// on initial string length
class StringTokenizer : public analysis::TypedTokenizer<StringTokenizer>,
                        private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept { return "keyword"; }

  struct Options {
    using Owner = StringTokenizer;
  };
  static ptr Make(Options /*opts*/) {
    return std::make_unique<StringTokenizer>();
  }

  TokenTraits Traits() const noexcept final {
    return {.terms = TokenTraits::Terms::Keyword};
  }

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);
};

extern template class analysis::TypedTokenizer<StringTokenizer>;

}  // namespace irs
