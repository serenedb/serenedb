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

#include "analyzer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "token_attributes.hpp"

namespace irs::analysis {

// an analyzer capable of breaking up delimited text into tokens as per
// RFC4180 (without starting new records on newlines)
class DelimitedTokenizer final : public TypedAnalyzer<DelimitedTokenizer>,
                                 private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept { return "delimiter"; }

  struct Options {
    using Owner = DelimitedTokenizer;
    std::string delimiter;
  };
  static ptr Make(Options opts);

  explicit DelimitedTokenizer(std::string_view delimiter);
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  // token value with evaluated quotes
  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;

  bytes_view _data;
  bytes_view _delim;
  bstring _delim_buf;
  bstring _term_buf;  // buffer for the last evaluated term
  attributes _attrs;
};

}  // namespace irs::analysis
