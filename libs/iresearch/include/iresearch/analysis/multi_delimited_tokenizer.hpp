////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "analyzer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "token_attributes.hpp"

namespace irs {
namespace analysis {

////////////////////////////////////////////////////////////////////////////////
/// @brief an analyzer capable of breaking up delimited text into tokens
///        separated by a set of strings.
////////////////////////////////////////////////////////////////////////////////
class MultiDelimitedTokenizer : public TypedAnalyzer<MultiDelimitedTokenizer>,
                                private util::Noncopyable {
 public:
  struct Options {
    using Owner = MultiDelimitedTokenizer;
    std::vector<bstring> delimiters;
  };

  static constexpr std::string_view type_name() noexcept {
    return "multi_delimiter";
  }

  static Analyzer::ptr Make(Options opts);

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  bool reset(std::string_view input) final {
    _data = ViewCast<byte_type>(input);
    _start = _data.data();
    return true;
  }

 protected:
  using Attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;
  const byte_type* _start{};
  bytes_view _data{};
  Attributes _attrs;
};

}  // namespace analysis
}  // namespace irs
