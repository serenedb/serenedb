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
#include <string_view>

#include "analyzer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "token_attributes.hpp"

namespace irs::analysis {

class SplitByNonAlphaTokenizer final
  : public TypedAnalyzer<SplitByNonAlphaTokenizer>,
    private util::Noncopyable {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "splitByNonAlpha";
  }

  struct Options {
    using Owner = SplitByNonAlphaTokenizer;
    bool to_lower = false;
  };
  static ptr Make(Options opts);

  explicit SplitByNonAlphaTokenizer(bool to_lower) noexcept
    : _to_lower{to_lower} {}

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;

  std::string_view _data;
  size_t _pos = 0;
  bool _to_lower;
  std::string _term_buf;
  attributes _attrs;
};

}  // namespace irs::analysis
