////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include <unicode/locid.h>

#include "analyzers.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "token_attributes.hpp"

namespace irs {
namespace analysis {

////////////////////////////////////////////////////////////////////////////////
/// @class normalizing_token_stream
/// @brief an tokenizer capable of normalizing the text, treated as a single
///        token, i.e. case conversion and accent removal
/// @note expects UTF-8 encoded input
////////////////////////////////////////////////////////////////////////////////
class NormalizingTokenizer final : public TypedAnalyzer<NormalizingTokenizer>,
                                   private util::Noncopyable {
 public:
  struct OptionsT {
    icu::Locale locale;
    CaseConvertT case_convert{CaseConvertT::kNone};  // no extra normalization
    bool accent{true};                               // no extra normalization

    OptionsT() : locale{"C"} { locale.setToBogus(); }
  };

  static constexpr std::string_view type_name() noexcept { return "norm"; }
  static void init();  // for trigering registration in a static build

  explicit NormalizingTokenizer(const OptionsT& options);
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  // token value with evaluated quotes
  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;

  struct StateT;
  struct StateDeleterT {
    void operator()(StateT*) const noexcept;
  };

  attributes _attrs;
  std::unique_ptr<StateT, StateDeleterT> _state;
  bool _term_eof;
};

}  // namespace analysis
}  // namespace irs
