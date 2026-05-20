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
#include "iresearch/utils/snowball_stemmer.hpp"
#include "token_attributes.hpp"

namespace irs {
namespace analysis {

// an tokenizer capable of stemming the text, treated as a single token,
// for supported languages
// expects UTF-8 encoded input
class StemmingTokenizer final : public TypedAnalyzer<StemmingTokenizer>,
                                private util::Noncopyable {
 public:
  struct OptionsT {
    icu::Locale locale;

    OptionsT() : locale{"C"} { locale.setToBogus(); }
  };

  static constexpr std::string_view type_name() noexcept { return "stem"; }
  static void init();  // for trigering registration in a static build

  explicit StemmingTokenizer(const OptionsT& options);
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  // token value with evaluated quotes
  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;

  attributes _attrs;
  OptionsT _options;
  stemmer_ptr _stemmer;
  bool _stemmer_initialized = false;
  bool _term_eof = true;
};

}  // namespace analysis
}  // namespace irs
