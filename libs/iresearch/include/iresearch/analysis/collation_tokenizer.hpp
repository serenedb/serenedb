////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <unicode/locid.h>

#include "analyzer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/icu_locale_serde.hpp"
#include "token_attributes.hpp"

namespace irs::analysis {

// an tokenizer capable of converting UTF-8 encoded input into a sortable
// token as per specified locale
// expects UTF-8 encoded input
class CollationTokenizer final : public TypedAnalyzer<CollationTokenizer>,
                                 private util::Noncopyable {
 public:
  struct Options {
    using Owner = CollationTokenizer;
    icu::Locale locale = irs::MakeBogusLocale();
    bool force_utf8 = true;
  };
  static ptr Make(Options opts);

  static constexpr std::string_view type_name() noexcept { return "collation"; }

  explicit CollationTokenizer(Options options);

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() noexcept final {
    const auto eof = !_term_eof;
    _term_eof = true;
    return eof;
  }
  bool reset(std::string_view data) final;

 private:
  struct StateT;
  struct StateDeleterT {
    void operator()(StateT*) const noexcept;
  };

  // token value with evaluated quotes
  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;

  attributes _attrs;
  std::unique_ptr<StateT, StateDeleterT> _state;
  bool _term_eof;
};

}  // namespace irs::analysis
