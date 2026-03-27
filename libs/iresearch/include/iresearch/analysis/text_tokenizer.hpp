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
/// @author Vasiliy Nabatchikov
/// @author Andrei Lobov
/// @author Yuriy Popov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/container/flat_hash_set.h>
#include <unicode/locid.h>

#include "analyzers.hpp"
#include "basics/shared.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "token_attributes.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

/// @note expects UTF-8 encoded input
class TextTokenizer final : public TypedAnalyzer<TextTokenizer>,
                            private util::Noncopyable {
 public:
  using stopwords_t = absl::flat_hash_set<std::string>;

  struct OptionsT {
    // lowercase tokens, match original implementation
    Case case_convert{Case::Lower};
    stopwords_t explicit_stopwords;
    icu::Locale locale;
    std::string stopwords_path{
      0};  // string with zero char indicates 'no value set'
    size_t min_gram{};
    size_t max_gram{};

    // needed for mark empty explicit_stopwords as valid and prevent loading
    // from defaults
    bool explicit_stopwords_set{};
    bool
      accent{};  // remove accents from letters, match original implementation
    bool stemming{
      true};  // try to stem if possible, match original implementation
    // needed for mark empty min_gram as valid and prevent loading from defaults
    bool min_gram_set{};
    // needed for mark empty max_gram as valid and prevent loading from defaults
    bool max_gram_set{};
    bool preserve_original{};  // emit input data as a token
    // needed for mark empty preserve_original as valid and prevent loading from
    // defaults
    bool preserve_original_set{};

    OptionsT() : locale{"C"} { locale.setToBogus(); }
  };

  struct StateT;

  static const char* gStopwordPathEnvVariable;

  static constexpr std::string_view type_name() noexcept { return "text"; }
  static void init();  // for triggering registration in a static build
  static ptr make(std::string_view locale);
  static void clear_cache();

  TextTokenizer(const OptionsT& options, const stopwords_t& stopwords);
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }
  bool next() final;
  bool reset(std::string_view data) final;

 private:
  using attributes = std::tuple<IncAttr, OffsAttr, TermAttr>;

  struct StateDeleterT {
    void operator()(StateT*) const noexcept;
  };

  bool next_word();
  bool next_ngram();

  bstring _term_buf;  // buffer for value if value cannot be referenced directly
  attributes _attrs;
  std::unique_ptr<StateT, StateDeleterT> _state;
};

}  // namespace irs::analysis
