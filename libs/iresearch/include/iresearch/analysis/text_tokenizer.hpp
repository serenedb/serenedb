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

#include "basics/shared.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/icu_locale_serde.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

/// @note expects UTF-8 encoded input
class TextTokenizer final : public TypedTokenizer<TextTokenizer>,
                            private util::Noncopyable {
 public:
  using stopwords_t = absl::flat_hash_set<std::string>;

  struct Options {
    using Owner = TextTokenizer;
    icu::Locale locale = irs::MakeBogusLocale();
    // lowercase tokens, match original implementation
    Case case_convert{Case::Lower};
    stopwords_t explicit_stopwords;
    // single zero char indicates 'no value set' -- empty string means a custom
    // (empty) path was explicitly requested.
    std::string stopwords_path = std::string(1, '\0');
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
  };
  static ptr Make(Options opts);

  struct State;

  static const char* gStopwordPathEnvVariable;

  static constexpr std::string_view type_name() noexcept { return "text"; }

  TextTokenizer(Options options, stopwords_t stopwords);

  TokenTraits Traits() const noexcept final {
    return {
      .explicit_pos = true,
      .offsets = true,
    };
  }

  auto PrepareBatch() const { return std::tuple{_case_convert, _search_ngram}; }

  template<TokenLayout Layout, Case C, bool SearchNGram>
  bool DoFill(duckdb::string_t value, TokenSink& sink);

 private:
  struct StateDeleter {
    void operator()(State*) const noexcept;
  };

  struct Word {
    std::string_view term;
    uint32_t start{};
    uint32_t end{};
  };

  template<Case C>
  bool next_word(const icu::UnicodeString& data, Word& word);

  template<TokenLayout Layout, Case C, bool SearchNGram>
  void FillValue(const icu::UnicodeString& data, TokenSink& sink);
  template<TokenLayout Layout, Case C, bool SearchNGram>
  void AsciiFillValue(TokenSink& sink, duckdb::string_t value);
  template<TokenLayout Layout, bool StableTerm>
  void EmitWordNGrams(TokenSink& sink, uint32_t& pos, std::string_view term,
                      uint32_t offs_start);

  std::unique_ptr<State, StateDeleter> _state;
  Case _case_convert = Case::Lower;
  bool _search_ngram = false;
  bool _ascii_fast = false;
};

extern template class TypedTokenizer<TextTokenizer>;

}  // namespace irs::analysis
