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

  struct StateT;

  static const char* gStopwordPathEnvVariable;

  static constexpr std::string_view type_name() noexcept { return "text"; }

  TextTokenizer(Options options, stopwords_t stopwords);

  TokenTraits Traits() const noexcept final { return {.dense_pos = false}; }

  template<TokenLayout Layout>
  bool DoFill(std::string_view value, TokenEmitter& sink);

  void ForceUnicodePath(bool force) noexcept { _force_unicode = force; }

 private:
  struct StateDeleterT {
    void operator()(StateT*) const noexcept;
  };

  bool next_word();

  template<TokenLayout Layout>
  void FillValue(TokenEmitter& sink);
  template<TokenLayout Layout>
  void AsciiFillValue(TokenEmitter& sink, std::string_view value);
  template<TokenLayout Layout>
  void EmitWordNGrams(TokenEmitter& sink, uint32_t& pos);

  std::unique_ptr<StateT, StateDeleterT> _state;
  bool _ascii_case_safe = false;
  bool _force_unicode = false;
};

extern template class TypedTokenizer<TextTokenizer>;

}  // namespace irs::analysis
