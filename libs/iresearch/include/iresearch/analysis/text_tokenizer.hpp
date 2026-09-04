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

#include <unicode/brkiter.h>
#include <unicode/locid.h>
#include <unicode/normalizer2.h>
#include <unicode/translit.h>
#include <unicode/unistr.h>

#include <string>
#include <vector>

#include "basics/shared.hpp"
#include "iresearch/analysis/stopword_set.hpp"
#include "iresearch/analysis/text/dict/stem_cache.hpp"
#include "iresearch/utils/icu_locale_serde.hpp"
#include "iresearch/utils/snowball_stemmer.hpp"
#include "tokenizer.hpp"

namespace irs::analysis {

class TextTokenizer final : public TypedTokenizer<TextTokenizer>,
                            private util::Noncopyable {
 public:
  struct Options {
    using Owner = TextTokenizer;
    icu::Locale locale = irs::MakeBogusLocale();
    Case case_convert{Case::Lower};
    std::vector<std::string> explicit_stopwords;
    std::string stopwords_path = std::string(1, '\0');
    size_t min_gram{};
    size_t max_gram{};
    bool explicit_stopwords_set{};
    bool accent{};
    bool stemming{true};
    bool min_gram_set{};
    bool max_gram_set{};
    bool preserve_original{};
    bool preserve_original_set{};
  };
  static ptr Make(Options opts, duckdb::SharedObjectCache& cache);

  static const char* gStopwordPathEnvVariable;

  static constexpr std::string_view type_name() noexcept { return "text"; }

  TextTokenizer(Options options,
                duckdb::shared_ptr<const StopwordSet> stopwords);

  TokenTraits Traits() const noexcept final {
    return {
      .explicit_pos = _search_ngram,
      .offsets = true,
    };
  }

  BlockTraits WantedBlockTraits() const noexcept final {
    return {.ascii = _ascii_fast};
  }

  auto PrepareBatch(BlockTraits traits) const {
    return std::tuple{_case_convert, _search_ngram,
                      traits.ascii && _ascii_fast};
  }

  size_t MemoryUsage() const noexcept final;

  template<TokenLayout Layout, Case C, bool SearchNGram, bool KnownAscii>
  bool DoFill(duckdb::string_t raw, TokenSink& sink);

 private:
  struct Word {
    std::string_view term;
    uint32_t start{};
    uint32_t end{};
  };

  bool InitIcu(bool accent, bool stemming);

  std::string_view Stem(std::string_view word) {
    SDB_ASSERT(_stemmer);
    const auto stemmed = _stem_cache.Stem(_stemmer.get(), MakeTermView(word));
    return stemmed ? *stemmed : word;
  }

  template<Case C>
  bool NextWord(const icu::UnicodeString& data, Word& word);

  template<Case C>
  IRS_FORCE_INLINE bool AsciiTerm(const char* src, uint32_t size,
                                  const char* shadow_base, uint32_t begin,
                                  std::string_view& term);

  template<TokenLayout Layout, Case C, bool SearchNGram>
  void FillValue(TokenSink& sink, const duckdb::string_t& raw,
                 const icu::UnicodeString& data);
  template<TokenLayout Layout, Case C, bool SearchNGram>
  void AsciiFillValue(TokenSink& sink, duckdb::string_t raw);
  template<TokenLayout Layout>
  void EmitWordNGrams(TokenSink& sink, const duckdb::string_t& value,
                      uint32_t& pos, std::string_view term,
                      uint32_t offs_start);

  icu::Locale _locale;
  duckdb::shared_ptr<const StopwordSet> _stopwords;
  std::unique_ptr<icu::Transliterator> _transliterator;
  std::unique_ptr<icu::BreakIterator> _break_iterator;
  const icu::Normalizer2* _normalizer{};
  stemmer_ptr _stemmer;
  dict::StemCache _stem_cache;
  icu::UnicodeString _token;
  std::string _term_buf;
  std::string _shadow_buf;
  size_t _min_gram;
  size_t _max_gram;
  Case _case_convert;
  bool _search_ngram;
  bool _preserve_original;
  bool _ascii_fast;
};

extern template class TypedTokenizer<TextTokenizer>;

}  // namespace irs::analysis
