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

#include "text_tokenizer.hpp"

#include <simdutf.h>

#include <algorithm>
#include <string_view>

#include "basics/log.h"
#include "basics/string_utils.h"
#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/dict/stopwords_loader.hpp"
#include "iresearch/analysis/text/normalize/icu.hpp"
#include "iresearch/analysis/text/words/ascii.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/snowball_stemmer.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

constexpr size_t kBulkCaseConvertLimit = 16 * 1024;

namespace {

duckdb::unique_ptr<StopwordSet> BuildStopwords(
  const TextTokenizer::Options& options) {
  auto stopwords = duckdb::make_uniq<StopwordSet>(options.explicit_stopwords);
  if (!dict::ResolveStopwords(*stopwords, options.locale.getLanguage(),
                              options.stopwords_path,
                              options.explicit_stopwords_set)) {
    return nullptr;
  }
  stopwords->ShrinkToFit();
  return stopwords;
}

uint32_t Utf8Length(const icu::UnicodeString& data, int32_t begin,
                    int32_t end) noexcept {
  return static_cast<uint32_t>(simdutf::utf8_length_from_utf16(
    reinterpret_cast<const char16_t*>(data.getBuffer()) + begin,
    static_cast<size_t>(end - begin)));
}

}  // namespace

size_t TextTokenizer::MemoryUsage() const noexcept {
  return _term_buf.capacity() + _shadow_buf.capacity() +
         static_cast<size_t>(_token.getCapacity()) * sizeof(char16_t) +
         _stem_cache.MemoryBytes();
}

const char* TextTokenizer::gStopwordPathEnvVariable =
  dict::kStopwordPathEnvVariable;

bool TextTokenizer::InitIcu(bool accent, bool stemming) {
  auto err = UErrorCode::U_ZERO_ERROR;

  _normalizer = icu::Normalizer2::getNFCInstance(err);
  if (!U_SUCCESS(err) || !_normalizer) {
    _normalizer = nullptr;
    SDB_WARN(IRESEARCH,
             "Warning while instantiation icu::Normalizer2 for text from "
             "locale: ",
             _locale.getName(), ", ", u_errorName(err));
    return false;
  }

  if (!accent) {
    _transliterator = normalize::MakeStripTransliterator(false, err);
    if (!U_SUCCESS(err) || !_transliterator) {
      _transliterator.reset();
      SDB_WARN(IRESEARCH,
               "Warning while instantiation icu::Transliterator for text from "
               "locale: ",
               _locale.getName(), ", ", u_errorName(err));
      return false;
    }
  }

  _break_iterator.reset(icu::BreakIterator::createWordInstance(_locale, err));
  if (!U_SUCCESS(err) || !_break_iterator) {
    _break_iterator.reset();
    SDB_WARN(IRESEARCH,
             "Warning while instantiation icu::BreakIterator for text from "
             "locale: ",
             _locale.getName(), ", ", u_errorName(err));
    return false;
  }

  if (stemming) {
    _stemmer = make_stemmer_ptr(_locale.getLanguage(), nullptr);
    if (!_stemmer) {
      SDB_WARN(IRESEARCH, "Failed to create stemmer for text from locale: ",
               _locale.getName());
    }
  }

  return true;
}

TextTokenizer::TextTokenizer(Options options,
                             duckdb::shared_ptr<const StopwordSet> stopwords)
  : _locale{std::move(options.locale)},
    _stopwords{std::move(stopwords)},
    _min_gram{options.min_gram},
    _max_gram{options.max_gram_set ? options.max_gram
                                   : std::numeric_limits<size_t>::max()},
    _case_convert{options.case_convert},
    _search_ngram{options.min_gram_set || options.max_gram_set ||
                  options.preserve_original_set},
    _preserve_original{options.preserve_original},
    _ascii_fast{classify::AsciiCaseSafe(_locale.getName())} {
  if (!InitIcu(options.accent, options.stemming)) {
    THROW_SQL_ERROR(
      ERR_MSG("text: failed to initialize the analyzer for the locale"));
  }
}

Tokenizer::ptr TextTokenizer::Make(Options opts,
                                   duckdb::SharedObjectCache& cache) {
  if (opts.locale.isBogus()) {
    THROW_SQL_ERROR(ERR_MSG("text: invalid locale"));
  }
  if (opts.min_gram_set && opts.max_gram_set && opts.min_gram > opts.max_gram) {
    THROW_SQL_ERROR(ERR_MSG("text: min_gram must not exceed max_gram"));
  }
  auto stopwords = BuildStopwords(opts);
  if (!stopwords) {
    THROW_SQL_ERROR(
      ERR_MSG("text: failed to load stopwords from the configured path"));
  }
  return std::make_unique<TextTokenizer>(
    std::move(opts), StopwordSet::GetOrBuild(cache, std::move(stopwords)));
}

template<TokenLayout Layout, Case C, bool SearchNGram, bool KnownAscii>
bool TextTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  const auto size = raw.GetSize();
  if (size > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return false;
  }

  if constexpr (KnownAscii) {
    AsciiFillValue<Layout, C, SearchNGram>(sink, raw);
    return true;
  } else {
    const auto udata = icu::UnicodeString::fromUTF8(
      icu::StringPiece{raw.GetData(), static_cast<int32_t>(size)});

    _break_iterator->setText(udata);

    FillValue<Layout, C, SearchNGram>(sink, raw, udata);
    return true;
  }
}

template<Case C>
bool TextTokenizer::NextWord(const icu::UnicodeString& data, Word& word) {
  const auto call_start = _break_iterator->current();
  for (auto start = call_start, end = _break_iterator->next();
       icu::BreakIterator::DONE != end;
       start = end, end = _break_iterator->next()) {
    if (UWordBreak::UBRK_WORD_NONE == _break_iterator->getRuleStatus()) {
      continue;
    }

    normalize::NormalizeCaseStrip<C>(
      *_normalizer, _locale, _transliterator.get(),
      data.tempSubString(start, end - start), _token);

    _term_buf.clear();
    _token.toUTF8String(_term_buf);

    if (_stopwords->Contains(std::string_view{_term_buf})) {
      continue;
    }

    word.term = _stemmer ? Stem(_term_buf) : std::string_view{_term_buf};
    word.start = word.end + Utf8Length(data, call_start, start);
    word.end = word.start + Utf8Length(data, start, end);
    return true;
  }

  return false;
}

template<TokenLayout Layout>
void TextTokenizer::EmitWordNGrams(TokenSink& sink,
                                   const duckdb::string_t& value, uint32_t& pos,
                                   std::string_view term, uint32_t offs_start) {
  const auto* begin = reinterpret_cast<const byte_type*>(term.data());
  const auto* end = begin + term.size();
  SDB_ASSERT(begin != end);
  const size_t min_gram = _min_gram;
  const size_t max_gram = _max_gram;
  const bool preserve_original = _preserve_original;
  const uint32_t value_size = value.GetSize();

  const byte_type* it = begin;
  uint32_t length = 0;
  do {
    it = utf8_utils::Next(it, end);
  } while (++length < min_gram && it != end);

  bool first = true;
  for (;;) {
    bool word_done = it == end;
    if (length > max_gram) {
      word_done = true;
      if (!preserve_original) {
        return;
      }
      it = end;
    }
    if (length >= min_gram || preserve_original) {
      const auto size = static_cast<uint32_t>(std::distance(begin, it));
      if (first) {
        ++pos;
        first = false;
      }
      sink.Emit<Layout>(
        value, term.data(), size, pos,
        Offs{offs_start, std::min(offs_start + size, value_size)});
    }
    if (word_done) {
      return;
    }
    it = utf8_utils::Next(it, end);
    ++length;
  }
}

template<Case C>
IRS_FORCE_INLINE bool TextTokenizer::AsciiTerm(const char* src, uint32_t size,
                                               const char* shadow_base,
                                               uint32_t begin,
                                               std::string_view& term) {
  std::string_view word;
  if constexpr (C == Case::None) {
    word = {src, size};
  } else if (shadow_base != nullptr) [[likely]] {
    word = {shadow_base + begin, size};
  } else {
    sdb::basics::StrResizeAmortized(_term_buf, size);
    casing::CaseConvertAscii<C == Case::Lower>(_term_buf.data(), src, size);
    word = {_term_buf.data(), size};
  }
  if (_stopwords->Contains(word)) {
    return false;
  }
  term = _stemmer ? Stem(word) : word;
  return true;
}

template<TokenLayout Layout, Case C, bool SearchNGram>
void TextTokenizer::AsciiFillValue(TokenSink& sink, duckdb::string_t raw) {
  const char* const data = raw.GetData();
  const uint32_t n = raw.GetSize();
  uint32_t pos = 0;
  const char* shadow = nullptr;
  if constexpr (C != Case::None && !SearchNGram) {
    if (n <= kBulkCaseConvertLimit) [[likely]] {
      sdb::basics::StrResizeAmortized(_shadow_buf, n);
      casing::CaseConvertAscii<C == Case::Lower>(_shadow_buf.data(), data, n);
      shadow = _shadow_buf.data();
    }
  }
  words::ScanAsciiRuns(raw, [&](const words::AsciiSegment& seg) {
    if (!seg.has_alpha && !seg.has_digit) {
      return;
    }
    const uint32_t size = seg.end - seg.begin;
    const char* src = data + seg.begin;

    if constexpr (SearchNGram) {
      std::string_view term;
      if (!AsciiTerm<C>(src, size, nullptr, seg.begin, term)) {
        return;
      }
      EmitWordNGrams<Layout>(sink, raw, pos, term, seg.begin);
    } else {
      std::string_view term;
      if (!AsciiTerm<C>(src, size, shadow, seg.begin, term)) {
        return;
      }
      sink.Emit<Layout>(raw, term.data(), static_cast<uint32_t>(term.size()),
                        ++pos, Offs{seg.begin, seg.end});
    }
  });
}

template<TokenLayout Layout, Case C, bool SearchNGram>
void TextTokenizer::FillValue(TokenSink& sink, const duckdb::string_t& raw,
                              const icu::UnicodeString& data) {
  const uint32_t size = raw.GetSize();
  uint32_t pos = 0;
  Word word;
  while (NextWord<C>(data, word)) {
    const Offs offs{std::min(word.start, size), std::min(word.end, size)};
    if constexpr (SearchNGram) {
      EmitWordNGrams<Layout>(sink, raw, pos, word.term, offs.start);
    } else {
      sink.Emit<Layout>(word.term.data(),
                        static_cast<uint32_t>(word.term.size()), ++pos, offs);
    }
  }
}

template class TypedTokenizer<TextTokenizer>;

}  // namespace irs::analysis
