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

#include <absl/strings/ascii.h>
#include <libstemmer.h>
#include <simdutf.h>
#include <unicode/brkiter.h>      // for icu::BreakIterator
#include <unicode/normalizer2.h>  // for icu::Normalizer2
#include <unicode/translit.h>     // for icu::Transliterator
#include <unicode/uclean.h>       // for u_cleanup

#include <cctype>  // for std::isspace(...)
#include <duckdb/common/vector/flat_vector.hpp>
#include <filesystem>
#include <fstream>
#include <string_view>

#include "absl/strings/str_cat.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/file_utils_ext.hpp"
#include "basics/log.h"
#include "basics/misc.hpp"
#include "basics/string_utils.h"
#include "iresearch/analysis/ascii_words.hpp"
#include "iresearch/analysis/classify.hpp"
#include "iresearch/analysis/stem_cache.hpp"
#include "iresearch/analysis/term_view.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/first_len_filter.hpp"
#include "iresearch/utils/snowball_stemmer.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

// Whole-value fold shadows go through the member scratch; cap what the
// scratch retains so adversarial values do not pin memory. Larger values
// fold per token.
constexpr size_t kBulkFoldLimit = 16 * 1024;
namespace {

struct IcuObjects {
  bool Valid() const noexcept {
    // 'break_iterator' indicates that 'icu_objects' struct initialized
    return nullptr != break_iterator;
  }

  void Clear() noexcept {
    transliterator.reset();
    break_iterator.reset();
    normalizer = nullptr;
    stemmer.reset();
  }

  std::unique_ptr<icu::Transliterator> transliterator;
  std::unique_ptr<icu::BreakIterator> break_iterator;
  const icu::Normalizer2* normalizer{};  // reusable object owned by ICU
  stemmer_ptr stemmer;
};

}  // namespace

struct TextTokenizer::State : IcuObjects {
  Options options;
  Prefiltered<stopwords_t> stopwords;
  StemCache stem_cache;
  icu::UnicodeString token;  // normalize target scratch
  std::string word_buf;      // utf8 word / ascii case-conversion scratch
  std::string fold_buf;      // whole-value ascii fold shadow (bulk path)

  State(Options opts, stopwords_t stopw)
    : options{std::move(opts)}, stopwords{std::move(stopw)} {}

  std::string_view StemCached(std::string_view word) {
    SDB_ASSERT(stemmer);
    if (const auto stemmed = stem_cache.Stem(stemmer.get(), word)) {
      return *stemmed;
    }
    return word;
  }
};

namespace {

// Retrieves a set of ignored words from FS at the specified custom path
bool GetStopwords(TextTokenizer::stopwords_t& buf, std::string_view language,
                  std::string_view path = {}) {
  std::filesystem::path stopword_path;

  const auto* custom_stopword_path =
    !IsNull(path) ? path.data()
                  : std::getenv(TextTokenizer::gStopwordPathEnvVariable);

  if (custom_stopword_path) {
    stopword_path.assign(custom_stopword_path);
    file_utils::EnsureAbsolute(stopword_path);
  } else {
    std::filesystem::path::string_type cwd;
    file_utils::ReadCwd(cwd);

    // use CWD if the environment variable STOPWORD_PATH_ENV_VARIABLE is
    // undefined
    stopword_path = std::move(cwd);
  }

  try {
    bool result = false;
    stopword_path /= std::string_view(language);

    if (!file_utils::ExistsDirectory(result, stopword_path.c_str()) ||
        !result) {
      if (custom_stopword_path) {
        SDB_ERROR(IRESEARCH,
                  absl::StrCat("Failed to load stopwords from path: ",
                               stopword_path.string()));
        return false;
      }
      SDB_TRACE(IRESEARCH,
                absl::StrCat("Failed to load stopwords from default path: ",
                             stopword_path.string(),
                             ". Tokenizer will continue without stopwords"));
      return true;
    }

    TextTokenizer::stopwords_t stopwords;
    auto visitor = [&stopwords, &stopword_path](auto name) -> bool {
      bool result = false;
      const auto path = stopword_path / name;

      if (!file_utils::ExistsFile(result, path.c_str())) {
        SDB_ERROR(IRESEARCH, absl::StrCat("Failed to identify stopword path: ",
                                          path.string()));

        return false;
      }

      if (!result) {
        return true;  // skip non-files
      }

      std::ifstream in(path.native());

      if (!in) {
        SDB_ERROR(
          IRESEARCH,
          absl::StrCat("Failed to load stopwords from path: ", path.string()));

        return false;
      }

      for (std::string line; std::getline(in, line);) {
        size_t i = 0;

        // find first whitespace
        for (size_t length = line.size(); i < length && !std::isspace(line[i]);
             ++i) {
        }

        // skip lines starting with whitespace
        if (i > 0) {
          stopwords.insert(line.substr(0, i));
        }
      }

      return true;
    };

    if (!file_utils::VisitDirectory(stopword_path.c_str(), visitor, false)) {
      return !custom_stopword_path;
    }

    buf.insert(stopwords.begin(), stopwords.end());

    return true;
  } catch (...) {
    SDB_ERROR(IRESEARCH,
              absl::StrCat("Caught error while loading stopwords from path: ",
                           stopword_path.string()));
  }

  return false;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief builds a set of stopwords for options
/// load rules:
/// 'explicit_stopwords' + 'stopwordsPath' = load from both
/// 'explicit_stopwords' only - load from 'explicit_stopwords'
/// 'stopwordsPath' only - load from 'stopwordsPath'
///  none (empty explicit_Stopwords  and flg explicit_stopwords_set not set) -
///  load from default location
////////////////////////////////////////////////////////////////////////////////
bool BuildStopwords(const TextTokenizer::Options& options,
                    TextTokenizer::stopwords_t& buf) {
  if (!options.explicit_stopwords.empty()) {
    // explicit stopwords always go
    buf.insert(options.explicit_stopwords.begin(),
               options.explicit_stopwords.end());
  }

  if (options.stopwords_path.empty() || options.stopwords_path[0] != 0) {
    // we have a custom path. let`s try loading
    // if we have stopwordsPath - do not  try default location. Nothing to do
    // there anymore
    return GetStopwords(buf, options.locale.getLanguage(),
                        options.stopwords_path);
  }
  if (!options.explicit_stopwords_set && options.explicit_stopwords.empty()) {
    //  no stopwordsPath, explicit_stopwords empty and not marked as valid -
    //  load from defaults
    return GetStopwords(buf, options.locale.getLanguage());
  }

  return true;
}

template<Case C>
bool ProcessTerm(TextTokenizer::State& state, icu::UnicodeString&& data,
                 std::string_view& term) {
  auto& token = state.token;
  std::string& word_utf8 = state.word_buf;
  // normalize unicode
  auto err =
    UErrorCode::U_ZERO_ERROR;  // a value that passes the U_SUCCESS() test

  state.normalizer->normalize(data, token, err);

  if (!U_SUCCESS(err)) {
    token =
      std::move(data);  // use non-normalized value if normalization failure
  }

  // case-convert unicode
  if constexpr (C == Case::Lower) {
    token.toLower(state.options.locale);  // inplace case-conversion
  } else if constexpr (C == Case::Upper) {
    token.toUpper(state.options.locale);  // inplace case-conversion
  }

  // collate value, e.g. remove accents
  if (state.transliterator) {
    state.transliterator->transliterate(token);
  }

  word_utf8.clear();
  token.toUTF8String(word_utf8);

  // skip ignored tokens
  if (state.stopwords.Contains(word_utf8)) {
    return false;
  }

  // find the token stem
  if (state.stemmer) {
    static_assert(sizeof(sb_symbol) == sizeof(char));
    const auto* value = reinterpret_cast<const sb_symbol*>(word_utf8.c_str());

    value = sb_stemmer_stem(state.stemmer.get(), value,
                            static_cast<int>(word_utf8.size()));

    if (value) {
      term = std::string_view(reinterpret_cast<const char*>(value),
                              sb_stemmer_length(state.stemmer.get()));

      return true;
    }
  }

  // use the value of the unstemmed token
  term = word_utf8;

  return true;
}

bool InitFromOptions(const TextTokenizer::Options& options, IcuObjects* objects,
                     bool print_errors) {
  auto err =
    UErrorCode::U_ZERO_ERROR;  // a value that passes the U_SUCCESS() test

  // reusable object owned by ICU
  objects->normalizer = icu::Normalizer2::getNFCInstance(err);

  if (!U_SUCCESS(err) || !objects->normalizer) {
    objects->normalizer = nullptr;

    if (print_errors) {
      SDB_WARN(IRESEARCH,
               "Warning while instantiation icu::Normalizer2 for "
               "text_token_stream from locale: ",
               options.locale.getName(), ", ", u_errorName(err));
    }

    return false;
  }

  if (!options.accent) {
    // transliteration rule taken verbatim from:
    // http://userguide.icu-project.org/transforms/general
    const icu::UnicodeString collation_rule(
      "NFD; [:Nonspacing Mark:] Remove; NFC");  // do not allocate statically
                                                // since it causes memory
                                                // leaks in ICU

    // reusable object owned by *this
    objects->transliterator.reset(icu::Transliterator::createInstance(
      collation_rule, UTransDirection::UTRANS_FORWARD, err));

    if (!U_SUCCESS(err) || !objects->transliterator) {
      objects->transliterator.reset();

      if (print_errors) {
        SDB_WARN(IRESEARCH,
                 "Warning while instantiation icu::Transliterator for "
                 "text_token_stream from locale: ",
                 options.locale.getName(), ", ", u_errorName(err));
      }

      return false;
    }
  }

  // reusable object owned by *this
  objects->break_iterator.reset(
    icu::BreakIterator::createWordInstance(options.locale, err));

  if (!U_SUCCESS(err) || !objects->break_iterator) {
    objects->break_iterator.reset();

    if (print_errors) {
      SDB_WARN(IRESEARCH,
               "Warning while instantiation icu::BreakIterator for "
               "text_token_stream from locale: ",
               options.locale.getName(), ", ", u_errorName(err));
    }

    return false;
  }

  // optional since not available for all locales
  if (options.stemming) {
    // reusable object owned by *this
    objects->stemmer = make_stemmer_ptr(options.locale.getLanguage(),
                                        nullptr);  // defaults to utf-8

    if (!objects->stemmer && print_errors) {
      SDB_WARN(IRESEARCH,
               "Failed to create stemmer for text_token_stream from locale: ",
               options.locale.getName());
    }
  }

  return true;
}

}  // namespace

void TextTokenizer::StateDeleter::operator()(State* p) const noexcept {
  delete p;
}

const char* TextTokenizer::gStopwordPathEnvVariable =
  "IRESEARCH_TEXT_STOPWORD_PATH";

TextTokenizer::TextTokenizer(Options options, stopwords_t stopwords)
  : _state{new State{std::move(options), std::move(stopwords)}} {
  const auto& opts = _state->options;
  _case_convert = opts.case_convert;
  _search_ngram =
    opts.min_gram_set || opts.max_gram_set || opts.preserve_original_set;
  _ascii_fast = AsciiCaseSafe(opts.locale.getName());
  if (!InitFromOptions(opts, _state.get(), true)) {
    THROW_SQL_ERROR(
      ERR_MSG("text: failed to initialize the analyzer for the locale"));
  }
}

Tokenizer::ptr TextTokenizer::Make(Options opts) {
  if (opts.locale.isBogus()) {
    THROW_SQL_ERROR(ERR_MSG("text: invalid locale"));
  }
  if (opts.min_gram_set && opts.max_gram_set && opts.min_gram > opts.max_gram) {
    THROW_SQL_ERROR(ERR_MSG("text: min_gram must not exceed max_gram"));
  }
  TextTokenizer::stopwords_t stopwords;
  if (!BuildStopwords(opts, stopwords)) {
    THROW_SQL_ERROR(
      ERR_MSG("text: failed to load stopwords from the configured path"));
  }
  return std::make_unique<TextTokenizer>(std::move(opts), std::move(stopwords));
}

template<TokenLayout Layout, Case C, bool SearchNGram>
bool TextTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  const auto size = raw.GetSize();
  if (size > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return false;
  }

  const char* const data = raw.GetData();
  if (_ascii_fast && simdutf::validate_ascii(data, size)) {
    AsciiFillValue<Layout, C, SearchNGram>(sink, raw);
    return true;
  }

  const auto udata = icu::UnicodeString::fromUTF8(
    icu::StringPiece{data, static_cast<int32_t>(size)});

  // tokenise the unicode data
  _state->break_iterator->setText(udata);

  FillValue<Layout, C, SearchNGram>(udata, sink);
  return true;
}

template<Case C>
bool TextTokenizer::next_word(const icu::UnicodeString& data, Word& word) {
  // find boundaries of the next word
  for (auto start = _state->break_iterator->current(), prev_end = start,
            end = _state->break_iterator->next();
       icu::BreakIterator::DONE != end;
       start = end, end = _state->break_iterator->next()) {
    // skip whitespace and unsuccessful terms
    if (UWordBreak::UBRK_WORD_NONE == _state->break_iterator->getRuleStatus() ||
        !ProcessTerm<C>(*_state, data.tempSubString(start, end - start),
                        word.term)) {
      continue;
    }

    // `data` comes from fromUTF8 (always well-formed UTF-16) and the break
    // iterator lands on code-point boundaries, so the range never splits a
    // surrogate pair.
    auto utf8_length = [&data](uint32_t begin, uint32_t end) noexcept {
      return static_cast<uint32_t>(simdutf::utf8_length_from_utf16(
        reinterpret_cast<const char16_t*>(data.getBuffer()) + begin,
        end - begin));
    };

    word.start = word.end + utf8_length(prev_end, start);
    word.end = word.start + utf8_length(start, end);

    return true;
  }

  return false;
}

// All ngrams of the current word, first at the word's (advanced) position,
// the rest at the same position; min/max/preserve_original rules unchanged
// from the legacy per-call iteration.
template<TokenLayout Layout, bool StableTerm>
void TextTokenizer::EmitWordNGrams(TokenSink& sink, uint32_t& pos,
                                   std::string_view term, uint32_t offs_start) {
  const auto* begin = reinterpret_cast<const byte_type*>(term.data());
  const auto* end = begin + term.size();
  SDB_ASSERT(begin != end);
  const auto& options = _state->options;

  const byte_type* it = begin;
  uint32_t length = 0;
  do {
    it = utf8_utils::Next(it, end);
  } while (++length < options.min_gram && it != end);

  // every ngram is a prefix of the word: a cycle-stable word (view into the
  // input) is emitted verbatim, a volatile one is copied per gram (short
  // grams inline-pack with no arena traffic)
  bool first = true;
  for (;;) {
    bool word_done = it == end;
    if (options.max_gram_set && length > options.max_gram) {
      word_done = true;
      if (!options.preserve_original) {
        return;
      }
      it = end;
    }
    if (length >= options.min_gram || options.preserve_original) {
      const auto size = static_cast<uint32_t>(std::distance(begin, it));
      if (first) {
        ++pos;
        first = false;
      }
      if constexpr (StableTerm) {
        sink.Emit<Layout>(MakeTermView(begin, size), pos,
                          Offs{offs_start, offs_start + size});
      } else {
        sink.Emit<Layout>(
          size,
          [&](byte_type* mem) IRS_FORCE_INLINE {
            std::memcpy(mem, begin, size);
            return size;
          },
          pos, Offs{offs_start, offs_start + size});
      }
    }
    if (word_done) {
      return;
    }
    it = utf8_utils::Next(it, end);
    ++length;
  }
}

// ASCII values skip ICU wholesale: word boundaries via the shared ASCII
// UAX#29 scan (only alnum-bearing segments are words, matching the break
// iterator's UBRK_WORD_NONE filter), normalization and accent stripping are
// identity on ASCII, case conversion is a bulk ASCII map (tailored-case
// locales keep the unicode path), stopwords compare post-case bytes, and
// stems are memoized per distinct word.
template<TokenLayout Layout, Case C, bool SearchNGram>
void TextTokenizer::AsciiFillValue(TokenSink& sink, duckdb::string_t value) {
  const char* const data = value.GetData();
  const uint32_t n = value.GetSize();
  auto& state = *_state;
  uint32_t pos = 0;
  const auto fold = [](char* dst, const char* src, uint32_t n) {
    if constexpr (C == Case::Lower) {
      absl::ascii_internal::AsciiStrToLower(dst, src, n);
    } else {
      absl::ascii_internal::AsciiStrToUpper(dst, src, n);
    }
  };
  // one whole-value fold into the member scratch instead of a per-word
  // Reserve+fold; short words emit as inline interned views of it
  const char* shadow = nullptr;
  if constexpr (C != Case::None && !SearchNGram) {
    if (n <= kBulkFoldLimit) [[likely]] {
      sdb::basics::StrResizeAmortized(state.fold_buf, n);
      fold(state.fold_buf.data(), data, n);
      shadow = state.fold_buf.data();
    }
  }
  ScanAsciiWords(value, [&](const AsciiSegment& seg) {
    if (!seg.has_alpha && !seg.has_digit) {
      return;
    }
    const uint32_t size = seg.end - seg.begin;
    const char* src = data + seg.begin;

    if constexpr (SearchNGram) {
      std::string_view word;
      if constexpr (C == Case::None) {
        word = {src, size};
      } else {
        sdb::basics::StrResizeAmortized(state.word_buf, size);
        fold(state.word_buf.data(), src, size);
        word = {state.word_buf.data(), size};
      }
      if (state.stopwords.Contains(word)) {
        return;
      }
      const std::string_view term =
        state.stemmer ? state.StemCached(word) : word;
      // the word survived as a view into the input: stable across cycles
      if constexpr (C == Case::None) {
        if (term.data() == src) {
          EmitWordNGrams<Layout, true>(sink, pos, term, seg.begin);
          return;
        }
      }
      EmitWordNGrams<Layout, false>(sink, pos, term, seg.begin);
    } else {
      std::string_view word;
      if constexpr (C == Case::None) {
        word = {src, size};
      } else if (shadow != nullptr) [[likely]] {
        word = {shadow + seg.begin, size};
      } else {
        sdb::basics::StrResizeAmortized(state.word_buf, size);
        fold(state.word_buf.data(), src, size);
        word = {state.word_buf.data(), size};
      }
      if (state.stopwords.Contains(word)) {
        return;
      }
      const Offs offs{seg.begin, seg.end};
      const auto intern = [&](std::string_view term) IRS_FORCE_INLINE {
        sink.Emit<Layout>(
          term.size(),
          [&](byte_type* mem) IRS_FORCE_INLINE {
            std::memcpy(mem, term.data(), term.size());
            return static_cast<uint32_t>(term.size());
          },
          ++pos, offs);
      };
      if (state.stemmer) {
        const std::string_view term = state.StemCached(word);
        if constexpr (C == Case::None) {
          if (term.data() == word.data()) {
            sink.Emit<Layout>(MakeTermView(word.data(), size), ++pos, offs);
            return;
          }
        }
        intern(term);
      } else if constexpr (C == Case::None) {
        sink.Emit<Layout>(MakeTermView(word.data(), size), ++pos, offs);
      } else {
        intern(word);
      }
    }
  });
}

template<TokenLayout Layout, Case C, bool SearchNGram>
void TextTokenizer::FillValue(const icu::UnicodeString& data, TokenSink& sink) {
  uint32_t pos = 0;
  Word word;
  if constexpr (SearchNGram) {
    while (next_word<C>(data, word)) {
      EmitWordNGrams<Layout, false>(sink, pos, word.term, word.start);
    }
    return;
  }
  while (next_word<C>(data, word)) {
    sink.Emit<Layout>(
      word.term.size(),
      [&](byte_type* mem) IRS_FORCE_INLINE {
        std::memcpy(mem, word.term.data(), word.term.size());
        return static_cast<uint32_t>(word.term.size());
      },
      ++pos, Offs{word.start, word.end});
  }
}

template class TypedTokenizer<TextTokenizer>;

}  // namespace irs::analysis
