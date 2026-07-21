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
#include "iresearch/analysis/batch/ascii_words.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/first_len_filter.hpp"
#include "iresearch/utils/snowball_stemmer.hpp"
#include "iresearch/utils/utf8_utils.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
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

struct TextTokenizer::StateT : IcuObjects {
  icu::UnicodeString data;
  icu::UnicodeString token;
  Options options;
  stopwords_t stopwords;
  bstring term_buf;
  std::string tmp_buf;  // used by processTerm(...)
  bytes_view term;
  FirstLenFilter stop_filter;
  absl::flat_hash_map<std::string, std::string> stem_cache;
  uint32_t start{};
  uint32_t end{};

  static constexpr size_t kMaxCachedStemKey = 64;
  static constexpr size_t kMaxStemCacheEntries = 65536;

  StateT(Options opts, stopwords_t stopw)
    : options(std::move(opts)), stopwords(std::move(stopw)) {}

  bytes_view StemCached(const std::string& word) {
    SDB_ASSERT(stemmer);
    const bool cacheable = word.size() <= kMaxCachedStemKey;
    if (cacheable) {
      if (const auto it = stem_cache.find(word); it != stem_cache.end()) {
        return ViewCast<byte_type>(std::string_view{it->second});
      }
    }
    static_assert(sizeof(sb_symbol) == sizeof(char));
    const auto* value = reinterpret_cast<const sb_symbol*>(word.c_str());
    value =
      sb_stemmer_stem(stemmer.get(), value, static_cast<int>(word.size()));
    if (value == nullptr) {
      term_buf.assign(reinterpret_cast<const byte_type*>(word.data()),
                      word.size());
      return term_buf;
    }
    const std::string_view stemmed{
      reinterpret_cast<const char*>(value),
      static_cast<size_t>(sb_stemmer_length(stemmer.get()))};
    if (cacheable) {
      if (stem_cache.size() >= kMaxStemCacheEntries) [[unlikely]] {
        stem_cache.clear();
      }
      stem_cache.emplace(word, stemmed);
    }
    return ViewCast<byte_type>(stemmed);
  }

  bool IsSearchNGram() const {
    // if min or max or preserveOriginal are set then search ngram
    return options.min_gram_set || options.max_gram_set ||
           options.preserve_original_set;
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

bool ProcessTerm(TextTokenizer::StateT& state, icu::UnicodeString&& data) {
  // normalize unicode
  auto err =
    UErrorCode::U_ZERO_ERROR;  // a value that passes the U_SUCCESS() test

  state.normalizer->normalize(data, state.token, err);

  if (!U_SUCCESS(err)) {
    state.token =
      std::move(data);  // use non-normalized value if normalization failure
  }

  // case-convert unicode
  switch (state.options.case_convert) {
    case Case::Lower:
      state.token.toLower(state.options.locale);  // inplace case-conversion
      break;
    case Case::Upper:
      state.token.toUpper(state.options.locale);  // inplace case-conversion
      break;
    case Case::None:
      break;
  }

  // collate value, e.g. remove accents
  if (state.transliterator) {
    state.transliterator->transliterate(state.token);
  }

  std::string& word_utf8 = state.tmp_buf;

  word_utf8.clear();
  state.token.toUTF8String(word_utf8);

  // skip ignored tokens
  if (state.stop_filter.MayContain(word_utf8) &&
      state.stopwords.contains(word_utf8)) {
    return false;
  }

  // find the token stem
  if (state.stemmer) {
    static_assert(sizeof(sb_symbol) == sizeof(char));
    const auto* value = reinterpret_cast<const sb_symbol*>(word_utf8.c_str());

    value = sb_stemmer_stem(state.stemmer.get(), value,
                            static_cast<int>(word_utf8.size()));

    if (value) {
      static_assert(sizeof(byte_type) == sizeof(sb_symbol));
      state.term = bytes_view(reinterpret_cast<const byte_type*>(value),
                              sb_stemmer_length(state.stemmer.get()));

      return true;
    }
  }

  // use the value of the unstemmed token
  static_assert(sizeof(byte_type) == sizeof(char));
  state.term_buf.assign(reinterpret_cast<const byte_type*>(word_utf8.c_str()),
                        word_utf8.size());
  state.term = state.term_buf;

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

void TextTokenizer::StateDeleterT::operator()(StateT* p) const noexcept {
  delete p;
}

const char* TextTokenizer::gStopwordPathEnvVariable =
  "IRESEARCH_TEXT_STOPWORD_PATH";

TextTokenizer::TextTokenizer(Options options, stopwords_t stopwords)
  : _state{new StateT{std::move(options), std::move(stopwords)}} {
  const std::string_view lang = _state->options.locale.getLanguage();
  _ascii_case_safe = lang != "tr" && lang != "az" && lang != "lt";
  for (const auto& word : _state->stopwords) {
    _state->stop_filter.Add(word);
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
  IcuObjects obj;
  if (!InitFromOptions(opts, &obj, true)) {
    THROW_SQL_ERROR(
      ERR_MSG("text: failed to initialize the analyzer for the locale"));
  }
  return std::make_unique<TextTokenizer>(std::move(opts), std::move(stopwords));
}

template<TokenLayout Layout>
bool TextTokenizer::DoFill(std::string_view data, TokenEmitter& sink) {
  if (data.size() > std::numeric_limits<uint32_t>::max()) {
    // can't handle data which is longer than
    // std::numeric_limits<uint32_t>::max()
    return false;
  }

  if (!_state->Valid() &&
      !InitFromOptions(_state->options, _state.get(), false)) {
    _state->Clear();
    return false;
  }

  // Create ICU UnicodeString
  if (data.size() >
      static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return false;
  }

  if (_ascii_case_safe && !_force_unicode &&
      simdutf::validate_ascii(data.data(), data.size())) {
    _state->term = {};
    _state->start = 0;
    _state->end = 0;
    AsciiFillValue<Layout>(sink, data);
    return true;
  }

  _state->data = icu::UnicodeString::fromUTF8(
    icu::StringPiece{data.data(), static_cast<int32_t>(data.size())});

  // tokenise the unicode data
  _state->break_iterator->setText(_state->data);

  // reset term state for ngrams
  _state->term = {};
  _state->start = 0;
  _state->end = 0;

  FillValue<Layout>(sink);
  return true;
}

bool TextTokenizer::next_word() {
  // find boundaries of the next word
  for (auto start = _state->break_iterator->current(), prev_end = start,
            end = _state->break_iterator->next();
       icu::BreakIterator::DONE != end;
       start = end, end = _state->break_iterator->next()) {
    // skip whitespace and unsuccessful terms
    if (UWordBreak::UBRK_WORD_NONE == _state->break_iterator->getRuleStatus() ||
        !ProcessTerm(*_state, _state->data.tempSubString(start, end - start))) {
      continue;
    }

    // TODO(mbkkt) simdutf::utf8_length_from_utf16
    auto utf8_length = [data = &_state->data](uint32_t begin,
                                              uint32_t end) noexcept {
      uint32_t length = 0;
      while (begin < end) {
        const auto cp = data->char32At(begin);
        if (cp == utf8_utils::kInvalidChar32) {
          SDB_ASSERT(length == 0);
          return 0U;
        }
        length += utf8_utils::LengthFromChar32(cp);
        begin += 1U + uint32_t{!U_IS_BMP(cp)};
      }
      return length;
    };

    _state->start = _state->end + utf8_length(prev_end, start);
    _state->end = _state->start + utf8_length(start, end);

    return true;
  }

  return false;
}

// All ngrams of the current word, first at the word's (advanced) position,
// the rest at the same position; min/max/preserve_original rules unchanged
// from the legacy per-call iteration.
template<TokenLayout Layout>
void TextTokenizer::EmitWordNGrams(TokenEmitter& sink, uint32_t& pos) {
  const auto* begin = _state->term.data();
  const auto* end = _state->term.data() + _state->term.size();
  SDB_ASSERT(begin != end);
  auto& buf = sink.buf;
  const auto& options = _state->options;

  const byte_type* it = begin;
  uint32_t length = 0;
  do {
    it = utf8_utils::Next(it, end);
  } while (++length < options.min_gram && it != end);

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
      const auto i = sink.Next();
      buf.terms[i] = sink.Intern(bytes_view{begin, size});
      if constexpr (Layout != TokenLayout::Terms) {
        buf.pos[i] = pos;
      }
      if constexpr (Layout == TokenLayout::TermsPosOffs) {
        buf.offs_start[i] = _state->start;
        buf.offs_end[i] = _state->start + size;
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
template<TokenLayout Layout>
void TextTokenizer::AsciiFillValue(TokenEmitter& sink, std::string_view value) {
  auto& state = *_state;
  auto& buf = sink.buf;
  uint32_t pos = 0;
  ScanAsciiWords(value, [&](const AsciiSegment& seg) {
    if (!seg.has_alpha && !seg.has_digit) {
      return;
    }
    const uint32_t size = seg.end - seg.begin;
    std::string& word = state.tmp_buf;
    sdb::basics::StrResizeAmortized(word, size);
    switch (state.options.case_convert) {
      case Case::Lower:
        absl::ascii_internal::AsciiStrToLower(word.data(),
                                              value.data() + seg.begin, size);
        break;
      case Case::Upper:
        absl::ascii_internal::AsciiStrToUpper(word.data(),
                                              value.data() + seg.begin, size);
        break;
      case Case::None:
        std::memcpy(word.data(), value.data() + seg.begin, size);
        break;
    }
    if (state.stop_filter.MayContain(word) && state.stopwords.contains(word)) {
      return;
    }
    if (state.stemmer) {
      state.term = state.StemCached(word);
    } else {
      state.term = ViewCast<byte_type>(std::string_view{word});
    }
    state.start = seg.begin;
    state.end = seg.end;
    if (state.IsSearchNGram()) {
      EmitWordNGrams<Layout>(sink, pos);
      return;
    }
    const auto i = sink.Next();
    buf.terms[i] = sink.Intern(state.term);
    if constexpr (Layout != TokenLayout::Terms) {
      buf.pos[i] = ++pos;
    }
    if constexpr (Layout == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = seg.begin;
      buf.offs_end[i] = seg.end;
    }
  });
}

template<TokenLayout Layout>
void TextTokenizer::FillValue(TokenEmitter& sink) {
  auto& buf = sink.buf;
  uint32_t pos = 0;
  if (_state->IsSearchNGram()) {
    while (next_word()) {
      EmitWordNGrams<Layout>(sink, pos);
    }
    return;
  }
  while (next_word()) {
    const auto i = sink.Next();
    buf.terms[i] = sink.Intern(_state->term);
    if constexpr (Layout != TokenLayout::Terms) {
      buf.pos[i] = ++pos;
    }
    if constexpr (Layout == TokenLayout::TermsPosOffs) {
      buf.offs_start[i] = _state->start;
      buf.offs_end[i] = _state->end;
    }
  }
}

template class TypedTokenizer<TextTokenizer>;

}  // namespace irs::analysis
