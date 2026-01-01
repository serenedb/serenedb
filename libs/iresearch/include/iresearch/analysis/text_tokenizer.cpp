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

#include <absl/container/node_hash_map.h>
#include <frozen/unordered_map.h>
#include <libstemmer.h>
#include <unicode/brkiter.h>      // for icu::BreakIterator
#include <unicode/normalizer2.h>  // for icu::Normalizer2
#include <unicode/translit.h>     // for icu::Transliterator
#include <unicode/uclean.h>       // for u_cleanup
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>
#include <vpack/vpack.h>

#include <cctype>  // for std::isspace(...)
#include <filesystem>
#include <fstream>
#include <mutex>
#include <string_view>

#include "absl/strings/str_cat.h"
#include "basics/file_utils_ext.hpp"
#include "basics/logger/logger.h"
#include "basics/misc.hpp"
#include "basics/runtime_utils.hpp"
#include "basics/thread_utils.hpp"
#include "basics/utf8_utils.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/snowball_stemmer.hpp"
#include "iresearch/utils/vpack_utils.hpp"

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
  struct NGramState {
    const byte_type* it = nullptr;
    uint32_t length = 0;
  };

  icu::UnicodeString data;
  icu::UnicodeString token;
  const OptionsT& options;
  const stopwords_t& stopwords;
  bstring term_buf;
  std::string tmp_buf;  // used by processTerm(...)
  NGramState ngram;
  bytes_view term;
  uint32_t start{};
  uint32_t end{};

  StateT(const OptionsT& opts, const stopwords_t& stopw)
    : options(opts), stopwords(stopw) {}

  bool IsSearchNGram() const {
    // if min or max or preserveOriginal are set then search ngram
    return options.min_gram_set || options.max_gram_set ||
           options.preserve_original_set;
  }

  bool IsNGramFinished() const { return 0 == ngram.length; }

  void SetNGramFinished() noexcept { ngram.length = 0; }
};

namespace {

struct CachedOptions : public TextTokenizer::OptionsT {
  TextTokenizer::stopwords_t stopwords;

  CachedOptions(TextTokenizer::OptionsT&& options,
                TextTokenizer::stopwords_t&& stopwords)
    : TextTokenizer::OptionsT{std::move(options)},
      stopwords{std::move(stopwords)} {}
};

struct StrHash {
  using is_transparent = void;

  size_t operator()(std::string_view v) const { return absl::HashOf(v); }
  size_t operator()(hashed_string_view v) const { return v.Hash(); }
};

absl::node_hash_map<std::string, CachedOptions, StrHash> gCachedStateByKey;
constinit absl::Mutex gMutex{absl::kConstInit};

// Retrieves a set of ignored words from FS at the specified custom path
bool GetStopwords(TextTokenizer::stopwords_t& buf, std::string_view language,
                  std::string_view path = {}) {
  std::filesystem::path stopword_path;

  const auto* custom_stopword_path =
    !IsNull(path) ? path.data()
                  : irs::Getenv(TextTokenizer::gStopwordPathEnvVariable);

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
        SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                  absl::StrCat("Failed to load stopwords from path: ",
                               stopword_path.string()));
        return false;
      }
      SDB_TRACE("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Failed to load stopwords from default path: ",
                             stopword_path.string(),
                             ". Analyzer will continue without stopwords"));
      return true;
    }

    TextTokenizer::stopwords_t stopwords;
    auto visitor = [&stopwords, &stopword_path](auto name) -> bool {
      bool result = false;
      const auto path = stopword_path / name;

      if (!file_utils::ExistsFile(result, path.c_str())) {
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
          absl::StrCat("Failed to identify stopword path: ", path.string()));

        return false;
      }

      if (!result) {
        return true;  // skip non-files
      }

      std::ifstream in(path.native());

      if (!in) {
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
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
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
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
bool BuildStopwords(const TextTokenizer::OptionsT& options,
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

// create an analyzer based on the supplied cache_key and options
Analyzer::ptr Construct(hashed_string_view cache_key,
                        TextTokenizer::OptionsT&& options,
                        TextTokenizer::stopwords_t&& stopwords) {
  CachedOptions* options_ptr = nullptr;
  {
    std::lock_guard lock{gMutex};
    auto r = gCachedStateByKey.try_emplace(cache_key, std::move(options),
                                           std::move(stopwords));
    options_ptr = &r.first->second;
  }

  return std::make_unique<TextTokenizer>(*options_ptr, options_ptr->stopwords);
}

// create an analyzer based on the supplied cache_key
Analyzer::ptr Construct(icu::Locale&& locale) {
  if (locale.isBogus()) {
    return nullptr;
  }

  {
    hashed_string_view key{locale.getName()};
    std::lock_guard lock{gMutex};

    auto itr = gCachedStateByKey.find(key);

    if (itr != gCachedStateByKey.end()) {
      return std::make_unique<TextTokenizer>(itr->second,
                                             itr->second.stopwords);
    }
  }

  try {
    TextTokenizer::OptionsT options;
    TextTokenizer::stopwords_t stopwords;
    options.locale = locale;

    if (!BuildStopwords(options, stopwords)) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
               "Failed to retrieve 'stopwords' while constructing "
               "text_token_stream with cache key: ",
               options.locale.getName());

      return nullptr;
    }

    return Construct(hashed_string_view{options.locale.getName()},
                     std::move(options), std::move(stopwords));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat(
                "Caught error while constructing text_token_stream cache key: ",
                locale.getName()));
  }

  return nullptr;
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
    case TextTokenizer::kLower:
      state.token.toLower(state.options.locale);  // inplace case-conversion
      break;
    case TextTokenizer::kUpper:
      state.token.toUpper(state.options.locale);  // inplace case-conversion
      break;
    case TextTokenizer::kNone:
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
  if (state.stopwords.contains(word_utf8)) {
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

constexpr std::string_view kLocaleParamName = "locale";
constexpr std::string_view kCaseConvertParamName = "case";
constexpr std::string_view kStopwordsParamName = "stopwords";
constexpr std::string_view kStopwordsPathParamName = "stopwordsPath";
constexpr std::string_view kAccentParamName = "accent";
constexpr std::string_view kStemmingParamName = "stemming";
constexpr std::string_view kEdgeNGramParamName = "edgeNGram";
constexpr std::string_view kMinParamName = "min";
constexpr std::string_view kMaxParamName = "max";
constexpr std::string_view kPreserveOriginalParamName = "preserveOriginal";

constexpr frozen::unordered_map<std::string_view, TextTokenizer::CaseConvertT,
                                3>
  kCaseConvertMap = {
    {"lower", TextTokenizer::CaseConvertT::kLower},
    {"none", TextTokenizer::CaseConvertT::kNone},
    {"upper", TextTokenizer::CaseConvertT::kUpper},
};

bool InitFromOptions(const TextTokenizer::OptionsT& options,
                     IcuObjects* objects, bool print_errors) {
  auto err =
    UErrorCode::U_ZERO_ERROR;  // a value that passes the U_SUCCESS() test

  // reusable object owned by ICU
  objects->normalizer = icu::Normalizer2::getNFCInstance(err);

  if (!U_SUCCESS(err) || !objects->normalizer) {
    objects->normalizer = nullptr;

    if (print_errors) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
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
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
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
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
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
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
               "Failed to create stemmer for text_token_stream from locale: ",
               options.locale.getName());
    }
  }

  return true;
}

bool LocaleFromString(const std::string& locale_name, icu::Locale& locale) {
  locale = icu::Locale::createFromName(locale_name.c_str());

  if (!locale.isBogus()) {
    locale = icu::Locale{locale.getLanguage(), locale.getCountry(),
                         locale.getVariant()};
  }

  if (locale.isBogus()) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
             "Failed to instantiate locale from the supplied string '",
             locale_name,
             "' while constructing text_token_stream from VPack arguments");

    return false;
  }

  return true;
}

bool LocaleFromSlice(vpack::Slice slice, icu::Locale& locale) {
  if (!slice.isString()) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-string value in '",
             kLocaleParamName,
             "' while constructing text_token_stream from VPack arguments");

    return false;
  }

  return LocaleFromString(slice.copyString(), locale);
}

bool ParseVPackOptions(const vpack::Slice slice,
                       TextTokenizer::OptionsT& options) {
  vpack::Slice locale_slice;
  if (!slice.isObject() ||
      !(locale_slice = slice.get(kLocaleParamName)).isString()) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Missing '", kLocaleParamName,
             "' while constructing text_token_stream from VPack");

    return false;
  }

  try {
    if (!LocaleFromSlice(locale_slice, options.locale)) {
      return false;
    }

    if (auto case_convert_slice = slice.get(kCaseConvertParamName);
        !case_convert_slice.isNone()) {
      if (!case_convert_slice.isString()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-string value in '",
                 kCaseConvertParamName,
                 "' while constructing text_token_stream from VPack arguments");

        return false;
      }

      const auto* it = kCaseConvertMap.find(case_convert_slice.stringView());

      if (it == kCaseConvertMap.end()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid value in '",
                 kCaseConvertParamName,
                 "' while constructing text_token_stream from VPack arguments");

        return false;
      }

      options.case_convert = it->second;
    }

    if (auto stop_words_slice = slice.get(kStopwordsParamName);
        !stop_words_slice.isNone()) {
      if (!stop_words_slice.isArray()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid value in '",
                 kStopwordsParamName,
                 "' while constructing text_token_stream from VPack arguments");

        return false;
      }
      // mark - we have explicit list (even if it is empty)
      options.explicit_stopwords_set = true;
      for (const auto itr : vpack::ArrayIterator(stop_words_slice)) {
        if (!itr.isString()) {
          SDB_WARN(
            "xxxxx", sdb::Logger::IRESEARCH, "Non-string value in '",
            kStopwordsParamName,
            "' while constructing text_token_stream from VPack arguments");

          return false;
        }
        options.explicit_stopwords.emplace(itr.stringView());
      }
    }

    if (auto ignored_words_path_slice = slice.get(kStopwordsPathParamName);
        !ignored_words_path_slice.isNone()) {
      if (!ignored_words_path_slice.isString()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-string value in '",
                 kStopwordsPathParamName,
                 "' while constructing text_token_stream from VPack arguments");

        return false;
      }
      options.stopwords_path = ignored_words_path_slice.stringView();
    }

    if (auto accent_slice = slice.get(kAccentParamName);
        !accent_slice.isNone()) {
      if (!accent_slice.isBool()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-boolean value in '",
                 kAccentParamName,
                 "' while constructing text_token_stream from VPack arguments");

        return false;
      }
      options.accent = accent_slice.getBool();
    }

    if (auto stemming_slice = slice.get(kStemmingParamName);
        !stemming_slice.isNone()) {
      if (!stemming_slice.isBool()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-boolean value in '",
                 kStemmingParamName,
                 "' while constructing text_token_stream from VPack arguments");

        return false;
      }

      options.stemming = stemming_slice.getBool();
    }

    if (auto ngram_slice = slice.get(kEdgeNGramParamName);
        !ngram_slice.isNone()) {
      if (!ngram_slice.isObject()) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-object value in '",
                 kEdgeNGramParamName,
                 "' while constructing text_token_stream from VPack arguments");

        return false;
      }

      if (auto v = ngram_slice.get(kMinParamName);
          v.isNumber<decltype(options.min_gram)>()) {
        options.min_gram = v.getNumber<decltype(options.min_gram)>();
        options.min_gram_set = true;
      }

      if (auto v = ngram_slice.get(kMaxParamName);
          v.isNumber<decltype(options.min_gram)>()) {
        options.max_gram = v.getNumber<decltype(options.min_gram)>();
        options.max_gram_set = true;
      }

      if (auto v = ngram_slice.get(kPreserveOriginalParamName); v.isBool()) {
        options.preserve_original = v.getBool();
        options.preserve_original_set = true;
      }

      if (options.min_gram_set && options.max_gram_set) {
        return options.min_gram <= options.max_gram;
      }
    }

    IcuObjects obj;
    InitFromOptions(options, &obj, true);

    return true;
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing text_token_stream from VPack"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing text_token_stream from VPack "
              "arguments");
  }

  return false;
}

// builds analyzer config from internal options in json format
// options reference to analyzer options storage
// definition string for storing json document with config
bool MakeVPackConfig(const TextTokenizer::OptionsT& options,
                     vpack::Builder* builder) {
  vpack::ObjectBuilder object(builder);
  {
    // locale
    const auto* const locale_name = options.locale.getBaseName();
    builder->add(kLocaleParamName, locale_name);

    // case convert
    const auto case_value = absl::c_find_if(
      kCaseConvertMap,
      [&options](const auto& v) { return v.second == options.case_convert; });

    if (case_value != kCaseConvertMap.end()) {
      builder->add(kCaseConvertParamName, case_value->first);
    } else {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat("Invalid case_convert value in text analyzer options: ",
                     static_cast<int>(options.case_convert)));
      return false;
    }

    // stopwords
    if (!options.explicit_stopwords.empty() || options.explicit_stopwords_set) {
      // explicit_stopwords_set  marks that even empty stopwords list is valid
      std::vector<std::string_view> sorted_words;
      if (!options.explicit_stopwords.empty()) {
        // for simplifying comparison between properties we need deterministic
        // order of stopwords
        sorted_words.reserve(options.explicit_stopwords.size());
        for (const auto& stopword : options.explicit_stopwords) {
          sorted_words.emplace_back(stopword);
        }
        absl::c_sort(sorted_words);
      }
      {
        vpack::ArrayBuilder array(builder, kStopwordsParamName.data());

        for (const auto& stopword : sorted_words) {
          builder->add(stopword);
        }
      }
    }

    // Accent
    builder->add(kAccentParamName, options.accent);

    // Stem
    builder->add(kStemmingParamName, options.stemming);

    // stopwords path
    if (options.stopwords_path.empty() || options.stopwords_path[0] != 0) {
      // if stopwordsPath is set  - output it (empty string is also valid value
      // =  use of CWD)
      builder->add(kStopwordsPathParamName, options.stopwords_path);
    }
  }

  // ensure disambiguating casts below are safe. Casts required for clang
  // compiler on Mac
  static_assert(sizeof(uint64_t) >= sizeof(size_t),
                "sizeof(uint64_t) >= sizeof(size_t)");

  if (options.min_gram_set || options.max_gram_set ||
      options.preserve_original_set) {
    vpack::ObjectBuilder sub_object(builder, kEdgeNGramParamName.data());

    // min_gram
    if (options.min_gram_set) {
      builder->add(kMinParamName, static_cast<uint64_t>(options.min_gram));
    }

    // max_gram
    if (options.max_gram_set) {
      builder->add(kMaxParamName, static_cast<uint64_t>(options.max_gram));
    }

    // preserve_original
    if (options.preserve_original_set) {
      builder->add(kPreserveOriginalParamName, options.preserve_original);
    }
  }

  return true;
}

// args is a jSON encoded object with the following attributes:
// "locale"(string): locale of the analyzer <required>
// "case"(string enum): modify token case using "locale"
// "accent"(bool): leave accents
// "stemming"(bool): use stemming
// "stopwords([string...]): set of words to ignore
// "stopwordsPath"(string): custom path, where to load stopwords
// "min" (number): minimum ngram size
// "max" (number): maximum ngram size
// "preserveOriginal" (boolean): preserve or not the original term
// if none of stopwords and stopwordsPath specified, stopwords are loaded from
// default location
Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  try {
    const hashed_string_view slice_ref{
      std::string_view{slice.startAs<char>(), slice.byteSize()}};
    {
      std::lock_guard lock{gMutex};
      auto itr = gCachedStateByKey.find(slice_ref);

      if (itr != gCachedStateByKey.end()) {
        return std::make_unique<TextTokenizer>(itr->second,
                                               itr->second.stopwords);
      }
    }

    TextTokenizer::OptionsT options;
    if (ParseVPackOptions(slice, options)) {
      TextTokenizer::stopwords_t stopwords;
      if (!BuildStopwords(options, stopwords)) {
        SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
                 "Failed to retrieve 'stopwords' from path while constructing "
                 "text_token_stream from VPack arguments");

        return nullptr;
      }
      return Construct(slice_ref, std::move(options), std::move(stopwords));
    }
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing text_token_stream from VPack "
              "arguments");
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

bool NormalizeVPackConfig(const vpack::Slice slice,
                          vpack::Builder* vpack_builder) {
  TextTokenizer::OptionsT options;
  if (ParseVPackOptions(slice, options)) {
    return MakeVPackConfig(options, vpack_builder);
  }
  return false;
}

bool NormalizeVPackConfig(std::string_view args, std::string& definition) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  vpack::Builder builder;
  bool res = NormalizeVPackConfig(slice, &builder);
  if (res) {
    definition.assign(builder.slice().startAs<char>(),
                      builder.slice().byteSize());
  }
  return res;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief args is a locale name
////////////////////////////////////////////////////////////////////////////////
Analyzer::ptr MakeText(std::string_view args) {
  icu::Locale locale;

  if (LocaleFromString(static_cast<std::string>(args), locale)) {
    return Construct(std::move(locale));
  }
  return nullptr;
}

bool NormalizeTextConfig(std::string_view args, std::string& definition) {
  icu::Locale locale;

  if (LocaleFromString(static_cast<std::string>(args), locale)) {
    definition = locale.getBaseName();
    return true;
  }

  return false;
}

Analyzer::ptr MakeJson(std::string_view args) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while constructing normalizing_tokenizer");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing normalizing_tokenizer from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing normalizing_tokenizer from "
              "JSON");
  }
  return nullptr;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while normalizing normalizing_tokenizer");
      return false;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    vpack::Builder builder;
    if (NormalizeVPackConfig(vpack->slice(), &builder)) {
      definition = builder.toString();
      return !definition.empty();
    }
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while normalizing normalizing_tokenizer from JSON"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while normalizing normalizing_tokenizer from "
              "JSON");
  }
  return false;
}

}  // namespace

void TextTokenizer::StateDeleterT::operator()(StateT* p) const noexcept {
  delete p;
}

const char* TextTokenizer::gStopwordPathEnvVariable =
  "IRESEARCH_TEXT_STOPWORD_PATH";

TextTokenizer::TextTokenizer(const OptionsT& options,
                             const stopwords_t& stopwords)
  : _state{new StateT{options, stopwords}} {}

void TextTokenizer::init() {
  REGISTER_ANALYZER_VPACK(TextTokenizer, MakeVPack, NormalizeVPackConfig);
  REGISTER_ANALYZER_JSON(TextTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_TEXT(TextTokenizer, MakeText, NormalizeTextConfig);
}

void TextTokenizer::clear_cache() {
  std::lock_guard lock{gMutex};
  gCachedStateByKey.clear();
}

Analyzer::ptr TextTokenizer::make(std::string_view locale) {
  return MakeText(locale);
}

bool TextTokenizer::reset(std::string_view data) {
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

  _state->data = icu::UnicodeString::fromUTF8(
    icu::StringPiece{data.data(), static_cast<int32_t>(data.size())});

  // tokenise the unicode data
  _state->break_iterator->setText(_state->data);

  // reset term state for ngrams
  _state->term = {};
  _state->start = 0;
  _state->end = 0;
  _state->SetNGramFinished();
  std::get<IncAttr>(_attrs).value = 1;

  return true;
}

bool TextTokenizer::next() {
  if (_state->IsSearchNGram()) {
    while (true) {
      // if a word has not started for ngrams yet
      if (_state->IsNGramFinished() && !next_word()) {
        return false;
      }
      // get next ngram taking into account min and max
      if (next_ngram()) {
        return true;
      }
    }
  } else if (next_word()) {
    std::get<TermAttr>(_attrs).value = _state->term;

    auto& offset = std::get<irs::OffsAttr>(_attrs);
    offset.start = _state->start;
    offset.end = _state->end;

    return true;
  }

  return false;
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

bool TextTokenizer::next_ngram() {
  const auto* begin = _state->term.data();
  const auto* end = _state->term.data() + _state->term.size();
  SDB_ASSERT(begin != end);

  auto& inc = std::get<IncAttr>(_attrs);

  // if there are no ngrams yet then a new word started
  if (_state->IsNGramFinished()) {
    _state->ngram.it = begin;
    inc.value = 1;
    // find the first ngram > min
    do {
      _state->ngram.it = utf8_utils::Next(_state->ngram.it, end);
    } while (++_state->ngram.length < _state->options.min_gram &&
             _state->ngram.it != end);
  } else {
    // not first ngram in a word
    inc.value = 0;  // staying on the current pos
    _state->ngram.it = utf8_utils::Next(_state->ngram.it, end);
    ++_state->ngram.length;
  }

  bool finished{};
  Finally set_ngram_finished = [this, &finished]() noexcept -> void {
    if (finished) {
      _state->SetNGramFinished();
    }
  };

  // if a word has finished
  if (_state->ngram.it == end) {
    // no unwatched ngrams in a word
    finished = true;
  }

  // if length > max
  if (_state->options.max_gram_set &&
      _state->ngram.length > _state->options.max_gram) {
    // no unwatched ngrams in a word
    finished = true;
    if (_state->options.preserve_original) {
      _state->ngram.it = end;
    } else {
      return false;
    }
  }

  // if length >= min or preserveOriginal
  if (_state->ngram.length >= _state->options.min_gram ||
      _state->options.preserve_original) {
    // ensure disambiguating casts below are safe. Casts required for clang
    // compiler on Mac
    static_assert(sizeof(byte_type) == sizeof(char));

    auto size = static_cast<uint32_t>(std::distance(begin, _state->ngram.it));
    _term_buf.assign(_state->term.data(), size);
    std::get<TermAttr>(_attrs).value = _term_buf;

    auto& offset = std::get<irs::OffsAttr>(_attrs);
    offset.start = _state->start;
    offset.end = _state->start + size;

    return true;
  }

  return false;
}

}  // namespace irs::analysis
