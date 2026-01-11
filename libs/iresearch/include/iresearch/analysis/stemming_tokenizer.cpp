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

#include "stemming_tokenizer.hpp"

#include <libstemmer.h>
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>

#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kLocaleParamName = "locale";

bool LocaleFromSlice(vpack::Slice slice, icu::Locale& locale) {
  if (!slice.isString()) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-string value in '",
             kLocaleParamName,
             "' while constructing stemming_tokenizer from VPack arguments");

    return false;
  }

  const auto locale_name = slice.copyString();

  locale = icu::Locale::createFromName(locale_name.c_str());

  if (!locale.isBogus()) {
    locale = icu::Locale{locale.getLanguage()};
  }

  if (locale.isBogus()) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
             "Failed to instantiate locale from the supplied string '",
             locale_name,
             "' while constructing stemming_tokenizer from VPack arguments");

    return false;
  }

  // validate creation of sb_stemmer, defaults to utf-8
  stemmer_ptr stemmer = make_stemmer_ptr(locale.getLanguage(), nullptr);

  if (!stemmer) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
             "Failed to instantiate sb_stemmer from locale '", locale_name,
             "' while constructing stemming_token_stream from VPack arguments");
  }

  return true;
}

bool ParseVPackOptions(const vpack::Slice slice,
                       StemmingTokenizer::OptionsT& opts) {
  if (!slice.isObject()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Slice for stemming_tokenizer  is not an object");
    return false;
  }

  try {
    const auto locale_slice = slice.get(kLocaleParamName);

    if (locale_slice.isNone()) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                absl::StrCat("Missing '", kLocaleParamName,
                             "' while constructing stemming_tokenizer from "
                             "VPack arguments"));

      return false;
    }

    return LocaleFromSlice(locale_slice, opts.locale);
  } catch (const std::exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing stemming_tokenizer from VPack"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing stemming_tokenizer from VPack "
              "arguments");
  }

  return false;
}
////////////////////////////////////////////////////////////////////////////////
/// @brief args is a jSON encoded object with the following attributes:
///        "locale"(string): the locale to use for stemming <required>
////////////////////////////////////////////////////////////////////////////////
Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  StemmingTokenizer::OptionsT opts;

  if (ParseVPackOptions(slice, opts)) {
    return std::make_unique<StemmingTokenizer>(opts);
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}
///////////////////////////////////////////////////////////////////////////////
/// @brief builds analyzer config from internal options in json format
/// @param locale reference to analyzer`s locale
/// @param definition string for storing json document with config
///////////////////////////////////////////////////////////////////////////////
bool MakeVPackConfig(const StemmingTokenizer::OptionsT& opts,
                     vpack::Builder* builder) {
  vpack::ObjectBuilder object(builder);
  {
    // locale
    const auto* locale_name = opts.locale.getName();
    builder->add(kLocaleParamName, locale_name);
  }
  return true;
}

bool NormalizeVPackConfig(const vpack::Slice slice, vpack::Builder* builder) {
  StemmingTokenizer::OptionsT opts;

  if (ParseVPackOptions(slice, opts)) {
    return MakeVPackConfig(opts, builder);
  }
  return false;
}

bool NormalizeVPackConfig(std::string_view args, std::string& config) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  vpack::Builder builder;
  if (NormalizeVPackConfig(slice, &builder)) {
    config.assign(builder.slice().startAs<char>(), builder.slice().byteSize());
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
              "Caught error while normalizing normalizing_tokenizer from JSON");
  }
  return false;
}

}  // namespace

StemmingTokenizer::StemmingTokenizer(const OptionsT& options)
  : _options{options} {}

void StemmingTokenizer::init() {
  REGISTER_ANALYZER_JSON(StemmingTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(StemmingTokenizer, MakeVPack, NormalizeVPackConfig);
}

bool StemmingTokenizer::next() {
  if (_term_eof) {
    return false;
  }

  _term_eof = true;

  return true;
}

bool StemmingTokenizer::reset(std::string_view data) {
  if (!_stemmer) {
    // defaults to utf-8
    _stemmer = make_stemmer_ptr(_options.locale.getLanguage(), nullptr);
  }

  auto& term = std::get<TermAttr>(_attrs);

  term.value = {};  // reset

  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());

  _term_eof = false;

  // find the token stem
  std::string_view utf8_data{data};

  if (_stemmer) {
    if (utf8_data.size() >
        static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
      return false;
    }

    static_assert(sizeof(sb_symbol) == sizeof(char));
    const auto* value = reinterpret_cast<const sb_symbol*>(utf8_data.data());

    value = sb_stemmer_stem(_stemmer.get(), value,
                            static_cast<int>(utf8_data.size()));

    if (value) {
      static_assert(sizeof(byte_type) == sizeof(sb_symbol));
      term.value = bytes_view(reinterpret_cast<const byte_type*>(value),
                              sb_stemmer_length(_stemmer.get()));

      return true;
    }
  }

  // use the value of the unstemmed token
  static_assert(sizeof(byte_type) == sizeof(char));
  term.value = ViewCast<byte_type>(utf8_data);

  return true;
}

}  // namespace irs::analysis
