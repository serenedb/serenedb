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

#include "normalizing_tokenizer.hpp"

#include <frozen/unordered_map.h>
#include <unicode/normalizer2.h>  // for icu::Normalizer2
#include <unicode/translit.h>     // for icu::Transliterator
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>

#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {

struct NormalizingTokenizer::StateT {
  icu::UnicodeString data;
  icu::UnicodeString token;
  std::string term_buf;
  const icu::Normalizer2* normalizer{};  // reusable object owned by ICU
  std::unique_ptr<icu::Transliterator> transliterator;
  const OptionsT options;

  explicit StateT(const OptionsT& opts) : options{opts} {}
};

namespace {

constexpr std::string_view kLocaleParamName = "locale";
constexpr std::string_view kCaseConvertParamName = "case";
constexpr std::string_view kAccentParamName = "accent";

constexpr frozen::unordered_map<std::string_view,
                                NormalizingTokenizer::CaseConvertT, 3>
  kCaseConvertMap = {
    {"lower", NormalizingTokenizer::kLower},
    {"none", NormalizingTokenizer::kNone},
    {"upper", NormalizingTokenizer::kUpper},
};

bool LocaleFromSlice(vpack::Slice slice, icu::Locale& locale) {
  if (!slice.isString()) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-string value in '",
             kLocaleParamName,
             "' while constructing normalizing_tokenizer from "
             "VPack arguments");

    return false;
  }

  const auto locale_name = slice.copyString();

  locale = icu::Locale::createFromName(locale_name.c_str());

  if (!locale.isBogus()) {
    locale = icu::Locale{locale.getLanguage(), locale.getCountry(),
                         locale.getVariant()};
  }

  if (locale.isBogus()) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
             "Failed to instantiate locale from the supplied string '",
             locale_name,
             "' while constructing normalizing_tokenizer from VPack "
             "arguments");

    return false;
  }

  return true;
}

bool ParseVPackOptions(const vpack::Slice slice,
                       NormalizingTokenizer::OptionsT& options) {
  if (!slice.isObject()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Slice for normalizing_tokenizer is not an object");
    return false;
  }

  try {
    const auto locale_slice = slice.get(kLocaleParamName);

    if (!locale_slice.isNone()) {
      if (!LocaleFromSlice(locale_slice, options.locale)) {
        return false;
      }

      // optional string enum
      if (auto case_convert_slice = slice.get(kCaseConvertParamName);
          !case_convert_slice.isNone()) {
        if (!case_convert_slice.isString()) {
          SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-string value in '",
                   kCaseConvertParamName,
                   "' while constructing normalizing_tokenizer "
                   "from VPack arguments");

          return false;
        }

        const auto* it = kCaseConvertMap.find(case_convert_slice.stringView());

        if (it == kCaseConvertMap.end()) {
          SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Invalid value in '",
                   kCaseConvertParamName,
                   "' while constructing normalizing_tokenizer "
                   "from VPack arguments");

          return false;
        }

        options.case_convert = it->second;
      }

      // optional bool
      if (auto accent_slice = slice.get(kAccentParamName);
          !accent_slice.isNone()) {
        if (!accent_slice.isBool()) {
          SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, "Non-boolean value in '",
                   kAccentParamName,
                   "' while constructing normalizing_tokenizer "
                   "from VPack arguments");

          return false;
        }

        options.accent = accent_slice.getBool();
      }

      return true;
    }

    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Missing '", kLocaleParamName,
                           "' while constructing normalizing_tokenizer from "
                           "VPack arguments"));
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing normalizing_tokenizer from VPack"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing normalizing_tokenizer from "
              "VPack arguments");
  }
  return false;
}

// args is a jSON encoded object with the following attributes:
// "locale"(string): the locale to use for stemming <required>
// "case"(string enum): modify token case using "locale"
// "accent"(bool): leave accents
Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  NormalizingTokenizer::OptionsT options;
  if (ParseVPackOptions(slice, options)) {
    return std::make_unique<NormalizingTokenizer>(std::move(options));
  }
  return nullptr;
}

Analyzer::ptr MakeVPack(std::string_view args) {
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

// builds analyzer config from internal options in json format
// options reference to analyzer options storage
// definition string for storing json document with config
bool MakeVPackConfig(const NormalizingTokenizer::OptionsT& options,
                     vpack::Builder* builder) {
  vpack::ObjectBuilder object(builder);
  {
    // locale
    const auto& locale_name = options.locale.getBaseName();
    builder->add(kLocaleParamName, locale_name);

    // case convert
    const auto case_value = absl::c_find_if(
      kCaseConvertMap,
      [&](const auto& v) { return v.second == options.case_convert; });
    if (case_value != kCaseConvertMap.end()) {
      builder->add(kCaseConvertParamName, case_value->first);
    } else {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat("Invalid case_convert value in text analyzer options: ",
                     static_cast<int>(options.case_convert)));
      return false;
    }

    // Accent
    builder->add(kAccentParamName, options.accent);
  }

  return true;
}

bool NormalizeVPackConfig(const vpack::Slice slice, vpack::Builder* builder) {
  NormalizingTokenizer::OptionsT options;
  if (ParseVPackOptions(slice, options)) {
    return MakeVPackConfig(options, builder);
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

void NormalizingTokenizer::StateDeleterT::operator()(StateT* p) const noexcept {
  delete p;
}

NormalizingTokenizer::NormalizingTokenizer(const OptionsT& options)
  : _state{new StateT{options}}, _term_eof{true} {}

void NormalizingTokenizer::init() {
  REGISTER_ANALYZER_JSON(NormalizingTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(NormalizingTokenizer, MakeVPack,
                          NormalizeVPackConfig);
}

bool NormalizingTokenizer::next() {
  if (_term_eof) {
    return false;
  }

  _term_eof = true;

  return true;
}

bool NormalizingTokenizer::reset(std::string_view data) {
  auto err =
    UErrorCode::U_ZERO_ERROR;  // a value that passes the U_SUCCESS() test

  if (!_state->normalizer) {
    // reusable object owned by ICU
    _state->normalizer = icu::Normalizer2::getNFCInstance(err);

    if (!U_SUCCESS(err) || !_state->normalizer) {
      _state->normalizer = nullptr;

      return false;
    }
  }

  if (!_state->options.accent && !_state->transliterator) {
    // transliteration rule taken verbatim from:
    // http://userguide.icu-project.org/transforms/general do not allocate
    // statically since it causes memory leaks in ICU
    const icu::UnicodeString collation_rule(
      "NFD; [:Nonspacing Mark:] Remove; NFC");

    // reusable object owned by *this
    _state->transliterator.reset(icu::Transliterator::createInstance(
      collation_rule, UTransDirection::UTRANS_FORWARD, err));

    if (!U_SUCCESS(err) || !_state->transliterator) {
      _state->transliterator.reset();

      return false;
    }
  }

  // convert input string for use with ICU
  if (data.size() >
      static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return false;
  }

  _state->data = icu::UnicodeString::fromUTF8(
    icu::StringPiece{data.data(), static_cast<int32_t>(data.size())});

  // normalize unicode
  _state->normalizer->normalize(_state->data, _state->token, err);

  if (!U_SUCCESS(err)) {
    // use non-normalized value if normalization failure
    _state->token = std::move(_state->data);
  }

  // case-convert unicode
  switch (_state->options.case_convert) {
    case kLower:
      _state->token.toLower(_state->options.locale);  // inplace case-conversion
      break;
    case kUpper:
      _state->token.toUpper(_state->options.locale);  // inplace case-conversion
      break;
    case kNone:
      break;
  }

  // collate value, e.g. remove accents
  if (_state->transliterator) {
    _state->transliterator->transliterate(
      _state->token);  // inplace translitiration
  }

  _state->term_buf.clear();
  _state->token.toUTF8String(_state->term_buf);

  // use the normalized value
  static_assert(sizeof(byte_type) == sizeof(char));
  std::get<TermAttr>(_attrs).value =
    ViewCast<byte_type>(std::string_view{_state->term_buf});
  auto& offset = std::get<irs::OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());
  _term_eof = false;

  return true;
}

}  // namespace irs::analysis
