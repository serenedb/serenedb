////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "collation_tokenizer.hpp"

#include <unicode/coll.h>
#include <unicode/locid.h>
#include <vpack/builder.h>
#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/slice.h>

#include <string_view>

#include "basics/logger/log_level.h"
#include "basics/logger/logger.h"
#include "collation_tokenizer_encoder.hpp"
#include "iresearch/utils/vpack_utils.hpp"

namespace irs::analysis {
namespace {

constexpr std::string_view kLocaleParamName = "locale";

bool LocaleFromSlice(vpack::Slice slice, icu::Locale& locale) {
  if (!slice.isString()) {
    SDB_WARN(
      "xxxxx", sdb::Logger::IRESEARCH, "Non-string value in '",
      kLocaleParamName,
      "' while constructing collation_token_stream from VPack arguments");

    return false;
  }

  const auto locale_name = slice.copyString();

  locale = icu::Locale::createCanonical(locale_name.c_str());

  if (locale.isBogus()) {
    SDB_WARN(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Failed to instantiate locale from the supplied string '", locale_name,
      "' while constructing collation_token_stream from VPack arguments");

    return false;
  }

  // validate creation of icu::Collator
  auto err = UErrorCode::U_ZERO_ERROR;
  std::unique_ptr<icu::Collator> collator{
    icu::Collator::createInstance(locale, err)};

  if (!collator) {
    SDB_WARN("xxxxx", sdb::Logger::IRESEARCH,
             "Can't instantiate icu::Collator from locale: ", locale_name);
    return false;
  }

  // print warn message
  if (err != UErrorCode::U_ZERO_ERROR) {
    const auto message = absl::StrCat(
      "Failure while instantiation of icu::Collator from locale: ", locale_name,
      ", ", u_errorName(err));
    if (U_FAILURE(err)) {
      SDB_WARN("xxxxx", sdb::Logger::IRESEARCH, message);
    } else {
      SDB_TRACE("xxxxx", sdb::Logger::IRESEARCH, message);
    }
  }

  return U_SUCCESS(err);
}

bool ParseVPackOptions(const vpack::Slice slice,
                       CollationTokenizer::OptionsT& options) {
  if (!slice.isObject()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Slice for collation_token_stream is not an object");
    return false;
  }

  try {
    const auto locale_slice = slice.get(kLocaleParamName);

    if (locale_slice.isNone()) {
      SDB_ERROR(
        "xxxxx", sdb::Logger::IRESEARCH,
        absl::StrCat(
          "Missing '", kLocaleParamName,
          "' while constructing collation_token_stream from VPack arguments"));

      return false;
    }

    return LocaleFromSlice(locale_slice, options.locale);
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing collation_token_stream from VPack"));
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing collation_token_stream from "
              "VPack arguments");
  }
  return false;
}

// args is a jSON encoded object with the following attributes:
// "locale"(string): the locale to use for stemming <required>
Analyzer::ptr MakeVPack(const vpack::Slice slice) {
  CollationTokenizer::OptionsT options;
  if (ParseVPackOptions(slice, options)) {
    return std::make_unique<CollationTokenizer>(std::move(options));
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
bool MakeVPackConfig(const CollationTokenizer::OptionsT& options,
                     vpack::Builder* builder) {
  vpack::ObjectBuilder object{builder};

  const auto* const locale_name = options.locale.getName();
  builder->add(kLocaleParamName, locale_name);
  return true;
}

bool NormalizeVPackConfig(const vpack::Slice slice, vpack::Builder* builder) {
  CollationTokenizer::OptionsT options;
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
                "Null arguments while constructing collation_token_stream");
      return nullptr;
    }
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Caught error '", ex.what(),
                   "' while constructing collation_token_stream from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while constructing collation_token_stream from JSON");
  }
  return nullptr;
}

bool NormalizeJsonConfig(std::string_view args, std::string& definition) {
  try {
    if (IsNull(args)) {
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Null arguments while normalizing collation_token_stream");
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
                   "' while normalizing collation_token_stream from JSON"));
  } catch (...) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      "Caught error while normalizing collation_token_stream from JSON");
  }
  return false;
}

constexpr size_t kMaxTokenSize = 1 << 15;

}  // namespace

struct CollationTokenizer::StateT {
  const OptionsT options;
  std::unique_ptr<icu::Collator> collator;
  byte_type term_buf[kMaxTokenSize];

  explicit StateT(const OptionsT& opts) : options(opts) {}
};

void CollationTokenizer::init() {
  REGISTER_ANALYZER_JSON(CollationTokenizer, MakeJson, NormalizeJsonConfig);
  REGISTER_ANALYZER_VPACK(CollationTokenizer, MakeVPack, NormalizeVPackConfig);
}

void CollationTokenizer::StateDeleterT::operator()(StateT* p) const noexcept {
  delete p;
}

CollationTokenizer::CollationTokenizer(const OptionsT& options)
  : _state{new StateT(options)}, _term_eof{true} {}

bool CollationTokenizer::reset(std::string_view data) {
  if (!_state->collator) {
    auto err = UErrorCode::U_ZERO_ERROR;
    _state->collator.reset(
      icu::Collator::createInstance(_state->options.locale, err));

    if (!U_SUCCESS(err) || !_state->collator) {
      _state->collator.reset();

      return false;
    }
  }

  if (data.size() >
      static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
    return false;  // ICU UnicodeString signatures can handle at most INT32_MAX
  }

  const icu::UnicodeString icu_token = icu::UnicodeString::fromUTF8(
    icu::StringPiece(data.data(), static_cast<int32_t>(data.size())));

  byte_type raw_term_buf[kMaxTokenSize];
  static_assert(sizeof raw_term_buf == sizeof _state->term_buf);

  auto* buf = _state->options.force_utf8 ? raw_term_buf : _state->term_buf;
  int32_t term_size =
    _state->collator->getSortKey(icu_token, buf, kMaxTokenSize);

  // https://unicode-org.github.io/icu-docs/apidoc/released/icu4c/classicu_1_1Collator.html
  // according to ICU docs sort keys are always zero-terminated,
  // there is no reason to store terminal zero in term dictionary
  SDB_ASSERT(term_size > 0);
  --term_size;
  SDB_ASSERT(0 == buf[term_size]);
  if (term_size > static_cast<int32_t>(kMaxTokenSize)) {
    SDB_ERROR(
      "xxxxx", sdb::Logger::IRESEARCH,
      absl::StrCat("Collated token is ", term_size,
                   " bytes length which exceeds maximum allowed length of ",
                   static_cast<int32_t>(sizeof raw_term_buf), " bytes"));
    return false;
  }
  auto term_buf_idx = static_cast<size_t>(term_size);
  if (_state->options.force_utf8) {
    // enforce valid UTF-8 string
    SDB_ASSERT(buf == raw_term_buf);
    term_buf_idx = 0;
    for (decltype(term_size) i{}; i < term_size; ++i) {
      static_assert(sizeof(raw_term_buf[i]) * (1 << CHAR_BIT) <=
                    kRecalcMap.size());
      const auto [offset, size] = kRecalcMap[raw_term_buf[i]];
      if ((term_buf_idx + size) > sizeof _state->term_buf) {
        SDB_ERROR(
          "xxxxx", sdb::Logger::IRESEARCH,
          absl::StrCat("Collated token is more than ", sizeof _state->term_buf,
                       " bytes length after encoding."));
        return false;
      }
      SDB_ASSERT(size <= 2);
      _state->term_buf[term_buf_idx++] = kBytesRecalcMap[offset];
      if (size == 2) {
        _state->term_buf[term_buf_idx++] = kBytesRecalcMap[offset + 1];
      }
    }
  }

  std::get<TermAttr>(_attrs).value = {_state->term_buf, term_buf_idx};
  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());
  _term_eof = false;

  return true;
}

}  // namespace irs::analysis
