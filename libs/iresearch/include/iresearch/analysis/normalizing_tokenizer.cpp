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

#include <absl/strings/ascii.h>
#include <absl/strings/internal/resize_uninitialized.h>
#include <simdutf.h>
#include <unicode/ustring.h>  // for u_strToUTF8

#include "iresearch/analysis/classify.hpp"
#include "iresearch/analysis/term_view.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

template<Case C>
IRS_FORCE_INLINE void AsciiCaseCopy(char* dst, const char* src, uint32_t size) {
  if constexpr (C == Case::Lower) {
    absl::ascii_internal::AsciiStrToLower(dst, src, size);
  } else if constexpr (C == Case::Upper) {
    absl::ascii_internal::AsciiStrToUpper(dst, src, size);
  } else {
    std::memcpy(dst, src, size);
  }
}

}  // namespace

NormalizingTokenizer::NormalizingTokenizer(Options options)
  : _options{std::move(options)} {
  if (_options.locale.isBogus()) {
    THROW_SQL_ERROR(ERR_MSG("norm: invalid locale"));
  }
  _ascii_fast = _options.case_convert == Case::None ||
                AsciiCaseSafe(_options.locale.getName());
}

std::tuple<Case, bool> NormalizingTokenizer::PrepareBatch() {
  if (!_normalizer) {
    auto err = UErrorCode::U_ZERO_ERROR;
    // reusable object owned by ICU
    _normalizer = icu::Normalizer2::getNFCInstance(err);
    if (!U_SUCCESS(err) || !_normalizer) {
      THROW_SQL_ERROR(ERR_MSG("norm: failed to create NFC normalizer"));
    }

    if (!_options.accent) {
      // transliteration rule taken verbatim from:
      // http://userguide.icu-project.org/transforms/general do not allocate
      // statically since it causes memory leaks in ICU
      const icu::UnicodeString collation_rule(
        "NFD; [:Nonspacing Mark:] Remove; NFC");

      _transliterator.reset(icu::Transliterator::createInstance(
        collation_rule, UTransDirection::UTRANS_FORWARD, err));
      if (!U_SUCCESS(err) || !_transliterator) {
        THROW_SQL_ERROR(ERR_MSG("norm: failed to create transliterator"));
      }
    }
  }
  return {_options.case_convert, _options.accent};
}

Tokenizer::ptr NormalizingTokenizer::Make(Options opts) {
  return std::make_unique<NormalizingTokenizer>(std::move(opts));
}

bool NormalizingTokenizer::AsciiFastEligible(
  const duckdb::string_t& value) const noexcept {
  if (!_ascii_fast) {
    return false;
  }
  const char* const data = value.GetData();
  const uint32_t size = value.GetSize();
  if (size <= 16) [[likely]] {
    return IsAsciiShort(data, size);
  }
  return simdutf::validate_ascii(data, size);
}

bool NormalizingTokenizer::AsciiRewrite(const duckdb::string_t& value,
                                        std::string& out) const {
  if (!AsciiFastEligible(value)) {
    return false;
  }
  const char* const data = value.GetData();
  const uint32_t size = value.GetSize();
  absl::strings_internal::STLStringResizeUninitialized(&out, size);
  switch (_options.case_convert) {
    case Case::Lower:
      AsciiCaseCopy<Case::Lower>(out.data(), data, size);
      break;
    case Case::Upper:
      AsciiCaseCopy<Case::Upper>(out.data(), data, size);
      break;
    case Case::None:
      AsciiCaseCopy<Case::None>(out.data(), data, size);
      break;
  }
  return true;
}

template<TokenLayout Layout, Case C>
void NormalizingTokenizer::AsciiEmit(const duckdb::string_t& raw,
                                     TokenSink& sink) {
  const auto size = static_cast<uint32_t>(raw.GetSize());
  if constexpr (C == Case::None) {
    sink.Emit<Layout>(raw);
  } else if (size <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
    sink.Emit<Layout>(FoldTermViewAscii<C == Case::Lower>(raw));
  } else {
    const char* const data = raw.GetData();
    sink.Emit<Layout>(size, [&](byte_type* out) IRS_FORCE_INLINE {
      AsciiCaseCopy<C>(reinterpret_cast<char*>(out), data, size);
      return size;
    });
  }
}

template<Case C, bool Accent>
const icu::UnicodeString& NormalizingTokenizer::Normalize(
  const duckdb::string_t& data) {
  auto err =
    UErrorCode::U_ZERO_ERROR;  // a value that passes the U_SUCCESS() test

  auto udata = icu::UnicodeString::fromUTF8(
    icu::StringPiece{data.GetData(), static_cast<int32_t>(data.GetSize())});

  // normalize unicode
  _normalizer->normalize(udata, _token, err);

  if (!U_SUCCESS(err)) {
    // use non-normalized value if normalization failure
    _token = std::move(udata);
  }

  // case-convert unicode (inplace)
  if constexpr (C == Case::Lower) {
    _token.toLower(_options.locale);
  } else if constexpr (C == Case::Upper) {
    _token.toUpper(_options.locale);
  }

  // collate value, e.g. remove accents (inplace); transliterator exists iff
  // accent is off
  if constexpr (!Accent) {
    SDB_ASSERT(_transliterator);
    _transliterator->transliterate(_token);
  }

  return _token;
}

template<TokenLayout Layout, Case C, bool Accent>
bool NormalizingTokenizer::UnicodeEmit(const duckdb::string_t& raw,
                                       TokenSink& sink) {
  SDB_ASSERT(_normalizer);
  constexpr auto kMaxIcuBytes =
    static_cast<uint32_t>(std::numeric_limits<int32_t>::max());
  const auto size = static_cast<uint32_t>(raw.GetSize());
  if (raw.GetSize() > kMaxIcuBytes) {
    return false;
  }

  // already-NFC input with no case fold and no accent stripping survives
  // verbatim: prove it and emit a view of the input, no conversions at all
  if constexpr (C == Case::None && Accent) {
    auto err = UErrorCode::U_ZERO_ERROR;
    if (_normalizer->isNormalizedUTF8(
          {raw.GetData(), static_cast<int32_t>(size)}, err) &&
        U_SUCCESS(err)) {
      sink.Emit<Layout>(raw);
      return true;
    }
  }

  const auto& token = Normalize<C, Accent>(raw);
  const auto cap = 3 * static_cast<size_t>(token.length());
  if (cap == 0) {
    sink.Emit<Layout>(duckdb::string_t{});
    return true;
  }
  if (cap > kMaxIcuBytes) [[unlikely]] {
    return false;
  }
  // `token` is well-formed UTF-16 (fromUTF8 substitutes ill-formed input,
  // ICU ops preserve well-formedness) and cap covers max expansion, so the
  // conversion cannot fail.
  sink.Emit<Layout>(cap, [&](byte_type* mem) IRS_FORCE_INLINE {
    auto err = UErrorCode::U_ZERO_ERROR;
    int32_t utf8_len = 0;
    u_strToUTF8(reinterpret_cast<char*>(mem), static_cast<int32_t>(cap),
                &utf8_len, token.getBuffer(), token.length(), &err);
    if (!U_SUCCESS(err)) [[unlikely]] {
      SDB_ASSERT(false);
      return uint32_t{0};
    }
    return static_cast<uint32_t>(utf8_len);
  });
  return true;
}

template<TokenLayout Layout, Case C, bool Accent>
bool NormalizingTokenizer::DoFill(const duckdb::string_t& raw,
                                  TokenSink& sink) {
  if (AsciiFastEligible(raw)) {
    AsciiEmit<Layout, C>(raw, sink);
    return true;
  }
  return UnicodeEmit<Layout, C, Accent>(raw, sink);
}

template class TypedTokenizer<NormalizingTokenizer>;

}  // namespace irs::analysis
