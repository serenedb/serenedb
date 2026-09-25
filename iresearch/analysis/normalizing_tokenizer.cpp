////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "normalizing_tokenizer.hpp"

#include <unicode/locid.h>
#include <unicode/normalizer2.h>
#include <unicode/translit.h>
#include <unicode/unistr.h>
#include <unicode/ustring.h>

#include "iresearch/analysis/text/case/case.hpp"
#include "iresearch/analysis/text/normalize/normalize.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::analysis {
namespace {

template<sz_normal_form_t Form>
void ComposeInto(std::string_view in, std::string& out) {
  out.resize_and_overwrite(normalize::Bound<Form>(in.size()),
                           [&](char* p, size_t) IRS_FORCE_INLINE {
                             return normalize::Compose<Form>(in, p);
                           });
}

template<sz_normal_form_t Form>
void DecomposeInto(std::string_view in, std::string& out) {
  out.resize_and_overwrite(normalize::Bound<Form>(in.size()),
                           [&](char* p, size_t) IRS_FORCE_INLINE {
                             return normalize::Decompose<Form>(in, p);
                           });
}

template<sz_normal_form_t Form>
IRS_NO_INLINE bool StripAccents(std::string_view& bytes, std::string& norm_buf,
                                std::string& strip_buf) {
  switch (normalize::StripTwoByte<Form>(bytes, strip_buf)) {
    case normalize::StripResult::Unchanged:
      return false;
    case normalize::StripResult::Stripped:
      bytes = strip_buf;
      return false;
    case normalize::StripResult::Unsupported:
      break;
  }
  DecomposeInto<Form>(bytes, norm_buf);
  normalize::StripNonspacingMarks(norm_buf, strip_buf);
  bytes = strip_buf;
  return true;
}

const icu::Normalizer2* MakeNormalizer(NormForm form, UErrorCode& err) {
  switch (form) {
    case NormForm::Nfc:
      return icu::Normalizer2::getNFCInstance(err);
    case NormForm::Nfkc:
      return icu::Normalizer2::getNFKCInstance(err);
    case NormForm::Nfd:
      return icu::Normalizer2::getNFDInstance(err);
    case NormForm::Nfkd:
      return icu::Normalizer2::getNFKDInstance(err);
    case NormForm::NfkcCf:
      return icu::Normalizer2::getNFKCCasefoldInstance(err);
  }
  return nullptr;
}

std::unique_ptr<icu::Transliterator> MakeStripTransliterator(NormForm form,
                                                             UErrorCode& err) {
  const char* rule = "NFD; [:Nonspacing Mark:] Remove; NFC";
  switch (form) {
    case NormForm::Nfc:
      break;
    case NormForm::Nfkc:
    case NormForm::NfkcCf:
      rule = "NFKD; [:Nonspacing Mark:] Remove; NFKC";
      break;
    case NormForm::Nfd:
      rule = "NFD; [:Nonspacing Mark:] Remove";
      break;
    case NormForm::Nfkd:
      rule = "NFKD; [:Nonspacing Mark:] Remove";
      break;
  }
  return std::unique_ptr<icu::Transliterator>{
    icu::Transliterator::createInstance(icu::UnicodeString{rule},
                                        UTransDirection::UTRANS_FORWARD, err)};
}

template<Case C>
void NormalizeCaseStrip(const icu::Normalizer2& normalizer,
                        const icu::Normalizer2& renormalizer,
                        const icu::Locale& locale, bool fold,
                        uint32_t fold_options, icu::Transliterator* strip,
                        icu::UnicodeString& data, icu::UnicodeString& out) {
  auto err = UErrorCode::U_ZERO_ERROR;
  normalizer.normalize(data, out, err);
  if (!U_SUCCESS(err)) {
    out = data;
  }
  if constexpr (C != Case::None) {
    if constexpr (C == Case::Lower) {
      if (fold) {
        out.foldCase(fold_options);
      } else {
        out.toLower(locale);
      }
    } else {
      out.toUpper(locale);
    }
    if (strip == nullptr) {
      err = UErrorCode::U_ZERO_ERROR;
      if (!renormalizer.isNormalized(out, err) && U_SUCCESS(err)) {
        renormalizer.normalize(out, data, err);
        if (U_SUCCESS(err)) {
          out.swap(data);
        }
      }
    }
  }
  if (strip != nullptr) {
    strip->transliterate(out);
  }
}

bool IsTurkic(const icu::Locale& locale) noexcept {
  if (locale.isBogus()) {
    return false;
  }
  const std::string_view language{locale.getLanguage()};
  return language == "tr" || language == "az";
}

}  // namespace

NormalizingTokenizer::NormalizingTokenizer(Options options)
  : _options{std::move(options)} {
  const char* locale_name =
    _options.locale.isBogus() ? "" : _options.locale.getName();
  if (_options.fold) {
    _options.case_convert = Case::Lower;
    const bool turkic = IsTurkic(_options.locale);
    _fold_options =
      turkic ? U_FOLD_CASE_EXCLUDE_SPECIAL_I : U_FOLD_CASE_DEFAULT;
    _case_path = turkic ? CasePath::Icu : CasePath::Fast;
  } else if (_options.case_convert == Case::None ||
             casing::SimpleCaseSafe(locale_name)) {
    _case_path = CasePath::Fast;
  } else if (casing::AsciiCaseSafe(locale_name)) {
    _case_path = CasePath::IcuNonAscii;
  } else {
    _case_path = CasePath::Icu;
  }
}

std::tuple<Case, bool, bool> NormalizingTokenizer::PrepareBatch(
  BlockTraits traits) {
  const bool casefold_form = _options.form == NormForm::NfkcCf;
  if ((_case_path != CasePath::Fast || casefold_form) && !_normalizer) {
    auto err = UErrorCode::U_ZERO_ERROR;
    _normalizer = MakeNormalizer(_options.form, err);
    _renormalizer =
      MakeNormalizer(casefold_form ? NormForm::Nfkc : _options.form, err);
    if (!U_SUCCESS(err) || !_normalizer || !_renormalizer) {
      THROW_SQL_ERROR(ERR_MSG("normalize_tokens: failed to create normalizer"));
    }

    if (!_options.accent) {
      _transliterator = MakeStripTransliterator(_options.form, err);
      if (!U_SUCCESS(err) || !_transliterator) {
        THROW_SQL_ERROR(
          ERR_MSG("normalize_tokens: failed to create transliterator"));
      }
    }
  }
  const Case convert = casefold_form && _options.case_convert == Case::None
                         ? Case::Lower
                         : _options.case_convert;
  return {convert, _options.accent,
          traits.ascii && _case_path != CasePath::Icu};
}

template<Case C>
size_t NormalizingTokenizer::CaseBound(size_t size) const noexcept {
  if constexpr (C == Case::Lower) {
    if (_options.fold) {
      return sz::kFoldGrowth * size;
    }
  }
  return casing::CaseConvertUtf8Bound(size);
}

template<Case C>
size_t NormalizingTokenizer::ConvertCase(std::string_view bytes,
                                         byte_type* out) const noexcept {
  if constexpr (C == Case::Lower) {
    if (_options.fold) {
      return sz::Fold(bytes.data(), bytes.size(), reinterpret_cast<char*>(out));
    }
  }
  return casing::CaseConvertUtf8<C == Case::Lower>(bytes, out);
}

Tokenizer::ptr NormalizingTokenizer::Make(Options opts) {
  return std::make_unique<NormalizingTokenizer>(std::move(opts));
}

template<TokenLayout Layout, Case C, bool Accent, typename Sink>
bool NormalizingTokenizer::UnicodeEmit(const duckdb::string_t& raw,
                                       Sink& sink) {
  SDB_ASSERT(_normalizer);
  constexpr auto kMaxIcuBytes =
    static_cast<uint32_t>(std::numeric_limits<int32_t>::max());
  if (raw.GetSize() > kMaxIcuBytes) {
    sink.template Emit<Layout>(raw);
    return true;
  }

  if constexpr (!Accent) {
    SDB_ASSERT(_transliterator);
  }
  const auto size = static_cast<int32_t>(raw.GetSize());
  if (auto* buf = _udata.getBuffer(size)) {
    auto err = UErrorCode::U_ZERO_ERROR;
    int32_t len = 0;
    u_strFromUTF8WithSub(buf, size, &len, raw.GetData(), size, 0xFFFD, nullptr,
                         &err);
    _udata.releaseBuffer(U_SUCCESS(err) ? len : 0);
  } else {
    _udata.remove();
  }
  NormalizeCaseStrip<C>(
    *_normalizer, *_renormalizer, _options.locale, _options.fold, _fold_options,
    Accent ? nullptr : _transliterator.get(), _udata, _token);
  const auto cap = 3 * static_cast<size_t>(_token.length());
  if (cap == 0) {
    sink.template Emit<Layout>(duckdb::string_t{});
    return true;
  }
  if (cap > kMaxIcuBytes) [[unlikely]] {
    sink.template Emit<Layout>(raw);
    return true;
  }
  sink.template Emit<Layout>(cap, [&](byte_type* mem) IRS_FORCE_INLINE {
    auto err = UErrorCode::U_ZERO_ERROR;
    int32_t utf8_len = 0;
    u_strToUTF8(reinterpret_cast<char*>(mem), static_cast<int32_t>(cap),
                &utf8_len, _token.getBuffer(), _token.length(), &err);
    if (!U_SUCCESS(err)) [[unlikely]] {
      SDB_ASSERT(false);
      return uint32_t{0};
    }
    return static_cast<uint32_t>(utf8_len);
  });
  return true;
}

template<TokenLayout Layout, Case C, bool Accent, NormForm F, typename Sink>
bool NormalizingTokenizer::FastUnicodeEmit(const duckdb::string_t& raw,
                                           Sink& sink) {
  constexpr auto kForm =
    F == NormForm::Nfkc ? sz_normal_form_nfkc_k : sz_normal_form_nfc_k;
  const char* data = raw.GetData();
  const uint32_t size = raw.GetSize();
  if (classify::IsAsciiEarlyOut(data, size)) {
    if constexpr (C == Case::None) {
      sink.template Emit<Layout>(raw);
    } else {
      sink.template EmitCaseConverted<Layout, C == Case::Lower>(raw);
    }
    return true;
  }
  std::string_view bytes{data, size};
  bool compose = false;
  if constexpr (!Accent) {
    if (!normalize::StripSafe<kForm>(data, size)) {
      compose = StripAccents<kForm>(bytes, _norm_buf, _strip_buf);
    }
  } else if (normalize::Denormalized<kForm>(data, size)) {
    compose = true;
  }
  if constexpr (C == Case::None) {
    if (compose) {
      sink.template Emit<Layout>(normalize::Bound<kForm>(bytes.size()),
                                 [&](byte_type* out) IRS_FORCE_INLINE {
                                   return normalize::Compose<kForm>(
                                     bytes, reinterpret_cast<char*>(out));
                                 });
      return true;
    }
    if (bytes.data() != data) {
      sink.template Emit<Layout>(bytes.data(),
                                 static_cast<uint32_t>(bytes.size()));
      return true;
    }
    sink.template Emit<Layout>(raw);
    return true;
  }
  if (compose) {
    ComposeInto<kForm>(bytes, _norm_buf);
    bytes = _norm_buf;
  }
  sink.template Emit<Layout>(
    normalize::Bound<kForm>(CaseBound<C>(bytes.size())),
    [&](byte_type* out) IRS_FORCE_INLINE {
      const auto n = ConvertCase<C>(bytes, out);
      const std::string_view converted{reinterpret_cast<const char*>(out), n};
      if (!normalize::Denormalized<kForm>(converted.data(), converted.size()))
        [[likely]] {
        return n;
      }
      ComposeInto<kForm>(converted, _norm_buf);
      std::memcpy(out, _norm_buf.data(), _norm_buf.size());
      return _norm_buf.size();
    });
  return true;
}

template<TokenLayout Layout, Case C, bool Accent, NormForm F, typename Sink>
bool NormalizingTokenizer::DecomposedEmit(const duckdb::string_t& raw,
                                          Sink& sink) {
  constexpr auto kForm =
    F == NormForm::Nfkd ? sz_normal_form_nfkd_k : sz_normal_form_nfd_k;
  constexpr auto kComposed =
    F == NormForm::Nfkd ? sz_normal_form_nfkc_k : sz_normal_form_nfc_k;
  const char* data = raw.GetData();
  const uint32_t size = raw.GetSize();
  if (classify::IsAsciiValue(data, size)) {
    if constexpr (C == Case::None) {
      sink.template Emit<Layout>(raw);
    } else {
      sink.template EmitCaseConverted<Layout, C == Case::Lower>(raw);
    }
    return true;
  }
  std::string_view bytes{data, size};
  bool decomposed = false;
  if constexpr (!Accent) {
    if (!normalize::StripSafe<kComposed>(data, size)) {
      ComposeInto<kForm>(bytes, _norm_buf);
      normalize::StripNonspacingMarks(_norm_buf, _strip_buf);
      bytes = _strip_buf;
      decomposed = true;
    }
  }
  if constexpr (C == Case::None) {
    if (decomposed) {
      sink.template Emit<Layout>(bytes.size(),
                                 [&](byte_type* out) IRS_FORCE_INLINE {
                                   std::memcpy(out, bytes.data(), bytes.size());
                                   return bytes.size();
                                 });
      return true;
    }
  } else {
    if (!decomposed) {
      ComposeInto<kForm>(bytes, _strip_buf);
      bytes = _strip_buf;
    }
    _norm_buf.resize_and_overwrite(
      CaseBound<C>(bytes.size()), [&](char* p, size_t) IRS_FORCE_INLINE {
        return ConvertCase<C>(bytes, reinterpret_cast<byte_type*>(p));
      });
    bytes = _norm_buf;
  }
  sink.template Emit<Layout>(normalize::Bound<kForm>(bytes.size()),
                             [&](byte_type* out) IRS_FORCE_INLINE {
                               return normalize::Compose<kForm>(
                                 bytes, reinterpret_cast<char*>(out));
                             });
  return true;
}

template<TokenLayout Layout, Case C, bool Accent, bool KnownAscii,
         typename Sink>
bool NormalizingTokenizer::DoFill(const duckdb::string_t& raw, Sink& sink) {
  if constexpr (KnownAscii) {
    if constexpr (C == Case::None) {
      sink.template Emit<Layout>(raw);
    } else {
      sink.template EmitCaseConverted<Layout, C == Case::Lower>(raw);
    }
    return true;
  } else {
    if (_options.form == NormForm::NfkcCf) {
      if (_options.case_convert == Case::None) {
        return UnicodeEmit<Layout, Case::None, Accent>(raw, sink);
      }
      return UnicodeEmit<Layout, C, Accent>(raw, sink);
    }
    if constexpr (C != Case::None) {
      if (_case_path != CasePath::Fast) {
        return UnicodeEmit<Layout, C, Accent>(raw, sink);
      }
    }
    switch (_options.form) {
      case NormForm::Nfkc:
        return FastUnicodeEmit<Layout, C, Accent, NormForm::Nfkc>(raw, sink);
      case NormForm::Nfd:
        return DecomposedEmit<Layout, C, Accent, NormForm::Nfd>(raw, sink);
      case NormForm::Nfkd:
        return DecomposedEmit<Layout, C, Accent, NormForm::Nfkd>(raw, sink);
      case NormForm::Nfc:
      case NormForm::NfkcCf:
        break;
    }
    return FastUnicodeEmit<Layout, C, Accent, NormForm::Nfc>(raw, sink);
  }
}

template class TypedTokenizer<NormalizingTokenizer>;
template struct TypedTokenStage<NormalizingTokenizer>;

}  // namespace irs::analysis
