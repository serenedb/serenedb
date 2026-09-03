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

#include <unicode/ustring.h>

#include "iresearch/analysis/text/classify/block_masks.hpp"
#include "iresearch/analysis/text/normalize/icu.hpp"
#include "iresearch/analysis/text/normalize/normalize.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "pg/sql_exception_macro.h"

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

}  // namespace

NormalizingTokenizer::NormalizingTokenizer(Options options)
  : _options{std::move(options)} {
  const char* locale_name =
    _options.locale.isBogus() ? "" : _options.locale.getName();
  if (_options.case_convert == Case::None ||
      classify::SimpleCaseSafe(locale_name)) {
    _case_path = CasePath::Fast;
  } else if (classify::AsciiCaseSafe(locale_name)) {
    _case_path = CasePath::IcuNonAscii;
  } else {
    _case_path = CasePath::Icu;
  }
}

std::tuple<Case, bool, bool> NormalizingTokenizer::PrepareBatch(
  BlockTraits traits) {
  if (_case_path != CasePath::Fast && !_normalizer) {
    auto err = UErrorCode::U_ZERO_ERROR;
    const bool nfkc = _options.form == NormForm::Nfkc;
    _normalizer = nfkc ? icu::Normalizer2::getNFKCInstance(err)
                       : icu::Normalizer2::getNFCInstance(err);
    if (!U_SUCCESS(err) || !_normalizer) {
      THROW_SQL_ERROR(ERR_MSG("norm: failed to create normalizer"));
    }

    if (!_options.accent) {
      _transliterator = normalize::MakeStripTransliterator(nfkc, err);
      if (!U_SUCCESS(err) || !_transliterator) {
        THROW_SQL_ERROR(ERR_MSG("norm: failed to create transliterator"));
      }
    }
  }
  return {_options.case_convert, _options.accent,
          traits.ascii && _case_path != CasePath::Icu};
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
  normalize::NormalizeCaseStrip<C>(*_normalizer, _options.locale,
                                   Accent ? nullptr : _transliterator.get(),
                                   _udata, _token);
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
  std::string_view bytes{data, size};
  bool compose = false;
  if constexpr (!Accent) {
    if (!normalize::StripSafe<kForm>(data, size)) {
      DecomposeInto<kForm>(bytes, _norm_buf);
      normalize::StripNonspacingMarks(_norm_buf, _strip_buf);
      bytes = _strip_buf;
      compose = true;
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
    sink.template Emit<Layout>(raw);
    return true;
  }
  if (compose) {
    ComposeInto<kForm>(bytes, _norm_buf);
    bytes = _norm_buf;
  }
  sink.template Emit<Layout>(
    normalize::Bound<kForm>(casing::CaseConvertUtf8Bound(bytes.size())),
    [&](byte_type* out) IRS_FORCE_INLINE {
      const auto n = casing::CaseConvertUtf8<C == Case::Lower>(bytes, out);
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
    if constexpr (C != Case::None) {
      if (_case_path != CasePath::Fast) {
        return UnicodeEmit<Layout, C, Accent>(raw, sink);
      }
    }
    if (_options.form == NormForm::Nfkc) {
      return FastUnicodeEmit<Layout, C, Accent, NormForm::Nfkc>(raw, sink);
    }
    return FastUnicodeEmit<Layout, C, Accent, NormForm::Nfc>(raw, sink);
  }
}

template class TypedTokenizer<NormalizingTokenizer>;
template class TypedTokenStage<NormalizingTokenizer>;

}  // namespace irs::analysis
