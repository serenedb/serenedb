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

#include "iresearch/analysis/text/case/case.hpp"
#include "iresearch/analysis/text/normalize/normalize.hpp"
#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::analysis {
namespace {

using duckdb::text::CaseFolding;
using duckdb::text::CaseMapping;
using duckdb::text::NormalizationForm;

class TermOutput final : public duckdb::text::TransformOutput {
 public:
  explicit TermOutput(ArenaTerm& term)
    : TransformOutput{reinterpret_cast<char*>(term.Data()), term.Capacity()},
      _term{term} {}

  void Grow(size_t, size_t needed) final {
    _term.Grow(needed);
    data = reinterpret_cast<char*>(_term.Data());
    capacity = _term.Capacity();
  }

 private:
  ArenaTerm& _term;
};

template<sz_normal_form_t Form>
void ComposeInto(std::string_view in, std::string& out) {
  out.resize_and_overwrite(normalize::Bound<Form>(in.size()),
                           [&](char* p, size_t) IRS_FORCE_INLINE {
                             return normalize::Compose<Form>(in, p);
                           });
}

NormalizationForm ToNormalizationForm(NormForm form) noexcept {
  switch (form) {
    case NormForm::Nfc:
      return NormalizationForm::NFC;
    case NormForm::Nfkc:
      return NormalizationForm::NFKC;
    case NormForm::Nfd:
      return NormalizationForm::NFD;
    case NormForm::Nfkd:
      return NormalizationForm::NFKD;
    case NormForm::NfkcCf:
      return NormalizationForm::NFKC_CF;
  }
  return NormalizationForm::NFC;
}

bool IsTurkic(const duckdb::text::Locale& locale) noexcept {
  const auto language = locale.GetLanguage();
  return language == "tr" || language == "az";
}

bool CasesLikeFastPath(const NormalizingTokenizer::Options& options) {
  if (options.form == NormForm::NfkcCf) {
    return false;
  }
  if (options.fold) {
    return !IsTurkic(options.locale);
  }
  return casing::SimpleCaseSafe(options.locale.GetName().c_str());
}

CaseMapping ToCaseMapping(const NormalizingTokenizer::Options& options,
                          bool fast) {
  if (options.fold) {
    return CaseMapping::FOLD;
  }
  switch (options.case_convert) {
    case Case::Lower:
      return fast ? CaseMapping::SIMPLE_LOWER : CaseMapping::LOWER;
    case Case::Upper:
      return fast ? CaseMapping::SIMPLE_UPPER : CaseMapping::UPPER;
    case Case::None:
      break;
  }
  return CaseMapping::NONE;
}

duckdb::text::TransformOptions MakeTransformOptions(
  const NormalizingTokenizer::Options& options) {
  const bool fast = CasesLikeFastPath(options);
  return {
    .form = ToNormalizationForm(options.form),
    .case_mapping = ToCaseMapping(options, fast),
    .locale = options.locale.GetCaseLocale(),
    .folding = options.fold && IsTurkic(options.locale) ? CaseFolding::TURKIC
                                                        : CaseFolding::DEFAULT,
    .strip_marks = !options.accent,
    .strip_before_case = fast,
  };
}

}  // namespace

NormalizingTokenizer::NormalizingTokenizer(Options options)
  : _options{std::move(options)}, _transform{MakeTransformOptions(_options)} {
  const char* locale_name = _options.locale.GetName().c_str();
  if (_options.fold) {
    _options.case_convert = Case::Lower;
    _case_path = IsTurkic(_options.locale) ? CasePath::Icu : CasePath::Fast;
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

template<TokenLayout Layout, typename Sink>
bool NormalizingTokenizer::UnicodeEmit(const duckdb::string_t& raw,
                                       Sink& sink) {
  const std::string_view input{raw.GetData(), raw.GetSize()};
  sink.template EmitGrowable<Layout>(
    input.size(), [&](ArenaTerm& term) IRS_FORCE_INLINE {
      TermOutput output{term};
      return _transform.Apply(input, output, _transform_buf);
    });
  return true;
}

template<TokenLayout Layout, Case C, NormForm F, typename Sink>
bool NormalizingTokenizer::FastUnicodeEmit(const duckdb::string_t& raw,
                                           Sink& sink) {
  constexpr auto kForm =
    F == NormForm::Nfkc ? sz_normal_form_nfkc_k : sz_normal_form_nfc_k;
  const char* data = raw.GetData();
  const uint32_t size = raw.GetSize();
  std::string_view bytes{data, size};
  const bool compose = normalize::Denormalized<kForm>(data, size);
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

template<TokenLayout Layout, Case C, NormForm F, typename Sink>
bool NormalizingTokenizer::DecomposedEmit(const duckdb::string_t& raw,
                                          Sink& sink) {
  constexpr auto kForm =
    F == NormForm::Nfkd ? sz_normal_form_nfkd_k : sz_normal_form_nfd_k;
  std::string_view bytes{raw.GetData(), raw.GetSize()};
  if constexpr (C != Case::None) {
    ComposeInto<kForm>(bytes, _decompose_buf);
    bytes = _decompose_buf;
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
  if constexpr (!KnownAscii) {
    if (_case_path == CasePath::Icu ||
        !classify::IsAsciiEarlyOut(raw.GetData(), raw.GetSize())) {
      if constexpr (!Accent) {
        return UnicodeEmit<Layout>(raw, sink);
      } else {
        if (_options.form == NormForm::NfkcCf) {
          return UnicodeEmit<Layout>(raw, sink);
        }
        if constexpr (C != Case::None) {
          if (_case_path != CasePath::Fast) {
            return UnicodeEmit<Layout>(raw, sink);
          }
        }
        switch (_options.form) {
          case NormForm::Nfkc:
            return FastUnicodeEmit<Layout, C, NormForm::Nfkc>(raw, sink);
          case NormForm::Nfd:
            return DecomposedEmit<Layout, C, NormForm::Nfd>(raw, sink);
          case NormForm::Nfkd:
            return DecomposedEmit<Layout, C, NormForm::Nfkd>(raw, sink);
          case NormForm::Nfc:
          case NormForm::NfkcCf:
            break;
        }
        return FastUnicodeEmit<Layout, C, NormForm::Nfc>(raw, sink);
      }
    }
  }
  if constexpr (C == Case::None) {
    sink.template Emit<Layout>(raw);
  } else {
    sink.template EmitCaseConverted<Layout, C == Case::Lower>(raw);
  }
  return true;
}

template class TypedTokenizer<NormalizingTokenizer>;
template struct TypedTokenStage<NormalizingTokenizer>;

}  // namespace irs::analysis
