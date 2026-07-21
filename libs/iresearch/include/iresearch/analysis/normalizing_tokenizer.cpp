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
#include <simdutf.h>
#include <unicode/normalizer2.h>  // for icu::Normalizer2
#include <unicode/translit.h>     // for icu::Transliterator

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <string_view>

#include "iresearch/analysis/batch/term_view.hpp"
#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/tokenizer.hpp"
#include "pg/sql_exception_macro.h"

namespace irs::analysis {

struct NormalizingTokenizer::StateT {
  icu::UnicodeString data;
  icu::UnicodeString token;
  std::string term_buf;
  const icu::Normalizer2* normalizer{};  // reusable object owned by ICU
  std::unique_ptr<icu::Transliterator> transliterator;
  const Options options;

  explicit StateT(Options opts) : options{std::move(opts)} {}
};

void NormalizingTokenizer::StateDeleterT::operator()(StateT* p) const noexcept {
  delete p;
}

NormalizingTokenizer::NormalizingTokenizer(Options options)
  : _state{new StateT{std::move(options)}} {
  const std::string_view lang = _state->options.locale.getLanguage();
  _ascii_case_safe = lang != "tr" && lang != "az" && lang != "lt";
}

// ASCII values are NFC-invariant and carry no nonspacing marks, so
// normalization and accent stripping are identity; case conversion stays
// within ASCII except under the locale-tailored case mappings (tr/az dotted
// I, lt accent preservation), which keep the unicode path.
bool NormalizingTokenizer::AsciiFastEligible(
  std::string_view value) const noexcept {
  return _ascii_case_safe && !_force_unicode &&
         simdutf::validate_ascii(value.data(), value.size());
}

bool NormalizingTokenizer::AsciiRewrite(std::string_view value,
                                        std::string& out) const {
  if (!AsciiFastEligible(value)) {
    return false;
  }
  if (value.empty()) {
    out.clear();
    return true;
  }
  out.resize(value.size());
  switch (_state->options.case_convert) {
    case Case::Lower:
      absl::ascii_internal::AsciiStrToLower(out.data(), value.data(),
                                            value.size());
      break;
    case Case::Upper:
      absl::ascii_internal::AsciiStrToUpper(out.data(), value.data(),
                                            value.size());
      break;
    case Case::None:
      std::memcpy(out.data(), value.data(), value.size());
      break;
  }
  return true;
}

template<TokenLayout Layout>
void NormalizingTokenizer::AsciiEmit(TokenEmitter& sink,
                                     std::string_view value) {
  auto& buf = sink.buf;
  const auto size = static_cast<uint32_t>(value.size());
  const auto i = sink.Next();
  if (_state->options.case_convert == Case::None) {
    buf.terms[i] = MakeTermView(value.data(), size);
  } else {
    char inline_buf[duckdb::string_t::INLINE_LENGTH];
    char* out = size <= duckdb::string_t::INLINE_LENGTH
                  ? inline_buf
                  : reinterpret_cast<char*>(sink.Reserve(size));
    if (_state->options.case_convert == Case::Lower) {
      absl::ascii_internal::AsciiStrToLower(out, value.data(), size);
    } else {
      absl::ascii_internal::AsciiStrToUpper(out, value.data(), size);
    }
    buf.terms[i] = MakeTermView(out, size);
  }
  if constexpr (Layout == TokenLayout::TermsPosOffs) {
    buf.offs_start[i] = 0;
    buf.offs_end[i] = size;
  }
}

Tokenizer::ptr NormalizingTokenizer::Make(Options opts) {
  if (opts.locale.isBogus()) {
    THROW_SQL_ERROR(ERR_MSG("norm: invalid locale"));
  }
  return std::make_unique<NormalizingTokenizer>(std::move(opts));
}

bool NormalizingTokenizer::Normalize(std::string_view data) {
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
    case Case::Lower:
      _state->token.toLower(_state->options.locale);  // inplace case-conversion
      break;
    case Case::Upper:
      _state->token.toUpper(_state->options.locale);  // inplace case-conversion
      break;
    case Case::None:
      break;
  }

  // collate value, e.g. remove accents
  if (_state->transliterator) {
    _state->transliterator->transliterate(
      _state->token);  // inplace translitiration
  }

  _state->term_buf.clear();
  _state->token.toUTF8String(_state->term_buf);
  _input_size = static_cast<uint32_t>(data.size());

  return true;
}

template<TokenLayout Layout>
bool NormalizingTokenizer::DoFill(std::string_view value, TokenEmitter& sink) {
  if (AsciiFastEligible(value)) {
    AsciiEmit<Layout>(sink, value);
    return true;
  }
  if (!Normalize(value)) {
    sink.buf.one_to_one = false;
    return false;
  }
  sink.EmitInterned<Layout>(
    ViewCast<byte_type>(std::string_view{_state->term_buf}), 0, _input_size);
  return true;
}

template class TypedTokenizer<NormalizingTokenizer>;

}  // namespace irs::analysis
