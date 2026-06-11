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

#include <unicode/normalizer2.h>  // for icu::Normalizer2
#include <unicode/translit.h>     // for icu::Transliterator

#include <string_view>

#include "basics/exceptions.h"
#include "iresearch/analysis/tokenizer.hpp"

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
  : _state{new StateT{std::move(options)}}, _term_eof{true} {}

Analyzer::ptr NormalizingTokenizer::Make(Options opts) {
  if (opts.locale.isBogus()) {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER, "norm: invalid locale");
  }
  return std::make_unique<NormalizingTokenizer>(std::move(opts));
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

  // use the normalized value
  static_assert(sizeof(byte_type) == sizeof(char));
  std::get<TermAttr>(_attrs).value =
    ViewCast<byte_type>(std::string_view{_state->term_buf});
  auto& offset = std::get<OffsAttr>(_attrs);
  offset.start = 0;
  offset.end = static_cast<uint32_t>(data.size());
  _term_eof = false;

  return true;
}

}  // namespace irs::analysis
