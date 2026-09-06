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

#pragma once

#include <unicode/locid.h>
#include <unicode/normalizer2.h>
#include <unicode/translit.h>
#include <unicode/unistr.h>

#include <memory>

#include "iresearch/analysis/tokenizer.hpp"

namespace irs::analysis::normalize {

inline std::unique_ptr<icu::Transliterator> MakeStripTransliterator(
  bool nfkc, UErrorCode& err) {
  const icu::UnicodeString rule(nfkc ? "NFKD; [:Nonspacing Mark:] Remove; NFKC"
                                     : "NFD; [:Nonspacing Mark:] Remove; NFC");
  return std::unique_ptr<icu::Transliterator>{
    icu::Transliterator::createInstance(rule, UTransDirection::UTRANS_FORWARD,
                                        err)};
}

template<Case C>
void NormalizeCaseStrip(const icu::Normalizer2& normalizer,
                        const icu::Locale& locale, icu::Transliterator* strip,
                        const icu::UnicodeString& data,
                        icu::UnicodeString& out) {
  auto err = UErrorCode::U_ZERO_ERROR;
  normalizer.normalize(data, out, err);
  if (!U_SUCCESS(err)) {
    out = data;
  }
  if constexpr (C == Case::Lower) {
    out.toLower(locale);
  } else if constexpr (C == Case::Upper) {
    out.toUpper(locale);
  }
  if (strip != nullptr) {
    strip->transliterate(out);
  }
}

}  // namespace irs::analysis::normalize
