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

#include "iresearch/analysis/text/case/case.hpp"

#include "ucase.h"

namespace irs::analysis::casing {

// ASCII values are NFC-invariant and carry no nonspacing marks, so
// normalization and accent stripping are identity; case conversion stays
// within ASCII except under the locale-tailored case mappings (tr/az dotted
// I, lt accent preservation), which keep the unicode path.
bool AsciiCaseSafe(const char* locale_name) noexcept {
  const auto case_locale = ucase_getCaseLocale(locale_name);
  return case_locale != UCASE_LOC_TURKISH &&
         case_locale != UCASE_LOC_LITHUANIAN;
}

// Locales whose ICU case mappings are tailored beyond the locale-independent
// simple-case table (tr/az dotted I, lt dot above, el uppercasing) keep the
// ICU path; everywhere else simple case is the accepted drift class.
bool SimpleCaseSafe(const char* locale_name) noexcept {
  const auto case_locale = ucase_getCaseLocale(locale_name);
  return case_locale != UCASE_LOC_TURKISH &&
         case_locale != UCASE_LOC_LITHUANIAN && case_locale != UCASE_LOC_GREEK;
}

}  // namespace irs::analysis::casing
