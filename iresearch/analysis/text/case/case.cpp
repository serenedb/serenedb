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

#include <text_locale.hpp>

namespace irs::analysis::casing {

using duckdb::text::CaseLocale;
using duckdb::text::Locale;

bool AsciiCaseSafe(const char* locale_name) noexcept {
  const auto case_locale = Locale::CaseLocaleOf(locale_name);
  return case_locale != CaseLocale::TURKISH &&
         case_locale != CaseLocale::LITHUANIAN;
}

bool SimpleCaseSafe(const char* locale_name) noexcept {
  const auto case_locale = Locale::CaseLocaleOf(locale_name);
  return case_locale != CaseLocale::TURKISH &&
         case_locale != CaseLocale::LITHUANIAN &&
         case_locale != CaseLocale::GREEK;
}

}  // namespace irs::analysis::casing
