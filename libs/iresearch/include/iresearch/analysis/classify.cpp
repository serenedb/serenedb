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

#include "iresearch/analysis/classify.hpp"

#include "iresearch/utils/utf8_utils.hpp"
#include "ucase.h"

namespace irs {

void BuildUtf8CpBounds(const byte_type* data, size_t size, bool valid_utf8,
                       std::vector<uint32_t>& out) {
  out.clear();
  if (valid_utf8) {
    size_t offset = 0;
    while (size - offset >= classify::kClassifyBlock) {
      VisitSetBits(ClassifyUtf8LeadBlock(data + offset), [&](uint32_t bit) {
        out.push_back(static_cast<uint32_t>(offset + bit));
      });
      offset += classify::kClassifyBlock;
    }
    for (; offset < size; ++offset) {
      if ((data[offset] & 0xC0) != 0x80) {
        out.push_back(static_cast<uint32_t>(offset));
      }
    }
  } else {
    const auto* end = data + size;
    for (const auto* it = data; it != end; it = utf8_utils::Next(it, end)) {
      out.push_back(static_cast<uint32_t>(it - data));
    }
  }
  out.push_back(static_cast<uint32_t>(size));
}

// ASCII values are NFC-invariant and carry no nonspacing marks, so
// normalization and accent stripping are identity; case conversion stays
// within ASCII except under the locale-tailored case mappings (tr/az dotted
// I, lt accent preservation), which keep the unicode path.
bool AsciiCaseSafe(const char* locale_name) noexcept {
  const auto case_locale = ucase_getCaseLocale(locale_name);
  return case_locale != UCASE_LOC_TURKISH &&
         case_locale != UCASE_LOC_LITHUANIAN;
}

}  // namespace irs
