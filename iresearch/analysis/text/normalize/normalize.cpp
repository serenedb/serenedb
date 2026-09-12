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

#include "iresearch/analysis/text/normalize/normalize.hpp"

#include <utf8proc.hpp>

namespace irs::analysis::normalize {

void StripNonspacingMarks(std::string_view in, std::string& out) {
  out.clear();
  out.reserve(in.size());
  const auto* it = reinterpret_cast<const byte_type*>(in.data());
  const auto* end = it + in.size();
  while (it != end) {
    const auto* cp_start = it;
    const uint32_t cp = utf8_utils::ToChar32(it, end);
    if (cp != utf8_utils::kInvalidChar32 &&
        duckdb::utf8proc_category(static_cast<utf8proc_int32_t>(cp)) ==
          duckdb::UTF8PROC_CATEGORY_MN) {
      continue;
    }
    out.append(reinterpret_cast<const char*>(cp_start), it - cp_start);
  }
}

}  // namespace irs::analysis::normalize
