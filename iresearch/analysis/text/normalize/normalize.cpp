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

#include <optional>
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

namespace {

struct TwoByteStrip {
  static constexpr uint32_t kIdentity = ~uint32_t{0};

  std::array<uint32_t, 0x800> entries;
  std::string blob;
};

template<sz_normal_form_t Form>
TwoByteStrip BuildTwoByteStrip() {
  TwoByteStrip table;
  table.entries.fill(TwoByteStrip::kIdentity);
  std::string decomposed;
  std::string stripped;
  std::string composed;
  for (uint32_t cp = 0x80; cp < 0x800; ++cp) {
    const char utf8[] = {static_cast<char>(0xC0 | (cp >> 6)),
                         static_cast<char>(0x80 | (cp & 0x3F))};
    const std::string_view in{utf8, sizeof utf8};
    decomposed.resize(Bound<Form>(in.size()));
    decomposed.resize(Decompose<Form>(in, decomposed.data()));
    StripNonspacingMarks(decomposed, stripped);
    composed.resize(Bound<Form>(stripped.size()));
    composed.resize(Compose<Form>(stripped, composed.data()));
    if (composed != in) {
      table.entries[cp] = static_cast<uint32_t>(table.blob.size() << 8) |
                          static_cast<uint32_t>(composed.size());
      table.blob += composed;
    }
  }
  return table;
}

template<sz_normal_form_t Form>
const TwoByteStrip& TwoByteStripOf() {
  static const auto kTable = BuildTwoByteStrip<Form>();
  return kTable;
}

}  // namespace

template<sz_normal_form_t Form>
StripResult StripTwoByte(std::string_view in, std::string& out) {
  const auto& table = TwoByteStripOf<Form>();
  const auto* p = reinterpret_cast<const uint8_t*>(in.data());
  const size_t n = in.size();
  const auto entry = [&](size_t i) -> std::optional<uint32_t> {
    const uint8_t lead = p[i];
    if (lead < 0xC2 || lead > 0xDF || i + 1 == n ||
        (p[i + 1] & 0xC0) != 0x80) {
      return std::nullopt;
    }
    return table.entries[((lead & 0x1Fu) << 6) | (p[i + 1] & 0x3Fu)];
  };
  size_t i = 0;
  for (; i < n; ++i) {
    if (p[i] < 0x80) {
      continue;
    }
    const auto e = entry(i);
    if (!e) {
      return StripResult::Unsupported;
    }
    if (*e != TwoByteStrip::kIdentity) {
      break;
    }
    ++i;
  }
  if (i == n) {
    return StripResult::Unchanged;
  }
  out.assign(in.data(), i);
  while (i < n) {
    if (p[i] < 0x80) {
      out.push_back(static_cast<char>(p[i++]));
      continue;
    }
    const auto e = entry(i);
    if (!e) {
      return StripResult::Unsupported;
    }
    if (*e == TwoByteStrip::kIdentity) {
      out.append(in.data() + i, 2);
    } else {
      out.append(table.blob.data() + (*e >> 8), *e & 0xFF);
    }
    i += 2;
  }
  return StripResult::Stripped;
}

template StripResult StripTwoByte<sz_normal_form_nfc_k>(std::string_view,
                                                        std::string&);
template StripResult StripTwoByte<sz_normal_form_nfkc_k>(std::string_view,
                                                         std::string&);

}  // namespace irs::analysis::normalize
