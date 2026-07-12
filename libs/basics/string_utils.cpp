////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include "string_utils.h"

#include <absl/strings/ascii.h>
#include <absl/strings/escaping.h>
#include <absl/strings/internal/memutil.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <fast_float/fast_float.h>
#include <math.h>
#include <stdlib.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <regex>
#include <system_error>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/log.h"
#include "basics/sink.h"
#include "basics/static_strings.h"
#include "pg/sql_exception_macro.h"

namespace {

// TODO(gnusi) escaping from simdjson!
static char constexpr kEscapeTable[256] = {
  // 0    1    2    3    4    5    6    7    8    9    A    B    C    D    E
  // F
  'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'b', 't', 'n', 'u', 'f', 'r',  'u',
  'u',  // 00
  'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u', 'u',  'u',
  'u',  // 10
  0,   0,   '"', 0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  '/',  // 20
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,  // 30~4F
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   '\\', 0,
  0,   0,  // 50
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,  // 60~FF
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,   0,    0,
  0,   0,   0,   0,   0,   0,   0,   0,   0,
};

bool IsVowel(char c) noexcept {
  switch (c) {
    case 'a':
    case 'e':
    case 'i':
    case 'o':
    case 'u':
      return true;
    default:
      return false;
  }
}

}  // namespace
namespace sdb::basics::string_utils {

std::string_view Trim(std::string_view source_str, std::string_view trim_str) {
  auto s = source_str.find_first_not_of(trim_str);
  if (s == std::string_view::npos) {
    return {};
  }
  auto e = source_str.find_last_not_of(trim_str);
  return {source_str.data() + s, e - s + 1};
}

void TrimInPlace(std::string& str, std::string_view trim_str) {
  auto s = str.find_first_not_of(trim_str);
  if (s == std::string_view::npos) {
    str.clear();
    return;
  }
  auto e = str.find_last_not_of(trim_str);
  if (s == 0) {
    str.erase(e + 1);
  } else {
    str = str.substr(s, e - s + 1);
  }
}

std::string_view RTrim(std::string_view source_str, std::string_view trim_str) {
  auto e = source_str.find_last_not_of(trim_str);
  return {source_str.data(), e + 1};
}

std::string Itoa(int32_t attr) { return absl::StrCat(attr); }

std::string Itoa(int64_t attr) { return absl::StrCat(attr); }

std::string Itoa(uint64_t attr) { return absl::StrCat(attr); }

uint64_t Uint64(const char* value, size_t size) noexcept {
  uint64_t result = 0;
  fast_float::from_chars(value, value + size, result);
  return result;
}

uint32_t Uint32(const char* value, size_t size) noexcept {
  uint32_t result = 0;
  fast_float::from_chars(value, value + size, result);
  return result;
}

std::string EncodeHex(const char* value, size_t length) {
  return absl::BytesToHexString({value, length});
}

std::string FormatSize(uint64_t value) {
  std::string out, label;
  if (value < 1000) {
    if (value == 1) {
      out = "1";
      label = "byte";
    } else {
      out = std::to_string(value);
      label = "bytes";
    }
  } else if (value < 1000'000ULL) {
    out = std::to_string((double)value / double(1000));
    label = "KB";
  } else if (value < 1'000'000'000ULL) {
    out = std::to_string((double)value / 1e6);
    label = "MB";
  } else if (value < 1'000'000'000'000ULL) {
    out = std::to_string((double)value / 1e9);
    label = "GB";
  } else if (value < 1'000'000'000'000'000ULL) {
    out = std::to_string((double)value / 1e12);
    label = "TB";
  }
  // Normalise the decimal separator in case the C locale ever flips on us.
  if (auto comma = out.find(','); comma != std::string::npos) {
    out[comma] = '.';
  }
  auto pos = out.find('.');
  if (pos != std::string::npos) {
    out.resize(pos + 2);
  }
  return out + ' ' + label;
}

// TODO(gnusi) use more specialized error codes?
template<typename Sink>
void EscapeJsonStr(std::string_view str, Sink* sink,
                   EscapeJsonOptions options) {
  SDB_ASSERT(sink);

  auto dump_unicode_char = [&](uint16_t value) {
    sink->PushStr("\\u");

    uint16_t p;
    p = (value & 0xf000U) >> 12;
    sink->PushChr((p < 10) ? ('0' + p) : ('A' + p - 10));

    p = (value & 0x0f00U) >> 8;
    sink->PushChr((p < 10) ? ('0' + p) : ('A' + p - 10));

    p = (value & 0x00f0U) >> 4;
    sink->PushChr((p < 10) ? ('0' + p) : ('A' + p - 10));

    p = (value & 0x000fU);
    sink->PushChr((p < 10) ? ('0' + p) : ('A' + p - 10));
  };

  auto len = str.size();
  auto src = str.data();

  const uint8_t* p = reinterpret_cast<const uint8_t*>(src);
  const uint8_t* e = p + len;
  while (p < e) {
    uint8_t c = *p;

    if ((c & 0x80U) == 0) {
      // check for control characters
      char esc = kEscapeTable[c];

      if (esc) {
        if (options.escape_control) {
          if (c != '/' || options.escape_forward_slashes) {
            // escape forward slashes only when requested
            sink->PushChr('\\');
          }
          sink->PushChr(static_cast<char>(esc));
          if (esc == 'u') {
            uint16_t i1 = (((uint16_t)c) & 0xf0U) >> 4;
            uint16_t i2 = (((uint16_t)c) & 0x0fU);

            sink->PushStr("00");
            sink->PushChr(
              static_cast<char>((i1 < 10) ? ('0' + i1) : ('A' + i1 - 10)));
            sink->PushChr(
              static_cast<char>((i2 < 10) ? ('0' + i2) : ('A' + i2 - 10)));
          }
        } else {
          if (esc == '"' || esc == '/' || esc == '\\') {
            sink->PushChr('\\');
            sink->PushChr(static_cast<char>(esc));
          } else {
            sink->PushChr(' ');
          }
        }
      } else {
        sink->PushChr(static_cast<char>(c));
      }
    } else if ((c & 0xe0U) == 0xc0U) {
      // two-byte sequence
      if (p + 1 >= e) {
        THROW_SQL_ERROR(ERR_MSG("invalid UTF-8 sequence"));
      }

      if (options.escape_unicode) {
        uint16_t value =
          ((((uint16_t)*p & 0x1fU) << 6) | ((uint16_t)*(p + 1) & 0x3fU));
        dump_unicode_char(value);

      } else {
        sink->PushStr({reinterpret_cast<const char*>(p), 2});
      }
      ++p;
    } else if ((c & 0xf0U) == 0xe0U) {
      // three-byte sequence
      if (p + 2 >= e) {
        THROW_SQL_ERROR(ERR_MSG("invalid UTF-8 sequence"));
      }

      if (options.escape_unicode) {
        uint16_t value =
          ((((uint16_t)*p & 0x0fU) << 12) |
           (((uint16_t)*(p + 1) & 0x3fU) << 6) | ((uint16_t)*(p + 2) & 0x3fU));
        dump_unicode_char(value);
      } else {
        sink->PushStr({reinterpret_cast<const char*>(p), 3});
      }
      p += 2;
    } else if ((c & 0xf8U) == 0xf0U) {
      // four-byte sequence
      if (p + 3 >= e) {
        THROW_SQL_ERROR(ERR_MSG("invalid UTF-8 sequence"));
      }

      if (options.escape_unicode) {
        uint32_t value =
          ((((uint32_t)*p & 0x0fU) << 18) |
           (((uint32_t)*(p + 1) & 0x3fU) << 12) |
           (((uint32_t)*(p + 2) & 0x3fU) << 6) | ((uint32_t)*(p + 3) & 0x3fU));
        // construct the surrogate pairs
        value -= 0x10000U;
        uint16_t high = (uint16_t)(((value & 0xffc00U) >> 10) + 0xd800);
        dump_unicode_char(high);
        uint16_t low = (value & 0x3ffU) + 0xdc00U;
        dump_unicode_char(low);
      } else {
        sink->PushStr({reinterpret_cast<const char*>(p), 4});
      }
      p += 3;
    }

    ++p;
  }
}

template void EscapeJsonStr<StrSink>(std::string_view str, StrSink* sink,
                                     EscapeJsonOptions options);
template void EscapeJsonStr<LenSink>(std::string_view str, LenSink* sink,
                                     EscapeJsonOptions options);
template void EscapeJsonStr<CordSink>(std::string_view str, CordSink* sink,
                                      EscapeJsonOptions options);

std::string_view GetArticle(std::string_view word) noexcept {
  if (word.empty()) {
    return "";
  }

  if (IsVowel(absl::ascii_tolower(word.front()))) {
    return "an";
  }

  return "a";
}

std::string GetPluralFormLowerCase(std::string_view word) {
  if (word.empty()) {
    return {};
  }

  std::string lower;
  lower.reserve(word.size() + 2);
  absl::StrAppend(&lower, absl::AsciiStrToLower(word));

  if (lower.ends_with('s') || lower.ends_with('x') || lower.ends_with('z') ||
      lower.ends_with("sh") || lower.ends_with("ch")) {
    absl::StrAppend(&lower, "es");
    return lower;
  }

  if (lower.size() >= 2 && lower.ends_with('y') &&
      !IsVowel(lower[lower.size() - 2])) {
    lower.back() = 'i';
    absl::StrAppend(&lower, "es");
    return lower;
  }

  lower.push_back('s');
  return lower;
}

}  // namespace sdb::basics::string_utils
