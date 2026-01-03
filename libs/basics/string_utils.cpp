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

#include "basics/arithmetic.h"
#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/sink.h"
#include "basics/static_strings.h"

namespace sdb {

int CompareIgnoreCase(std::string_view lhs, std::string_view rhs) noexcept {
  auto size = std::min(lhs.size(), rhs.size());
  if (int r =
        absl::strings_internal::memcasecmp(lhs.data(), rhs.data(), size)) {
    return r;
  }
  if (lhs.size() != rhs.size()) {
    return lhs.size() < rhs.size() ? -1 : 1;
  }
  return 0;
}

}  // namespace sdb

namespace {

static const char* gHexValuesUpper = "0123456789ABCDEF";

char SoundexCode(char c) {
  switch (c) {
    case 'b':
    case 'f':
    case 'p':
    case 'v':
      return '1';
    case 'c':
    case 'g':
    case 'j':
    case 'k':
    case 'q':
    case 's':
    case 'x':
    case 'z':
      return '2';
    case 'd':
    case 't':
      return '3';
    case 'l':
      return '4';
    case 'm':
    case 'n':
      return '5';
    case 'r':
      return '6';
    default:
      return '\0';
  }
}

unsigned char Consume(const char*& s) {
  return *reinterpret_cast<const unsigned char*>(s++);
}

template<typename InputType>
inline bool IsEqual(const InputType& c1, const InputType& c2) {
  return c1 == c2;
}

template<typename InputType, typename LengthType>
LengthType Levenshtein(const InputType* lhs, const InputType* rhs,
                       LengthType lhs_size, LengthType rhs_size) {
  SDB_ASSERT(lhs_size >= rhs_size);

  std::vector<LengthType> costs;
  costs.resize(rhs_size + 1);

  for (LengthType i = 0; i < rhs_size; ++i) {
    costs[i] = i;
  }

  LengthType next = 0;

  for (LengthType i = 0; i < lhs_size; ++i) {
    LengthType current = i + 1;

    for (LengthType j = 0; j < rhs_size; ++j) {
      LengthType cost = !(::IsEqual<InputType>(lhs[i], rhs[j]) ||
                          (i && j && ::IsEqual<InputType>(lhs[i - 1], rhs[j]) &&
                           ::IsEqual<InputType>(lhs[i], rhs[j - 1])));
      next = std::min(std::min(costs[j + 1] + 1, current + 1), costs[j] + cost);
      costs[j] = current;
      current = next;
    }
    costs[rhs_size] = next;
  }
  return next;
}

size_t LevenshteinDistance(std::vector<uint32_t>& vect1,
                           std::vector<uint32_t>& vect2) {
  if (vect1.empty() || vect2.empty()) {
    return vect1.size() ? vect1.size() : vect2.size();
  }

  if (vect1.size() < vect2.size()) {
    vect1.swap(vect2);
  }

  size_t lhs_size = vect1.size();
  size_t rhs_size = vect2.size();

  const uint32_t* l = vect1.data();
  const uint32_t* r = vect2.data();

  if (lhs_size < std::numeric_limits<uint8_t>::max()) {
    return static_cast<size_t>(::Levenshtein<uint32_t, uint8_t>(
      l, r, static_cast<uint8_t>(lhs_size), static_cast<uint8_t>(rhs_size)));
  } else if (lhs_size < std::numeric_limits<uint16_t>::max()) {
    return static_cast<size_t>(::Levenshtein<uint32_t, uint16_t>(
      l, r, static_cast<uint16_t>(lhs_size), static_cast<uint16_t>(rhs_size)));
  } else if (lhs_size < std::numeric_limits<uint32_t>::max()) {
    return static_cast<size_t>(::Levenshtein<uint32_t, uint32_t>(
      l, r, static_cast<uint32_t>(lhs_size), static_cast<uint32_t>(rhs_size)));
  }
  return static_cast<size_t>(::Levenshtein<uint32_t, uint64_t>(
    l, r, static_cast<uint64_t>(lhs_size), static_cast<uint64_t>(rhs_size)));
}

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

}  // namespace

namespace sdb::basics::string_utils {

std::string EscapeUnicode(std::string_view value, bool escape_slash) {
  size_t len = value.length();

  if (len == 0) {
    return std::string(value);
  }

  if (len >= (SIZE_MAX - 1) / 6) {
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }

  bool corrupted = false;

  auto buffer = std::make_unique<char[]>(6 * len + 1);
  char* qtr = buffer.get();
  const char* ptr = value.data();
  const char* end = ptr + len;

  for (; ptr < end; ++ptr, ++qtr) {
    switch (*ptr) {
      case '/':
        if (escape_slash) {
          *qtr++ = '\\';
        }

        *qtr = *ptr;
        break;

      case '\\':
      case '"':
        *qtr++ = '\\';
        *qtr = *ptr;
        break;

      case '\b':
        *qtr++ = '\\';
        *qtr = 'b';
        break;

      case '\f':
        *qtr++ = '\\';
        *qtr = 'f';
        break;

      case '\n':
        *qtr++ = '\\';
        *qtr = 'n';
        break;

      case '\r':
        *qtr++ = '\\';
        *qtr = 'r';
        break;

      case '\t':
        *qtr++ = '\\';
        *qtr = 't';
        break;

      case '\0':
        *qtr++ = '\\';
        *qtr++ = 'u';
        *qtr++ = '0';
        *qtr++ = '0';
        *qtr++ = '0';
        *qtr = '0';
        break;

      default: {
        uint8_t c = (uint8_t)*ptr;

        // character is in the normal latin1 range
        if ((c & 0x80) == 0) {
          // special character, escape
          if (c < 32) {
            *qtr++ = '\\';
            *qtr++ = 'u';
            *qtr++ = '0';
            *qtr++ = '0';

            uint16_t i1 = (static_cast<uint16_t>(c) & 0xF0) >> 4;
            uint16_t i2 = (static_cast<uint16_t>(c) & 0x0F);

            *qtr++ = (i1 < 10) ? ('0' + i1) : ('A' + i1 - 10);
            *qtr = (i2 < 10) ? ('0' + i2) : ('A' + i2 - 10);
          }

          // normal latin1
          else {
            *qtr = *ptr;
          }
        }

        // unicode range 0080 - 07ff
        else if ((c & 0xE0) == 0xC0) {
          if (ptr + 1 < end) {
            uint8_t d = (uint8_t)*(ptr + 1);

            // correct unicode
            if ((d & 0xC0) == 0x80) {
              ++ptr;

              *qtr++ = '\\';
              *qtr++ = 'u';

              uint16_t n = ((c & 0x1F) << 6) | (d & 0x3F);

              uint16_t i1 = (n & 0xF000) >> 12;
              uint16_t i2 = (n & 0x0F00) >> 8;
              uint16_t i3 = (n & 0x00F0) >> 4;
              uint16_t i4 = (n & 0x000F);

              *qtr++ = (i1 < 10) ? ('0' + i1) : ('A' + i1 - 10);
              *qtr++ = (i2 < 10) ? ('0' + i2) : ('A' + i2 - 10);
              *qtr++ = (i3 < 10) ? ('0' + i3) : ('A' + i3 - 10);
              *qtr = (i4 < 10) ? ('0' + i4) : ('A' + i4 - 10);
            }

            // corrupted unicode
            else {
              *qtr = *ptr;
              corrupted = true;
            }
          }

          // corrupted unicode
          else {
            *qtr = *ptr;
            corrupted = true;
          }
        }

        // unicode range 0800 - ffff
        else if ((c & 0xF0) == 0xE0) {
          if (ptr + 1 < end) {
            uint8_t d = (uint8_t)*(ptr + 1);

            // correct unicode
            if ((d & 0xC0) == 0x80) {
              if (ptr + 2 < end) {
                uint8_t e = (uint8_t)*(ptr + 2);

                // correct unicode
                *qtr = *ptr;
                if ((e & 0xC0) != 0x80) {
                  corrupted = true;
                }
              }
              // corrupted unicode
              else {
                *qtr = *ptr;
                corrupted = true;
              }
            }
            // corrupted unicode
            else {
              *qtr = *ptr;
              corrupted = true;
            }
          }
          // corrupted unicode
          else {
            *qtr = *ptr;
            corrupted = true;
          }

        }

        // unicode range 010000 - 10ffff -- NOT IMPLEMENTED
        else {
          *qtr = *ptr;
        }
      }

      break;
    }
  }

  *qtr = '\0';

  std::string result(buffer.get(), qtr - buffer.get());

  if (corrupted) {
    SDB_DEBUG("xxxxx", Logger::FIXME, "escaped corrupted unicode string");
  }

  return result;
}

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

std::string_view LTrim(std::string_view source_str, std::string_view trim_str) {
  auto s = source_str.find_first_not_of(trim_str);
  if (s == std::string_view::npos) {
    return {};
  }
  source_str.remove_prefix(s);
  return source_str;
}

std::string_view RTrim(std::string_view source_str, std::string_view trim_str) {
  auto e = source_str.find_last_not_of(trim_str);
  return {source_str.data(), e + 1};
}

std::string Replace(std::string_view source_str, std::string_view from_str,
                    std::string_view to_str) {
  return absl::StrReplaceAll(source_str, {{from_str, to_str}});
}

std::string UrlDecodePath(std::string_view str) {
  std::string result;
  // reserve enough room so we do not need to re-alloc
  result.reserve(str.size() + 16);

  const char* src = str.data();
  const char* end = src + str.size();

  while (src < end) {
    if (*src == '%') {
      if (src + 2 < end) {
        int h1 = Hex2int(src[1], -1);
        int h2 = Hex2int(src[2], -1);

        if (h1 == -1) {
          ++src;
        } else {
          if (h2 == -1) {
            result.push_back(h1);
            src += 2;
          } else {
            result.push_back(h1 << 4 | h2);
            src += 3;
          }
        }
      } else if (src + 1 < end) {
        int h1 = Hex2int(src[1], -1);

        if (h1 == -1) {
          ++src;
        } else {
          result.push_back(h1);
          src += 2;
        }
      } else {
        ++src;
      }
    } else {
      result.push_back(*src);
      ++src;
    }
  }

  return result;
}

std::string UrlDecode(std::string_view str) {
  std::string result;
  // reserve enough room so we do not need to re-alloc
  result.reserve(str.size() + 16);

  const char* src = str.data();
  const char* end = src + str.size();

  for (; src < end && *src != '%'; ++src) {
    if (*src == '+') {
      result.push_back(' ');
    } else {
      result.push_back(*src);
    }
  }

  while (src < end) {
    if (src + 2 < end) {
      int h1 = Hex2int(src[1], -1);
      int h2 = Hex2int(src[2], -1);

      if (h1 == -1) {
        src += 1;
      } else {
        if (h2 == -1) {
          result.push_back(h1);
          src += 2;
        } else {
          result.push_back(h1 << 4 | h2);
          src += 3;
        }
      }
    } else if (src + 1 < end) {
      int h1 = Hex2int(src[1], -1);

      if (h1 == -1) {
        src += 1;
      } else {
        result.push_back(h1);
        src += 2;
      }
    } else {
      src += 1;
    }

    for (; src < end && *src != '%'; ++src) {
      if (*src == '+') {
        result.push_back(' ');
      } else {
        result.push_back(*src);
      }
    }
  }

  return result;
}

std::string UrlEncode(const char* src, const size_t len) {
  static char gHexChars[16] = {'0', '1', '2', '3', '4', '5', '6', '7',
                               '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

  const char* end = src + len;

  if (len >= (SIZE_MAX - 1) / 3) {
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }

  std::string result;
  result.reserve(3 * len);

  for (; src < end; ++src) {
    if ('0' <= *src && *src <= '9') {
      result.push_back(*src);
    }

    else if ('a' <= *src && *src <= 'z') {
      result.push_back(*src);
    }

    else if ('A' <= *src && *src <= 'Z') {
      result.push_back(*src);
    }

    else if (*src == '-' || *src == '_' || *src == '~') {
      result.push_back(*src);
    }

    else {
      uint8_t n = (uint8_t)(*src);
      uint8_t n1 = n >> 4;
      uint8_t n2 = n & 0x0F;

      result.push_back('%');
      result.push_back(gHexChars[n1]);
      result.push_back(gHexChars[n2]);
    }
  }

  return result;
}

void EncodeUriComponent(std::string& result, const char* src, size_t len) {
  const char* end = src + len;

  if (result.size() + len >= (SIZE_MAX - 1) / 3) {
    SDB_THROW(ERROR_OUT_OF_MEMORY);
  }

  result.reserve(result.size() + 3 * len);

  for (; src < end; ++src) {
    if (*src == '-' || *src == '_' || *src == '.' || *src == '!' ||
        *src == '~' || *src == '*' || *src == '(' || *src == ')' ||
        *src == '\'' || (*src >= 'a' && *src <= 'z') ||
        (*src >= 'A' && *src <= 'Z') || (*src >= '0' && *src <= '9')) {
      // no need to encode this character
      result.push_back(*src);
    } else {
      // hex-encode the following character
      result.push_back('%');
      auto c = static_cast<unsigned char>(*src);
      result.push_back(::gHexValuesUpper[c >> 4]);
      result.push_back(::gHexValuesUpper[c % 16]);
    }
  }
}

static std::string Soundex(const char* src, size_t len) {
  const char* end = src + len;

  while (src < end) {
    // skip over characters (e.g. whitespace and other non-ASCII letters)
    // until we find something sensible
    if ((*src >= 'a' && *src <= 'z') || (*src >= 'A' && *src <= 'Z')) {
      break;
    }
    ++src;
  }

  std::string result;

  if (src != end) {
    // emit an upper-case character
    result.push_back(absl::ascii_toupper(*src));
    src++;
    char previous_code = '\0';

    while (src < end) {
      char current_code = ::SoundexCode(*src);
      if (current_code != '\0' && current_code != previous_code) {
        result.push_back(current_code);
        if (result.length() >= 4) {
          break;
        }
      }
      previous_code = current_code;
      src++;
    }

    // pad result string with '0' chars up to a length of 4
    while (result.length() < 4) {
      result.push_back('0');
    }
  }

  return result;
}

std::string Soundex(std::string_view value) {
  return Soundex(value.data(), value.size());
}

unsigned int LevenshteinDistance(const char* s1, size_t l1, const char* s2,
                                 size_t l2) {
  // convert input strings to vectors of (multi-byte) character numbers
  std::vector<uint32_t> vect1 = CharacterCodes(s1, l1);
  std::vector<uint32_t> vect2 = CharacterCodes(s2, l2);

  // calculate levenshtein distance on vectors of character numbers
  return static_cast<unsigned int>(::LevenshteinDistance(vect1, vect2));
}

std::vector<uint32_t> CharacterCodes(const char* s, size_t length) {
  const char* e = s + length;

  std::vector<uint32_t> char_nums;
  // be conservative, and reserve space for one number of input
  // string byte. this may be too much, but it avoids later
  // reallocation of the vector
  char_nums.reserve(length);

  while (s < e) {
    // note: consume advances the *s* pointer by one byte
    unsigned char c = ::Consume(s);
    uint32_t n = uint32_t(c);

    if ((c & 0x80U) == 0U) {
      // single-byte character
      char_nums.push_back(n);
    } else if ((c & 0xE0U) == 0xC0U) {
      // two-byte character
      if (s >= e) {
        SDB_THROW(ERROR_INTERNAL, "invalid UTF-8 sequence");
      }
      char_nums.push_back((n << 8U) + uint32_t(::Consume(s)));
    } else if ((c & 0xF0U) == 0xE0U) {
      // three-byte character
      if (s + 1 >= e) {
        SDB_THROW(ERROR_INTERNAL, "invalid UTF-8 sequence");
      }
      char_nums.push_back((n << 16U) + (uint32_t(::Consume(s)) << 8U) +
                          (uint32_t(::Consume(s))));
    } else if ((c & 0xF8U) == 0XF0U) {
      // four-byte character
      if (s + 2 >= e) {
        SDB_THROW(ERROR_INTERNAL, "invalid UTF-8 sequence");
      }
      char_nums.push_back((n << 24U) + (uint32_t(::Consume(s)) << 16U) +
                          (uint32_t(::Consume(s)) << 8U) +
                          (uint32_t(::Consume(s))));
    } else {
      SDB_THROW(ERROR_INTERNAL, "invalid UTF-8 sequence");
    }
  }

  return char_nums;
}

std::string Itoa(int16_t attr) { return absl::StrCat(attr); }

std::string Itoa(uint16_t attr) { return absl::StrCat(attr); }

std::string Itoa(int32_t attr) { return absl::StrCat(attr); }

std::string Itoa(uint32_t attr) { return absl::StrCat(attr); }

std::string Itoa(int64_t attr) { return absl::StrCat(attr); }

std::string Itoa(uint64_t attr) { return absl::StrCat(attr); }

size_t Itoa(uint64_t attr, char* buffer) {
  return absl::numbers_internal::FastIntToBuffer(attr, buffer) - buffer;
}

void Itoa(uint64_t attr, std::string& out) {
  const auto old = out.size();
  basics::StrAppend(out, kIntStrMaxLen);
  char* end = absl::numbers_internal::FastIntToBuffer(attr, out.data() + old);
  out.erase(end - out.data());
}

bool Boolean(std::string_view str) {
  if (str.empty()) {
    return false;
  }
  str = Trim(str);
  if (str.empty() || str.size() > 5) {
    return false;
  }
  auto lower = absl::AsciiStrToLower(str);
  return lower == "true" || lower == "yes" || lower == "on" || lower == "y" ||
         lower == "1" || lower == "✓";
}

int64_t Int64(const char* value, size_t size) noexcept {
  int64_t result = 0;
  std::from_chars(value, value + size, result);
  return result;
}

uint64_t Uint64(const char* value, size_t size) noexcept {
  uint64_t result = 0;
  std::from_chars(value, value + size, result);
  return result;
}

ResultOr<uint64_t> TryUint64(const char* value, size_t size) noexcept {
  uint64_t result = 0;
  auto [ptr, ec] = std::from_chars(value, value + size, result, 10);
  if (ec == std::errc()) {
    if (ptr != value + size) {
      return std::unexpected<Result>{std::in_place, ERROR_ILLEGAL_NUMBER};
    }
    return result;
  }
  return std::unexpected<Result>{std::in_place, ERROR_ILLEGAL_NUMBER,
                                 make_error_condition(ec).message()};
}

ResultOr<uint64_t> TryUint64(std::string_view value) noexcept {
  return TryUint64(value.data(), value.size());
}

uint64_t Uint64Trusted(const char* value, size_t length) noexcept {
  uint64_t result = 0;

  switch (length) {
    case 20:
      result += (value[length - 20] - '0') * 10000000000000000000ULL;
      [[fallthrough]];
    case 19:
      result += (value[length - 19] - '0') * 1000000000000000000ULL;
      [[fallthrough]];
    case 18:
      result += (value[length - 18] - '0') * 100000000000000000ULL;
      [[fallthrough]];
    case 17:
      result += (value[length - 17] - '0') * 10000000000000000ULL;
      [[fallthrough]];
    case 16:
      result += (value[length - 16] - '0') * 1000000000000000ULL;
      [[fallthrough]];
    case 15:
      result += (value[length - 15] - '0') * 100000000000000ULL;
      [[fallthrough]];
    case 14:
      result += (value[length - 14] - '0') * 10000000000000ULL;
      [[fallthrough]];
    case 13:
      result += (value[length - 13] - '0') * 1000000000000ULL;
      [[fallthrough]];
    case 12:
      result += (value[length - 12] - '0') * 100000000000ULL;
      [[fallthrough]];
    case 11:
      result += (value[length - 11] - '0') * 10000000000ULL;
      [[fallthrough]];
    case 10:
      result += (value[length - 10] - '0') * 1000000000ULL;
      [[fallthrough]];
    case 9:
      result += (value[length - 9] - '0') * 100000000ULL;
      [[fallthrough]];
    case 8:
      result += (value[length - 8] - '0') * 10000000ULL;
      [[fallthrough]];
    case 7:
      result += (value[length - 7] - '0') * 1000000ULL;
      [[fallthrough]];
    case 6:
      result += (value[length - 6] - '0') * 100000ULL;
      [[fallthrough]];
    case 5:
      result += (value[length - 5] - '0') * 10000ULL;
      [[fallthrough]];
    case 4:
      result += (value[length - 4] - '0') * 1000ULL;
      [[fallthrough]];
    case 3:
      result += (value[length - 3] - '0') * 100ULL;
      [[fallthrough]];
    case 2:
      result += (value[length - 2] - '0') * 10ULL;
      [[fallthrough]];
    case 1:
      result += (value[length - 1] - '0');
  }

  return result;
}

int32_t Int32(const char* value, size_t size) noexcept {
  int32_t result = 0;
  std::from_chars(value, value + size, result);
  return result;
}

uint32_t Uint32(const char* value, size_t size) noexcept {
  uint32_t result = 0;
  std::from_chars(value, value + size, result);
  return result;
}

double DoubleDecimal(const char* value, size_t size) {
  double v = 0.0;
  double e = 1.0;

  bool seen_decimal_point = false;

  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(value);
  const uint8_t* end = ptr + size;

  // check for the sign first
  if (*ptr == '-') {
    e = -e;
    ++ptr;
  } else if (*ptr == '+') {
    ++ptr;
  }

  for (; ptr < end; ++ptr) {
    uint8_t n = *ptr;

    if (n == '.' && !seen_decimal_point) {
      seen_decimal_point = true;
      continue;
    }

    if ('9' < n || n < '0') {
      break;
    }

    v = v * 10.0 + (n - 48);

    if (seen_decimal_point) {
      e = e * 10.0;
    }
  }

  // we have reached the end without an exponent
  if (ptr == end) {
    return v / e;
  }

  // invalid decimal representation
  if (*ptr != 'e' && *ptr != 'E') {
    return 0.0;
  }

  ++ptr;  // move past the 'e' or 'E'

  int32_t exp_sign = 1;
  int32_t exp_value = 0;

  // is there an exponent sign?
  if (*ptr == '-') {
    exp_sign = -1;
    ++ptr;
  } else if (*ptr == '+') {
    ++ptr;
  }

  for (; ptr < end; ++ptr) {
    uint8_t n = *ptr;

    if ('9' < n || n < '0') {
      return 0.0;
    }

    exp_value = exp_value * 10 + (n - 48);
  }

  exp_value = exp_value * exp_sign;

  return (v / e) * pow(10.0, double(exp_value));
}

std::string EncodeHex(const char* value, size_t length) {
  return absl::BytesToHexString({value, length});
}

std::string DecodeHex(std::string_view value) {
  if (std::string r; absl::HexStringToBytes(value, &r)) {
    return r;
  }
  return {};
}

void EscapeRegexParams(std::string& out, const char* ptr, size_t length) {
  for (size_t i = 0; i < length; ++i) {
    const char c = ptr[i];
    if (c == '?' || c == '+' || c == '[' || c == '(' || c == ')' || c == '{' ||
        c == '}' || c == '^' || c == '$' || c == '|' || c == '.' || c == '*' ||
        c == '\\') {
      // character with special meaning in a regex
      out.push_back('\\');
    }
    out.push_back(c);
  }
}

std::string EscapeRegexParams(std::string_view in) {
  std::string out;
  EscapeRegexParams(out, in.data(), in.size());
  return out;
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
  out = Replace(out, ",", ".");
  auto pos = out.find('.');
  if (pos != std::string::npos) {
    out.resize(pos + 2);
  }
  return out + ' ' + label;
}

std::string HeadersToString(
  const containers::FlatHashMap<std::string, std::string>& headers) {
  std::string headers_for_logging;
  headers_for_logging.reserve(headers.size() * 60);  // just a guess
  for (const auto& p : headers) {
    if (p.first == StaticStrings::kAuthorization) {
      absl::StrAppend(&headers_for_logging, StaticStrings::kAuthorization,
                      ": SENSITIVE_DETAILS_HIDDEN,");
    } else {
      absl::StrAppend(&headers_for_logging, EscapeUnicode(p.first), ":",
                      EscapeUnicode(p.second), ",");
    }
  }
  if (!headers_for_logging.empty()) {
    headers_for_logging.pop_back();
  }
  return headers_for_logging;
}

std::string_view GetEndpointFromUrl(std::string_view url) {
  const char* p = url.data();
  const char* e = p + url.size();
  size_t slashes = 0;

  while (p < e) {
    if (*p == '?') {
      // http(s)://example.com?foo=bar
      return url.substr(0, p - url.data());
    } else if (*p == '/') {
      if (++slashes == 3) {
        return url.substr(0, p - url.data());
      }
    }
    ++p;
  }

  return url;
}

std::string RemoveWhitespaceAndComments(const std::string& value) {
  // note:
  // this function is already called during static initialization.
  // the following regex objects are function-local statics, because
  // we cannot have them statically initialized on the TU level.
  static const std::regex kRemoveComments("#.*$", std::regex::ECMAScript);
  static const std::regex kRemoveTabs("^[ \t\r\n]+|[ \t\r\n]+$",
                                      std::regex::ECMAScript);

  // replace trailing comments
  auto no_comment = std::regex_replace(value, kRemoveComments, "");
  // replace leading spaces, replace trailing spaces
  return std::regex_replace(no_comment, kRemoveTabs, "");
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
        SDB_THROW(ERROR_BAD_PARAMETER, "invalid UTF-8 sequence");
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
        SDB_THROW(ERROR_BAD_PARAMETER, "invalid UTF-8 sequence");
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
        SDB_THROW(ERROR_BAD_PARAMETER, "invalid UTF-8 sequence");
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

}  // namespace sdb::basics::string_utils
