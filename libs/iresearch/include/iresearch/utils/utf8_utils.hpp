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
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <string>
#include <string_view>

#include "basics/assert.h"
#include "basics/shared.hpp"
#include "basics/system-compiler.h"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::utf8_utils {

inline constexpr uint8_t kMaxCharSize = 4;

// icu::UnicodeString::kInvalidUChar is private
inline constexpr uint32_t kInvalidChar32 = 0xFFFF;

// 0 -- return 0 for last invalid and intermediate
// 1 -- return 1 for last invalid
// everything else is just some len
// 4 -- only for valid input
// TODO(mbkkt) add benchmark to easy detect performance regression of this
// function. It higly depends on compiler optimization, don't touch likely!
// keep an eye on every if to be compiled to jmp
template<byte_type KError>
IRS_FORCE_INLINE constexpr byte_type LengthFromChar8(byte_type ch) noexcept {
  static_assert(KError <= 1 || KError == 4);
  if (ch < 0x80) [[likely]] {
    return 1;
  }
  if (ch < 0xE0) [[likely]] {
    if (KError == 0 && ch < 0xC0) {
      return 0;
    }
    return 2;
  }
  if (ch < 0xF0) [[likely]] {
    return 3;
  }
  if (ch < 0xF8) [[likely]] {
    return 4;
  }
  return KError;
}

IRS_FORCE_INLINE constexpr const byte_type* Next(
  const byte_type* it, const byte_type* end) noexcept {
  SDB_ASSERT(it);
  SDB_ASSERT(end);
  SDB_ASSERT(it < end);
  it += LengthFromChar8<1>(*it);
  return it < end ? it : end;
}

IRS_FORCE_INLINE constexpr const byte_type* Prev(const byte_type* begin,
                                                 const byte_type* it) noexcept {
  SDB_ASSERT(begin);
  SDB_ASSERT(it);
  SDB_ASSERT(begin < it);
  do {
    --it;
  } while ((*it & 0xC0) == 0x80 && begin < it);
  return it;
}

inline size_t Length(bytes_view str) {
  auto* it = str.data();
  auto* end = str.data() + str.size();
  size_t length = 0;
  for (; it != end; it = Next(it, end)) {
    ++length;
  }
  return length;
}

IRS_FORCE_INLINE constexpr uint32_t LengthFromChar32(uint32_t cp) {
  if (cp < 0x80) {
    return 1;
  }
  if (cp < 0x800) {
    return 2;
  }
  if (cp < 0x10000) {
    return 3;
  }
  return 4;
}

template<bool Checked>
inline uint32_t ToChar32Impl(const byte_type*& it,
                             const byte_type* end) noexcept {
  SDB_ASSERT(it);
  SDB_ASSERT(!Checked || end);
  SDB_ASSERT(!Checked || it < end);
  uint32_t cp = *it++;
  auto length = LengthFromChar8<(Checked ? 0 : 4)>(cp);
  if constexpr (Checked) {
    if (length == 0 || it + length - 1 > end) [[unlikely]] {
      return kInvalidChar32;
    }
  }
  auto next = [&] { return static_cast<uint32_t>(*it++) & 0x3F; };
  switch (length) {
    case 1:
      return cp;
    case 2:
      cp = (cp & 0x1F) << 6;
      break;
    case 3:
      cp = (cp & 0xF) << 12;
      cp |= next() << 6;
      break;
    case 4:
      cp = (cp & 0x7) << 18;
      cp |= next() << 12;
      cp |= next() << 6;
      break;
    default:
      SDB_UNREACHABLE();
  }
  cp |= next();
  return cp;
}

IRS_FORCE_INLINE inline uint32_t ToChar32(const byte_type*& it,
                                          const byte_type* end) noexcept {
  return ToChar32Impl<true>(it, end);
}

IRS_FORCE_INLINE inline uint32_t ToChar32(const byte_type*& it) noexcept {
  return ToChar32Impl<false>(it, nullptr);
}

inline constexpr uint32_t FromChar32(uint32_t cp, byte_type* begin) noexcept {
  if (cp < 0x80) {
    begin[0] = static_cast<byte_type>(cp);
    return 1;
  }
  auto convert = [](uint32_t cp, uint32_t mask = 0x3F, uint32_t header = 0x80) {
    return static_cast<byte_type>((cp & mask) | header);
  };

  if (cp < 0x800) {
    begin[0] = convert(cp >> 6, 0x1F, 0xC0);
    begin[1] = convert(cp);
    return 2;
  }

  if (cp < 0x10000) {
    begin[0] = convert(cp >> 12, 0xF, 0xE0);
    begin[1] = convert(cp >> 6);
    begin[2] = convert(cp);
    return 3;
  }

  begin[0] = convert(cp >> 18, 0x7, 0xF0);
  begin[1] = convert(cp >> 12);
  begin[2] = convert(cp >> 6);
  begin[3] = convert(cp);
  return 4;
}

template<bool Checked, typename OutputIterator>
inline bool ToUTF32(bytes_view data, OutputIterator&& out) {
  const auto* begin = data.data();
  for (const auto* end = begin + data.size(); begin != end;) {
    const auto cp = ToChar32Impl<Checked>(begin, end);
    if constexpr (Checked) {
      if (cp == kInvalidChar32) {
        return false;
      }
    }
    *out = cp;
  }
  return true;
}

// Verify that |str| is well-formed UTF-8: every byte is part of a sequence
// whose lead byte declares a valid 1..4 byte length, the bytes don't run off
// the end, and continuation bytes have the form 10xxxxxx.
inline bool IsValid(std::string_view str) noexcept {
  const auto* it = reinterpret_cast<const byte_type*>(str.data());
  const auto* end = it + str.size();
  while (it != end) {
    const auto length = LengthFromChar8<0>(*it);
    if (length == 0 || it + length > end) {
      return false;
    }
    for (uint8_t i = 1; i < length; ++i) {
      if ((it[i] & 0xC0) != 0x80) {
        return false;
      }
    }
    it += length;
  }
  return true;
}

// Transcode arbitrary bytes into valid UTF-8. Bytes that are part of a valid
// UTF-8 sequence pass through unchanged -- so multi-byte codepoints (e.g.
// `é` = 0xC3 0xA9) survive intact when they appear in a tokenizer output.
// Bytes that aren't part of a valid sequence (lone 0x80..0xFF, truncated
// multi-byte sequences, etc.) are mapped to their own Unicode codepoint via
// Latin-1: bytes 0x80..0xFF expand to the two-byte UTF-8 sequence
// (0xC0 | (b >> 6), 0x80 | (b & 0x3F)).
//
// Useful for embedding tokenizer output into UTF-8-only string types -- e.g.
// the wildcard / ngram analyzers' word-boundary sentinel byte (0xFF) is
// invalid as a standalone UTF-8 byte; this routine converts it (and any
// other invalid bytes) into a stable, reversible UTF-8 form.
inline std::string ToUtf8Safe(std::string_view bytes) {
  if (IsValid(bytes)) {
    return std::string{bytes};
  }
  std::string out;
  out.reserve(bytes.size() + 4);
  const auto* it = reinterpret_cast<const byte_type*>(bytes.data());
  const auto* end = it + bytes.size();
  while (it != end) {
    const auto length = LengthFromChar8<0>(*it);
    bool valid = length > 0 && it + length <= end;
    for (uint8_t i = 1; valid && i < length; ++i) {
      if ((it[i] & 0xC0) != 0x80) {
        valid = false;
      }
    }
    if (valid) {
      out.append(reinterpret_cast<const char*>(it), length);
      it += length;
    } else {
      const auto b = *it;
      out.push_back(static_cast<char>(0xC0 | (b >> 6)));
      out.push_back(static_cast<char>(0x80 | (b & 0x3F)));
      ++it;
    }
  }
  return out;
}

}  // namespace irs::utf8_utils
