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

#include "basics/assert.h"
#include "basics/shared.hpp"
#include "basics/system-compiler.h"
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

}  // namespace irs::utf8_utils
