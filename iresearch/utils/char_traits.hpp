////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <cstring>
#include <string>
#include <type_traits>

namespace irs {

struct CharTraitsU8 {
  using char_type = uint8_t;
  using int_type = int;
  using pos_type = std::streampos;
  using off_type = std::streamoff;
  using state_type = std::mbstate_t;

  static constexpr void assign(char_type& c1, const char_type& c2) noexcept {
    c1 = c2;
  }

  static constexpr bool eq(const char_type& c1, const char_type& c2) noexcept {
    return c1 == c2;
  }

  static constexpr bool lt(const char_type& c1, const char_type& c2) noexcept {
    return (static_cast<unsigned char>(c1) < static_cast<unsigned char>(c2));
  }

  static constexpr int compare(const char_type* s1, const char_type* s2,
                               size_t n) noexcept {
    if (n == 0) {
      return 0;
    }
    return std::memcmp(s1, s2, n);
  }

  static constexpr const char_type* find(const char_type* s, size_t n,
                                         const char_type& a) noexcept {
    if (n == 0) {
      return nullptr;
    }
    return static_cast<const char_type*>(std::memchr(s, a, n));
  }

  static constexpr char_type* move(char_type* s1, const char_type* s2,
                                   size_t n) noexcept {
    if (n == 0) {
      return s1;
    }
    return static_cast<char_type*>(std::memmove(s1, s2, n));
  }

  static constexpr char_type* copy(char_type* s1, const char_type* s2,
                                   size_t n) noexcept {
    if (n == 0) {
      return s1;
    }
    return static_cast<char_type*>(std::memcpy(s1, s2, n));
  }

  static constexpr char_type* assign(char_type* s, size_t n,
                                     char_type a) noexcept {
    if (n == 0) {
      return s;
    }
    return static_cast<char_type*>(std::memset(s, a, n));
  }

  static constexpr char_type to_char_type(const int_type& c) noexcept {
    return static_cast<char_type>(c);
  }

  // To keep both the byte 0xff and the eof symbol 0xffffffff
  // from ending up as 0xffffffff.
  static constexpr int_type to_int_type(const char_type& c) noexcept {
    return static_cast<int_type>(static_cast<unsigned char>(c));
  }

  static constexpr bool eq_int_type(const int_type& c1,
                                    const int_type& c2) noexcept {
    return c1 == c2;
  }

  static constexpr int_type eof() noexcept { return static_cast<int_type>(-1); }

  static constexpr int_type not_eof(const int_type& c) noexcept {
    return (c == eof()) ? 0 : c;
  }
};

template<typename Char>
using char_traits = std::conditional_t<std::is_same_v<Char, uint8_t>,
                                       CharTraitsU8, std::char_traits<Char>>;

}  // namespace irs
