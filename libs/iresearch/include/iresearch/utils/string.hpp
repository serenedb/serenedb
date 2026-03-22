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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/hash/hash.h>
#include <vpack/char_traits.h>

#include <cstring>
#include <string>
#include <string_view>

#include "absl/strings/match.h"
#include "basics/assert.h"
#include "basics/shared.hpp"
#include "iresearch/types.hpp"

namespace irs {

// NOLINTBEGIN
template<typename Char, typename Allocator = std::allocator<Char>>
using basic_string =
  std::basic_string<Char, vpack::char_traits<Char>, Allocator>;
template<typename Char>
using basic_string_view =
  std::basic_string_view<Char, vpack::char_traits<Char>>;

using bstring = basic_string<byte_type>;
using bytes_view = basic_string_view<byte_type>;
// NOLINTEND

template<typename Char>
inline constexpr Char kEmptyChar{};

template<typename Char>
constexpr basic_string_view<Char> kEmptyStringView{&kEmptyChar<Char>, 0};

template<typename Char>
constexpr bool IsNull(basic_string_view<Char> str) noexcept {
  return str.data() == nullptr;
}

template<typename ElemDst, typename ElemSrc>
constexpr inline basic_string_view<ElemDst> ViewCast(
  basic_string_view<ElemSrc> src) noexcept {
  // static_assert(!std::is_same_v<ElemDst, ElemSrc>);
  static_assert(sizeof(ElemDst) == sizeof(ElemSrc));

  return {reinterpret_cast<const ElemDst*>(src.data()), src.size()};
}

template<typename Char>
inline size_t CommonPrefixLength(basic_string_view<Char> lhs,
                                 basic_string_view<Char> rhs) noexcept {
  return absl::FindLongestCommonPrefix(ViewCast<char>(lhs), ViewCast<char>(rhs))
    .size();
}

}  // namespace irs
namespace absl::hash_internal {

// std or absl::hash_internal required because these classes in std

template<typename H>
H AbslHashValue(H h, irs::bytes_view str) {
  return H::combine(std::move(h), irs::ViewCast<char>(str));
}

template<typename H>
H AbslHashValue(H h, const irs::bstring& str) {
  return AbslHashValue(std::move(h), irs::bytes_view{str});
}

}  // namespace absl::hash_internal
