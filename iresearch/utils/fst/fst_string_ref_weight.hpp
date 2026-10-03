////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/utils/string.hpp"

namespace irs {

class ByteRefWeight {
 public:
  constexpr ByteRefWeight() = default;

  constexpr explicit ByteRefWeight(bytes_view rhs) noexcept : _str{rhs} {}

  constexpr ByteRefWeight& operator=(bytes_view rhs) noexcept {
    _str = rhs;
    return *this;
  }

  friend constexpr bool operator==(ByteRefWeight lhs,
                                   ByteRefWeight rhs) noexcept {
    return lhs._str == rhs._str;
  }

  constexpr bytes_view Impl() const noexcept { return _str; }

  constexpr const byte_type& operator[](size_t i) const noexcept {
    return _str[i];
  }

  constexpr const byte_type* c_str() const noexcept { return _str.data(); }

  constexpr bool Empty() const noexcept { return _str.empty(); }

  constexpr void Clear() noexcept { _str = {}; }

  constexpr size_t Size() const noexcept { return _str.size(); }

  constexpr const byte_type* begin() const noexcept { return _str.data(); }
  constexpr const byte_type* end() const noexcept {
    return _str.data() + _str.size();
  }

  // intentionally implicit
  constexpr operator bytes_view() const noexcept { return _str; }

 private:
  bytes_view _str;
};

using byte_ref_weight = ByteRefWeight;

}  // namespace irs
