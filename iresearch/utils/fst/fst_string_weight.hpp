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

#include <algorithm>
#include <utility>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByteWeight {
 public:
  using iterator = bstring::const_iterator;

  ByteWeight() = default;

  explicit ByteWeight(bytes_view rhs) : _str{rhs.data(), rhs.size()} {}

  template<typename Iterator>
  ByteWeight(Iterator begin, Iterator end) : _str(begin, end) {}

  ByteWeight& operator=(bytes_view rhs) {
    _str.assign(rhs.data(), rhs.size());
    return *this;
  }

  friend bool operator==(const ByteWeight& lhs,
                         const ByteWeight& rhs) noexcept {
    return lhs._str == rhs._str;
  }

  const bstring& Impl() const noexcept { return _str; }

  byte_type& operator[](size_t i) noexcept { return _str[i]; }

  const byte_type& operator[](size_t i) const noexcept { return _str[i]; }

  const byte_type* c_str() const noexcept { return _str.c_str(); }

  void Resize(size_t size) noexcept { _str.resize(size); }

  bool Empty() const noexcept { return _str.empty(); }

  void Clear() noexcept { _str.clear(); }

  size_t Size() const noexcept { return _str.size(); }

  void PushBack(byte_type label) { _str.push_back(label); }

  template<typename Iterator>
  void PushBack(Iterator begin, Iterator end) {
    _str.append(begin, end);
  }

  void PushBack(bytes_view w) { _str.append(w.data(), w.size()); }

  void Reserve(size_t capacity) { _str.reserve(capacity); }

  iterator begin() const noexcept { return _str.begin(); }
  iterator end() const noexcept { return _str.end(); }

  // intentionally implicit
  operator bytes_view() const noexcept { return _str; }

  // intentionally implicit
  operator bstring() && noexcept { return std::move(_str); }

 private:
  bstring _str;
};

using byte_weight = ByteWeight;

// Longest common prefix
inline bytes_view Plus(bytes_view lhs, bytes_view rhs) noexcept {
  if (rhs.size() > lhs.size()) {
    std::swap(lhs, rhs);
  }
  const auto* end =
    std::mismatch(rhs.data(), rhs.data() + rhs.size(), lhs.data()).first;
  return {rhs.data(), static_cast<size_t>(end - rhs.data())};
}

inline ByteWeight Times(bytes_view lhs, bytes_view rhs) {
  ByteWeight product;
  product.Reserve(lhs.size() + rhs.size());
  product.PushBack(lhs);
  product.PushBack(rhs);
  return product;
}

// Left division: `lhs` without its prefix `rhs`
inline bytes_view DivideLeft(bytes_view lhs, bytes_view rhs) noexcept {
  if (rhs.size() > lhs.size()) {
    return {};
  }
  SDB_ASSERT(lhs.starts_with(rhs), ViewCast<char>(rhs), " is not prefix of ",
             ViewCast<char>(lhs));
  return {lhs.data() + rhs.size(), lhs.size() - rhs.size()};
}

}  // namespace irs
