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

#pragma once

#include <algorithm>
#include <cstddef>
#include <functional>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/noncopyable.hpp"

namespace irs {

// Retains the top-K elements of a stream ordered by `Less`; the K greatest
// survive. Elements accumulate in a buffer reserved for 2*K and are trimmed back
// to K with std::nth_element whenever the buffer fills, giving amortized O(1)
// insertion and O(K) memory without a maintained heap. The buffer is also
// partitioned the first time it reaches K, so once Full() reports true Min()
// (the smallest retained element, at index K-1) is a valid rejection threshold:
// callers peek it to skip materializing an element that cannot make the cut.
template<typename T, typename Less = std::less<T>>
class TopKHeap : private util::Noncopyable {
 public:
  explicit TopKHeap(size_t capacity, Less less = {})
    : _less{std::move(less)}, _capacity{capacity} {
    _items.reserve(2 * capacity);
  }

  size_t Capacity() const noexcept { return _capacity; }
  size_t Size() const noexcept { return _items.size(); }
  bool Empty() const noexcept { return _items.empty(); }
  bool Full() const noexcept { return _items.size() >= _capacity; }

  // Smallest retained element; valid only when Full().
  const T& Min() const noexcept {
    SDB_ASSERT(Full() && _capacity != 0);
    return _items[_capacity - 1];
  }

  template<typename U>
  void Push(U&& item) {
    if (_capacity == 0) {
      return;
    }
    _items.push_back(std::forward<U>(item));
    // Partition on first reaching K (so Min() becomes a valid cutoff) and again
    // whenever the buffer fills to 2*K (to bound memory back to K).
    if (const auto size = _items.size();
        size == _capacity || size == 2 * _capacity) {
      Trim();
    }
  }

  // Fold another buffer's elements into this one.
  void Merge(TopKHeap&& other) {
    for (auto& item : other._items) {
      Push(std::move(item));
    }
    other.Clear();
  }

  // Trim down to the surviving top-K and return them.
  std::vector<T>& Finalize() {
    Trim();
    return _items;
  }

  void Clear() noexcept { _items.clear(); }

 private:
  void Trim() {
    if (_capacity == 0 || _items.size() < _capacity) {
      return;
    }
    std::nth_element(
      _items.begin(), _items.begin() + (_capacity - 1), _items.end(),
      [this](const T& lhs, const T& rhs) { return _less(rhs, lhs); });
    _items.erase(_items.begin() + _capacity, _items.end());
  }

  [[no_unique_address]] Less _less;
  std::vector<T> _items;
  size_t _capacity;
};

}  // namespace irs
