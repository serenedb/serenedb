////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <vector>

#include "iresearch/utils/containers/flat_hash_set.hpp"

namespace irs {

// Implementation of MinHash variant with a single hash function.
class MinHash {
 public:
  explicit MinHash(size_t size)
    : _max_size{std::max(size, size_t{1})}, _left{_max_size} {
    _min_hashes.reserve(_left);

    // +1 because we insert a new hash
    // value before removing an old one.
    _dedup.reserve(_left + 1);
  }

  // Update MinHash with the new value.
  // `noexcept` because we reserved enough space in constructor already.
  void Insert(uint64_t hash_value) {
    if ((_left == 0 && hash_value >= _min_hashes.front()) ||
        !_dedup.emplace(hash_value).second) {
      return;
    }
    if (_left != 0) {
      _min_hashes.emplace_back(hash_value);
      if (--_left == 0) {
        absl::c_make_heap(_min_hashes);
      }
    } else {
      absl::c_pop_heap(_min_hashes);
      _dedup.erase(_min_hashes.back());
      _min_hashes.back() = hash_value;
      absl::c_push_heap(_min_hashes);
    }
  }

  // Provides access to the accumulated MinHash signature.
  auto begin() const noexcept { return std::begin(_min_hashes); }
  auto end() const noexcept { return std::end(_min_hashes); }

  // Reset MinHash to the initial state.
  void Clear() noexcept {
    _min_hashes.clear();
    _dedup.clear();
    _left = _max_size;
  }

 private:
  std::vector<uint64_t> _min_hashes;
  containers::FlatHashSet<uint64_t>
    _dedup;  // guard against duplicated hash values
  size_t _max_size;
  size_t _left;
};

}  // namespace irs
