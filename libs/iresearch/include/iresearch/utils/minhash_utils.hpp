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

#include <absl/container/flat_hash_set.h>

#include <span>
#include <vector>

#include "basics/math_utils.hpp"

namespace irs {

// Implementation of MinHash variant with a single hash function.
class MinHash {
 public:
  // Returns number of hashes required to preserve probabilistic error
  // threshold.
  static constexpr size_t MaxSize(double_t err) noexcept {
    return err > 0. && err < 1.
             ? math::Ceil64(1. / (err * err))
             : (err < 1. ? std::numeric_limits<size_t>::max() : 0);
  }

  // Returns expected probabilistic error according
  // the size of MinHash signature.
  static constexpr double_t Error(size_t size) noexcept {
    return size ? 1. / std::sqrt(size)
                : std::numeric_limits<double_t>::infinity();
  }

  explicit MinHash(size_t size)
    : _max_size{std::max(size, size_t{1})}, _left{_max_size} {
    _min_hashes.reserve(_left);

    // +1 because we insert a new hash
    // value before removing an old one.
    _dedup.reserve(_left + 1);
  }

  // Update MinHash with the new value.
  // `noexcept` because we reserved enough space in constructor already.
  IRS_NO_INLINE void Insert(uint64_t hash_value) {
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

  // Return `true` if MinHash signature is empty, false - otherwise.
  size_t Empty() const noexcept { return _min_hashes.empty(); }

  // Return actual size of accumulated MinHash signature.
  size_t Size() const noexcept { return _dedup.size(); }

  // Return the expected size of MinHash signature.
  size_t MaxSize() const noexcept { return _max_size; }

  // Return Jaccard coefficient of 2 MinHash signatures.
  // `rhs` members are meant to be unique.
  double Jaccard(std::span<const uint64_t> rhs) const noexcept {
    const auto intersect = absl::c_accumulate(
      rhs, uint64_t{0}, [&](uint64_t acc, uint64_t hash_value) noexcept {
        return acc + uint64_t{_dedup.contains(hash_value)};
      });
    const auto cardinality = Size() + rhs.size() - intersect;

    return cardinality
             ? static_cast<double>(intersect) / static_cast<double>(cardinality)
             : 1.0;
  }

  // Return Jaccard coefficient of 2 MinHash signatures.
  double Jaccard(const MinHash& rhs) const noexcept {
    if (Size() > rhs.Size()) {
      return Jaccard(rhs._min_hashes);
    } else {
      return rhs.Jaccard(_min_hashes);
    }
  }

  // Reset MinHash to the initial state.
  void Clear() noexcept {
    _min_hashes.clear();
    _dedup.clear();
    _left = MaxSize();
  }

 private:
  std::vector<uint64_t> _min_hashes;
  absl::flat_hash_set<uint64_t> _dedup;  // guard against duplicated hash values
  size_t _max_size;
  size_t _left;
};

}  // namespace irs
