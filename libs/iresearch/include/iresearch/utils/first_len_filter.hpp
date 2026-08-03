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
#include <array>
#include <cstdint>
#include <string_view>

namespace irs {

// Negative prefilter for small string sets probed by miss-dominated lookups:
// one bit per (first byte ^ last byte, min(length, 15)) triple -- the last
// byte costs one load of an already-hot key and cuts same-(first,len)
// false positives (measured -9% on the english stopword arm vs first-only).
// MayContain() == false guarantees the key is absent; true falls through to the
// exact probe. Permissive (passes everything) until the first Add(), so
// containers built without populating the filter stay correct and merely skip
// the shortcut.
class FirstLenFilter {
 public:
  void Add(std::string_view value) noexcept {
    Arm();
    if (value.empty()) {
      _has_empty = true;
      return;
    }
    _table[Bucket(value)] |= LenBit(value.size());
  }

  // An ad-hoc two-byte mix beats absl::HashOf here on every arm (measured):
  // the standard hash's dispersion is wasted at 256 buckets and its cost is
  // the wrong side of the filter's budget -- the tier only pays because it
  // is much cheaper than the probe it guards.
  static uint8_t Bucket(std::string_view value) noexcept {
    return static_cast<uint8_t>(static_cast<uint8_t>(value.front()) ^
                                (static_cast<uint8_t>(value.back()) * 17));
  }

  // Arms the filter without adding a key: an armed-but-empty filter rejects
  // everything, unlike the permissive default a filter carries before it is
  // populated. Lets callers built from an empty key set short-circuit.
  void Arm() noexcept {
    if (!_armed) {
      _table.fill(0);
      _has_empty = false;
      _armed = true;
    }
  }

  bool MayContain(std::string_view value) const noexcept {
    if (value.empty()) [[unlikely]] {
      return _has_empty;
    }
    return (_table[Bucket(value)] & LenBit(value.size())) != 0;
  }

 private:
  static constexpr size_t kBuckets = 16;

  static uint16_t LenBit(size_t size) noexcept {
    return static_cast<uint16_t>(uint32_t{1}
                                 << std::min<size_t>(size, kBuckets - 1));
  }

  static constexpr std::array<uint16_t, 256> Permissive() noexcept {
    std::array<uint16_t, 256> table{};
    table.fill(0xFFFF);
    return table;
  }

  std::array<uint16_t, 256> _table = Permissive();
  bool _has_empty = true;
  bool _armed = false;
};

// A hash container behind its FirstLenFilter: construct from the fully built
// container, the filter arms itself from the keys. Contains() serves sets,
// Find() serves maps; both prefilter before the exact probe.
template<typename Container>
class Prefiltered {
 public:
  Prefiltered() = default;
  explicit Prefiltered(Container&& container)
    : _container{std::move(container)} {
    // arm up front so an empty container rejects every probe with a single
    // filter check, instead of the permissive default falling through to a
    // fruitless hash lookup on every call
    _filter.Arm();
    for (const auto& entry : _container) {
      _filter.Add(KeyOf(entry));
    }
  }

  bool Contains(std::string_view value) const noexcept {
    if (!_filter.MayContain(value)) [[likely]] {
      return false;
    }
    return _container.contains(value);
  }

  template<typename C = Container>
  const typename C::mapped_type* Find(std::string_view value) const noexcept {
    if (!_filter.MayContain(value)) [[likely]] {
      return nullptr;
    }
    const auto it = _container.find(value);
    return it == _container.end() ? nullptr : &it->second;
  }

  bool Empty() const noexcept { return _container.empty(); }

 private:
  template<typename Entry>
  static std::string_view KeyOf(const Entry& entry) noexcept {
    if constexpr (requires { entry.first; }) {
      return entry.first;
    } else {
      return entry;
    }
  }

  Container _container;
  FirstLenFilter _filter;
};

}  // namespace irs
