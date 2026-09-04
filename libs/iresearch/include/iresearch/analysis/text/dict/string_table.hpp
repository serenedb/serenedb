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

#include <absl/algorithm/container.h>

#include <duckdb/common/crypto/md5.hpp>
#include <iterator>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/shared.hpp"
#include "iresearch/analysis/text/term_view.hpp"

namespace irs::analysis::dict {
namespace detail {

template<typename Value>
IRS_FORCE_INLINE constexpr size_t SizeOf(const Value& value) noexcept {
  if constexpr (std::is_same_v<Value, duckdb::string_t>) {
    return value.GetSize();
  } else {
    return std::string_view{value}.size();
  }
}

template<typename Value>
IRS_FORCE_INLINE constexpr std::string_view ViewOf(
  const Value& value) noexcept {
  if constexpr (std::is_same_v<Value, duckdb::string_t>) {
    return {value.GetData(), value.GetSize()};
  } else {
    return std::string_view{value};
  }
}

// Two-tier exact string membership/lookup: keys up to string_t's inline
// length live as their canonical 16-byte inline handle, longer keys as
// `LongKey` -- `std::string` owns its bytes, `std::string_view` requires
// them to outlive the table. Mutation is a build-time privilege: share
// instances as const.
template<template<typename...> typename Table, typename LongKey,
         typename... Mapped>
class StringTable {
  static_assert(sizeof...(Mapped) <= 1);

 protected:
  using InlineTable = Table<__uint128_t, Mapped...>;
  using LongTable = Table<LongKey, Mapped...>;

  static constexpr const auto& KeyOf(const auto& entry) noexcept {
    if constexpr (sizeof...(Mapped) == 0) {
      return entry;
    } else {
      return entry.first;
    }
  }

 public:
  IRS_FORCE_INLINE bool Contains(this const auto& self,
                                 const auto& value) noexcept {
    if (SizeOf(value) <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
      return self._inline.contains(InlineTermHandle(value));
    }
    return self._long.contains(ViewOf(value));
  }

  void ShrinkToFit() {
    _inline.rehash(0);
    _long.rehash(0);
  }

  bool Empty() const noexcept { return _inline.empty() && _long.empty(); }

  size_t Size() const noexcept { return _inline.size() + _long.size(); }

  void EraseHalf() {
    const auto half = [](auto& table) {
      table.erase(
        table.begin(),
        std::next(table.begin(), static_cast<ptrdiff_t>(table.size() / 2)));
    };
    half(_inline);
    half(_long);
  }

  void Reserve(size_t inline_words, size_t long_words) {
    _inline.reserve(inline_words);
    _long.reserve(long_words);
  }

  size_t MemoryBytes() const noexcept {
    size_t size =
      _inline.capacity() * sizeof(typename InlineTable::value_type) +
      _long.capacity() * sizeof(typename LongTable::value_type);
    for (const auto& entry : _long) {
      size += KeyOf(entry).size();
    }
    return size;
  }

  bool operator==(const StringTable& rhs) const {
    return _inline == rhs._inline && _long == rhs._long;
  }

  // Hex digest of a canonical image of the key set: equal key sets produce
  // equal digests regardless of build order; mapped values do not
  // participate.
  void Hash(
    std::span<char, duckdb::MD5Context::MD5_HASH_LENGTH_TEXT> hex) const {
    duckdb::MD5Context digest;
    std::vector<__uint128_t> inline_words;
    inline_words.reserve(_inline.size());
    for (const auto& entry : _inline) {
      inline_words.push_back(KeyOf(entry));
    }
    absl::c_sort(inline_words);
    std::vector<std::string_view> long_words;
    long_words.reserve(_long.size());
    for (const auto& entry : _long) {
      long_words.push_back(KeyOf(entry));
    }
    absl::c_sort(long_words);
    const uint64_t count = inline_words.size();
    digest.Add(duckdb::const_data_ptr_cast(&count), sizeof(count));
    if (!inline_words.empty()) {
      digest.Add(duckdb::const_data_ptr_cast(inline_words.data()),
                 inline_words.size() * sizeof(__uint128_t));
    }
    for (const auto word : long_words) {
      const uint64_t size = word.size();
      digest.Add(duckdb::const_data_ptr_cast(&size), sizeof(size));
      digest.Add(duckdb::const_data_ptr_cast(word.data()), word.size());
    }
    digest.FinishHex(hex.data());
  }

 protected:
  StringTable() = default;
  StringTable(StringTable&&) = default;
  StringTable& operator=(StringTable&&) = default;

  InlineTable _inline;
  LongTable _long;
};

}  // namespace detail

template<typename LongKey>
class StringSet
  : public detail::StringTable<sdb::containers::FlatHashSet, LongKey> {
 public:
  void Insert(LongKey word) {
    if (word.size() <= duckdb::string_t::INLINE_LENGTH) {
      this->_inline.insert(InlineTermHandle(std::string_view{word}));
      return;
    }
    this->_long.insert(std::move(word));
  }
};

template<typename LongKey, typename Mapped>
class StringMap final
  : public detail::StringTable<sdb::containers::FlatHashMap, LongKey, Mapped> {
 public:
  // The reference is valid until the next insertion.
  Mapped& operator[](LongKey word) {
    if (word.size() <= duckdb::string_t::INLINE_LENGTH) {
      return this->_inline[InlineTermHandle(std::string_view{word})];
    }
    return this->_long[std::move(word)];
  }

  IRS_FORCE_INLINE const Mapped* Find(this const auto& self,
                                      const auto& value) noexcept {
    if (detail::SizeOf(value) <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
      const auto it = self._inline.find(InlineTermHandle(value));
      return it == self._inline.end() ? nullptr : &it->second;
    }
    const auto it = self._long.find(detail::ViewOf(value));
    return it == self._long.end() ? nullptr : &it->second;
  }

  void ForEachMapped(this auto& self, auto&& visit) {
    for (auto& [key, mapped] : self._inline) {
      visit(mapped);
    }
    for (auto& [key, mapped] : self._long) {
      visit(mapped);
    }
  }
};

}  // namespace irs::analysis::dict
