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

#include <algorithm>
#include <array>
#include <bit>
#include <cstring>
#include <duckdb/common/crypto/md5.hpp>
#include <iterator>
#include <span>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/shared.hpp"
#include "iresearch/analysis/text/term_view.hpp"

namespace irs::analysis::dict {
namespace detail {

static_assert(std::endian::native == std::endian::little);

template<size_t N>
struct Key {
  std::array<uint64_t, N> words;

  friend IRS_FORCE_INLINE bool operator==(const Key& lhs,
                                          const Key& rhs) noexcept {
    return [&]<size_t... I>(std::index_sequence<I...>) {
      return ((lhs.words[I] == rhs.words[I]) && ...);
    }(std::make_index_sequence<N>{});
  }

  template<typename H>
  friend H AbslHashValue(H h, const Key& key) {
    return [&]<size_t... I>(std::index_sequence<I...>) {
      return H::combine(std::move(h), key.words[N - 1 - I]...);
    }(std::make_index_sequence<N>{});
  }
};

inline constexpr size_t kFixedClasses = 2;
inline constexpr size_t kMaxFixedSize = 3 * sizeof(uint64_t);

IRS_FORCE_INLINE inline uint64_t Load8(const char* p) noexcept {
  uint64_t word;
  std::memcpy(&word, p, sizeof(word));
  return word;
}

IRS_FORCE_INLINE inline size_t LeadingZeroBytes(uint64_t word) noexcept {
  return static_cast<size_t>(std::countl_zero(word)) >> 3;
}

IRS_FORCE_INLINE inline Key<3> PackKey(const char* p, uint32_t size) noexcept {
  SDB_ASSERT(size > duckdb::string_t::INLINE_LENGTH && size <= kMaxFixedSize);
  const auto tail = Load8(p + size - sizeof(uint64_t));
  if (size <= 2 * sizeof(uint64_t)) {
    return {Load8(p), tail >> ((2 * sizeof(uint64_t) - size) * 8), 0};
  }
  return {Load8(p), Load8(p + sizeof(uint64_t)),
          tail >> ((3 * sizeof(uint64_t) - size) * 8)};
}

using InlineKey = Key<2>;

template<size_t N>
size_t SizeOf(const Key<N>& key) noexcept {
  if constexpr (N == 2) {
    return static_cast<uint32_t>(key.words[0]);
  } else {
    static_assert(N == 3);
    if (key.words[2] == 0) {
      return 2 * sizeof(uint64_t) - LeadingZeroBytes(key.words[1]);
    }
    return 3 * sizeof(uint64_t) - LeadingZeroBytes(key.words[2]);
  }
}

template<size_t N>
std::string_view ViewOf(const Key<N>& key) noexcept {
  const auto* bytes = reinterpret_cast<const char*>(key.words.data());
  if constexpr (N == 2) {
    return {bytes + sizeof(uint32_t), SizeOf(key)};
  } else {
    return {bytes, SizeOf(key)};
  }
}

IRS_FORCE_INLINE inline InlineKey InlineKeyOf(duckdb::string_t term) noexcept {
  SDB_ASSERT(term.GetSize() <= duckdb::string_t::INLINE_LENGTH);
  return std::bit_cast<InlineKey>(term);
}

template<typename Value>
IRS_FORCE_INLINE duckdb::string_t TermOf(const Value& value) noexcept {
  if constexpr (std::is_same_v<Value, duckdb::string_t>) {
    return value;
  } else {
    return MakeTermView(std::string_view{value});
  }
}

template<typename F>
IRS_FORCE_INLINE decltype(auto) Dispatch(duckdb::string_t term, F&& f) {
  const uint32_t size = term.GetSize();
  if (size <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
    return f(InlineKeyOf(term));
  }
  const char* p = term.GetData();
  if (size > kMaxFixedSize || p[size - 1] == 0) [[unlikely]] {
    return f(std::string_view{p, size});
  }
  return f(PackKey(p, size));
}

template<typename Key>
inline constexpr bool kIsLongKey =
  std::is_same_v<std::decay_t<Key>, std::string_view>;

template<template<typename...> typename Table, typename LongKey,
         typename... Mapped>
class StringTable {
  static_assert(sizeof...(Mapped) <= 1);

 protected:
  template<size_t N>
  using FixedTable = Table<Key<N>, Mapped...>;
  using LongTable = Table<LongKey, Mapped...>;

  static constexpr const auto& KeyOf(const auto& entry) noexcept {
    if constexpr (sizeof...(Mapped) == 0) {
      return entry;
    } else {
      return entry.first;
    }
  }

  template<size_t N>
  IRS_FORCE_INLINE auto& TableFor(this auto& self, const Key<N>&) noexcept {
    return std::get<N - 2>(self._tables);
  }

  IRS_FORCE_INLINE auto& TableFor(this auto& self, std::string_view) noexcept {
    return std::get<kFixedClasses>(self._tables);
  }

  void ForEachTable(this auto& self, auto&& visit) {
    std::apply([&](auto&... table) { (visit(table), ...); }, self._tables);
  }

 public:
  IRS_FORCE_INLINE bool Contains(this const auto& self,
                                 const auto& value) noexcept {
    return Dispatch(TermOf(value), [&](const auto& key) IRS_FORCE_INLINE {
      return self.TableFor(key).contains(key);
    });
  }

  void ShrinkToFit() {
    ForEachTable([](auto& table) { table.rehash(0); });
  }

  bool Empty() const noexcept { return Size() == 0; }

  size_t Size() const noexcept {
    size_t size = 0;
    ForEachTable([&](const auto& table) { size += table.size(); });
    return size;
  }

  void EraseHalf() {
    size_t remove = Size() / 2;
    ForEachTable([&](auto& table) {
      const auto count = std::min(remove, table.size());
      table.erase(table.begin(),
                  std::next(table.begin(), static_cast<ptrdiff_t>(count)));
      remove -= count;
    });
  }

  void Reserve(size_t words) {
    ForEachTable(
      [=](auto& table) { table.reserve(words / (kFixedClasses + 1)); });
  }

  size_t MemoryBytes() const noexcept {
    size_t size = 0;
    ForEachTable([&](const auto& table) {
      size += table.capacity() *
              sizeof(typename std::decay_t<decltype(table)>::value_type);
    });
    for (const auto& entry : std::get<kFixedClasses>(_tables)) {
      size += KeyOf(entry).size();
    }
    return size;
  }

  bool operator==(const StringTable& rhs) const {
    return _tables == rhs._tables;
  }

  void Hash(
    std::span<char, duckdb::MD5Context::MD5_HASH_LENGTH_TEXT> hex) const {
    std::vector<__uint128_t> inline_words;
    std::vector<std::string_view> long_words;
    ForEachTable([&](const auto& table) {
      for (const auto& entry : table) {
        const auto& key = KeyOf(entry);
        using K = std::decay_t<decltype(key)>;
        if constexpr (std::is_same_v<K, InlineKey>) {
          inline_words.push_back(std::bit_cast<__uint128_t>(key));
        } else if constexpr (std::is_same_v<K, Key<3>>) {
          long_words.push_back(ViewOf(key));
        } else {
          const std::string_view word{key};
          if (word.size() <= duckdb::string_t::INLINE_LENGTH) {
            inline_words.push_back(
              std::bit_cast<__uint128_t>(MakeTermView(word)));
          } else {
            long_words.push_back(word);
          }
        }
      }
    });
    absl::c_sort(inline_words);
    absl::c_sort(long_words);
    duckdb::MD5Context digest;
    const uint64_t count = inline_words.size();
    digest.Add(duckdb::const_data_ptr_cast(&count), sizeof(count));
    if (!inline_words.empty()) {
      digest.Add(duckdb::const_data_ptr_cast(inline_words.data()),
                 inline_words.size() * sizeof(__uint128_t));
    }
    for (const auto& word : long_words) {
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

  std::tuple<FixedTable<2>, FixedTable<3>, LongTable> _tables;
};

}  // namespace detail

template<typename LongKey>
class StringSet
  : public detail::StringTable<sdb::containers::FlatHashSet, LongKey> {
 public:
  void Insert(LongKey word) {
    detail::Dispatch(MakeTermView(std::string_view{word}),
                     [&](const auto& key) {
                       auto& table = this->TableFor(key);
                       if constexpr (detail::kIsLongKey<decltype(key)>) {
                         table.emplace(std::move(word));
                       } else {
                         table.emplace(key);
                       }
                     });
  }
};

template<typename LongKey, typename Mapped>
class StringMap final
  : public detail::StringTable<sdb::containers::FlatHashMap, LongKey, Mapped> {
 public:
  Mapped& operator[](LongKey word) {
    return detail::Dispatch(MakeTermView(std::string_view{word}),
                            [&](const auto& key) -> Mapped& {
                              auto& table = this->TableFor(key);
                              if constexpr (detail::kIsLongKey<decltype(key)>) {
                                return table[std::move(word)];
                              } else {
                                return table[key];
                              }
                            });
  }

  IRS_FORCE_INLINE const Mapped* Find(this const auto& self,
                                      const auto& value) noexcept {
    return detail::Dispatch(
      detail::TermOf(value),
      [&](const auto& key) IRS_FORCE_INLINE -> const Mapped* {
        const auto& table = self.TableFor(key);
        const auto it = table.find(key);
        return it == table.end() ? nullptr : &it->second;
      });
  }

  void ForEachMapped(this auto& self, auto&& visit) {
    self.ForEachTable([&](auto& table) {
      for (auto& [key, mapped] : table) {
        visit(mapped);
      }
    });
  }
};

}  // namespace irs::analysis::dict
