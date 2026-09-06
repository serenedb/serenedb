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

#include <absl/hash/hash.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <span>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/noncopyable.hpp"
#include "basics/resource_manager.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/text/term_view.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class TermDictionary : util::Noncopyable {
 public:
  struct HashedTerm {
    duckdb::string_t term;
    uint32_t hash;
  };

  using Entry = duckdb::string_t;
  static_assert(sizeof(Entry) == 16);

  struct Ref {
    uint32_t id;
    uint32_t hash;
  };
  static_assert(sizeof(Ref) == 8);

  static constexpr size_t kBatch = 1024;

  TermDictionary(duckdb::ArenaAllocator& arena, IResourceManager& rm)
    : _map{0, RefHashEq{&_entries}, RefHashEq{&_entries}},
      _entries{ManagedTypedAllocator<Entry>{rm}},
      _arena{&arena} {}

  uint32_t Insert(const duckdb::string_t& term) {
    return InsertHashed({term, TermHash(term)});
  }

  IRS_FORCE_INLINE void Insert(std::span<const duckdb::string_t> terms,
                               std::span<uint32_t> out_ids) {
    const auto n = terms.size();
    SDB_ASSERT(n <= out_ids.size());
    for (size_t begin = 0; begin < n; begin += kBatch) {
      const auto count = std::min(n - begin, kBatch);
      InsertBatch(terms.subspan(begin, count), out_ids.data() + begin);
    }
  }

  uint32_t AppendUnique(const duckdb::string_t& term) {
    _entries.emplace_back(Intern(term));
    return static_cast<uint32_t>(_entries.size() - 1);
  }

  std::span<const Entry> Entries() const noexcept {
    return {_entries.data(), _entries.size()};
  }

  size_t Size() const noexcept { return _entries.size(); }

  void Reserve(size_t expected_terms) { _entries.reserve(expected_terms); }

  void ReserveMap(size_t expected_terms) { _map.reserve(expected_terms); }

  size_t Memory() const noexcept {
    return _entries.capacity() * sizeof(Entry) +
           _map.capacity() * (sizeof(Ref) + 1);
  }

 private:
  struct RefHashEq {
    using is_transparent = void;

    size_t operator()(Ref ref) const noexcept { return TableHash(ref.hash); }
    size_t operator()(const HashedTerm& key) const noexcept {
      return TableHash(key.hash);
    }

    bool operator()(Ref lhs, Ref rhs) const noexcept {
      return lhs.id == rhs.id;
    }
    bool operator()(Ref lhs, const HashedTerm& rhs) const noexcept {
      return lhs.hash == rhs.hash && (*entries)[lhs.id] == rhs.term;
    }
    bool operator()(const HashedTerm& lhs, Ref rhs) const noexcept {
      return operator()(rhs, lhs);
    }

    const ManagedVector<Entry>* entries;
  };

  static size_t TableHash(uint32_t hash) noexcept {
    return static_cast<size_t>(hash) << 32 | hash;
  }

  static uint32_t TermHash(const duckdb::string_t& term) noexcept {
    if (term.GetSize() <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
      __uint128_t v;
      std::memcpy(&v, &term, sizeof v);
      return static_cast<uint32_t>(absl::HashOf(v));
    }
    return static_cast<uint32_t>(absl::Hash<bytes_view>{}(AsBytesView(term)));
  }

  void InsertBatch(std::span<const duckdb::string_t> terms, uint32_t* out_ids) {
    constexpr size_t kPrefetchAhead = 8;
    const auto n = terms.size();
    SDB_ASSERT(n <= kBatch);

    uint32_t hashes[kBatch];
    for (size_t i = 0; i < n; ++i) {
      hashes[i] = TermHash(terms[i]);
    }
    for (size_t i = 0; i < n; ++i) {
      if (i + kPrefetchAhead < n) {
        _map.prefetch(
          HashedTerm{terms[i + kPrefetchAhead], hashes[i + kPrefetchAhead]});
      }
      out_ids[i] = InsertHashed({terms[i], hashes[i]});
    }
  }

  uint32_t InsertHashed(const HashedTerm& key) {
    SDB_ASSERT(_map.size() == _entries.size());

    bool is_new = false;
    const auto it = _map.lazy_emplace(
      key, [&, id = static_cast<uint32_t>(_entries.size())](const auto& ctor) {
        ctor(Ref{id, key.hash});
        is_new = true;
      });

    if (!is_new) [[likely]] {
      return it->id;
    }

    try {
      _entries.emplace_back(Intern(key.term));
      return static_cast<uint32_t>(_entries.size() - 1);
    } catch (...) {
      _map.erase(it);
      throw;
    }
  }

  duckdb::string_t Intern(const duckdb::string_t& term) {
    const auto size = term.GetSize();
    if (size <= duckdb::string_t::INLINE_LENGTH) {
      return term;
    }
    auto* mem = _arena->Allocate(size);
    std::memcpy(mem, term.GetData(), size);
    return duckdb::string_t{reinterpret_cast<const char*>(mem),
                            static_cast<uint32_t>(size)};
  }

  sdb::containers::FlatHashSet<Ref, RefHashEq, RefHashEq> _map;
  ManagedVector<Entry> _entries;
  duckdb::ArenaAllocator* _arena;
};

}  // namespace irs
