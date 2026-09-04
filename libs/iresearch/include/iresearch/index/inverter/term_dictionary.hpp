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

#include <duckdb/common/types/string_type.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <span>

#include "basics/containers/flat_hash_set.h"
#include "basics/noncopyable.hpp"
#include "basics/resource_manager.hpp"
#include "iresearch/analysis/token_sink.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class TermDictionary : util::Noncopyable {
 public:
  struct HashedTerm {
    duckdb::string_t term;
    size_t hash;
  };

  using Entry = HashedTerm;
  static_assert(sizeof(Entry) == 24);

  // Dictionaries at or below this many distinct terms stay cache-resident, so
  // the probe hits cache and the prefetch-pipelined batch insert can't pay
  // back its extra hash/id passes; above it the table goes cold and prefetch
  // wins. Measured crossover (BM_DictResolveSweep, uniform mid-card corpus):
  // fused +4..9% at <=2048, parity at 4096, pipelined +2..7% at >=8192.
  static constexpr size_t kFusedProbeThreshold = 4096;

  TermDictionary(duckdb::ArenaAllocator& arena, IResourceManager& rm)
    : _map{0, RefHashEq{&_entries}, RefHashEq{&_entries}},
      _entries{ManagedTypedAllocator<Entry>{rm}},
      _arena{&arena} {}

  uint32_t Insert(const duckdb::string_t& term) {
    return InsertHashed({term, TermHash(term)});
  }

  // Picks fused vs prefetch-pipelined by dictionary size: a hot table can't
  // pay back the extra hash pass, a cold one hides its misses behind the
  // prefetch runway.
  IRS_FORCE_INLINE void Insert(std::span<const duckdb::string_t> terms,
                               std::span<uint64_t> hash_scratch,
                               std::span<uint32_t> out_ids) {
    const auto n = terms.size();
    SDB_ASSERT(n <= hash_scratch.size());
    SDB_ASSERT(n <= out_ids.size());
    if (Size() < kFusedProbeThreshold) {
      for (size_t j = 0; j < n; ++j) {
        out_ids[j] = Insert(terms[j]);
      }
      return;
    }
    for (size_t j = 0; j < n; ++j) {
      hash_scratch[j] = TermHash(terms[j]);
    }
    InsertHashed(terms, hash_scratch.first(n), out_ids.data());
  }

  uint32_t AppendUnique(const duckdb::string_t& term) {
    _entries.emplace_back(Intern(term), size_t{0});
    return static_cast<uint32_t>(_entries.size() - 1);
  }

  std::span<const Entry> Entries() const noexcept {
    return {_entries.data(), _entries.size()};
  }

  size_t Size() const noexcept { return _entries.size(); }

  void Reserve(size_t expected_terms) {
    _map.reserve(expected_terms);
    _entries.reserve(expected_terms);
  }

  void ShrinkEmptyMap() {
    if (_map.empty()) {
      _map.rehash(0);
    }
  }

  size_t Memory() const noexcept {
    return _entries.capacity() * sizeof(Entry) +
           _map.capacity() * (sizeof(uint32_t) + 1);
  }

  struct MemBreakdown {
    size_t entries_size = 0;
    size_t entries_capacity = 0;
    size_t map_capacity = 0;
    size_t interned_bytes = 0;
    size_t inline_terms = 0;
    size_t long_terms = 0;
  };

  MemBreakdown Breakdown() const noexcept {
    MemBreakdown b;
    b.entries_size = _entries.size();
    b.entries_capacity = _entries.capacity();
    b.map_capacity = _map.capacity();
    for (const auto& e : _entries) {
      if (e.term.GetSize() <= duckdb::string_t::INLINE_LENGTH) {
        ++b.inline_terms;
      } else {
        ++b.long_terms;
        b.interned_bytes += e.term.GetSize();
      }
    }
    return b;
  }

 private:
  struct RefHashEq {
    using is_transparent = void;

    size_t operator()(uint32_t ref) const noexcept {
      return (*entries)[ref].hash;
    }
    size_t operator()(const HashedTerm& key) const noexcept { return key.hash; }

    bool operator()(uint32_t lhs, uint32_t rhs) const noexcept {
      return lhs == rhs;
    }
    bool operator()(uint32_t lhs, const HashedTerm& rhs) const noexcept {
      return (*entries)[lhs].term == rhs.term;
    }
    bool operator()(const HashedTerm& lhs, uint32_t rhs) const noexcept {
      return operator()(rhs, lhs);
    }

    const ManagedVector<Entry>* entries;
  };

  static size_t TermHash(const duckdb::string_t& term) noexcept {
    if (term.GetSize() <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
      __uint128_t v;
      std::memcpy(&v, &term, sizeof v);
      return absl::HashOf(v);
    }
    return absl::Hash<bytes_view>{}(AsBytesView(term));
  }

  void InsertHashed(std::span<const duckdb::string_t> terms,
                    std::span<const uint64_t> hashes, uint32_t* out_ids) {
    SDB_ASSERT(terms.size() == hashes.size());
    constexpr size_t kPrefetchAhead = 8;

    const auto n = terms.size();
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
        ctor(id);
        is_new = true;
      });

    if (!is_new) [[likely]] {
      return *it;
    }

    try {
      _entries.emplace_back(Intern(key.term), key.hash);
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

  sdb::containers::FlatHashSet<uint32_t, RefHashEq, RefHashEq> _map;
  ManagedVector<Entry> _entries;
  duckdb::ArenaAllocator* _arena;
};

}  // namespace irs
