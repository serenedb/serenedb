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

#include <duckdb/common/types/string_type.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <string_view>

#include "basics/containers/flat_hash_set.h"
#include "basics/noncopyable.hpp"
#include "basics/resource_manager.hpp"
#include "iresearch/analysis/batch/term_view.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class TermDictionary : util::Noncopyable {
 public:
  // Up to this many first occurrences of a term live inline in its entry
  // (TokenLayout::Terms fields -- docs-only/freq): the log only holds the
  // rest and the scatter folds the inline docs into the term's region ahead
  // of the log's. There is no occurrence counter: a zero slot
  // (doc_limits::invalid) is free, and flush derives per-term counts from a
  // histogram over the log plus the inline slots. The freed bytes hold the
  // term's hash, so the map stores bare u32 ids.
  static constexpr uint32_t kInlineOccs = 2;

  struct HashedTerm {
    duckdb::string_t term;
    size_t hash;
  };

  struct Entry : HashedTerm {
    Entry(bytes_view term, size_t hash) noexcept
      : HashedTerm{{reinterpret_cast<const char*>(term.data()),
                    static_cast<uint32_t>(term.size())},
                   hash} {}

    bytes_view TermBytes() const noexcept {
      return {reinterpret_cast<const byte_type*>(term.GetData()),
              term.GetSize()};
    }

    doc_id_t inline_docs[kInlineOccs]{};
  };
  static_assert(sizeof(Entry) == 32);

  TermDictionary(duckdb::ArenaAllocator& arena, IResourceManager& rm)
    : _map{0, RefHash{&_entries}, RefEq{&_entries}},
      _entries{ManagedTypedAllocator<Entry>{rm}},
      _arena{&arena} {}

  // Inline terms hash the canonical 16-byte string_t value (two words, one
  // 128-bit multiply, per-process seeded); longer terms hash the exact bytes
  // via absl::Hash. No overread either way, so batch terms only need to stay
  // valid for the duration of the consumer callback.
  template<typename T>
  static size_t TermHash(const T& term) noexcept {
    const auto probe = ProbeTerm(term);
    if (probe.GetSize() <= duckdb::string_t::INLINE_LENGTH) [[likely]] {
      uint64_t w0;
      uint64_t w1;
      std::memcpy(&w0, &probe, sizeof w0);
      std::memcpy(&w1, reinterpret_cast<const char*>(&probe) + sizeof w0,
                  sizeof w1);
      const auto m = static_cast<__uint128_t>(w0 ^ kHashSeed) * (w1 ^ kHashMul);
      return static_cast<size_t>(m) ^ static_cast<size_t>(m >> 64);
    }
    return absl::Hash<bytes_view>{}(bytes_view{
      reinterpret_cast<const byte_type*>(probe.GetData()), probe.GetSize()});
  }

  // Resolve-only: interns the term on first sight and returns its id. All
  // occurrence accounting (inline capture, log pushes) is the inverter's,
  // kept atomic with its validation: a resolved-but-never-used term is a
  // legal leftover of a rejected batch and is dropped at flush. There is no
  // term size cap: every real source is u32-bounded by construction (values
  // are duckdb string_t, analyzer terms derive from them), asserted at the
  // one size_t->u32 narrowing in ProbeTerm.
  template<typename T>
  uint32_t Resolve(const T& term) {
    return Resolve(term, TermHash(term));
  }

  template<typename T>
  uint32_t Resolve(const T& term, size_t hash) {
    return ResolveHashed({ProbeTerm(term), hash});
  }

  bool TryInline(uint32_t id, doc_id_t doc) noexcept {
    SDB_ASSERT(id < _entries.size());
    for (auto& slot : _entries[id].inline_docs) {
      if (!slot) {
        slot = doc;
        return true;
      }
    }
    return false;
  }

  // Batched probe: hashes precomputed (TermHash), probes software-pipelined.
  template<typename T>
  void ResolveBatch(std::span<const T> terms, std::span<const uint64_t> hashes,
                    uint32_t* out_ids) {
    SDB_ASSERT(terms.size() == hashes.size());
    constexpr size_t kPrefetchAhead = 8;

    const auto n = terms.size();
    for (size_t i = 0; i < n; ++i) {
      if (i + kPrefetchAhead < n) {
        _map.prefetch(HashedTerm{ProbeTerm(terms[i + kPrefetchAhead]),
                                 hashes[i + kPrefetchAhead]});
      }
      out_ids[i] = Resolve(terms[i], hashes[i]);
    }
  }

  std::span<const Entry> Entries() const noexcept {
    return {_entries.data(), _entries.size()};
  }

  size_t Size() const noexcept { return _entries.size(); }

  void Reserve(size_t expected_terms) {
    _map.reserve(expected_terms);
    _entries.reserve(expected_terms);
  }

  size_t Memory() const noexcept {
    return _entries.capacity() * sizeof(Entry) +
           _map.capacity() * sizeof(uint32_t);
  }

 private:
  static constexpr uint64_t kHashMul = 0xe7037ed1a0b428dbULL;
  inline static const uint64_t kHashSeed =
    absl::Hash<uint64_t>{}(0xa0761d6478bd642fULL) | 1;

  static duckdb::string_t ProbeTerm(const duckdb::string_t& term) noexcept {
    return term;
  }
  static duckdb::string_t ProbeTerm(std::string_view term) noexcept {
    SDB_ASSERT(term.size() <= std::numeric_limits<uint32_t>::max());
    return MakeTermView(term.data(), static_cast<uint32_t>(term.size()));
  }
  static duckdb::string_t ProbeTerm(bytes_view term) noexcept {
    SDB_ASSERT(term.size() <= std::numeric_limits<uint32_t>::max());
    return MakeTermView(term.data(), static_cast<uint32_t>(term.size()));
  }

  struct RefHash {
    using is_transparent = void;

    size_t operator()(uint32_t ref) const noexcept {
      return (*entries)[ref].hash;
    }
    size_t operator()(const HashedTerm& key) const noexcept { return key.hash; }

    const ManagedVector<Entry>* entries;
  };

  struct RefEq {
    using is_transparent = void;

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

  uint32_t ResolveHashed(const HashedTerm& key) {
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
      const bytes_view term{
        reinterpret_cast<const byte_type*>(key.term.GetData()),
        key.term.GetSize()};
      _entries.emplace_back(Intern(term), key.hash);
      return static_cast<uint32_t>(_entries.size() - 1);
    } catch (...) {
      _map.erase(it);
      throw;
    }
  }

  bytes_view Intern(bytes_view term) {
    if (term.size() <= duckdb::string_t::INLINE_LENGTH) {
      return term;
    }
    auto* mem = _arena->Allocate(term.size());
    std::memcpy(mem, term.data(), term.size());
    return {reinterpret_cast<const byte_type*>(mem), term.size()};
  }

  sdb::containers::FlatHashSet<uint32_t, RefHash, RefEq> _map;
  ManagedVector<Entry> _entries;
  duckdb::ArenaAllocator* _arena;
};

}  // namespace irs
