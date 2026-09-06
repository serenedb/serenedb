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

#include <cstring>
#include <memory>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "basics/down_cast.h"
#include "basics/noncopyable.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/states/multiterm_state.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct TermHash {
  using is_transparent = void;

  size_t operator()(bytes_view term) const noexcept {
    return absl::HashOf(term);
  }
};

struct TermEq {
  using is_transparent = void;

  bool operator()(bytes_view lhs, bytes_view rhs) const noexcept {
    return lhs == rhs;
  }
};

class MultiTermCollector final : public FieldPrepareCollector {
 public:
  MultiTermCollector(const Scorer* scorer, StatsArena& stats, uint32_t threads)
    : FieldPrepareCollector{scorer, stats, threads, 0, false},
      _slot{StatsSlot(scorer)},
      _threads(threads) {}

  const byte_type* Collect(uint32_t thread, bytes_view term,
                           const PostingMeta& meta) {
    SDB_ASSERT(thread < _threads.size());
    auto& own = _threads[thread];
    auto it = own.terms.find(term);
    if (it == own.terms.end()) {
      it = own.terms.emplace(bstring{term}, Slot{.stats = Allocate(own)}).first;
    }
    it->second.counter.Collect(meta);
    return it->second.stats;
  }

  void Finish(StatsArena&) final {
    if (_scorer == nullptr) {
      return;
    }
    const auto field = _counters.TotalField();
    struct Merged {
      TermCollector counter;
      std::vector<byte_type*> slots;
    };
    sdb::containers::NodeHashMap<bstring, Merged, TermHash, TermEq> merged;
    for (auto& own : _threads) {
      for (auto& [term, slot] : own.terms) {
        auto& one = merged[term];
        one.counter.docs_with_term += slot.counter.docs_with_term;
        one.counter.total_term_freq += slot.counter.total_term_freq;
        one.slots.push_back(slot.stats);
      }
    }
    for (auto& [term, one] : merged) {
      auto* const first = one.slots.front();
      _scorer->collect(first, &field, &one.counter);
      for (size_t i = 1; i != one.slots.size(); ++i) {
        std::memcpy(one.slots[i], first, _slot);
      }
    }
  }

 private:
  static constexpr size_t kChunkSlots = 64;

  struct Slot {
    TermCollector counter;
    byte_type* stats = nullptr;
  };

  struct Thread {
    sdb::containers::NodeHashMap<bstring, Slot, TermHash, TermEq> terms;
    std::vector<std::unique_ptr<byte_type[]>> chunks;
    size_t used = 0;
  };

  byte_type* Allocate(Thread& own) {
    if (own.used == own.chunks.size() * kChunkSlots) {
      own.chunks.emplace_back(
        std::make_unique<byte_type[]>(_slot * kChunkSlots));
    }
    auto* const slot =
      own.chunks.back().get() + (own.used % kChunkSlots) * _slot;
    ++own.used;
    return slot;
  }

  size_t _slot;
  std::vector<Thread> _threads;
};

class MultiTermVisitor : util::Noncopyable {
 public:
  MultiTermVisitor(const PrepareContext& ctx, MultiTermState& state,
                   const TermReader& field)
    : _state{state}, _thread{ctx.thread} {
    if (ctx.collector == nullptr) {
      return;
    }
    const auto record = ctx.Record();
    if (!ScoresPerDoc(record.scorer)) {
      _stats = record.stats;
      return;
    }
    _collector = &sdb::basics::downCast<MultiTermCollector>(*ctx.collector);
    _collector->Field(_thread).Collect(field);
  }

  void Prepare(const SubReader&, const TermReader& field,
               TermIterator& terms) noexcept {
    _state.Prepare(&field);
    _terms = &terms;
  }

  bool Visit(score_t boost) {
    SDB_ASSERT(_terms);
    const auto& meta = _terms->cookie();
    const byte_type* stats = _stats;
    if (_collector != nullptr) {
      stats = _collector->Collect(_thread, _terms->value(), meta);
    }
    _state.Push(meta, boost, stats);
    return true;
  }

 private:
  MultiTermState& _state;
  MultiTermCollector* _collector = nullptr;
  const byte_type* _stats = nullptr;
  TermIterator* _terms = nullptr;
  uint32_t _thread;
};

}  // namespace irs
