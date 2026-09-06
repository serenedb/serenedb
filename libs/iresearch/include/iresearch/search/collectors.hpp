////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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

#include <absl/base/optimization.h>

#include <algorithm>
#include <memory>
#include <span>
#include <vector>

#include "basics/down_cast.h"
#include "basics/shared.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/stats_arena.hpp"

namespace irs {

inline size_t GetStatsSize(const Scorer* scorer) noexcept {
  return scorer ? scorer->stats_size() : 0;
}

inline size_t StatsSlot(const Scorer* scorer) noexcept {
  return (std::max<size_t>(GetStatsSize(scorer), 8) + 7) / 8 * 8;
}

struct FieldCollector {
  void Collect(const TermReader& field) noexcept;

  uint64_t docs_with_field = 0;
  uint64_t total_term_freq = 0;
};

struct TermCollector {
  void Collect(const PostingMeta& meta) noexcept {
    docs_with_term += meta.docs_count;
    total_term_freq += meta.freq;
  }

  uint64_t docs_with_term = 0;
  uint64_t total_term_freq = 0;
};

struct alignas(ABSL_CACHELINE_SIZE) CounterBlock {
  static constexpr size_t kTerms =
    (ABSL_CACHELINE_SIZE - sizeof(FieldCollector)) / sizeof(TermCollector);

  FieldCollector field;
  TermCollector terms[kTerms];
};

class CounterSlots {
 public:
  CounterSlots(uint32_t threads, size_t terms)
    : _blocks(static_cast<size_t>(threads) * BlocksPerThread(terms)),
      _per_thread{BlocksPerThread(terms)},
      _terms{terms},
      _threads{threads} {
    SDB_ASSERT(threads >= 1);
  }

  uint32_t Threads() const noexcept { return _threads; }

  FieldCollector& Field(uint32_t thread) noexcept {
    SDB_ASSERT(thread < _threads);
    return _blocks[static_cast<size_t>(thread) * _per_thread].field;
  }

  TermCollector& Term(uint32_t thread, size_t i) noexcept {
    SDB_ASSERT(thread < _threads);
    SDB_ASSERT(i < _terms);
    return _blocks[static_cast<size_t>(thread) * _per_thread +
                   i / CounterBlock::kTerms]
      .terms[i % CounterBlock::kTerms];
  }

  FieldCollector TotalField() const noexcept {
    FieldCollector out;
    for (size_t t = 0; t != _threads; ++t) {
      const auto& src = _blocks[t * _per_thread].field;
      out.docs_with_field += src.docs_with_field;
      out.total_term_freq += src.total_term_freq;
    }
    return out;
  }

  TermCollector TotalTerm(size_t i) const noexcept {
    SDB_ASSERT(i < _terms);
    TermCollector out;
    for (size_t t = 0; t != _threads; ++t) {
      const auto& src = _blocks[t * _per_thread + i / CounterBlock::kTerms]
                          .terms[i % CounterBlock::kTerms];
      out.docs_with_term += src.docs_with_term;
      out.total_term_freq += src.total_term_freq;
    }
    return out;
  }

 private:
  static size_t BlocksPerThread(size_t terms) noexcept {
    return std::max<size_t>(
      1, (terms + CounterBlock::kTerms - 1) / CounterBlock::kTerms);
  }

  search::FixedArray<CounterBlock> _blocks;
  size_t _per_thread;
  size_t _terms;
  uint32_t _threads;
};

class CompoundCollector;

class PrepareCollector {
 public:
  using ptr = std::unique_ptr<PrepareCollector>;

  explicit PrepareCollector(const Scorer* scorer) noexcept : _scorer{scorer} {}

  virtual ~PrepareCollector() = default;

  virtual void Finish(StatsArena& stats) = 0;

  virtual CompoundCollector* AsCompound() noexcept { return nullptr; }

  const Scorer* GetScorer() const noexcept { return _scorer; }

  search::StatsRecord Record() const noexcept { return {_stats, _scorer}; }

  void Retain(memory::managed_ptr<const memory::Managed> query) {
    _retained.emplace_back(std::move(query));
  }

 protected:
  const byte_type* _stats = nullptr;
  const Scorer* _scorer = nullptr;

 private:
  std::vector<memory::managed_ptr<const memory::Managed>> _retained;
};

class FieldPrepareCollector : public PrepareCollector {
 public:
  FieldPrepareCollector(const Scorer* scorer, StatsArena& stats,
                        uint32_t threads)
    : FieldPrepareCollector{scorer, stats, threads, 0, true} {}

  FieldCollector& Field(uint32_t thread) noexcept {
    return _counters.Field(thread);
  }

  void Finish(StatsArena& stats) override;

 protected:
  FieldPrepareCollector(const Scorer* scorer, StatsArena& stats,
                        uint32_t threads, size_t terms, bool own_slot)
    : PrepareCollector{scorer}, _counters{threads, terms} {
    if (own_slot) {
      _stats = stats.Allocate(StatsSlot(scorer));
    }
  }

  CounterSlots _counters;
};

class ByTermsCollector final : public FieldPrepareCollector {
 public:
  ByTermsCollector(const Scorer* scorer, size_t size, StatsArena& stats,
                   uint32_t threads);

  using PrepareCollector::Record;

  search::StatsRecord Record(size_t i) const noexcept {
    SDB_ASSERT(i < _size);
    return {_stats + i * _slot, _scorer};
  }

  TermCollector& Term(uint32_t thread, size_t i) noexcept {
    return _counters.Term(thread, i);
  }

  size_t Size() const noexcept { return _size; }

  void Finish(StatsArena& stats) final;

 private:
  size_t _size;
  size_t _slot;
};

class PhraseCollector final : public FieldPrepareCollector {
 public:
  PhraseCollector(const Scorer* scorer, size_t size, StatsArena& stats,
                  uint32_t threads)
    : FieldPrepareCollector{scorer, stats, threads, 0, true},
      _size{size},
      _parts(static_cast<size_t>(threads) * size) {}

  std::vector<TermCollector>& Part(uint32_t thread, size_t i) noexcept {
    SDB_ASSERT(i < _size);
    return _parts[thread * _size + i];
  }

  size_t Size() const noexcept { return _size; }

  void Finish(StatsArena& stats) final;

 private:
  size_t _size;
  search::FixedArray<std::vector<TermCollector>> _parts;
};

class AllCollector final : public PrepareCollector {
 public:
  AllCollector(const Scorer* scorer, StatsArena& stats)
    : PrepareCollector{scorer} {
    _stats = stats.Allocate(StatsSlot(scorer));
  }

  void Finish(StatsArena& stats) final;
};

class CompoundCollector final : public PrepareCollector {
 public:
  explicit CompoundCollector(const Scorer* scorer) noexcept
    : PrepareCollector{scorer} {}

  void Add(PrepareCollector::ptr child) {
    _children.emplace_back(std::move(child));
  }

  CompoundCollector* AsCompound() noexcept final { return this; }

  PrepareCollector* Child(size_t i) noexcept { return _children[i].get(); }

  auto Size() const noexcept { return _children.size(); }

  void Finish(StatsArena& stats) final;

 private:
  std::vector<PrepareCollector::ptr> _children;
};

}  // namespace irs
