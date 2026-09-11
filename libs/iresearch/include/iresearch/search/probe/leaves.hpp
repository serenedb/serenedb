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
#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "basics/shared.hpp"
#include "iresearch/search/detail/fixed_array.hpp"
#include "iresearch/search/scorers/make_probe.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/probe/concept.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::probe {

template<Type Leaf, size_t N = 0>
class AndLeaves {
 public:
  template<typename Init>
  AndLeaves(size_t size, Init&& init)
    : _leaves{size, std::forward<Init>(init)} {
    SDB_ASSERT(_leaves.size() > 1);
  }

  template<typename... Args>
  explicit AndLeaves(std::piecewise_construct_t, Args&&... args)
    : _leaves{std::piecewise_construct, std::forward<Args>(args)...} {
    static_assert(N > 1);
  }

  AndLeaves(AndLeaves&&) = delete;
  AndLeaves& operator=(AndLeaves&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    for (auto& leaf : _leaves) {
      if (const auto probe = leaf.Probe(target); probe != target) {
        return probe;
      }
    }
    return target;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    for (auto& leaf : _leaves) {
      leaf.FetchScoreArgs(slot);
    }
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    for (auto& leaf : _leaves) {
      leaf.CollectScorers(out);
    }
  }

 private:
  detail::RunOf<Leaf, N> _leaves;
};

template<Type Leaf, size_t N = 0, bool Scored = false>
class OrLeaves {
 public:
  static constexpr bool kDecides = true;
  static constexpr bool kBounded = requires(Leaf& leaf, doc_id_t doc) {
    { leaf.AdvanceBlock(doc) } -> std::same_as<doc_id_t>;
    { leaf.MaxScore(doc) } -> std::same_as<score_t>;
  };

  template<typename Init>
  OrLeaves(size_t size, Init&& init)
    : _leaves{size, std::forward<Init>(init)}, _held{size} {
    SDB_ASSERT(!_leaves.empty());
  }

  OrLeaves(OrLeaves&&) = delete;
  OrLeaves& operator=(OrLeaves&&) = delete;

  doc_id_t Probe(doc_id_t target) {
    auto next = doc_limits::eof();
    if constexpr (Scored) {
      _hit = false;
    }
    for (size_t i = 0, count = _leaves.size(); i != count; ++i) {
      const auto doc = _leaves[i].Probe(target);
      if (doc == target) {
        if constexpr (Scored) {
          _doc = target;
          _first = static_cast<uint32_t>(i);
          _hit = true;
        }
        return target;
      }
      next = std::min(next, doc);
    }
    return next;
  }

  doc_id_t AdvanceBlock(doc_id_t target)
    requires kBounded
  {
    auto end = doc_limits::eof();
    for (auto& leaf : _leaves) {
      end = std::min(end, leaf.AdvanceBlock(target));
    }
    return end;
  }

  score_t MaxScore(doc_id_t last) noexcept
    requires kBounded
  {
    score_t bound = 0;
    for (auto& leaf : _leaves) {
      bound += leaf.MaxScore(last);
    }
    return bound;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot)
    requires Scored
  {
    SDB_ASSERT(slot < kScoreBlock);
    if (!_hit) {
      return;
    }
    SetBit(_held[_first], slot);
    _leaves[_first].FetchScoreArgs(slot);
    for (size_t i = _first + 1, count = _leaves.size(); i != count; ++i) {
      if (_leaves[i].Probe(_doc) != _doc) {
        continue;
      }
      SetBit(_held[i], slot);
      _leaves[i].FetchScoreArgs(slot);
    }
  }

  ScoreFunction PrepareScore(ScoreMergeType inner, score_t absorbed)
    requires Scored
  {
    return detail::MakeProbeOf(inner, _leaves, _held, absorbed);
  }

 private:
  detail::RunOf<Leaf, N> _leaves;
  [[no_unique_address]] utils::Need<Scored, detail::RunOf<uint32_t, N>> _held;
  [[no_unique_address]] utils::Need<Scored, doc_id_t> _doc =
    doc_limits::invalid();
  [[no_unique_address]] utils::Need<Scored, uint32_t> _first = 0;
  [[no_unique_address]] utils::Need<Scored, bool> _hit = false;
};

template<Type Leaf, size_t N = 0, bool Scored = false>
class ThresholdLeaves {
 public:
  static constexpr bool kDecides = true;

  template<typename Init>
  ThresholdLeaves(size_t size, Init&& init, uint32_t min_match)
    : _probes{size, std::forward<Init>(init)},
      _held{size},
      _matched{min_match},
      _min_match{min_match} {
    SDB_ASSERT(_min_match > 1);
    SDB_ASSERT(_probes.size() >= _min_match);
  }

  ThresholdLeaves(ThresholdLeaves&&) = delete;
  ThresholdLeaves& operator=(ThresholdLeaves&&) = delete;

  doc_id_t Probe(doc_id_t target) {
    if constexpr (Scored) {
      auto next = doc_limits::eof();
      uint32_t hits = 0;
      auto left = static_cast<uint32_t>(_probes.size());
      for (size_t i = 0, count = _probes.size(); i != count; ++i) {
        const auto doc = _probes[i].Probe(target);
        if (doc == target) {
          _matched[hits++] = static_cast<uint32_t>(i);
          if (hits == _min_match) {
            _doc = target;
            return target;
          }
        } else {
          next = std::min(next, doc);
        }
        if (hits + --left < _min_match) {
          return target + 1;
        }
      }
      return next;
    } else {
      uint32_t hits = 0;
      uint32_t left = static_cast<uint32_t>(_probes.size());
      for (auto& probe : _probes) {
        hits += static_cast<uint32_t>(probe.Probe(target) == target);
        if (hits == _min_match) {
          return target;
        }
        --left;
        if (hits + left < _min_match) {
          break;
        }
      }
      return target + 1;
    }
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot)
    requires Scored
  {
    SDB_ASSERT(slot < kScoreBlock);
    for (const auto matched : _matched) {
      SetBit(_held[matched], slot);
      _probes[matched].FetchScoreArgs(slot);
    }
    for (size_t i = _matched[_min_match - 1] + 1, count = _probes.size();
         i != count; ++i) {
      if (_probes[i].Probe(_doc) != _doc) {
        continue;
      }
      SetBit(_held[i], slot);
      _probes[i].FetchScoreArgs(slot);
    }
  }

  ScoreFunction PrepareScore(ScoreMergeType inner, score_t absorbed)
    requires Scored
  {
    return detail::MakeProbeOf(inner, _probes, _held, absorbed);
  }

 private:
  detail::RunOf<Leaf, N> _probes;
  [[no_unique_address]] utils::Need<Scored, detail::RunOf<uint32_t, N>> _held;
  [[no_unique_address]] utils::Need<Scored, detail::RunOf<uint32_t, N>>
    _matched;
  [[no_unique_address]] utils::Need<Scored, doc_id_t> _doc =
    doc_limits::invalid();
  uint32_t _min_match;
};

template<Type Leaf, size_t N = 0>
class BoostLeaves {
 public:
  static constexpr bool kDecides = false;

  template<typename Init>
  BoostLeaves(size_t size, Init&& init)
    : _leaves{size, std::forward<Init>(init)}, _held{size} {
    SDB_ASSERT(!_leaves.empty());
  }

  BoostLeaves(BoostLeaves&&) = delete;
  BoostLeaves& operator=(BoostLeaves&&) = delete;

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    _doc = target;
    return target;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    FetchScoreArgs(slot, _doc);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot, doc_id_t doc) {
    SDB_ASSERT(slot < kScoreBlock);
    for (size_t i = 0, count = _leaves.size(); i != count; ++i) {
      if (_leaves[i].Probe(doc) != doc) {
        continue;
      }
      SetBit(_held[i], slot);
      _leaves[i].FetchScoreArgs(slot);
    }
  }

  ScoreFunction PrepareScore(ScoreMergeType inner) {
    return detail::MakeProbeOf(inner, _leaves, _held);
  }

  ScoreFunction PrepareScore(ScoreMergeType inner, ScoreFunction&& required,
                             score_t absorbed) {
    std::vector<ScoreFunction> probed;
    probed.reserve(_leaves.size());
    for (auto& leaf : _leaves) {
      probed.emplace_back(leaf.PrepareScore());
    }
    return detail::MakeProbeScore(inner, std::move(required), std::move(probed),
                                  _held.data(), absorbed);
  }

 private:
  detail::RunOf<Leaf, N> _leaves;
  detail::RunOf<uint32_t, N> _held;
  doc_id_t _doc = doc_limits::invalid();
};

class NoLeaves {
 public:
  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) noexcept { return target; }
  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t) noexcept {}
  void CollectScorers(std::vector<ScoreFunction>&) const noexcept {}
};

}  // namespace irs::probe
