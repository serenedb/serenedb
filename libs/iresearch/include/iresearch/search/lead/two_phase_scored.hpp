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

#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "basics/empty.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/score_provider.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename Slots>
class TwoPhaseScored {
 public:
  static constexpr bool kHasFreq =
    requires(const Slots& slots) { slots.Freq(); };
  static constexpr bool kHasBoost =
    requires(const Slots& slots) { slots.Boost(); };

  template<typename... Args>
  TwoPhaseScored(const SubReader& segment, const TermReader& field,
                 const search::ScoreArgs& args, Args&&... slots)
    : _slots{std::forward<Args>(slots)...}, _recipe{&segment, &field, args} {}

  TwoPhaseScored(TwoPhaseScored&&) = delete;
  TwoPhaseScored& operator=(TwoPhaseScored&&) = delete;

  doc_id_t Advance() { return Converge(_slots.Next(_doc)); }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    return Converge(_slots.Seek(target));
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept {
    if constexpr (kHasFreq) {
      _freqs[slot] = _slots.Freq();
    }
    if constexpr (kHasBoost) {
      _boosts[slot] = _slots.Boost();
    }
  }

  ScoreFunction PrepareScore() {
    if constexpr (kHasFreq) {
      std::get<FreqBlockAttr>(_provider.attrs).value = _freqs;
    }
    if constexpr (kHasBoost) {
      std::get<BoostBlockAttr>(_provider.attrs).value = _boosts;
    }
    SDB_ASSERT(_recipe.segment != nullptr && _recipe.field != nullptr);
    SDB_ASSERT(_recipe.args.scorer != nullptr);
    return _recipe.args.scorer->PrepareScorer({
      .segment = *_recipe.segment,
      .field = _recipe.field->meta(),
      .doc_attrs = _provider,
      .fetcher = _recipe.args.fetcher,
      .stats = _recipe.args.stats,
      .boost = _recipe.args.boost,
    });
  }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    search::AppendScorer(out, PrepareScore());
  }

 private:
  doc_id_t Converge(doc_id_t target) {
    while (!doc_limits::eof(target)) {
      if (_slots.Match(target)) {
        return _doc = target;
      }
      target = _slots.Next(target);
    }
    return _doc = target;
  }

  using Attrs = std::conditional_t<
    kHasBoost, std::tuple<FreqBlockAttr, BoostBlockAttr>,
    std::conditional_t<kHasFreq, std::tuple<FreqBlockAttr>, std::tuple<>>>;

  struct Provider final : AttributeProvider {
    Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
      if constexpr (std::tuple_size_v<Attrs> == 0) {
        return nullptr;
      } else {
        return irs::GetMutable(attrs, type);
      }
    }

    [[no_unique_address]] Attrs attrs;
  };

  [[no_unique_address]] utils::Need<kHasFreq, uint32_t[doc_limits::kBlockSize]>
    _freqs{};
  [[no_unique_address]] utils::Need<kHasBoost, score_t[doc_limits::kBlockSize]>
    _boosts{};
  Slots _slots;
  Provider _provider;
  search::LeafRecipe _recipe;
  doc_id_t _doc = doc_limits::invalid();
};

}  // namespace irs::lead
