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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::detail {

template<typename Slots>
class SlotsScore {
 public:
  static constexpr bool kHasFreq =
    requires(const Slots& slots) { slots.Freq(); };
  static constexpr bool kHasScale =
    requires(const Slots& slots) { slots.Scale(); };
  static constexpr bool kHasSpread =
    requires(const Slots& slots) { slots.Spread(); };

  SlotsScore(const SubReader& segment, const TermReader& field,
             const ScoreArgs& args)
    : _recipe{&segment, &field, args} {}

  SlotsScore(SlotsScore&&) = delete;
  SlotsScore& operator=(SlotsScore&&) = delete;

  IRS_FORCE_INLINE void Fetch(const Slots& slots, uint32_t slot) noexcept {
    SDB_ASSERT(slot < kScoreBlock);
    if constexpr (kHasFreq) {
      _freqs[slot] = slots.Freq();
    }
    if constexpr (kHasScale) {
      _scales[slot] = slots.Scale();
    }
    if constexpr (kHasSpread) {
      _spreads[slot] = slots.Spread() * _recipe.args.boost;
    }
  }

  const score_t* Spreads() const noexcept
    requires kHasSpread
  {
    return _spreads;
  }

  ScoreFunction Prepare() {
    if constexpr (kHasFreq) {
      std::get<FreqBlockAttr>(_provider.attrs).value = _freqs;
    }
    if constexpr (kHasScale) {
      std::get<ScaleBlockAttr>(_provider.attrs).value = _scales;
    }
    SDB_ASSERT(_recipe.segment != nullptr && _recipe.field != nullptr);
    SDB_ASSERT(_recipe.args.scorer != nullptr);
    return _recipe.args.scorer->PrepareScorer({
      .segment = *_recipe.segment,
      .field = _recipe.field->meta(),
      .doc_attrs = _provider,
      .fetcher = *_recipe.args.fetcher,
      .stats = _recipe.args.stats,
      .boost = _recipe.args.boost,
    });
  }

 private:
  template<bool Want, typename T>
  using Maybe = std::conditional_t<Want, std::tuple<T>, std::tuple<>>;

  using Attrs =
    decltype(std::tuple_cat(std::declval<Maybe<kHasFreq, FreqBlockAttr>>(),
                            std::declval<Maybe<kHasScale, ScaleBlockAttr>>()));

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

  [[no_unique_address]] utils::Need<kHasFreq, uint32_t[kScoreBlock]> _freqs{};
  [[no_unique_address]] utils::Need<kHasScale, score_t[kScoreBlock]> _scales{};
  [[no_unique_address]] utils::Need<kHasSpread, score_t[kScoreBlock]>
    _spreads{};
  Provider _provider;
  LeafRecipe _recipe;
};

template<typename Slots, bool Scored>
class TwoPhaseBase {
 public:
  template<typename... Args>
  explicit TwoPhaseBase(Args&&... args)
    requires(!Scored)
    : _slots{std::forward<Args>(args)...} {}

  template<typename... Args>
  TwoPhaseBase(const SubReader& segment, const TermReader& field,
               const ScoreArgs& args, Args&&... slots)
    requires Scored
    : _score{segment, field, args}, _slots{std::forward<Args>(slots)...} {}

  TwoPhaseBase(TwoPhaseBase&&) = delete;
  TwoPhaseBase& operator=(TwoPhaseBase&&) = delete;

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept
    requires Scored
  {
    _score.Fetch(_slots, slot);
  }

  ScoreFunction PrepareScore()
    requires Scored
  {
    return _score.Prepare();
  }

  const score_t* Spreads() const noexcept
    requires(Scored && SlotsScore<Slots>::kHasSpread)
  {
    return _score.Spreads();
  }

  void CollectScorers(std::vector<ScoreFunction>& out)
    requires Scored
  {
    AppendScorer(out, PrepareScore());
  }

 protected:
  [[no_unique_address]] utils::Need<Scored, SlotsScore<Slots>> _score;
  Slots _slots;
};

}  // namespace irs::detail
