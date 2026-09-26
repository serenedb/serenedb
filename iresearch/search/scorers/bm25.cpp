////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "bm25.hpp"

#include <absl/algorithm/container.h>
#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>

#include <cstdint>
#include <exception>
#include <ranges>
#include <utility>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/score_bound_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/scale_score.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/scorers/scorer_options.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/attribute_provider.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

template<typename T>
constexpr const T* TryGetValue(const T* value) noexcept {
  return value;
}

constexpr std::nullptr_t TryGetValue(utils::Empty) noexcept { return nullptr; }

template<ScoreMergeType MergeType, bool HasScale>
IRS_FORCE_INLINE void Bm15(score_t* IRS_RESTRICT res, scores_size_t n,
                           const uint32_t* IRS_RESTRICT freq,
                           [[maybe_unused]] const score_t* IRS_RESTRICT scale,
                           score_t num, score_t c1) noexcept {
  SDB_ASSERT(c1 != 0.f);
  for (scores_size_t i = 0; i != n; ++i) {
    const auto tf = ScaledFreq<HasScale>(freq, scale, i);
    Merge<MergeType>(res[i], num - num / (1.f + tf / c1));
  }
}

template<ScoreMergeType MergeType, bool HasScale>
IRS_FORCE_INLINE void Bm25(score_t* IRS_RESTRICT res, scores_size_t n,
                           const uint32_t* IRS_RESTRICT freq,
                           const uint32_t* IRS_RESTRICT norm,
                           [[maybe_unused]] const score_t* IRS_RESTRICT scale,
                           score_t num, score_t norm_const,
                           score_t norm_length) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    const score_t c1 = norm_const + norm_length * TermCountToScore(norm[i]);
    const auto tf = ScaledFreq<HasScale>(freq, scale, i);
    Merge<MergeType>(res[i], num - num * c1 / (c1 + tf));
  }
}

template<bool HasScale>
struct Bm15Score : public ScoreOperator {
  Bm15Score(score_t boost, const BM25Stats& stats, const FreqBlockAttr* freq,
            const score_t* scale) noexcept
    : scale{scale},
      num{boost * stats.idf},
      norm_const{stats.norm_const},
      freq{freq} {
    SDB_ASSERT(this->freq);
  }

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    Bm15<MergeType, HasScale>(res, n, freq->value, TryGetValue(scale), num,
                              norm_const);
  }

  score_t Score() const noexcept final {
    score_t res{};
    ScoreImpl(&res, 1);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

  [[no_unique_address]] utils::Need<HasScale, const score_t*> scale;
  score_t num;
  score_t norm_const;
  const FreqBlockAttr* freq;
};

template<bool HasScale>
struct Bm25Score : public ScoreOperator {
  Bm25Score(score_t boost, const BM25Stats& stats, const FreqBlockAttr* freq,
            const uint32_t* norm, const score_t* scale) noexcept
    : scale{scale},
      num{boost * stats.idf},
      norm_const{stats.norm_const},
      freq{freq},
      norm{norm},
      norm_length{stats.norm_length} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    Bm25<MergeType, HasScale>(res, n, freq->value, norm, TryGetValue(scale),
                              num, norm_const, norm_length);
  }

  score_t Score() const noexcept final {
    score_t res{};
    ScoreImpl(&res, 1);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

  [[no_unique_address]] utils::Need<HasScale, const score_t*> scale;
  score_t num;
  score_t norm_const;
  const FreqBlockAttr* freq;
  const uint32_t* norm;
  score_t norm_length;
};

}  // namespace

void BM25::collect(byte_type* stats_buf, const irs::FieldCollector* field,
                   const irs::TermCollector* term) const {
  auto* stats = stats_cast(stats_buf);

  const auto docs_with_field = field ? field->docs_with_field : 0;
  const auto docs_with_term = term ? term->docs_with_term : 0;
  const auto total_term_freq = field ? field->total_term_freq : 0;

  stats->idf += score_t(
    std::log1p((static_cast<double>(docs_with_field - docs_with_term) + 0.5) /
               (static_cast<double>(docs_with_term) + 0.5)));
  SDB_ASSERT(stats->idf >= 0.f);

  if (!NeedsNorm()) {
    stats->norm_const = _k;
    return;
  }

  const score_t kb = _k * _b;

  stats->norm_const = _k - kb;
  if (total_term_freq && docs_with_field) {
    const auto avg_dl = static_cast<score_t>(total_term_freq) /
                        static_cast<score_t>(docs_with_field);
    stats->norm_length = kb / avg_dl;
  } else {
    stats->norm_length = kb;
  }
}

ScoreFunction BM25::PrepareScorer(const ScoreContext& ctx) const {
  auto* scale = [&] {
    auto* attr = irs::get<ScaleBlockAttr>(ctx.doc_attrs);
    return attr ? attr->value : nullptr;
  }();

  if (IsBM1()) {
    return MakeScaleScore(ctx, ctx.boost * stats_cast(ctx.stats)->idf);
  }

  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);

  if (!freq) {
    if (!_boost_as_score || 0.f == ctx.boost) {
      return ScoreFunction::Default();
    }

    return ScoreFunction::Constant(ctx.boost);
  }

  auto* stats = stats_cast(ctx.stats);

  return ResolveBool(scale != nullptr, [&]<bool HasScale>() {
    if (IsBM15()) {
      return ScoreFunction::Make<Bm15Score<HasScale>>(ctx.boost, *stats, freq,
                                                      scale);
    }

    const uint32_t* norm = [&] {
      auto* attr = irs::get<Norm>(ctx.doc_attrs);
      return attr ? &attr->value : nullptr;
    }();

    if (!norm) {
      auto norm_reader = ctx.segment.norms(ctx.field.norm);
      norm = ctx.fetcher.AddNorms(ctx.field.norm, std::move(norm_reader));
    }

    if (!norm) {
      norm = kNorms.data();
    }

    return ScoreFunction::Make<Bm25Score<HasScale>>(ctx.boost, *stats, freq,
                                                    norm, scale);
  });
}

ScoreBoundWriter::ptr BM25::PrepareScoreBoundWriter() const {
  if (IsBM1()) {
    SDB_ASSERT(BoundTypeOf(GetOptions()) == ScoreBoundType::None);
    return {};
  }
  if (IsBM15()) {
    SDB_ASSERT(BoundTypeOf(GetOptions()) == ScoreBoundType::MaxFreq);
    return std::make_unique<FreqNormWriter<kScoreBoundMaxFreq>>();
  }
  if (IsBM11()) {
    SDB_ASSERT(BoundTypeOf(GetOptions()) == ScoreBoundType::DivNorm);
    return std::make_unique<FreqNormWriter<kScoreBoundDivNorm>>();
  }
  SDB_ASSERT(BoundTypeOf(GetOptions()) == ScoreBoundType::MinNorm);
  if (_approximate) {
    return std::make_unique<FreqNormWriter<kScoreBoundAvgDL>>(_b);
  }
  return std::make_unique<FreqNormWriter<kScoreBoundBM25>>(_b);
}

ScoreBoundSource::ptr BM25::PrepareScoreBoundSource() const {
  if (IsBM1()) {
    SDB_ASSERT(BoundTypeOf(GetOptions()) == ScoreBoundType::None);
    return {};
  }
  if (IsBM15()) {
    SDB_ASSERT(BoundTypeOf(GetOptions()) == ScoreBoundType::MaxFreq);
    return std::make_unique<FreqNormSource<kScoreBoundFreq>>();
  }
  SDB_ASSERT(BoundTypeOf(GetOptions()) != ScoreBoundType::None);
  return std::make_unique<FreqNormSource<kScoreBoundFreq | kScoreBoundNorm>>();
}

bool BM25::Compatible(const ScorerOptions& persisted) const noexcept {
  const auto type = BoundTypeOf(GetOptions());
  if (type == ScoreBoundType::None || type != irs::BoundTypeOf(persisted)) {
    return false;
  }
  if (type != ScoreBoundType::MinNorm) {
    return true;
  }
  const auto* other = std::get_if<Options>(&persisted.params);
  return other && other->b == _b && other->approximate == _approximate;
}

std::string BM25::ToString() const {
  return absl::StrCat("bm25(k1=", _k, ", b=", _b, ")");
}

bool BM25::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = irs::utils::downCast<BM25>(other);
  return p._k == _k && p._b == _b;
}

}  // namespace irs
