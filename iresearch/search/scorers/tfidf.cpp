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

#include "tfidf.hpp"

#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>

#include <cmath>
#include <cstddef>
#include <string_view>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/score_bound_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/search/scorers/score_inputs.hpp"
#include "iresearch/search/scorers/scorer.hpp"
#include "iresearch/search/scorers/scorer_options.hpp"
#include "iresearch/utils/down_cast.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

IRS_FORCE_INLINE score_t TfIdf(score_t tf, score_t idf) noexcept {
  return std::sqrt(tf) * idf;
}

template<ScoreMergeType MergeType, bool HasNorm, bool HasScale>
IRS_FORCE_INLINE void TfIdf(score_t* IRS_RESTRICT res, scores_size_t n,
                            const uint32_t* IRS_RESTRICT freq,
                            [[maybe_unused]] const uint32_t* IRS_RESTRICT norm,
                            [[maybe_unused]] const score_t* IRS_RESTRICT scale,
                            score_t idf) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    const auto r = TfIdf(ScaledFreq<HasScale>(freq, scale, i), idf);
    if constexpr (HasNorm) {
      Merge<MergeType>(res[i], r / std::sqrt(TermCountToScore(norm[i])));
    } else {
      Merge<MergeType>(res[i], r);
    }
  }
}

template<bool HasNorm, bool HasScale>
struct TfIdfScore : public ScoreOperatorOf<TfIdfScore<HasNorm, HasScale>> {
  TfIdfScore(const uint32_t* norm, score_t boost, TFIDFStats idf,
             const FreqBlockAttr* freq, const score_t* scale = nullptr) noexcept
    : freq{freq}, scale{scale}, norm{norm}, idf{boost * idf.value} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    TfIdf<MergeType, HasNorm, HasScale>(res, n, freq->value, TryGetValue(norm),
                                        TryGetValue(scale), idf);
  }

  const FreqBlockAttr* freq;
  [[no_unique_address]] utils::Need<HasScale, const score_t*> scale;
  [[no_unique_address]] utils::Need<HasNorm, const uint32_t*> norm;
  score_t idf;
};

}  // namespace

void TFIDF::collect(byte_type* stats_buf, const FieldCollector* field,
                    const TermCollector* term) const {
  const auto docs_with_field = field ? field->docs_with_field : 0;
  const auto docs_with_term = term ? term->docs_with_term : 0;

  auto* idf = stats_cast(stats_buf);
  idf->value += static_cast<score_t>(
    std::log1p((docs_with_field + 1.0) / (docs_with_term + 1.0)));
}

ScoreFunction TFIDF::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);

  if (!freq) {
    if (!_boost_as_score || 0.f == ctx.boost) {
      return ScoreFunction::Default();
    }

    return ScoreFunction::Constant(ctx.boost);
  }

  const auto* scale = ScaleOf(ctx);

  const uint32_t* norm = _normalize ? FieldNormsOf(ctx) : nullptr;

  return ResolveBool(norm, [&]<bool HasNorms>() {
    return ResolveBool(scale, [&]<bool HasScale>() {
      const auto* stats = stats_cast(ctx.stats);
      return ScoreFunction::Make<TfIdfScore<HasNorms, HasScale>>(
        norm, ctx.boost, *stats, freq, scale);
    });
  });
}

ScoreBoundWriter::ptr TFIDF::PrepareScoreBoundWriter() const {
  if (_normalize) {
    SDB_ASSERT(BoundTypeOf(GetOptions()) == ScoreBoundType::DivNorm);
    return std::make_unique<FreqNormWriter<kScoreBoundDivNorm>>();
  }
  SDB_ASSERT(BoundTypeOf(GetOptions()) == ScoreBoundType::MaxFreq);
  return std::make_unique<FreqNormWriter<kScoreBoundMaxFreq>>();
}

ScoreBoundSource::ptr TFIDF::PrepareScoreBoundSource() const {
  if (_normalize) {
    SDB_ASSERT(BoundTypeOf(GetOptions()) == ScoreBoundType::DivNorm);
    return std::make_unique<
      FreqNormSource<kScoreBoundFreq | kScoreBoundNorm>>();
  }
  SDB_ASSERT(BoundTypeOf(GetOptions()) == ScoreBoundType::MaxFreq);
  return std::make_unique<FreqNormSource<kScoreBoundFreq>>();
}

bool TFIDF::Compatible(const ScorerOptions& persisted) const noexcept {
  return irs::BoundTypeOf(persisted) == BoundTypeOf(GetOptions());
}

std::string TFIDF::ToString() const {
  return absl::StrCat("tfidf(with_norms=", _normalize ? "true" : "false", ")");
}

bool TFIDF::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = irs::utils::downCast<TFIDF>(other);
  return p._normalize == _normalize;
}

}  // namespace irs
