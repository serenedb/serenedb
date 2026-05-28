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

#include "lm_jelinek_mercer.hpp"

#include <vpack/common.h>
#include <vpack/parser.h>
#include <vpack/serializer.h>
#include <vpack/slice.h>
#include <vpack/vpack.h>

#include <cmath>

#include "basics/down_cast.h"
#include "basics/empty.hpp"
#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/scorer_impl.hpp"
#include "iresearch/search/scorers.hpp"

namespace irs {
namespace {

template<typename T>
constexpr const T* TryGetValue(const T* value) noexcept {
  return value;
}

constexpr std::nullptr_t TryGetValue(utils::Empty /*value*/) noexcept {
  return nullptr;
}

struct ObjectParams {
  score_t lambda = LMJelinekMercer::LAMBDA();
};

Scorer::ptr MakeFromObject(const vpack::Slice slice) {
  ObjectParams params;
  auto r = vpack::ReadObjectNothrow(slice, params,
                                    {
                                      .skip_unknown = true,
                                      .strict = false,
                                    });
  if (!r.ok()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Error '", r.errorMessage(),
              "' while constructing lm_jm scorer from VPack");
    return {};
  }
  if (params.lambda <= 0.f || params.lambda > 1.f ||
      !std::isfinite(params.lambda)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "lm_jm lambda must be in (0, 1]");
    return {};
  }
  return std::make_unique<LMJelinekMercer>(params.lambda);
}

Scorer::ptr MakeFromArray(const vpack::Slice slice) {
  ObjectParams params;
  auto r = vpack::ReadTupleNothrow(slice, params);
  if (!r.ok()) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Error '", r.errorMessage(),
              "' while constructing lm_jm scorer from VPack array");
    return {};
  }
  if (params.lambda <= 0.f || params.lambda > 1.f ||
      !std::isfinite(params.lambda)) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "lm_jm lambda must be in (0, 1]");
    return {};
  }
  return std::make_unique<LMJelinekMercer>(params.lambda);
}

Scorer::ptr MakeVPack(const vpack::Slice slice) {
  switch (slice.type()) {
    case vpack::ValueType::Object:
      return MakeFromObject(slice);
    case vpack::ValueType::Array:
      return MakeFromArray(slice);
    default:
      SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
                "Invalid VPack arguments for lm_jm scorer");
      return nullptr;
  }
}

Scorer::ptr MakeVPack(std::string_view args) {
  if (IsNull(args)) {
    return std::make_unique<LMJelinekMercer>();
  }
  vpack::Slice slice(reinterpret_cast<const uint8_t*>(args.data()));
  return MakeVPack(slice);
}

Scorer::ptr MakeJson(std::string_view args) {
  if (IsNull(args)) {
    return std::make_unique<LMJelinekMercer>();
  }
  try {
    auto vpack = vpack::Parser::fromJson(args.data(), args.size());
    return MakeVPack(vpack->slice());
  } catch (const vpack::Exception& ex) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH, "Caught error '", ex.what(),
              "' while constructing VPack from JSON for lm_jm");
  } catch (...) {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Caught error while constructing VPack from JSON for lm_jm");
  }
  return nullptr;
}

// score(doc, term) = boost * log(1 + ((1 - lambda) * tf / dl) /
//                                    (lambda * collection_prob))
// With zero-norm guard: if dl == 0 we treat the doc as unmatched (score 0)
template<ScoreMergeType MergeType, bool HasBoost>
IRS_FORCE_INLINE void LmJmImpl(
  score_t* IRS_RESTRICT res, scores_size_t n, const uint32_t* IRS_RESTRICT freq,
  const uint32_t* IRS_RESTRICT norm,
  [[maybe_unused]] const score_t* IRS_RESTRICT boost, score_t num,
  score_t denom_inv, score_t const_boost) noexcept {
  // num = 1 - lambda, denom_inv = 1 / (lambda * collection_prob)
  for (scores_size_t i = 0; i != n; ++i) {
    const score_t tf = static_cast<score_t>(freq[i]);
    const score_t dl = static_cast<score_t>(norm[i]);
    score_t r = 0.f;
    if (dl > 0.f) {
      const score_t ratio = (num * tf / dl) * denom_inv;
      r = std::log1p(ratio);
    }
    if constexpr (HasBoost) {
      r *= const_boost * boost[i];
    } else {
      r *= const_boost;
    }
    Merge<MergeType>(res[i], r);
  }
}

template<bool HasFilterBoost>
struct LmJmScore : public ScoreOperator {
  LmJmScore(score_t boost, score_t lambda, const LMStats& stats,
            const FreqBlockAttr* freq, const uint32_t* norm,
            const score_t* fb) noexcept
    : freq{freq},
      norm{norm},
      filter_boost{fb},
      boost{boost},
      num{1.f - lambda},
      denom_inv{1.f / (lambda * stats.collection_prob)} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* res,
                                  scores_size_t n) const noexcept {
    LmJmImpl<MergeType, HasFilterBoost>(res, n, freq->value, norm,
                                        TryGetValue(filter_boost), num,
                                        denom_inv, boost);
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

  const FreqBlockAttr* freq;
  const uint32_t* norm;
  [[no_unique_address]] utils::Need<HasFilterBoost, const score_t*>
    filter_boost;
  score_t boost;
  score_t num;        // 1 - lambda
  score_t denom_inv;  // 1 / (lambda * collection_prob)
};

}  // namespace

void LMJelinekMercer::collect(byte_type* stats_buf,
                              const FieldCollector::Data* field,
                              const TermCollector* term) const {
  auto* stats = stats_cast(stats_buf);

  const auto* term_ptr = sdb::basics::downCast<LMTermCollector>(term);

  const auto ttf_field = field ? field->total_term_freq : 0;
  const auto ttf_term = term_ptr ? term_ptr->total_term_freq : 0;

  // DefaultCollectionModel: P(t|C) = (ttf_t + 1) / (ttf_field + 1).
  // Stats are allocated per-term (see TermCollectors::finish), so assignment
  // is safe; multi-term filters produce one prepared scorer per term with
  // its own stats buffer.
  const double num = static_cast<double>(ttf_term) + 1.0;
  const double den = static_cast<double>(ttf_field) + 1.0;
  stats->collection_prob = static_cast<score_t>(num / den);
}

TermCollector::ptr LMJelinekMercer::PrepareTermCollector() const {
  return std::make_unique<LMTermCollector>();
}

ScoreFunction LMJelinekMercer::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);
  if (!freq) {
    // No frequency (e.g. irs::all filter) -- zero score.
    return ScoreFunction::Default();
  }

  // Defensive: guard against a zero/unset collection probability (no stats
  // collected, e.g. empty index).
  auto* stats = stats_cast(ctx.stats);
  if (stats->collection_prob <= 0.f) {
    return ScoreFunction::Default();
  }

  const uint32_t* norm = [&] {
    auto* attr = irs::get<Norm>(ctx.doc_attrs);
    return attr ? &attr->value : nullptr;
  }();

  if (!norm && ctx.fetcher) {
    auto norm_reader = ctx.segment.norms(ctx.field.norm);
    norm = ctx.fetcher->AddNorms(ctx.field.norm, std::move(norm_reader));
  }

  if (!norm) {
    // Without norms we can't compute tf/dl. Fall back to zero scores so the
    // behaviour degrades gracefully instead of producing NaN.
    return ScoreFunction::Default();
  }

  auto* filter_boost = [&] {
    auto* attr = irs::get<BoostBlockAttr>(ctx.doc_attrs);
    return attr ? attr->value : nullptr;
  }();

  return ResolveBool(filter_boost != nullptr, [&]<bool HasBoost>() {
    return ScoreFunction::Make<LmJmScore<HasBoost>>(ctx.boost, _lambda, *stats,
                                                    freq, norm, filter_boost);
  });
}

bool LMJelinekMercer::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = sdb::basics::downCast<LMJelinekMercer>(other);
  return p._lambda == _lambda;
}

void LMJelinekMercer::init() {
  REGISTER_SCORER_JSON(LMJelinekMercer, MakeJson);
  REGISTER_SCORER_VPACK(LMJelinekMercer, MakeVPack);
}

}  // namespace irs
