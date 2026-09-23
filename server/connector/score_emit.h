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

#include <cmath>
#include <duckdb/common/enums/order_type.hpp>
#include <duckdb/planner/expression.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <limits>
#include <memory>
#include <vector>

namespace sdb::connector {

// Maps the per-doc score to the user-facing value. The score is already
// "larger = nearer" for every metric (ResolveScoringDistance negates the
// distance kernels), so the emit is applied directly -- no negation here.
enum class ScoreEmit : uint8_t {
  Identity,  // score        (cosine_similarity, inner_product)
  SqrtNeg,   // sqrt(-score) (l2_distance / `<->` / l2_norm)
  OneMinus,  // 1 - score    (cosine_distance)
  Negate,    // -score       (l1, l2_sqr, negative_ip, l1_norm)
};

// Maps one raw "larger = nearer" score to its user-facing value. Single source
// of truth: applied to the output scores at the emit boundary, and baked into
// the pushed score-column filter so the predicate is evaluated in the same
// (user-facing) space it was written in.
inline float ApplyScoreEmit(ScoreEmit emit, float score) {
  switch (emit) {
    case ScoreEmit::Identity:
      return score;
    case ScoreEmit::SqrtNeg:
      return std::sqrt(-score);
    case ScoreEmit::OneMinus:
      return 1.0F - score;
    case ScoreEmit::Negate:
      return -score;
  }
  SDB_UNREACHABLE();
}

struct VectorScorerOptions {
  irs::field_id field_id;
  std::vector<float> query_vector;
  // The query vector as an expression over prepared-statement parameters,
  // when it is not a constant at plan time; evaluated at execution into
  // `query_vector`. `dims` is the indexed dimension it must cast to.
  std::shared_ptr<const duckdb::Expression> query_expr;
  uint32_t dims = 0;
  irs::VectorMetric metric;
  ScoreEmit score_emit;
  duckdb::OrderType natural_order;
  irs::field_id centroids_id = irs::field_limits::invalid();
  irs::field_id postings_id = irs::field_limits::invalid();
  irs::VectorQuantization quant = irs::VectorQuantization::None;
  // Bits per component for the quantizers that take a width (tq, rabitq); 0
  // where the kind fixes it (sq8, sq4, pq). Read by the `auto` oversample,
  // which rescores coarse codes and leaves 4-bit-and-wider ones alone.
  uint32_t quant_bits = 0;
  irs::AnnKind kind = irs::AnnKind::Ivf;
  uint32_t nprobe = 1;
  uint32_t max_search_fanout = 16;
  uint32_t ef_search = 0;
  uint32_t ef_construction = 0;
  uint32_t posting_size = 0;
  uint32_t min_ef = 0;
  uint32_t top_k = 0;
  irs::HnswFilterMode hnsw_filter_mode = irs::HnswFilterMode::Auto;
  // Brute force over the stored vectors instead of the ANN index: the exact
  // answer, split across workers segment by segment.
  bool exact = false;
  float radius = std::numeric_limits<float>::max();
  bool radius_inclusive = false;

  float EffectiveRadius() const {
    if (radius == std::numeric_limits<float>::max()) {
      return radius;
    }
    // The radius filter runs on the natural (positive) distance kernel, so map
    // the user radius into that space -- the collector-side score negation does
    // not apply here.
    switch (score_emit) {
      case ScoreEmit::Identity:
        return radius;
      case ScoreEmit::SqrtNeg:
        return radius * radius;
      case ScoreEmit::OneMinus:
        return 1.0f - radius;
      case ScoreEmit::Negate:
        return (metric == irs::VectorMetric::L2Sqr ||
                metric == irs::VectorMetric::L1)
                 ? radius
                 : -radius;
    }
    SDB_UNREACHABLE();
  }
};

irs::Filter::ptr MakeVectorFilter(const VectorScorerOptions& vs,
                                  std::shared_ptr<const irs::Filter> inner,
                                  float radius);

}  // namespace sdb::connector
