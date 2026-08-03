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
#include <cmath>
#include <cstdint>

#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs::panorama {

inline constexpr uint32_t kLevelWidth = 32;

// Below this the level count is too small for the bound to pay for itself.
inline constexpr uint32_t kMinDim = 64;

// Guards the tail bound against float error in the stored suffix norms: an
// underestimated tail inflates the bound's negative term and could over-prune,
// so the tail is scaled up before the comparison. Squared, since the tests
// compare squares.
inline constexpr float kTailSlack = 1.0002f;

constexpr uint32_t Levels(uint32_t d) noexcept {
  return (d + kLevelWidth - 1) / kLevelWidth;
}

// A record is the `levels` suffix norms followed by the `d` rotated floats.
constexpr uint32_t RecordFloats(uint32_t d, uint32_t levels) noexcept {
  return d + levels;
}

constexpr uint32_t RecordSize(uint32_t d, uint32_t levels) noexcept {
  return RecordFloats(d, levels) * static_cast<uint32_t>(sizeof(float));
}

// `out[l] = sum of v[j]^2 over j >= l * kLevelWidth`, for l in [0, levels).
inline void ComputeTails(const float* v, uint32_t d, uint32_t levels,
                         float* out) {
  float acc = 0.f;
  for (uint32_t l = levels; l-- > 0;) {
    const uint32_t from = l * kLevelWidth;
    const uint32_t to = std::min(from + kLevelWidth, d);
    for (uint32_t j = from; j < to; ++j) {
      acc += v[j] * v[j];
    }
    out[l] = acc;
  }
}

// `out = R * q` for a row-major `d x d` R held as raw bytes. Reading the matrix
// straight out of the stats blob avoids copying `4 * d * d` bytes per query, at
// the cost of one pass per row instead of a BLAS gemv -- the product is memory
// bound at these sizes, so the copy was the expensive half.
inline void RotateQuery(const byte_type* rotation, const float* q, float* out,
                        uint32_t d) {
  const auto* qb = reinterpret_cast<const byte_type*>(q);
  const auto width = static_cast<uint16_t>(d);
  const size_t stride = size_t{d} * sizeof(float);
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = vector::DotProductImpl<float, float>::Compute(
      rotation + i * stride, qb, width);
  }
}

struct Query {
  const float* data = nullptr;
  const float* tails = nullptr;
  float norm = 0.f;
};

// Level-by-level distance with Cauchy-Schwarz pruning. Returns the exact score
// for a candidate that survives every level, and otherwise an upper bound on
// its score that is strictly below `threshold` -- so a consumer comparing
// `score > threshold` rejects it without needing to know it was pruned.
template<VectorMetric M, bool Count = false>
score_t ProgressiveScore(const Query& q, const float* record, uint32_t d,
                         uint32_t levels, score_t threshold,
                         uint64_t* scanned = nullptr) {
  const float* x = record + levels;

  float scale = 1.f;
  if constexpr (M == VectorMetric::Cosine) {
    const float xn = std::sqrt(record[0]);
    const float denom = q.norm * xn;
    if (denom == 0.f) {
      return 0.f;
    }
    scale = denom;
  }

  // The threshold in the space the loop accumulates in: negated squared
  // distance for L2, raw inner product for IP, and cosine's denominator folded
  // in so the IP-space comparison is the cosine one.
  float limit = threshold;
  if constexpr (M == VectorMetric::L2Sqr) {
    limit = -threshold;
  } else if constexpr (M == VectorMetric::Cosine) {
    limit = threshold * scale;
  }

  float acc = 0.f;
  for (uint32_t l = 0; l < levels; ++l) {
    const uint32_t from = l * kLevelWidth;
    const uint32_t to = std::min(from + kLevelWidth, d);
    if constexpr (Count) {
      *scanned += to - from;
    }
    {
#pragma clang fp reassociate(on) contract(fast)
      float sum = 0.f;
      if constexpr (M == VectorMetric::L2Sqr) {
        for (uint32_t j = from; j < to; ++j) {
          const float diff = q.data[j] - x[j];
          sum += diff * diff;
        }
      } else if constexpr (M == VectorMetric::L1) {
        for (uint32_t j = from; j < to; ++j) {
          sum += std::fabs(q.data[j] - x[j]);
        }
      } else {
        for (uint32_t j = from; j < to; ++j) {
          sum += q.data[j] * x[j];
        }
      }
      acc += sum;
    }

    // A rotation does not preserve L1, so that metric never gets a payload
    // with levels; the branch only keeps the reader total.
    if constexpr (M == VectorMetric::L1) {
      continue;
    } else {
      if (l + 1 == levels) {
        break;
      }
      const float qt = q.tails[l + 1];
      const float xt = record[l + 1];
      if constexpr (M == VectorMetric::L2Sqr) {
        const float b = acc + qt + xt - limit;
        if (b > 0.f && 4.f * qt * xt * kTailSlack < b * b) {
          return -(acc + qt + xt - 2.f * std::sqrt(qt * xt));
        }
      } else {
        const float c = limit - acc;
        if (c > 0.f && qt * xt * kTailSlack < c * c) {
          return (acc + std::sqrt(qt * xt)) / scale;
        }
      }
    }
  }

  if constexpr (M == VectorMetric::L2Sqr || M == VectorMetric::L1) {
    return -acc;
  } else {
    return acc / scale;
  }
}

}  // namespace irs::panorama
