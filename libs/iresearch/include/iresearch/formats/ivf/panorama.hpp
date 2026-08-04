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

inline constexpr uint32_t kMinDim = 64;

inline constexpr float kTailSlack = 1.0002f;

constexpr uint32_t Levels(uint32_t d) noexcept {
  return (d + kLevelWidth - 1) / kLevelWidth;
}
constexpr uint32_t RecordSize(uint32_t d, uint32_t levels) noexcept {
  return (d + levels) * static_cast<uint32_t>(sizeof(float));
}

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
