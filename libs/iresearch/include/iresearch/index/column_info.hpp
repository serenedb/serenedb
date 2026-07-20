////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <functional>
#include <memory>
#include <optional>
#include <string_view>

#include "basics/system-compiler.h"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {

enum class VectorMetric : uint8_t {
  L2Sqr = 0,
  InnerProduct,
  Cosine,
  L1,
};

template<VectorMetric Metric>
auto ComputeDistance(const byte_type* l, const byte_type* r, uint16_t d) {
  if constexpr (Metric == VectorMetric::L2Sqr) {
    return -vector::L2Space<float, float, float>::Dist(l, r, d);
  } else if constexpr (Metric == VectorMetric::L1) {
    return -vector::L1Space<float, float, float>::Dist(l, r, d);
  } else if constexpr (Metric == VectorMetric::InnerProduct) {
    return vector::DotProductImpl<float, float>::Compute(l, r, d);
  } else if constexpr (Metric == VectorMetric::Cosine) {
    auto [ll, lr, rr] =
      vector::CosineDistanceImpl<float, float, float>::Compute(l, r, d);
    const float denom = std::sqrt(ll) * std::sqrt(rr);
    return denom == 0.f ? 0.f : lr / denom;
  }
  SDB_UNREACHABLE();
}

enum class VectorQuantization : uint8_t {
  None = 0,
  SQ8,
  SQ4,
  PQ,
  RaBitQ,
};

inline constexpr uint32_t kRaBitQMinBits = 1;
inline constexpr uint32_t kRaBitQMaxBits = 9;

struct IvfInfo {
  struct Quantizer {
    VectorQuantization kind = VectorQuantization::None;
    uint32_t pq_m = 0;
    uint32_t nb_bits = 0;

    friend bool operator==(const Quantizer&, const Quantizer&) = default;
  };

  field_id centroids_id = field_limits::invalid();
  field_id postings_id = field_limits::invalid();

  // dimensionality of the data
  int d = 0;

  VectorMetric metric = VectorMetric::L2Sqr;
  Quantizer quant;

  float sample_factor = 0;

  uint32_t posting_size = 0;

  friend bool operator==(const IvfInfo&, const IvfInfo&) = default;
};

struct ColumnOptions {
  bool skip_validity = false;
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  std::optional<IvfInfo> ivf_info;
  bool hyperloglog = false;
};

using ColumnOptionsProvider = std::function<ColumnOptions(field_id)>;

struct NormColumnOptions {
  field_id id = field_limits::invalid();
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
};

using NormColumnOptionsProvider = std::function<NormColumnOptions(field_id)>;

// Per-column encoding config the writer consults at flush + merge time. The
// host supplies one per operation, so a long-lived writer is never coupled to
// its mutable metadata.
class IndexFieldOptions {
 public:
  virtual ~IndexFieldOptions() = default;
  virtual ColumnOptions GetColumnOptions(field_id id) const = 0;
  virtual NormColumnOptions GetNormColumnOptions(field_id id) const = 0;

  // Segment reuse gate: two writes share a segment only if their options are
  // equal (a segment must not mix encodings). Default is pointer identity --
  // COW means an unchanged config is the same object.
  virtual bool EqualOptions(const IndexFieldOptions& other) const noexcept {
    return this == &other;
  }
};

// May `next` resume a segment opened under `prev`? nullptr `next` is the
// fallback path that never varies, so it always matches.
inline bool CompatibleFieldOptions(const IndexFieldOptions* prev,
                                   const IndexFieldOptions* next) noexcept {
  if (next == nullptr || prev == next) {
    return true;
  }
  return prev != nullptr && prev->EqualOptions(*next);
}

// Adapts the std::function providers to IndexFieldOptions; the writer's
// fallback.
class FunctionFieldOptions final : public IndexFieldOptions {
 public:
  FunctionFieldOptions(ColumnOptionsProvider column_options,
                       NormColumnOptionsProvider norm_column_options) noexcept
    : _column_options{std::move(column_options)},
      _norm_column_options{std::move(norm_column_options)} {}

  ColumnOptions GetColumnOptions(field_id id) const final {
    return _column_options ? _column_options(id) : ColumnOptions{};
  }
  NormColumnOptions GetNormColumnOptions(field_id id) const final {
    return _norm_column_options ? _norm_column_options(id)
                                : NormColumnOptions{};
  }

 private:
  ColumnOptionsProvider _column_options;
  NormColumnOptionsProvider _norm_column_options;
};

}  // namespace irs
