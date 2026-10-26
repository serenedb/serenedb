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

#include "iresearch/utils/string.hpp"
#include "iresearch/utils/system_compiler.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

enum class VectorMetric : uint8_t {
  L2Sqr = 0,
  InnerProduct,
  Cosine,
  L1,
};

enum class VectorQuantization : uint8_t {
  None = 0,
  SQ8,
  SQ4,
  PQ,
  RaBitQ,
  TQ,
  USQ8,
  USQ4,
  BQ,
  OSQ,
  FP16,
  BF16,
};

inline constexpr bool IsHalfFloat(VectorQuantization q) noexcept {
  return q == VectorQuantization::FP16 || q == VectorQuantization::BF16;
}

inline constexpr bool IsUniformScalar(VectorQuantization q) noexcept {
  return q == VectorQuantization::USQ8 || q == VectorQuantization::USQ4;
}

inline constexpr bool IsScalar(VectorQuantization q) noexcept {
  return q == VectorQuantization::SQ8 || q == VectorQuantization::SQ4 ||
         IsUniformScalar(q);
}

inline constexpr uint32_t kRaBitQMinBits = 1;
inline constexpr uint32_t kRaBitQMaxBits = 9;

inline constexpr uint32_t kTQMinBits = 1;
inline constexpr uint32_t kTQMaxBits = 5;
inline constexpr uint32_t kTQDefaultBits = 3;

inline constexpr bool TQBitsValid(uint32_t bits) noexcept {
  return bits >= kTQMinBits && bits <= kTQMaxBits;
}

inline constexpr bool TQHalfBitValid(uint32_t bits) noexcept {
  return bits == 1;
}

inline constexpr uint32_t kBQDefaultBits = 1;
inline constexpr uint32_t kBQDefaultQueryBits = 1;

inline constexpr bool BQBitsValid(uint32_t bits, bool half_bit) noexcept {
  return half_bit ? bits == 1 : bits == 1 || bits == 2;
}

inline constexpr bool BQQueryBitsValid(uint32_t bits) noexcept {
  return bits == 1 || bits == 4 || bits == 8;
}

inline constexpr uint32_t kOSQDefaultBits = 1;

inline constexpr bool OSQBitsValid(uint32_t bits) noexcept {
  return bits == 1 || bits == 2 || bits == 4 || bits == 7 || bits == 8;
}

inline constexpr uint32_t OSQDefaultQueryBits(uint32_t bits) noexcept {
  return bits <= 4 ? 4 : bits;
}

inline constexpr bool OSQQueryBitsValid(uint32_t bits,
                                        uint32_t query_bits) noexcept {
  return bits <= 4 ? query_bits == 4 || query_bits == 8 : query_bits == bits;
}

inline constexpr VectorMetric EffectiveQuantMetric(
  VectorMetric metric) noexcept {
  return metric == VectorMetric::Cosine ? VectorMetric::InnerProduct : metric;
}

enum class AnnKind : uint8_t {
  Ivf = 0,
  Hnsw,
};

enum class HnswFilterMode : uint8_t {
  Auto = 0,
  Walk,
  Scan,
  Prune,
  TwoHop,
  Bridge,
};

enum class HnswColumnFilter : uint8_t {
  Auto = 0,
  Fold,
  Read,
};

struct AnnInfo {
  struct Quantizer {
    VectorQuantization kind = VectorQuantization::None;
    uint32_t pq_m = 0;
    uint32_t nb_bits = 0;
    bool half_bit = false;
    uint32_t query_bits = 0;

    friend bool operator==(const Quantizer&, const Quantizer&) = default;
  };

  AnnKind kind = AnnKind::Ivf;

  field_id centroids_id = field_limits::invalid();
  field_id postings_id = field_limits::invalid();

  // dimensionality of the data
  int d = 0;

  VectorMetric metric = VectorMetric::L2Sqr;
  Quantizer quant;

  float sample_factor = 0;

  uint32_t posting_size = 0;

  uint32_t m = 0;
  uint32_t ef_construction = 0;

  friend bool operator==(const AnnInfo&, const AnnInfo&) = default;
};

struct ColumnOptions {
  bool skip_validity = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  std::optional<AnnInfo> ann_info;
  bool hyperloglog = false;
};

using ColumnOptionsProvider = std::function<ColumnOptions(field_id)>;

using NormColumnIdProvider = std::function<field_id(field_id)>;

// Per-column encoding config the writer consults at flush + merge time. The
// host supplies one per operation, so a long-lived writer is never coupled to
// its mutable metadata.
class IndexFieldOptions {
 public:
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;

  virtual ~IndexFieldOptions() = default;
  virtual ColumnOptions GetColumnOptions(field_id id) const = 0;
  virtual field_id GetNormColumnId(field_id id) const = 0;
  virtual bool HasField(field_id) const { return true; }

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
                       NormColumnIdProvider norm_column_id,
                       uint32_t row_group_size) noexcept
    : _column_options{std::move(column_options)},
      _norm_column_id{std::move(norm_column_id)} {
    this->row_group_size = row_group_size;
  }

  ColumnOptions GetColumnOptions(field_id id) const final {
    return _column_options ? _column_options(id) : ColumnOptions{};
  }
  field_id GetNormColumnId(field_id id) const final {
    return _norm_column_id ? _norm_column_id(id) : field_limits::invalid();
  }

 private:
  ColumnOptionsProvider _column_options;
  NormColumnIdProvider _norm_column_id;
};

}  // namespace irs
