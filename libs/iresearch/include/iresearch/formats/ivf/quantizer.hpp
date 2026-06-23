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

#include <cstdint>
#include <memory>
#include <span>

#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ColWriter;
class ColumnReader;
class ReadContext;

// Per-segment quantizer build/write path. A quantizer turns each doc's vector
// into a compact (optionally residual) record and persists the plan's
// `[header | quant-vectors | quantizer info]` layout next to the rerank
// vectors. Scalar quantization (SQ8) is the first implementation; product /
// additive / RaBitQ quantizers slot in as new VectorQuantization values + new
// QuantizerWriter/QuantizerReader subclasses with no IVF plumbing changes.
class QuantizerWriter {
 public:
  virtual ~QuantizerWriter() = default;

  // Train quantization parameters over the compacted valid training matrix
  // (`rows` x d, row-major). `centroids` (nlist x d) and per-row `assign`
  // enable residual quantizers (PQ/AQ); scalar quantization ignores them.
  virtual void Train(const float* matrix, uint64_t rows, uint32_t d,
                     std::span<const float> centroids,
                     std::span<const uint32_t> assign) = 0;

  // Encode one document's vector, keyed by its doc id (doc-aligned output).
  virtual void Encode(doc_id_t doc, const float* vec) = 0;

  // Serialize the accumulated records into the `sq_id` companion store.
  // `total_rows` is the segment row count (codes are doc-aligned; gaps stay
  // zero and are never reranked).
  virtual void Serialize(ColWriter& cw, field_id sq_id,
                         uint64_t total_rows) = 0;

  virtual VectorQuantization Kind() const noexcept = 0;
};

// Per-segment quantizer read path: approximate light distance from the stored
// quant records, used to pre-rank candidates before the exact fp32 rerank.
class QuantizerReader {
 public:
  virtual ~QuantizerReader() = default;

  // Prepare for a query (dequantize the query / build PQ LUTs).
  virtual void SetQuery(std::span<const float> query, VectorMetric metric) = 0;

  // Approximate light distance for `doc`, in the same convention as
  // ResolveVectorDistance.
  virtual float Distance(doc_id_t doc) = 0;

  virtual VectorQuantization Kind() const noexcept = 0;
};

// Returns the build-side quantizer for `quant`, or nullptr for None (exact
// fp32 rerank only).
std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(VectorQuantization quant,
                                                     uint32_t d,
                                                     VectorMetric metric);

// Returns the read-side quantizer reading the `sq_id` companion store, or
// nullptr when the segment has no usable quantizer (the caller then falls back
// to exact fp32 rerank). Currently returns nullptr for every kind: in rerank
// mode SQ stats are intentionally not persisted yet (see vector_plan.md), so
// approximate distances are unavailable. Wired here so SQ-only / PQ / AQ can
// enable it without touching the query path.
std::unique_ptr<QuantizerReader> MakeQuantizerReader(VectorQuantization quant,
                                                     const ColumnReader& store,
                                                     ReadContext& ctx);

}  // namespace irs
