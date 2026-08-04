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

#include "iresearch/formats/ivf/quantizer_reader.hpp"
#include "iresearch/formats/ivf/vector_block_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ColumnReader;
class ReadContext;
class IndexOutput;
class IndexInput;

// The field's payload is ONE stream in `.pay`, continuous across run and
// cluster boundaries: a document's codes live at its stream ordinal (its
// "lane"), fast-scan packs never align to run or cluster edges, and the only
// padding is one partial pack at the stream's end. A cluster is the lanes
// `[first_lane, first_lane + num_docs)` -- its runs are adjacent by
// construction -- so everything is addressed by lane ordinal plus the field's
// stream base byte.
class QuantizerWriter {
 public:
  virtual ~QuantizerWriter() = default;

  virtual void Train(const float* vecs, size_t n) = 0;

  virtual void SetClusterCentroid(const float* /*centroid*/) {}

  // Appends `n` documents' codes to the stream, flushing packs as they fill.
  virtual void Encode(IndexOutput& out, const float* vecs, size_t n) = 0;

  // Flushes the stream's one partial pack. Called once per field.
  virtual void Finish(IndexOutput& /*out*/) {}

  virtual std::span<const byte_type> StatsBytes() const = 0;

  virtual VectorQuantization Kind() const noexcept = 0;

  virtual uint32_t CodeSize() const noexcept = 0;
};

class QuantizerCodebook
  : public std::enable_shared_from_this<QuantizerCodebook> {
 public:
  virtual ~QuantizerCodebook() = default;
  virtual std::unique_ptr<QuantizerReader> MakeReader(
    std::unique_ptr<IndexInput> pay_in, uint64_t pay_base) const = 0;
};

// Query-independent, deserialized quantizer statistics. Parsed once from the
// on-disk stats blob and shared across queries; binds a query into a
// QuantizerCodebook via MakeCodebook.
class QuantizerStats : public std::enable_shared_from_this<QuantizerStats> {
 public:
  virtual ~QuantizerStats() = default;
  virtual VectorQuantization Kind() const noexcept = 0;
  virtual std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const = 0;
};

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(
  VectorQuantization quant, uint32_t d, VectorMetric metric, uint32_t pq_m,
  uint32_t pq_niter, uint32_t nb_bits);

std::shared_ptr<const QuantizerStats> MakeQuantizerStats(
  VectorQuantization quant, uint32_t d, std::span<const byte_type> stats,
  VectorMetric metric);

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook,
  std::unique_ptr<IndexInput> pay_in, uint64_t pay_base);

}  // namespace irs
