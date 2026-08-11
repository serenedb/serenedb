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
#include <limits>
#include <memory>
#include <span>

#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ColumnReader;
class ReadContext;
class DataOutput;
class IndexOutput;

struct PayloadBlockSetting {
  uint32_t group_size = 1;
  uint32_t record_size = 0;

  size_t RecordCount(size_t docs) const noexcept {
    return (docs + group_size - 1) / group_size * group_size;
  }
};

class QuantizerWriter {
 public:
  static constexpr size_t kTrainStreaming = std::numeric_limits<size_t>::max();

  virtual ~QuantizerWriter() = default;

  virtual size_t TrainSamples(size_t /*rows*/) const noexcept { return 0; }

  virtual void Train(const float* vecs, size_t n) = 0;

  virtual void SetClusterCentroid(const float* /*centroid*/) {}

  virtual PayloadBlockSetting BlockSetting() const noexcept = 0;

  virtual void Encode(IndexOutput& out, const float* vecs, size_t n) = 0;

  virtual void Finish(IndexOutput& out) = 0;

  virtual uint32_t PendingLanes() const noexcept { return 0; }

  virtual void Serialize(DataOutput& out) const = 0;

  virtual VectorQuantization Kind() const noexcept = 0;

  virtual uint32_t ScanCostBytes() const noexcept = 0;
};

class QuantizerReader {
 public:
  virtual ~QuantizerReader() = default;
  virtual PayloadBlockSetting BlockSetting() const noexcept = 0;
  virtual void StartCluster(const float* centroid) = 0;
  virtual void ComputeBlock(std::span<const byte_type> block, score_t threshold,
                            score_t* out) = 0;
};

class QuantizerCodebook
  : public std::enable_shared_from_this<QuantizerCodebook> {
 public:
  virtual ~QuantizerCodebook() = default;
  virtual std::unique_ptr<QuantizerReader> MakeReader() const = 0;
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

constexpr bool QuantizerNeedsCentroid(VectorQuantization quant) noexcept {
  return quant == VectorQuantization::PQ || quant == VectorQuantization::RaBitQ;
}

bool PanoramaApplies(VectorMetric metric, uint32_t d) noexcept;

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(
  VectorQuantization quant, uint32_t d, VectorMetric metric, uint32_t pq_m,
  uint32_t pq_niter, uint32_t nb_bits);

std::shared_ptr<const QuantizerStats> MakeQuantizerStats(
  VectorQuantization quant, uint32_t d, std::span<const byte_type> stats,
  VectorMetric metric);

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook);

}  // namespace irs
