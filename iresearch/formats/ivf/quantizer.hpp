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

#include <absl/functional/function_ref.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <vector>

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

using TrainFanout =
  absl::FunctionRef<void(size_t, absl::FunctionRef<void(size_t)>)>;

class QuantizerWriter {
 public:
  static constexpr size_t kTrainStreaming = std::numeric_limits<size_t>::max();

  virtual ~QuantizerWriter() = default;

  virtual size_t TrainSamples(size_t /*rows*/) const noexcept { return 0; }

  virtual void Train(const float* vecs, size_t n) = 0;

  virtual void TrainParallel(const float* vecs, size_t n,
                             TrainFanout /*fanout*/) {
    Train(vecs, n);
  }

  virtual void SetClusterCentroid(const float* /*centroid*/) {}

  virtual size_t RefineSamples(size_t /*rows*/) const noexcept { return 0; }
  virtual void Refine(const float* /*vecs*/, size_t /*n*/) {}
  virtual void RefineDone() {}

  virtual PayloadBlockSetting BlockSetting() const noexcept = 0;

  virtual void Encode(IndexOutput& out, const float* vecs, size_t n) = 0;

  virtual bool EncodeInto(byte_type* /*dst*/, const float* /*vecs*/,
                          size_t /*n*/) {
    return false;
  }

  virtual std::unique_ptr<QuantizerWriter> CloneForEncode() const {
    return nullptr;
  }

  virtual uint32_t EncodeWidth() const noexcept { return 1; }

  virtual void Finish(IndexOutput& out) = 0;

  virtual uint32_t PendingLanes() const noexcept { return 0; }

  virtual void Serialize(DataOutput& out) const = 0;

  virtual VectorQuantization Kind() const noexcept = 0;
};

struct QuantizerLevelBound {
  uint32_t width = 0;
  uint32_t query_step = 0;
  std::span<const float> query;
  const float* rows = nullptr;
  uint32_t row_stride = 0;
};

class QuantizerReader {
 public:
  virtual ~QuantizerReader() = default;
  virtual PayloadBlockSetting BlockSetting() const noexcept = 0;
  virtual void StartCluster(const float* centroid) = 0;
  virtual void ComputeBlock(std::span<const byte_type> block, score_t threshold,
                            score_t* out) = 0;

  virtual void ComputeGathered(const byte_type* base, uint32_t record_size,
                               std::span<const uint32_t> ids, score_t threshold,
                               score_t* out);

  virtual bool ComputeGatheredBounded(const byte_type* /*base*/,
                                      uint32_t /*record_size*/,
                                      std::span<const uint32_t> /*ids*/,
                                      const QuantizerLevelBound& /*bound*/,
                                      score_t /*threshold*/, score_t* /*out*/) {
    return false;
  }

  virtual bool Decode(const byte_type* /*code*/, float* /*out*/) const {
    return false;
  }

  virtual bool SetQuery(std::span<const float> /*query*/) { return false; }

  virtual bool SupportsPairScores() const noexcept { return false; }

  virtual bool PreparePairTerms(const byte_type* /*base*/,
                                uint32_t /*record_size*/, uint64_t /*rows*/,
                                std::vector<float>& /*terms*/) {
    return false;
  }

  virtual void ScorePairBatch(const byte_type* /*base*/,
                              uint32_t /*record_size*/,
                              std::span<const float> /*terms*/,
                              uint32_t /*from*/,
                              std::span<const uint32_t> /*ids*/,
                              score_t* /*out*/) {}

 private:
  std::vector<byte_type> _gather;
};

class QuantizerCodebook
  : public std::enable_shared_from_this<QuantizerCodebook> {
 public:
  virtual ~QuantizerCodebook() = default;
  virtual std::unique_ptr<QuantizerReader> MakeReader() const = 0;
};

class QuantizerCodeBound {
 public:
  virtual ~QuantizerCodeBound() = default;

  virtual void Score(const byte_type* base, uint32_t record_size,
                     std::span<const uint32_t> ids, score_t* out) const = 0;

  virtual void Bounds(std::span<const score_t> scores, score_t* lower,
                      score_t* upper) const noexcept = 0;
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

  virtual std::shared_ptr<const QuantizerCodebook> MakeBuildCodebook(
    std::span<const float> query) const {
    return MakeCodebook(query);
  }

  virtual std::shared_ptr<const QuantizerCodebook> MakeRangeCodebook(
    std::span<const float> query) const {
    return MakeCodebook(query);
  }

  virtual std::shared_ptr<const QuantizerCodeBound> MakeCodeBound(
    std::span<const float> /*query*/, VectorMetric /*metric*/) const {
    return nullptr;
  }

  virtual std::span<const byte_type> CodebookKey() const noexcept { return {}; }
};

constexpr bool QuantizerNeedsCentroid(VectorQuantization quant) noexcept {
  return quant == VectorQuantization::PQ ||
         quant == VectorQuantization::RaBitQ ||
         quant == VectorQuantization::TQ || quant == VectorQuantization::OSQ;
}

bool PanoramaApplies(VectorMetric metric, uint32_t d) noexcept;

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(
  const AnnInfo::Quantizer& quant, uint32_t d, VectorMetric metric,
  uint32_t pq_niter = 0, bool row_major = false);

std::shared_ptr<const QuantizerStats> MakeQuantizerStats(
  VectorQuantization quant, uint32_t d, std::span<const byte_type> stats,
  VectorMetric metric, bool row_major = false);

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook);

void GenerateSigns(uint32_t rotated_d, int64_t seed, std::vector<float>& signs);

}  // namespace irs
