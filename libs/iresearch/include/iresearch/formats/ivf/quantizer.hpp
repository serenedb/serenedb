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

class QuantizerWriter {
 public:
  static constexpr size_t kTrainStreaming = std::numeric_limits<size_t>::max();

  virtual ~QuantizerWriter() = default;

  virtual size_t TrainSamples(size_t /*rows*/) const noexcept { return 0; }

  virtual void Train(const float* vecs, size_t n) = 0;

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

  // A second writer carrying the same trained state and its own encode
  // scratch, so several threads can EncodeInto disjoint row ranges at once.
  // Only meaningful once training and any centroid are settled; the clone is
  // for encoding only and must not be trained or serialized. Returns null when
  // the quantizer cannot be split, which leaves the caller encoding serially.
  virtual std::unique_ptr<QuantizerWriter> CloneForEncode() const {
    return nullptr;
  }

  virtual void Finish(IndexOutput& out) = 0;

  virtual uint32_t PendingLanes() const noexcept { return 0; }

  virtual void Serialize(DataOutput& out) const = 0;

  virtual VectorQuantization Kind() const noexcept = 0;
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

  virtual bool Decode(const byte_type* /*code*/, float* /*out*/) const {
    return false;
  }

  // Re-keys an existing reader in place. The scalar reader keeps the pointer
  // rather than copying, so `query` must stay alive and unmodified for as long
  // as this reader is scored -- two readers must not share one buffer.
  virtual bool SetQuery(std::span<const float> /*query*/) { return false; }

  // --- Symmetric code-to-code scoring -------------------------------------
  //
  // Scores two STORED rows against each other without a query. A graph build
  // needs this for the diversity heuristic and for back-link pruning, where
  // both sides are stored rows: routing those through SetQuery means decoding a
  // code back to floats and rebuilding the query LUT once per candidate, which
  // for TurboQuant is an FWHT plus a full LUT quantize/pack. These three
  // entry points let the build ask for the pair directly.
  //
  // Query-independent, so calling them never disturbs SetQuery state.

  virtual bool SupportsPairScores() const noexcept { return false; }

  // Fills `terms` with one float per row: the part of the pair estimate that
  // depends on a single row and can therefore be hoisted out of the O(rows *
  // degree) pair loop. Call once for the whole code array, then share `terms`
  // across every reader scoring pairs over it -- at 4 bytes/row it is far too
  // large to duplicate per build worker.
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
  return quant == VectorQuantization::PQ ||
         quant == VectorQuantization::RaBitQ ||
         quant == VectorQuantization::TQ || quant == VectorQuantization::TQMse;
}

bool PanoramaApplies(VectorMetric metric, uint32_t d) noexcept;

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(
  VectorQuantization quant, uint32_t d, VectorMetric metric, uint32_t pq_m,
  uint32_t pq_niter, uint32_t nb_bits, bool row_major = false);

std::shared_ptr<const QuantizerStats> MakeQuantizerStats(
  VectorQuantization quant, uint32_t d, std::span<const byte_type> stats,
  VectorMetric metric, bool row_major = false);

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook);

void GenerateSigns(uint32_t rotated_d, int64_t seed, std::vector<float>& signs);

}  // namespace irs
