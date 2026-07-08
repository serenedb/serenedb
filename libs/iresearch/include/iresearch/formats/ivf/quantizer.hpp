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

#include "iresearch/formats/ivf/vector_block_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ColumnReader;
class ReadContext;
class IndexOutput;
class IndexInput;

class QuantizerWriter {
 public:
  virtual ~QuantizerWriter() = default;

  virtual void Train(const float* vecs, size_t n) = 0;

  virtual void SetClusterCentroid(const float* /*centroid*/) {}

  virtual void BeginCluster(size_t /*total_docs*/) {}

  virtual void EncodeCluster(IndexOutput& out, const float* vecs,
                             size_t n) const = 0;

  virtual void FinishCluster(IndexOutput& /*out*/) {}

  virtual std::span<const byte_type> StatsBytes() const = 0;

  virtual VectorQuantization Kind() const noexcept = 0;

  virtual uint32_t CodeSize() const noexcept = 0;
};

class QuantizerReader {
 public:
  virtual ~QuantizerReader() = default;
  virtual void StartCluster(uint64_t pay_start, size_t num_docs,
                            const float* centroid) = 0;
  virtual void ComputeBlock(size_t offset, size_t length, score_t* out) = 0;
};

class QuantizerCodebook
  : public std::enable_shared_from_this<QuantizerCodebook> {
 public:
  virtual ~QuantizerCodebook() = default;
  virtual std::unique_ptr<QuantizerReader> MakeReader(
    std::unique_ptr<IndexInput> pay_in) const = 0;
};

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(
  VectorQuantization quant, uint32_t d, VectorMetric metric, uint32_t pq_m,
  uint32_t pq_niter, uint32_t nb_bits);

std::shared_ptr<const QuantizerCodebook> MakeQuantizerCodebook(
  VectorQuantization quant, uint32_t d, std::span<const byte_type> stats,
  std::span<const float> query, VectorMetric metric);

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook,
  std::unique_ptr<IndexInput> pay_in);

}  // namespace irs
