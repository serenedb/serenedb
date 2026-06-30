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

  virtual void EncodeCluster(IndexOutput& out, const float* vecs,
                             size_t n) const = 0;

  virtual std::span<const byte_type> StatsBytes() const = 0;

  virtual VectorQuantization Kind() const noexcept = 0;

  virtual uint32_t CodeSize() const noexcept = 0;
};

class QuantizerReader {
 public:
  virtual ~QuantizerReader() = default;
  virtual void SetQuery(std::span<const float> query, VectorMetric metric) = 0;
  virtual void StartCluster(uint64_t pay_start, size_t num_docs,
                            const float* centroid) = 0;
  virtual void ComputeBlock(size_t offset, size_t length, score_t boost,
                            score_t* out) = 0;
};

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(VectorQuantization quant,
                                                     uint32_t d,
                                                     VectorMetric metric,
                                                     uint32_t pq_m);

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  VectorQuantization quant, std::unique_ptr<IndexInput> pay_in, uint32_t d,
  std::span<const byte_type> stats);

}  // namespace irs
