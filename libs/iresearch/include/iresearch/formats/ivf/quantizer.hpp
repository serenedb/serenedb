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

class ColumnReader;
class ReadContext;
class IndexOutput;
class IndexInput;
class ScoreCollector;

class QuantizerWriter {
 public:
  virtual ~QuantizerWriter() = default;

  virtual void UpdateStats(const float* vecs, size_t n) = 0;

  virtual void EncodeCluster(IndexOutput& out, const float* vecs,
                             size_t n) const = 0;

  virtual void Finalize(IndexOutput& out) = 0;

  virtual VectorQuantization Kind() const noexcept = 0;

  virtual uint32_t CodeSize() const noexcept = 0;
};

class QuantizerReader {
 public:
  virtual ~QuantizerReader() = default;

  virtual void SetQuery(std::span<const float> query, VectorMetric metric) = 0;

  virtual void Search(uint64_t pay_start, std::span<const doc_id_t> docs,
                      score_t boost, ScoreCollector& collector) = 0;

  virtual VectorQuantization Kind() const noexcept = 0;
};

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(VectorQuantization quant,
                                                     uint32_t d,
                                                     VectorMetric metric);

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  VectorQuantization quant, std::unique_ptr<IndexInput> pay_in, uint32_t d);

}  // namespace irs
