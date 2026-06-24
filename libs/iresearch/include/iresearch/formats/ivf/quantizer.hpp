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

class QuantizerWriter {
 public:
  virtual ~QuantizerWriter() = default;

  virtual void Train(const float* matrix, uint64_t rows, uint32_t d,
                     std::span<const float> centroids,
                     std::span<const uint32_t> assign) = 0;

  virtual void Encode(doc_id_t doc, const float* vec) = 0;

  virtual void Serialize(ColWriter& cw, field_id sq_id,
                         uint64_t total_rows) = 0;

  virtual VectorQuantization Kind() const noexcept = 0;
};

class QuantizerReader {
 public:
  virtual ~QuantizerReader() = default;

  virtual void SetQuery(std::span<const float> query, VectorMetric metric) = 0;

  virtual float Distance(doc_id_t doc) = 0;

  virtual VectorQuantization Kind() const noexcept = 0;
};

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(VectorQuantization quant,
                                                     uint32_t d,
                                                     VectorMetric metric);

std::unique_ptr<QuantizerReader> MakeQuantizerReader(VectorQuantization quant,
                                                     const ColumnReader& store,
                                                     ReadContext& ctx);

}  // namespace irs
