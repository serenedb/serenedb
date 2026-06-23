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
#include <optional>
#include <string_view>

#include "iresearch/utils/string.hpp"
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
};

struct IvfInfo {
  field_id centroids_id = field_limits::invalid();
  field_id postings_id = field_limits::invalid();
  field_id sq_id = field_limits::invalid();

  // dimensionality of the data
  int d = 0;

  VectorMetric metric = VectorMetric::L2Sqr;
  VectorQuantization quant = VectorQuantization::None;

  uint32_t nlist = 0;

  uint32_t train_sample = 0;
};

struct ColumnOptions {
  bool skip_validity = false;
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  std::optional<IvfInfo> ivf_info;
  bool hyperloglog = false;
};

using ColumnOptionsProvider = std::function<ColumnOptions(field_id)>;

struct NormColumnOptions {
  field_id id = field_limits::invalid();
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
};

using NormColumnOptionsProvider = std::function<NormColumnOptions(field_id)>;

}  // namespace irs
