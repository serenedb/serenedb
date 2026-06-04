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

enum class HNSWMetric : uint8_t {
  L2Sqr = 0,
  NegativeIP,
  Cosine,
  L1,
};

struct HNSWInfo {
  doc_id_t max_doc = 0;

  // dimensionality of the data
  int d = 0;

  // HNSW M parameter
  int m = 32;

  // HNSW metric
  HNSWMetric metric = HNSWMetric::L2Sqr;

  // expansion factor at construction time
  int ef_construction = 40;
};

struct ColumnOptions {
  bool skip_validity = false;
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  std::optional<HNSWInfo> hnsw_info;
};

using ColumnOptionsProvider = std::function<ColumnOptions(field_id)>;

struct NormColumnOptions {
  field_id id = field_limits::invalid();
  uint32_t row_group_size = DEFAULT_ROW_GROUP_SIZE;
};

using NormColumnOptionsProvider =
  std::function<NormColumnOptions(std::string_view field_name)>;

}  // namespace irs
