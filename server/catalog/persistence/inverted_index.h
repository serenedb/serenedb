////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <absl/hash/hash.h>

#include <cstdint>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types.hpp>
#include <iresearch/index/column_info.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "catalog/persistence/index.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/table_options.h"

namespace sdb::catalog::persistence {

struct HNSWColumnConfig {
  int d = 0;
  int m = 32;
  int ef_construction = 40;
  irs::HNSWMetric metric = irs::HNSWMetric::L2Sqr;
};

struct ColumnSerialized {
  ObjectId text_dictionary = ObjectId::none();
  bool store_values = false;
  bool indexed_term_dict = false;
  bool distinct_count = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  search::Features features;
  std::optional<HNSWColumnConfig> hnsw_config;
  irs::field_id synthetic_column = irs::field_limits::invalid();
  uint32_t row_group_size = 0;
  uint32_t norm_row_group_size = 0;
  irs::field_id null_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id numeric_field_id = irs::field_limits::invalid();
};

struct ExpressionSerialized {
  std::string serialized_expr;
  std::string pretty_printed;
  std::vector<Column::Id> dependent_columns;
  duckdb::LogicalType return_type;
  irs::field_id synthetic_column = irs::field_limits::invalid();
  ObjectId text_dictionary = ObjectId::none();
  irs::field_id field_id = irs::field_limits::invalid();
  uint32_t norm_row_group_size = 0;
  search::Features features;
  irs::field_id null_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id numeric_field_id = irs::field_limits::invalid();
};

using ColumnSerializedMap =
  containers::NodeHashMap<Column::Id, ColumnSerialized>;

struct InvertedIndexData {
  std::string name;
  std::vector<Column::Id> column_ids;
  ColumnSerializedMap columns;
  std::vector<ExpressionSerialized> expressions;
  InvertedIndexOptions options;
};

}  // namespace sdb::catalog::persistence
