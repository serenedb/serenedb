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
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  search::Features features;
  std::optional<HNSWColumnConfig> hnsw_config;
  std::optional<irs::field_id> synthetic_column;
  uint32_t row_group_size = 0;
  uint32_t norm_row_group_size = 0;
};

struct ExpressionSerialized {
  std::string serialized_expr;
  std::string pretty_printed;
  std::vector<Column::Id> dependent_columns;
  duckdb::LogicalType return_type;
  std::optional<irs::field_id> synthetic_column;
  ObjectId text_dictionary = ObjectId::none();
  irs::field_id field_id = 0;
  uint32_t norm_row_group_size = 0;
  search::Features features;

  struct HasherBySerialized {
    using is_transparent = void;

    size_t operator()(std::string_view sv) const { return absl::HashOf(sv); }
    size_t operator()(const ExpressionSerialized& info) const {
      return (*this)(info.serialized_expr);
    }

    bool operator()(const ExpressionSerialized& a, std::string_view b) const {
      return a.serialized_expr == b;
    }
    bool operator()(const ExpressionSerialized& a,
                    const ExpressionSerialized& b) const {
      return a.serialized_expr == b.serialized_expr;
    }
  };
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
