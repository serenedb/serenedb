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

struct IVFColumnConfig {
  int d = 0;
  irs::VectorMetric metric = irs::VectorMetric::L2Sqr;
  irs::VectorQuantization quant = irs::VectorQuantization::None;
  uint32_t nlist = 0;
  uint32_t train_sample = 0;
  uint32_t cluster_iters = 0;
  irs::field_id centroids_id = irs::field_limits::invalid();
  irs::field_id postings_id = irs::field_limits::invalid();
  irs::field_id sq_id = irs::field_limits::invalid();
  uint32_t pq_m = 0;
};

// Persisted per-field iresearch config, keyed by field_id in InvertedIndexData.
// Carries no key identity (column/expression) -- that lives in the columns +
// expressions key arrays.
struct EntryConfigSerialized {
  ObjectId text_dictionary = ObjectId::none();
  bool store_values = false;
  bool indexed_term_dict = false;
  bool hyperloglog = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  search::Features features;
  std::optional<IVFColumnConfig> ivf_config;
  irs::field_id synthetic_column = irs::field_limits::invalid();
  uint32_t row_group_size = 0;
  uint32_t norm_row_group_size = 0;
  irs::field_id null_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id numeric_field_id = irs::field_limits::invalid();
};

// One expression key: its payload plus the iresearch field_id allocated for it
// (an expression has no natural column id). One self-contained unit -- no array
// kept parallel to a separate field-id vector.
struct ExpressionKey {
  ExpressionData data;
  irs::field_id field_id = irs::field_limits::invalid();
};

struct InvertedIndexData {
  std::string name;
  // Plain-column keys (de-duped). Each column key's field_id is its column id,
  // so no separate field_id is stored. Order is not load-bearing for inverted.
  std::vector<Column::Id> columns;
  std::vector<ExpressionKey> expression_keys;
  // Per-field iresearch config keyed by field_id.
  containers::NodeHashMap<irs::field_id, EntryConfigSerialized> entries;
  InvertedIndexOptions options;
};

}  // namespace sdb::catalog::persistence
