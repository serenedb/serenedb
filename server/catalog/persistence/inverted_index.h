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
  uint32_t pq_m = 0;
  uint32_t rabitq_bits = 0;
  float sample_factor = 0;
  uint32_t posting_size = 0;
  bool compression = true;
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

// One plain-column key: the column plus its allocated term field_id. For a
// transactional index `field_id == column` (identity); for a Search-table index
// the field_id is a distinct allocation, so several indexes on one column get
// separate term fields in the shared store. The plain-column mirror of
// ExpressionKey.
struct ColumnKey {
  Column::Id column = Column::kInvalidId;
  irs::field_id field_id = irs::field_limits::invalid();
};

// Persisted inverted-index catalog tuple. Templated only on the `columns`
// element so the two engines keep distinct on-disk layouts: a transactional
// index serializes bare `Column::Id`s (identical to the pre-search-table
// format, so old datadirs load unchanged), while a Search-table index
// serializes `ColumnKey`s that also carry each column's allocated term
// field_id. Every other field is shared, so the positional layout after
// `columns` is byte-identical across both engines.
template<typename ColumnEntry>
struct InvertedIndexDataT {
  std::string name;
  // Plain-column keys (de-duped). Order is not load-bearing for inverted.
  std::vector<ColumnEntry> columns;
  std::vector<ExpressionKey> expression_keys;
  // Per-field iresearch config keyed by field_id.
  containers::NodeHashMap<irs::field_id, EntryConfigSerialized> entries;
  InvertedIndexOptions options;
  // Partial-index predicate (CREATE INDEX ... WHERE): rows are indexed and
  // maintained only when it evaluates to true. An empty serialized_expr
  // means a full index. return_type is BOOLEAN.
  ExpressionData predicate;
  std::string comment;
};

// Legacy / transactional layout: bare column ids, matching the format from
// before Search tables existed.
using InvertedIndexData = InvertedIndexDataT<Column::Id>;
// Search-table layout: each column carries its allocated term field_id.
using SearchInvertedIndexData = InvertedIndexDataT<ColumnKey>;

}  // namespace sdb::catalog::persistence
