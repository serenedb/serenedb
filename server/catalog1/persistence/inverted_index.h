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
#include <duckdb/common/types.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/types.hpp>
#include <string>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "search/search_analyzer_impl.h"

namespace sdb::catalog::persistence {

struct InvertedIndexSettings {
  uint32_t row_group_size{122880};
  uint32_t refresh_interval_ms{1000};
  uint32_t reindex_interval_ms{0};
  uint32_t compaction_interval_ms{1000};
  uint32_t cleanup_interval_step{1};
  uint64_t segment_memory_max{268435456};
  uint32_t segment_docs_max{0};
  uint32_t compaction_max_segments{10};
  uint64_t compaction_max_segments_bytes{5368709120};
  uint64_t compaction_floor_segment_bytes{2097152};

  bool operator==(const InvertedIndexSettings& rhs) const = default;
};

enum class PkColumnKind : uint8_t {
  None = 0,
  Has = 1,
  Unable = 2,
};

struct PkPolicy {
  bool index_term = true;
  PkColumnKind column = PkColumnKind::Has;
};

struct KeyRecord {
  irs::field_id field_id = irs::field_limits::invalid();
  duckdb::LogicalType type;
  std::string normalized_expression;
};

struct FieldRecord {
  irs::field_id numeric_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id null_field_id = irs::field_limits::invalid();
  irs::field_id synthetic_column = irs::field_limits::invalid();
  search::Features features;
  bool store_values = false;
  bool indexed_term_dict = false;
  bool whole_value = false;
  bool is_keyword = false;
  irs::ColumnOptions column_options;
  std::string text_dictionary;
};

struct InvertedIndexData {
  InvertedIndexSettings settings;
  PkPolicy pk;
  std::vector<KeyRecord> keys;
  containers::NodeHashMap<irs::field_id, FieldRecord> fields;
};

}  // namespace sdb::catalog::persistence
