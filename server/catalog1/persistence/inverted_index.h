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
#include <duckdb/common/enums/compression_type.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/types.hpp>
#include <optional>
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

enum class OpclassKind : uint8_t {
  None = 0,
  Dictionary = 1,
  Included = 2,
  Ivf = 3,
};

// The `ivf` opclass, resolved at CREATE INDEX. `d` comes from the key's
// ARRAY(FLOAT, N); the rest from the opclass options and the sdb_ivf_*
// session settings.
struct InvertedIndexFieldIVF {
  int d = 0;
  irs::VectorMetric metric = irs::VectorMetric::L2Sqr;
  irs::VectorQuantization quant = irs::VectorQuantization::None;
  uint32_t pq_m = 0;
  uint32_t rabitq_bits = 0;
  float sample_factor = 0;
  uint32_t posting_size = 0;
  bool compression = true;
};

struct KeyRecord {
  irs::field_id field_id = irs::field_limits::invalid();
  irs::field_id block = irs::field_limits::invalid();
  OpclassKind kind = OpclassKind::None;
  std::string return_type;
  uint8_t return_type_id = 0;
  std::string serialized;
};

struct FieldRecord {
  // The per-kind JSON leaves. Allocated only for a JSON key whose analyzer
  // reads leaves rather than the whole value -- a geo analyzer consumes the
  // GeoJSON object itself, so it gets none and HasJsonLeafFields() keeps it
  // out of the leaf splitter.
  irs::field_id numeric_field_id = irs::field_limits::invalid();
  irs::field_id bool_field_id = irs::field_limits::invalid();
  irs::field_id null_field_id = irs::field_limits::invalid();
  // Carries either the tokenizer's own per-row payload or the field's norms --
  // never both, and nothing at all unless the analyzer asks for one.
  irs::field_id synthetic_column = irs::field_limits::invalid();
  search::Features features;
  bool store_values = false;
  bool indexed_term_dict = false;
  bool hyperloglog = false;
  // The analyzer reads the whole value rather than descending into it -- a geo
  // analyzer parses the GeoJSON object itself. Distinct from having no JSON
  // leaves, which is also true of a JSON key that names no opclass at all and
  // must still be rejected for holding an object.
  bool whole_value = false;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
  // Whether the named dictionary tokenizes verbatim (template='keyword').
  // Decided at CREATE INDEX, where the analyzer is resolved, because the
  // decoded config has no tokenizer map to resolve one with.
  bool is_keyword = false;
  std::optional<InvertedIndexFieldIVF> ivf_config;
};

struct InvertedIndexData {
  InvertedIndexSettings settings;
  PkPolicy pk;
  std::vector<KeyRecord> keys;
  containers::NodeHashMap<irs::field_id, FieldRecord> fields;
};

}  // namespace sdb::catalog::persistence
