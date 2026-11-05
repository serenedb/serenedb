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

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/index/field_meta.hpp>
#include <iresearch/utils/bytes_utils.hpp>
#include <span>
#include <vector>

#include "connector/primary_key.h"

namespace sdb::connector {

// One component of a row key, as the index holds it: which of the key column's
// member dictionaries the value lives in, and its term there. A field's
// postings are partitioned by `DictRef` and only then by term, so a lookup
// answers to both; a boolean's value is the member alone, its term being
// empty, and a numeric lives under a member no string ever reaches.
struct KeyTerm {
  irs::DictRef dict;
  irs::bstring term;

  auto operator<=>(const KeyTerm&) const = default;
};

// A whole primary key: one component per key column, in key-column order. Each
// is what a query for that value would look up, so a key built here compares
// against what the index holds.
using KeyTerms = std::vector<KeyTerm>;

// Every row's key, encoded with the index's own encoder.
// Appends, and resumes at `first_row`, so a caller that keeps its keys between
// probes extends them with what arrived since instead of encoding the whole
// buffer again. The buffer only ever grows between the points that drop it
// whole, which is what makes a row count enough to resume from.
void AppendRowKeys(duckdb::ColumnDataCollection& rows,
                   std::span<const primary_key::KeySlot> slots,
                   duckdb::idx_t first_row, std::vector<KeyTerms>& keys);

// The same, for a caller holding one chunk rather than a collection -- a WAL
// record's rows arrive that way.
void EncodeChunkKeys(duckdb::DataChunk& chunk,
                     std::span<const primary_key::KeySlot> slots,
                     std::vector<KeyTerms>& keys, duckdb::idx_t first_row = 0);

}  // namespace sdb::connector
