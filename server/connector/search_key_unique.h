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
#include <iresearch/index/index_reader.hpp>
#include <optional>
#include <span>
#include <vector>

#include "connector/key_terms.h"
#include "connector/primary_key.h"
#include "connector/search_unique_probe.h"

namespace sdb::connector {

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

// The key that two live positions of this transaction share, if any: either two
// positions among `segments` and `keys` -- the transaction contradicting itself
// -- or one of those and one `probe` finds in the tiers it holds. `keys` is the
// unflushed tail, already encoded because a commit's row buffer is emptied
// before it certifies.
//
// One key column leads the walk and the rest are read only where that one
// already collides, so the column chosen to lead decides the cost; it is picked
// here rather than by the caller, because the sources must all agree on it and
// nothing outside needs to know which it was.
std::optional<KeyTerms> FindDuplicateKey(
  std::span<const irs::SubReader* const> segments,
  std::span<const KeyTerms> keys, std::span<const primary_key::KeySlot> slots,
  UniqueProbe& probe);

}  // namespace sdb::connector
