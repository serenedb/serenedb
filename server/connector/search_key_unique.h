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
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/bytes_utils.hpp>
#include <optional>
#include <span>
#include <vector>

#include "connector/primary_key.h"
#include "connector/search_unique_probe.h"

namespace sdb::connector {

// A whole primary key: one term per key column, in key-column order.
using KeyTerms = std::vector<irs::bstring>;

// Every row's key, encoded with the sink's own encoder. A key that outlives
// its transaction -- a commit handing its unflushed rows to the pending delta
// -- must carry the same bytes a later probe looks for, which is why this and
// the walk below share one encoder rather than each having its own.
std::vector<KeyTerms> EncodeRowKeys(
  duckdb::ColumnDataCollection& rows,
  std::span<const primary_key::KeySlot> slots);

// The key that two live positions of this transaction share, if any: either two
// positions among `segments` and `rows` -- the transaction contradicting itself
// -- or one of those and one `probe` finds in published state. `rows` is the
// unflushed tail, null when there is none.
//
// One key column leads the walk and the rest are read only where that one
// already collides, so the column chosen to lead decides the cost; it is picked
// here rather than by the caller, because the sources must all agree on it and
// nothing outside needs to know which it was.
std::optional<KeyTerms> FindDuplicateKey(
  std::span<const irs::SubReader* const> segments,
  duckdb::ColumnDataCollection* rows,
  std::span<const primary_key::KeySlot> slots, UniqueProbe& probe);

}  // namespace sdb::connector
