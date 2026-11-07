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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <iresearch/index/field_meta.hpp>
#include <span>
#include <string>

#include "catalog/entry/inverted_index.h"
#include "connector/column_id.h"

namespace sdb::connector {

// The dictionary a constraint key's term is written under.
inline constexpr irs::DictRef kConstraintKeyDict{
  .layout = irs::DictLayout::String, .member = irs::DictMember::Strings};

// Writes the key of every row of `chunk`, whose columns are `column_ids`, into
// `keys` (BLOB): null where any key column is null.
void EncodeConstraintKeys(duckdb::DataChunk& chunk,
                          std::span<const ColumnId> column_ids,
                          const catalog::ConstraintKey& key,
                          duckdb::Vector& keys);

// The key's values as PostgreSQL shows them in a DETAIL: `(a, b)=(1, x)`.
std::string ConstraintKeyText(std::string_view key,
                              std::span<const std::string> names,
                              std::span<const duckdb::LogicalType> types);

}  // namespace sdb::connector
