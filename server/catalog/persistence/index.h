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

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <optional>
#include <string>
#include <vector>

#include "catalog/persistence/scorer_options.h"
#include "catalog/table_options.h"

namespace sdb::catalog::persistence {

enum class PkColumnKind : uint8_t {
  None,
  I64,
  I64I64,
  Unable,
  // User key_columns: stored as ONE STRUCT column of the columns' own types.
  // Appended last to keep the persisted ordinals above stable.
  Struct,
};

struct InvertedIndexOptions {
  uint32_t row_group_size = 0;
  uint32_t norm_row_group_size = 0;
  uint32_t refresh_interval_ms = 0;
  uint32_t compaction_interval_ms = 0;
  uint32_t cleanup_interval_step = 0;
  bool pk_term = true;
  PkColumnKind pk_column = PkColumnKind::I64;
  std::optional<ScorerOptions> topk_scorer;
  // CREATE INDEX WITH (key_columns = 'a, b'): the external-DB re-fetch key
  // columns; empty = default (pg ctid / CH PK). Persisted so build and lookup
  // agree.
  std::vector<std::string> key_columns;
};

// Shared expression payload for a computed index key. Each index kind persists
// its own key layout; only this leaf is common.
struct ExpressionData {
  std::string serialized_expr;
  std::vector<Column::Id> dependent_columns;
  duckdb::LogicalType return_type;
  std::string pretty_printed;
};

}  // namespace sdb::catalog::persistence
