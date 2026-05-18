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

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <cstdint>
#include <duckdb/common/types.hpp>
#include <limits>
#include <optional>
#include <span>
#include <vector>

#include "catalog/column_expr.h"
#include "catalog/fwd.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/sequence.h"
#include "pg/sql_collector.h"
#include "query/utils.h"
#include "utils/velox_vpack.h"

namespace sdb::catalog {

// NOLINTBEGIN
enum class ColumnStoreMode : uint8_t {
  kNormal = 0,
  kIndexOnly = 1,
};

struct Column {
  enum GeneratedType : uint8_t {
    kNone = 0,
    // TODO(mbkkt) swap these, to make it more like duckdb values?
    kStored = 1,
    kVirtual = 2,
  };

  bool IsGenerated() const noexcept {
    return generated_type != GeneratedType::kNone;
  }

  bool IsIndexOnly() const noexcept {
    return store_mode == ColumnStoreMode::kIndexOnly;
  }

  using Id = uint64_t;

  static constexpr Id kMaxRealId =
    std::numeric_limits<uint64_t>::max() - 1'000'000;
  static constexpr Id kGeneratedPKId = kMaxRealId + 1;
  static constexpr Id kInvertedIndexScoreId = kMaxRealId + 2;
  static constexpr Id kInvertedIndexOffsetsId = kMaxRealId + 3;

  static constexpr std::string_view kScoreName = "sdb_inverted_index_score";
  // Prefix used in virtual offsets column names. Ends with kReservedSymbol so
  // it can never collide with a user-defined column name.
  static constexpr std::string_view kOffsetsNamePrefix =
    "sdb_inverted_index_offsets$";

  // Encodes the searched column's catalog ID into the virtual column name.
  static std::string MakeOffsetsName(Id column_id) {
    static_assert(kOffsetsNamePrefix.ends_with(query::kReservedSymbol));
    return absl::StrCat(kOffsetsNamePrefix, column_id);
  }

  // LIST(INTEGER) -- flat offsets column: interleaved start,end pairs.
  static duckdb::LogicalType MakeOffsetsType() {
    return duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
  }

  Id id;
  duckdb::LogicalType type;
  std::string name;
  // if generated type is not kNone, expr = generated expression
  // else expr = default value expression (if any)
  std::shared_ptr<ColumnExpr> expr;
  GeneratedType generated_type = GeneratedType::kNone;
  ColumnStoreMode store_mode = ColumnStoreMode::kNormal;
};

struct CheckConstraint {
  ObjectId id;
  std::string name;
  std::shared_ptr<ColumnExpr> expr;

  // If this constraint is just `NOT NULL` on a single column of `columns`,
  // returns that column's index. Otherwise returns std::nullopt.
  std::optional<size_t> IsNotNull(
    std::span<const Column> columns) const noexcept;
};

struct TableStats {
  uint64_t num_rows = 0;
};

struct CreateTableOptions {
  // LocalCatalog resolves the sequence name (mangling on collision), stamps
  // owner_table_id, and installs the column's nextval default.
  struct SerialSequenceOption {
    Column::Id column_id;
    SequenceOptions options;
  };

  std::string name;
  std::vector<Column> columns;
  std::vector<Column::Id> pk_columns;
  std::vector<CheckConstraint> check_constraints;
  std::vector<SerialSequenceOption> sequences;
};
// NOLINTEND

}  // namespace sdb::catalog
