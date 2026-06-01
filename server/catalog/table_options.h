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
#include "catalog/object.h"
#include "catalog/sequence.h"
#include "query/utils.h"
#include "utils/velox_vpack.h"

namespace sdb::catalog {

// NOLINTBEGIN
enum class ColumnStoreMode : uint8_t {
  kNormal = 0,
  kIndexOnly = 1,
};

// Selects the physical backend for a TableShard's row store. Persisted in
// the shard's vpack payload so recovery can construct the right subclass.
enum class StorageKind : uint8_t {
  kRocksDB = 0,  // default -- RocksDB row store, what every existing table uses
  kSearch = 1,   // iresearch columnstore + RocksDB WAL markers
};

class Column final : public Object {
 public:
  enum GeneratedType : uint8_t {
    kNone = 0,
    // TODO(mbkkt) swap these, to make it more like duckdb values?
    kStored = 1,
    kVirtual = 2,
  };

  using Id = ObjectId;

  bool IsIndexOnly() const noexcept {
    return store_mode == ColumnStoreMode::kIndexOnly;
  }

  static constexpr uint64_t kMaxRealIdValue =
    std::numeric_limits<uint64_t>::max() - 1'000'000;
  static constexpr Id kGeneratedPKId{kMaxRealIdValue + 1};
  static constexpr Id kInvertedIndexScoreId{kMaxRealIdValue + 2};
  static constexpr Id kInvertedIndexOffsetsId{kMaxRealIdValue + 3};
  // Sentinel for "no/invalid column id". numeric_limits<Id>::max() does NOT
  // work (Id is a class wrapping uint64_t, not an arithmetic type).
  static constexpr Id kInvalidId{std::numeric_limits<uint64_t>::max()};

  static constexpr std::string_view kScoreName = "sdb_inverted_index_score";
  // Prefix used in virtual offsets column names. Ends with kReservedSymbol so
  // it can never collide with a user-defined column name.
  static constexpr std::string_view kOffsetsNamePrefix =
    "sdb_inverted_index_offsets$";

  static std::string MakeOffsetsName(Id column_id) {
    static_assert(kOffsetsNamePrefix.ends_with(query::kReservedSymbol));
    return absl::StrCat(kOffsetsNamePrefix, column_id.id());
  }

  // LIST(INTEGER) -- flat offsets column: interleaved start,end pairs.
  static duckdb::LogicalType MakeOffsetsType() {
    return duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
  }

  Column() : Object{{}, {}, {}, ObjectType::Column} {}

  Column(ObjectId owner_table_id, ObjectId id, std::string_view name,
         duckdb::LogicalType ty, std::shared_ptr<ColumnExpr> e = nullptr,
         GeneratedType gt = kNone)
    : Object{owner_table_id, id, std::string{name}, ObjectType::Column},
      type{std::move(ty)},
      expr{std::move(e)},
      generated_type{gt} {}

  bool IsGenerated() const noexcept {
    return generated_type != GeneratedType::kNone;
  }

  void WriteInternal(vpack::Builder&) const final {}
  std::shared_ptr<Object> Clone() const final;

  void SetId(Id id) noexcept { _id = id; }

  duckdb::LogicalType type;
  std::shared_ptr<ColumnExpr> expr;
  GeneratedType generated_type = GeneratedType::kNone;
  ColumnStoreMode store_mode = ColumnStoreMode::kNormal;
};

inline std::shared_ptr<Object> Column::Clone() const {
  return std::make_shared<Column>(*this);
}

inline void VPackWrite(auto ctx, const Column& col) {
  vpack::WriteTuple(
    ctx.vpack(),
    std::forward_as_tuple(col.GetId(), col.type, std::string{col.GetName()},
                          col.expr, col.generated_type));
}

inline void VPackRead(auto ctx, Column& col) {
  ObjectId id;
  duckdb::LogicalType type;
  std::string name;
  std::shared_ptr<ColumnExpr> expr;
  Column::GeneratedType gt = Column::kNone;
  auto tup = std::tie(id, type, name, expr, gt);
  vpack::ReadTuple(ctx.vpack(), tup);
  col = Column{ctx.arg(), id, name, std::move(type), std::move(expr), gt};
}

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
  StorageKind storage = StorageKind::kRocksDB;
  // For search-backed tables (`StorageKind::kSearch`): id of the per-shard
  // cache table in the internal `sdb_cache$` attached database. Allocated
  // by the caller before invoking CreateTable; populated into the catalog
  // `Table` entry so the cache table's lifecycle is durably linked to the
  // search table's. Unset for all other storage kinds.
  // See search_table_shard_native.md §1.4.
  ObjectId cache_table_id;
};
// NOLINTEND

}  // namespace sdb::catalog
