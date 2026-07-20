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
#include "catalog/search_table_options.h"
#include "catalog/sequence.h"
#include "query/utils.h"

namespace sdb::catalog {

// Persistent on-disk catalog format.
class Column final : public Object {
 public:
  enum GeneratedType : uint8_t {
    kNone = 0,
    // TODO(mbkkt) swap these, to make it more like duckdb values?
    kStored = 1,
    kVirtual = 2,
  };

  using Id = ObjectId;

  static constexpr uint64_t kMaxRealIdValue =
    std::numeric_limits<uint64_t>::max() - 1'000'000;
  static constexpr Id kGeneratedPKId{kMaxRealIdValue + 1};
  static constexpr Id kInvertedIndexScoreId{kMaxRealIdValue + 2};
  static constexpr Id kInvertedIndexOffsetsId{kMaxRealIdValue + 3};
  static constexpr Id kInvertedIndexTermId{kMaxRealIdValue + 4};
  static constexpr Id kInvertedIndexTermCountId{kMaxRealIdValue + 5};
  static constexpr Id kInvertedIndexTermFreqId{kMaxRealIdValue + 6};
  static constexpr Id kInvertedIndexTermScoreId{kMaxRealIdValue + 7};
  static constexpr Id kInvertedIndexTermRawId{kMaxRealIdValue + 8};
  // Sentinel for "no/invalid column id". numeric_limits<Id>::max() does NOT
  // work (Id is a class wrapping uint64_t, not an arithmetic type).
  static constexpr Id kInvalidId{std::numeric_limits<uint64_t>::max()};

  static constexpr std::string_view kScoreName = "sdb_inverted_index_score";
  static constexpr std::string_view kTermName = "sdb_inverted_index_term$";
  static constexpr std::string_view kTermRawName =
    "sdb_inverted_index_term_raw$";
  static constexpr std::string_view kTermCountName =
    "sdb_inverted_index_term_count$";
  static constexpr std::string_view kTermFreqName =
    "sdb_inverted_index_term_freq$";
  static constexpr std::string_view kTermScoreName =
    "sdb_inverted_index_term_score$";
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

  Column() : Object{Permissions{}, {}, {}, {}, ObjectType::Column} {}

  Column(ObjectId owner_table_id, ObjectId id, std::string_view name,
         duckdb::LogicalType ty, std::shared_ptr<ColumnExpr> e = nullptr,
         GeneratedType gt = kNone, Acl acl = {})
    : Object{Permissions{ObjectId{}, std::move(acl)}, owner_table_id, id,
             std::string{name}, ObjectType::Column},
      type{std::move(ty)},
      expr{std::move(e)},
      generated_type{gt} {}

  bool IsGenerated() const noexcept {
    return generated_type != GeneratedType::kNone;
  }

  // Column is never persisted as a standalone Object -- it rides inside
  // TableData (see table.cpp). The owner table id is not written; Table's ctor
  // re-stamps it on every column after deserialization.
  void Serialize(duckdb::Serializer& sink) const final;
  static Column Deserialize(duckdb::Deserializer& src);
  std::shared_ptr<Object> Clone() const final;

  void SetId(Id id) noexcept { _id = id; }

  duckdb::LogicalType type;
  std::shared_ptr<ColumnExpr> expr;
  GeneratedType generated_type = GeneratedType::kNone;
  std::string comment;
};

inline std::shared_ptr<Object> Column::Clone() const {
  return std::make_shared<Column>(*this);
}

// Persistent on-disk catalog format.
class CheckConstraint final : public Object {
 public:
  CheckConstraint()
    : Object{Permissions{}, {}, {}, {}, ObjectType::CheckConstraint} {}

  CheckConstraint(ObjectId owner_table_id, ObjectId id, std::string_view name,
                  std::shared_ptr<ColumnExpr> e)
    : Object{Permissions{}, owner_table_id, id, std::string{name},
             ObjectType::CheckConstraint},
      expr{std::move(e)} {}

  void Serialize(duckdb::Serializer& sink) const final;
  static CheckConstraint Deserialize(duckdb::Deserializer& src);
  std::shared_ptr<Object> Clone() const final {
    return std::make_shared<CheckConstraint>(*this);
  }

  // If this constraint is just `NOT NULL` on a single column of `columns`,
  // returns that column's index. Otherwise returns std::nullopt.
  std::optional<size_t> IsNotNull(
    std::span<const Column> columns) const noexcept;

  std::optional<std::string_view> NotNullColumnName() const noexcept;

  std::shared_ptr<ColumnExpr> expr;
};

// Which engine owns the table's row data. Both kinds are first-class and
// coexist: Transactional tables live as store tables in the engine's
// single-file database; Search is the eventually-consistent iresearch-only
// table engine.
enum class TableEngine : uint8_t {
  Transactional = 0,
  Search = 1,
};

// One FOREIGN KEY of a table: `columns` (on the owning table) reference
// `referenced_columns` of `referenced_table`, which must be its PRIMARY KEY.
// `name` is the constraint name (explicit `CONSTRAINT <name>` or PG auto-name).
struct TableForeignKey {
  std::string name;
  std::vector<Column::Id> columns;
  ObjectId referenced_table;
  std::vector<Column::Id> referenced_columns;
  // Constraint OID (pg_constraint.oid); allocated when the constraint is
  // created, stable for its lifetime.
  ObjectId id;
};

// One UNIQUE constraint of a table over `columns`, with its constraint `name`
// (explicit `CONSTRAINT <name>` or PG auto-name `<table>_<col>_key`).
struct TableUnique {
  std::string name;
  std::vector<Column::Id> columns;
  // Constraint OID (pg_constraint.oid) and the OID of its backing index
  // relation (pg_class.oid / pg_index.indexrelid / pg_constraint.conindid).
  // Allocated when the constraint is created, stable for its lifetime.
  ObjectId id;
  ObjectId index_id;
};

struct CreateTableOptions {
  // Catalog resolves the sequence name (mangling on collision), stamps
  // owner_table_id, and installs the column's nextval default.
  struct SerialSequenceOption {
    Column::Id column_id;
    SequenceOptions options;
  };

  std::string name;
  std::vector<Column> columns;
  std::vector<Column::Id> pk_columns;
  std::string pk_name;
  std::vector<CheckConstraint> check_constraints;
  std::vector<SerialSequenceOption> sequences;
  std::vector<TableUnique> unique_constraints;
  std::vector<TableForeignKey> foreign_keys;
  TableEngine engine = TableEngine::Transactional;
  // Background-maintenance options for Search-engine tables (empty otherwise).
  SearchTableOptions search_options;
};
// NOLINTEND

}  // namespace sdb::catalog
