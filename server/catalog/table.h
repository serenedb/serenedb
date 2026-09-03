////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "catalog/entry.h"
#include "catalog/persistence/search_table_options.h"
#include "catalog/sequence.h"
#include "catalog/table_options.h"

namespace sdb::search {

class SearchTable;

}  // namespace sdb::search
namespace sdb::catalog {

// A serenedb table's storage parameters ride in duckdb's own CreateInfo::tags
// (copied onto the entry, serialized with the CreateInfo, shown in
// duckdb_tables().tags). The keys are the ones the user writes in WITH (...) --
// what postgres puts in pg_class.reloptions. kGeneratedPkSeqTag is not a user
// option -- the id of the sequence feeding the synthetic primary key of a table
// that declares none -- hence the prefix separating it from the WITH keys.
inline constexpr std::string_view kStorageOption = "storage";
inline constexpr std::string_view kGeneratedPkSeqTag = "sdb_generated_pk_seq";

// The tags read off a CreateInfo or off the entry built from it; both carry the
// same map, so the readers take it directly.
using TableTags = duckdb::InsertionOrderPreservingMap<std::string>;

void WriteTableTags(TableTags& tags, TableEngine engine,
                    const persistence::SearchTableOptions& search_options,
                    ObjectId generated_pk_seq_id);
TableEngine ReadTableEngineTag(const TableTags& tags) noexcept;
persistence::SearchTableOptions ReadSearchOptionTags(
  const TableTags& tags) noexcept;
ObjectId ReadGeneratedPkSeqTag(const TableTags& tags) noexcept;

// One SERIAL column of a CREATE TABLE. The catalog resolves the sequence's
// name (mangling on collision), stamps the owning table id and sets the
// column's nextval default; none of that is knowable before the mutation runs
// under the catalog mutex.
struct SerialSequence {
  ObjectId column_id;
  SequenceOptions options;
};

// A table is duckdb's own duckdb::CreateTableInfo: every identity rides a
// duckdb structure (CreateInfo::oid/parent_oid, ColumnDefinition::catalog_oid,
// Constraint::oid, UniqueConstraint::host_index_id), the serenedb options ride
// tags, and owner/ACL/column grants live on the entry's permissions. Nothing is
// left for a subclass to hold; what serenedb adds is the operations below.
duckdb::unique_ptr<duckdb::CreateTableInfo> NewTableInfo();

inline void SetTableTags(duckdb::CreateTableInfo& info, TableEngine engine,
                         const persistence::SearchTableOptions& options,
                         ObjectId generated_pk_seq_id) {
  WriteTableTags(info.tags, engine, options, generated_pk_seq_id);
}

// The column `column_id` names, or null when the list holds no such column.
// An unset id names none: a column carries no stable id until the catalog
// issues one, and an attached stock-duckdb table's columns never get one.
const duckdb::ColumnDefinition* ColumnById(const duckdb::ColumnList& columns,
                                           ObjectId column_id) noexcept;
const duckdb::ColumnDefinition* ColumnById(const duckdb::CreateTableInfo& info,
                                           ObjectId column_id) noexcept;
// The column `name` names, matched exactly -- serenedb folds unquoted
// identifiers at parse time, so `t("A" int, "a" int)` is two columns.
const duckdb::ColumnDefinition* ColumnByName(
  const duckdb::CreateTableInfo& info, std::string_view name) noexcept;

bool IsColumnNotNull(const duckdb::CreateTableInfo& info,
                     ObjectId column_id) noexcept;

duckdb::unique_ptr<duckdb::CreateTableInfo> Clone(
  const duckdb::CreateTableInfo& info);

duckdb::unique_ptr<duckdb::CreateTableInfo> ChangeColumnType(
  const duckdb::CreateTableInfo& info, std::string_view column_name,
  duckdb::LogicalType new_type);
std::string_view CommentText(const duckdb::Value& comment) noexcept;
duckdb::Value CommentValue(std::string_view comment);

// The primary key among `constraints`, or null when they declare none. Takes
// the list rather than what holds it: a definition and the entry built from it
// carry the same one.
const duckdb::UniqueConstraint* TablePrimaryKey(
  std::span<const duckdb::unique_ptr<duckdb::Constraint>> constraints) noexcept;

// A foreign key is written down twice: the referencing table states it, and
// the referenced table carries the reciprocal entry duckdb enforces deletes
// through. `host_referenced_id` names the table at the other end of the key on
// both halves, so anything that means "the key this table states" asks this
// first.
bool StatesForeignKey(const duckdb::ForeignKeyConstraint& fk) noexcept;

// The referenced key of `fk`, as the referenced relation spells it now --
// resolved through the identities, because a RENAME COLUMN on the referenced
// side never reaches the referencing table's own definition. A null
// `referenced` falls back to the names the constraint wrote down.
duckdb::vector<duckdb::Identifier> ReferencedKeyNames(
  const duckdb::ForeignKeyConstraint& fk,
  const duckdb::CreateTableInfo* referenced);

}  // namespace sdb::catalog
