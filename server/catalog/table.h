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
#include "catalog/sequence.h"
#include "catalog/table_options.h"

namespace sdb::search {

class SearchTable;

}  // namespace sdb::search
namespace sdb::catalog {

// A serenedb table's storage parameters ride in duckdb's own CreateInfo::tags
// -- the map duckdb copies onto the entry, serializes with the CreateInfo and
// shows in duckdb_tables().tags. The keys are the ones the user writes in
// WITH (...), so the map reads back as the statement that produced it, which is
// also what postgres puts in pg_class.reloptions. Keeping them here rather than
// in a field beside the entry means the definition stays one struct.
//
// kGeneratedPkSeqTag is not a user option -- it is the id of the sequence
// feeding the synthetic primary key of a table that declares none -- but it is
// definition all the same and has the same durability requirement, hence the
// prefix that separates it from the WITH keys.
inline constexpr std::string_view kStorageOption = "storage";
inline constexpr std::string_view kGeneratedPkSeqTag = "sdb_generated_pk_seq";

// The tags read off a CreateInfo or off the entry built from it; both carry the
// same map, so the readers take it directly.
using TableTags = duckdb::InsertionOrderPreservingMap<std::string>;

void WriteTableTags(TableTags& tags, TableEngine engine,
                    const SearchTableOptions& search_options,
                    ObjectId generated_pk_seq_id);
TableEngine ReadTableEngineTag(const TableTags& tags) noexcept;
SearchTableOptions ReadSearchOptionTags(const TableTags& tags) noexcept;
ObjectId ReadGeneratedPkSeqTag(const TableTags& tags) noexcept;

// One SERIAL column of a CREATE TABLE. The catalog resolves the sequence's
// name (mangling on collision), stamps the owning table id and sets the
// column's nextval default; none of that is knowable before the mutation runs
// under the catalog mutex.
struct SerialSequence {
  ObjectId column_id;
  SequenceOptions options;
};

// The ids AddPrimaryKey stamps into the objects it creates. Allocated by the
// caller rather than inside the mutation: a deferred publish applies the
// mutation twice -- once into the transaction's overlay, once when the op is
// replayed against the committed snapshot at commit -- and an id drawn inside
// would differ between the two, so the WAL frame and the published version
// would disagree and the ids would move across a restart.
struct PrimaryKeyIds {
  ObjectId constraint_id;
  ObjectId index_id;
  // One per key column, for the NOT NULL a primary key implies. Positional, so
  // a column that is already NOT NULL simply leaves its id unused.
  std::vector<ObjectId> not_null_ids;
};

// The live state behind one table's rows: the iresearch shard a search table
// keeps them in, and the counter feeding the synthetic primary key of a table
// that declares none. Neither is part of the definition and neither is
// versioned -- a comment, a rename or an ALTER writes a new definition while
// the rows and the counter stay the same ones -- so both are held here and
// shared by every version.
//
// Never serialized: what the catalog log holds is the definition.
class TableRuntime {
 public:
  const std::shared_ptr<search::SearchTable>& GetData() const noexcept {
    return _data;
  }
  void SetData(std::shared_ptr<search::SearchTable> data) noexcept {
    _data = std::move(data);
  }

  const std::shared_ptr<SequenceCounter>& GetGeneratedPkSequence()
    const noexcept {
    return _generated_pk_seq;
  }
  void SetGeneratedPkSequence(
    std::shared_ptr<SequenceCounter> counter) noexcept {
    _generated_pk_seq = std::move(counter);
  }

 private:
  std::shared_ptr<search::SearchTable> _data;
  std::shared_ptr<SequenceCounter> _generated_pk_seq;
};

// One table, in the form a catalog entry is built from. duckdb's own
// CreateTableInfo already carries the columns, the constraints and -- through
// CreateInfo::tags -- the engine, the search options and the auto-PK sequence,
// so this extends it rather than replacing it: the ids ride the duckdb
// structures (ColumnDefinition::host_id, Constraint::host_id,
// UniqueConstraint::host_index_id) and only what upstream has nowhere to put
// is added here.
//
// The table's own owner and ACL are not here: they live on the entry, their
// one home, and travel beside the info in the record. The
// per-column grants are, because a column has no entry of its own to keep them
// on and duckdb's ColumnDefinition has no room for them.
//
// Mutations return a fresh info -- it is const and shared, and it is what a
// catalog entry is built from.
class CreateTableInfo final : public duckdb::CreateTableInfo {
 public:
  // Keyed by the column's ObjectId, which is ColumnDefinition::HostId(), so a
  // reader walking the column list finds the grants without a second lookup.
  // Only columns some GRANT has named are present.
  using ColumnAcls = containers::FlatHashMap<ObjectId, Acl>;

  CreateTableInfo();

  duckdb::unique_ptr<duckdb::CreateInfo> Copy() const final;

  // duckdb's CreateInfo dispatch hands back the base class, and this is what
  // puts a payload read from the catalog log into ours.
  static std::shared_ptr<CreateTableInfo> Adopt(duckdb::CreateTableInfo& base);

  ObjectId GetId() const noexcept { return ObjectId{oid}; }
  ObjectId GetParentId() const noexcept { return ObjectId{parent_oid}; }

  std::string_view GetName() const noexcept {
    return GetTableName().GetIdentifierName();
  }

  // Which engine owns the rows, the background-maintenance intervals that go
  // with it, and the sequence feeding the synthetic primary key of a table that
  // declares none. All three ride in `tags`, which is definition.
  TableEngine GetEngine() const noexcept { return ReadTableEngineTag(tags); }
  SearchTableOptions SearchOptions() const noexcept {
    return ReadSearchOptionTags(tags);
  }
  ObjectId GetGeneratedPkSeqId() const noexcept {
    return ReadGeneratedPkSeqTag(tags);
  }
  void SetTableTags(TableEngine engine, const SearchTableOptions& options,
                    ObjectId generated_pk_seq_id) {
    WriteTableTags(tags, engine, options, generated_pk_seq_id);
  }

  // Empty when the table carries no COMMENT ON.
  std::string_view Comment() const noexcept {
    return comment.IsNull() ? std::string_view{}
                            : duckdb::StringValue::Get(comment);
  }

  const ColumnAcls& GetColumnAcls() const noexcept { return _column_acls; }
  // A view into this version, never a copy: the pg_catalog projections build
  // an AclView over what they get back and read it after the walk.
  AclView GetColumnAcl(ObjectId column_id) const noexcept {
    const auto it = _column_acls.find(column_id);
    return it == _column_acls.end() ? AclView{} : AclView{it->second};
  }
  void SetColumnAcls(ColumnAcls acls) noexcept {
    _column_acls = std::move(acls);
  }

  // The column `column_id` names, or null when the table lists no such column.
  const duckdb::ColumnDefinition* ColumnById(ObjectId column_id) const noexcept;
  // The column `name` names, matched exactly -- serenedb folds unquoted
  // identifiers at parse time, so `t("A" int, "a" int)` is two columns.
  const duckdb::ColumnDefinition* ColumnByName(
    std::string_view name) const noexcept;

  // Whether a NOT NULL constraint covers `column_id`.
  bool IsColumnNotNull(ObjectId column_id) const noexcept;

  // Mutations. Each returns the next version of the definition, throwing a
  // PG-compatible pg::SqlException on a user error (missing column or
  // constraint, duplicate name); a null return is a sanctioned no-op, which is
  // what IF [NOT] EXISTS asks for.
  std::shared_ptr<CreateTableInfo> RenameColumn(
    std::string_view old_name, std::string_view new_name) const;
  std::shared_ptr<CreateTableInfo> RenameConstraint(
    std::string_view old_name, std::string_view new_name) const;
  std::shared_ptr<CreateTableInfo> DropConstraint(std::string_view name,
                                                  bool missing_ok) const;
  std::shared_ptr<CreateTableInfo> DropConstraint(ObjectId constraint_id) const;
  // `constraint_id` names the implied NOT NULL; see PrimaryKeyIds for why every
  // mutation takes its ids rather than allocating them.
  std::shared_ptr<CreateTableInfo> SetNotNull(std::string_view column_name,
                                              ObjectId constraint_id) const;
  std::shared_ptr<CreateTableInfo> DropNotNull(
    std::string_view column_name) const;
  // expr == nullptr drops the default; otherwise sets it.
  std::shared_ptr<CreateTableInfo> SetDefault(
    std::string_view column_name,
    duckdb::unique_ptr<duckdb::ParsedExpression> expr) const;
  std::shared_ptr<CreateTableInfo> DropColumnDefault(ObjectId column_id) const;
  // Appends a CHECK; the name is uniquified against the existing constraints.
  std::shared_ptr<CreateTableInfo> AddCheckConstraint(
    std::string name, duckdb::unique_ptr<duckdb::ParsedExpression> expr,
    ObjectId constraint_id) const;
  // Sets the primary key to `pk_columns` (by id) and adds the implied NOT NULL
  // for each key column. Throws if a PK already exists (a table can have only
  // one). `ids.not_null_ids` must hold one id per entry of `pk_columns`. An
  // empty `name` takes PG's own, which is fixed here rather than derived later:
  // the info is the durable record, and postgres does not move a constraint's
  // name when the table is renamed either.
  std::shared_ptr<CreateTableInfo> AddPrimaryKey(
    std::span<const ObjectId> pk_columns, std::string name,
    const PrimaryKeyIds& ids) const;
  std::shared_ptr<CreateTableInfo> AddUniqueConstraint(
    std::span<const ObjectId> columns, std::string name, ObjectId constraint_id,
    ObjectId index_id) const;
  // `column` must carry its host id; the catalog allocates one before the
  // mutation for the same reason PrimaryKeyIds exists.
  std::shared_ptr<CreateTableInfo> AddColumn(duckdb::ColumnDefinition column,
                                             bool if_not_exists) const;
  std::shared_ptr<CreateTableInfo> DropColumn(ObjectId column_id) const;
  std::shared_ptr<CreateTableInfo> ChangeColumnType(
    std::string_view column_name, duckdb::LogicalType new_type) const;
  std::shared_ptr<CreateTableInfo> ChangeColumnAcl(
    std::string_view column_name, absl::FunctionRef<void(Acl&)> mutate) const;
  std::shared_ptr<CreateTableInfo> SetComment(std::string_view comment) const;
  std::shared_ptr<CreateTableInfo> SetColumnComment(
    std::string_view column_name, std::string_view comment) const;
  std::shared_ptr<CreateTableInfo> DropForeignKeysReferencing(
    ObjectId referenced_table) const;

  // A copy of this version, ready to be edited into the next one. Shares the
  // runtime -- same table, same rows.
  std::shared_ptr<CreateTableInfo> Clone() const;

 private:
  ColumnAcls _column_acls;
};

// One column's grants out of a relation's map, which a reader resolves once
// and then indexes -- almost every table has no map at all.
inline AclView ColumnAclOf(const CreateTableInfo::ColumnAcls* acls,
                           ObjectId column_id) noexcept {
  if (acls == nullptr) {
    return {};
  }
  const auto it = acls->find(column_id);
  return it == acls->end() ? AclView{} : AclView{it->second};
}

// What a DROP of the referenced object does to a table that names it: the four
// definition rewrites a surviving table can need, which is the one thing
// duckdb's dependency flags have no way to say. Derived from the info, never
// recorded.
enum class TableRefKind : uint8_t {
  ColumnType,     // DropColumn
  ColumnDefault,  // DropColumnDefault
  Check,          // DropConstraint
  ForeignKey,     // DropForeignKeysReferencing
};

// One object a table's definition names, and the column or constraint of that
// table which names it.
struct TableReference {
  ObjectId referenced;
  ObjectId sub_id;
  TableRefKind kind{TableRefKind::ColumnType};
};

using TableInfoRef = std::shared_ptr<const CreateTableInfo>;
// A table beside the owner and ACL of the entry holding it. The entry is their
// one home, so a reader wanting both -- the pg_catalog projections, the
// checkpoint writer -- takes them side by side.
using HeldTable = std::pair<TableInfoRef, Permissions>;

// The primary key of `info`, or null when it declares none.
const duckdb::UniqueConstraint* TablePrimaryKey(
  const duckdb::CreateTableInfo& info) noexcept;

// The referenced key of `fk`, as the referenced relation spells it now.
// `referenced` is that relation's definition, or null when it is not reachable
// -- an unresolvable reference falls back to the names the constraint wrote
// down, which is all there is left of the key.
//
// The identities are what the key is resolved through: a RENAME COLUMN on the
// referenced side never reaches the referencing table's own definition, so the
// names in it are only what they were when it was written.
duckdb::vector<duckdb::Identifier> ReferencedKeyNames(
  const duckdb::ForeignKeyConstraint& fk, const CreateTableInfo* referenced);

}  // namespace sdb::catalog
