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

#include <cstddef>
#include <cstdint>
#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/parser/parsed_expression.hpp>
#include <memory>
#include <span>
#include <string>
#include <variant>
#include <vector>

#include "catalog/identifiers/object_id.h"

namespace duckdb {

class MemoryStream;

}  // namespace duckdb
namespace sdb::catalog {

class CreateTableInfo;
using TableInfoRef = std::shared_ptr<const CreateTableInfo>;
class CreateIndexInfoBase;
using IndexInfoRef = std::shared_ptr<const CreateIndexInfoBase>;

struct StoreIndexDef {
  enum class Kind : uint8_t {
    // Native ART index on the store table (btree/secondary in PG terms).
    Plain,
    // Inverted-index linkage: store-side BoundIndex feeding iresearch.
    Inverted,
  };

  ObjectId table_id;
  ObjectId index_id;
  // What the index is called, which is the name its catalog entry carries: the
  // physical index mirrors the catalog rather than living under a name of its
  // own, so an error about it already reads right and a rename has to reach it.
  std::string name;
  // Plain (ART): per-key SQL rendered in order, ready to drop into the index
  // key list -- a quoted column identifier, or a parenthesized expression such
  // as "(j + k)". Empty for inverted indexes.
  std::vector<std::string> keys;
  Kind kind = Kind::Inverted;
  bool unique = false;
  // Inverted only: the caller publishes the index into the live list itself
  // (online CREATE INDEX does it under the store table's checkpoint lock).
  bool defer_injection = false;
};

// One struct per store-DDL operation, each carrying only its own fields --
// same shape as wal::Entry, for the same reason. A single record with a `Kind`
// and the union of everyone's fields made every op carry everyone else's, and
// pushed AddPrimaryKey/AddUnique into borrowing a parameter bag.
//
// The relation is the id the catalog gave it, never its name: a rename cannot
// reach the id, and it is what the store resolves the target by.
namespace store_op {

struct CreateTable {
  ObjectId table_id;
};

struct DropTable {
  ObjectId table_id;
};

struct AddColumn {
  ObjectId table_id;
  std::string column;
  std::string type_sql;
  // Backfills existing rows; empty for no default.
  std::string default_sql;
  duckdb::CompressionType compression =
    duckdb::CompressionType::COMPRESSION_AUTO;
};

struct DropColumn {
  ObjectId table_id;
  std::string column;
  // The catalog id behind `column`. The store table is keyed by name now, but
  // an injection running mid-batch still has to recognise the column its own
  // statement is taking away -- see DataStore::IsColumnDropInFlight.
  ObjectId column_id;
};

struct RenameColumn {
  ObjectId table_id;
  std::string column;
  std::string new_name;
};

struct ChangeColumnType {
  ObjectId table_id;
  std::string column;
  std::string type_sql;
  // The USING cast; empty for the implicit one. This is the field the resulting
  // definition cannot carry and a replay cannot infer: the new type says where
  // the column lands, the USING expression is how the data gets there.
  std::string using_sql;
};

struct AddNotNull {
  ObjectId table_id;
  std::string column;
};

struct DropNotNull {
  ObjectId table_id;
  std::string column;
};

// CHECKs are identified by expression text: no SQL spells "the CHECK with this
// body", so both directions carry it.
struct AddCheck {
  ObjectId table_id;
  std::string expr;
};

struct DropCheck {
  ObjectId table_id;
  std::string expr;
};

struct AddPrimaryKey {
  ObjectId table_id;
  std::string constraint;
  std::vector<std::string> columns;
};

struct AddUnique {
  ObjectId table_id;
  std::string constraint;
  std::vector<std::string> columns;
};

// Inverted defs carry the catalog objects so the executor can build the
// injected bound index; ART defs run as store-side SQL. The two objects are not
// serialized -- a replayed op takes them from the catalog records of the same
// frame, which describe the very same versions.
struct CreateIndex {
  StoreIndexDef def;
  TableInfoRef table;
  IndexInfoRef index;
};

struct DropIndex {
  StoreIndexDef def;
};

// The physical index is filed under the catalog name, so ALTER INDEX ... RENAME
// has to move it -- the same reason RenameColumn exists.
struct RenameIndex {
  ObjectId table_id;
  ObjectId index_id;
  std::string from;
  std::string to;
};

using Op =
  std::variant<CreateTable, DropTable, AddColumn, DropColumn, RenameColumn,
               ChangeColumnType, AddNotNull, DropNotNull, AddCheck, DropCheck,
               AddPrimaryKey, AddUnique, CreateIndex, DropIndex, RenameIndex>;

// A store op together with the database whose attachment runs it. Rows live in
// each database's own duckdb file, so the target is part of the op rather than
// a property of the store.
struct Targeted {
  ObjectId database_id;
  Op op;
};

// Drops something that exists, so the catalog append is the ack point and the
// data work follows it. Everything else creates or reshapes.
bool IsDestructive(const Op& op) noexcept;

// The store half of a catalog batch, as it goes into and comes out of the
// catalog log. Invariant 3b: every record reconstructs into its store
// operation, so the ops travel with the records that describe their result and
// a database whose committed position is behind replays them verbatim.
void SerializeOps(std::span<const Targeted> ops, duckdb::MemoryStream& stream);
// The batch's database is written once, on the record that carries the ops.
std::vector<Targeted> DeserializeOps(ObjectId database_id,
                                     duckdb::MemoryStream& stream);

}  // namespace store_op
}  // namespace sdb::catalog
