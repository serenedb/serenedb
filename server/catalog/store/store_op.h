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

#include <duckdb/parser/parsed_data/parse_info.hpp>
#include <memory>

#include "catalog/identifiers/object_id.h"

namespace duckdb {

class MemoryStream;

}  // namespace duckdb
namespace sdb::catalog {

class CreateTableInfo;
using TableInfoRef = std::shared_ptr<const CreateTableInfo>;
class CreateIndexInfoBase;
using IndexInfoRef = std::shared_ptr<const CreateIndexInfoBase>;

namespace store_op {

// The data half of a catalog frame: the DDL the database holding the rows has
// to run, in the form duckdb itself writes into its own WAL. An AlterInfo
// reshapes the rows, a CreateIndexInfo builds an index over them, a DropInfo
// takes one away. Null is the one thing no statement describes -- building the
// storage of a table whose definition reached this database ahead of its rows.
//
// The relation is the id the catalog gave it, never its name: a rename cannot
// reach the id, and it is what the store resolves the target by.
struct Targeted {
  ObjectId database_id;
  ObjectId relation_id;
  std::shared_ptr<const duckdb::ParseInfo> info;
  // An inverted index is built from the catalog objects rather than by a plan.
  // The two are not serialized -- a replayed op takes them from the catalog
  // records of the same frame, which describe the very same versions.
  TableInfoRef table;
  IndexInfoRef index;
};

// Removes something that exists, so the catalog append is the ack point and the
// data work follows it. Everything else creates or reshapes.
bool IsDestructive(const Targeted& op) noexcept;

void SerializeOp(const Targeted& op, duckdb::MemoryStream& stream);
Targeted DeserializeOp(duckdb::MemoryStream& stream);

}  // namespace store_op
}  // namespace sdb::catalog
