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

#include <string>
#include <string_view>
#include <vector>

#include "basics/containers/node_hash_map.h"
#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/table.h"

namespace sdb::catalog {

// One per external table that needs a column/CHECK mutation, in two steps:
// `info` is everything the cascade does that leaves the store table's shape
// alone -- clearing a DEFAULT, a CHECK, a FOREIGN KEY -- and `reshaped` adds
// the column removals on top of it. Null means the cascade did not reach that
// half; `Final()` is the version that is recorded and published.
struct TableRewrite {
  ObjectId schema_id;
  ObjectId id;
  // The version the cascade started from, for the recording pass: what it drops
  // from the store table is the difference between the two.
  TableInfoRef before;
  Permissions perm;
  TableInfoRef info;
  TableInfoRef reshaped;
  // The indexes on this table the cascade does not take. A column removal
  // reshapes the store table, which the store refuses while an index covers a
  // column past the one going away, so they are dropped and recreated around
  // it.
  std::vector<IndexInfoRef> surviving_indexes;
  // The version CommitDropPlan recorded, published by PublishDropPlan
  // once the batch's store ops have run: the entry hands duckdb the store
  // table's DataTable, and one resolved before the reshape still carries the
  // dropped column -- which the next checkpoint then writes out over a
  // definition that no longer has it.
  TableInfoRef published;

  const TableInfoRef& Final() const noexcept {
    return reshaped ? reshaped : info;
  }
};

// One object as a cascade names it: the plan carries the parent, the name and
// the kind, which is what the drop of its entry and its record both need.
struct EntryDrop {
  ObjectId parent_id;
  ObjectId id;
  std::string name;
  duckdb::CatalogType type;
};

// Cross-tree catalog mutations needed alongside the seed's tombstone.
struct DropPlan {
  containers::NodeHashMap<ObjectId, TableRewrite> table_rewrites;
  // The views and functions the cascade takes, in the order it reached them.
  std::vector<EntryDrop> entry_drops;
  std::vector<IndexInfoRef> index_drops;
  // Indexes that go with the relation they are built on rather than because
  // anything depended on them -- PG's AUTO dependency, so RESTRICT does not
  // block on them. They are listed only when that relation's entry IS the
  // object (an index over a view): a table carries its indexes in its own drop
  // task's subtree, while a view drop is a single record with no subtree, so
  // nothing else would remove them.
  std::vector<IndexInfoRef> owned_index_drops;

  // RESTRICT would have blocked.
  bool IsCascade() const noexcept {
    return !table_rewrites.empty() || !entry_drops.empty() ||
           !index_drops.empty();
  }

  // PG-style RESTRICT DETAIL text.
  std::string FormatDependentsDetail(std::string_view seed_kind,
                                     std::string_view seed_name) const;
};

}  // namespace sdb::catalog
