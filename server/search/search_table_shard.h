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

#include <filesystem>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/directory.hpp>
#include <memory>

#include "catalog/identifiers/object_id.h"
#include "storage_engine/table_shard.h"

namespace sdb::search {

// Table shard backed by an iresearch columnstore (StorageKind::kSearch).
// Created by MakeTableShard(kSearch, ...) and torn down via the static
// TableShard::DropArtifacts(kSearch, ...) helper. The directory layout is
// derived from (db_id, schema_id, table_id, shard_id); see GetPath.
//
// PR 2.2 scope: lifecycle only -- the iresearch writer is created (and an
// empty commit is forced on first-create) but no rows are ever written
// through this class in M2. INSERT / SELECT / UPDATE / DELETE / TRUNCATE
// land in M3-M6 as parallel physical operators.
class SearchTableShard final : public TableShard {
 public:
  // CREATE TABLE path: creates the iresearch directory and an empty writer.
  SearchTableShard(ObjectId db_id, ObjectId schema_id, ObjectId table_id,
                   const catalog::TableStats& stats);

  // Recovery / restore path: opens an existing iresearch directory.
  SearchTableShard(ObjectId db_id, ObjectId schema_id, ObjectId table_id,
                   ObjectId shard_id, const catalog::TableStats& stats);

  // Lets the iresearch writer flush/close cleanly. The shared_ptr's normal
  // destruction order would do this anyway, but binding it to the dtor here
  // documents the lifecycle contract: drop_task's DropArtifacts assumes
  // the on-disk state is consistent before it wipes the directory.
  ~SearchTableShard() override;

  // Directory layout: <search persisted_path for db>/<schema_id>/<table_id>/.
  // No shard_id component: the current arch is single-shard-per-table, and
  // putting shard_id in the path would split create-time and recovery-time
  // paths (the new-shard ctor runs before RegisterObject has stamped an id,
  // so GetId() == 0 then but non-zero on recovery -- a divergence the drop
  // path can't detect after the shard is gone).
  static std::filesystem::path GetPath(ObjectId db_id, ObjectId schema_id,
                                       ObjectId table_id);

 private:
  void OpenWriter();

  ObjectId _db_id;
  ObjectId _schema_id;
  bool _is_new;
  std::unique_ptr<irs::Directory> _dir;
  std::shared_ptr<irs::IndexWriter> _writer;
};

}  // namespace sdb::search
