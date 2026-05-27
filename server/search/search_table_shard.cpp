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

#include "search/search_table_shard.h"

#include <absl/strings/str_cat.h>

#include <iresearch/formats/formats.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <system_error>

#include "basics/exceptions.h"
#include "query/duckdb_engine.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

std::filesystem::path SearchTableShard::GetPath(ObjectId db_id,
                                                ObjectId schema_id,
                                                ObjectId table_id) {
  SDB_ASSERT(db_id.isSet());
  SDB_ASSERT(schema_id.isSet());
  SDB_ASSERT(table_id.isSet());
  auto path = GetSearchEngine().GetPersistedPath(db_id);
  path /= absl::StrCat(schema_id);
  path /= absl::StrCat(table_id);
  return path;
}

SearchTableShard::SearchTableShard(ObjectId db_id, ObjectId schema_id,
                                   ObjectId table_id,
                                   const catalog::TableStats& stats)
  : TableShard{table_id, stats},
    _db_id{db_id},
    _schema_id{schema_id},
    _is_new{true} {
  _storage = catalog::StorageKind::kSearch;
  OpenWriter();
}

SearchTableShard::SearchTableShard(ObjectId db_id, ObjectId schema_id,
                                   ObjectId table_id, ObjectId shard_id,
                                   const catalog::TableStats& stats)
  : TableShard{shard_id, table_id, stats},
    _db_id{db_id},
    _schema_id{schema_id},
    _is_new{false} {
  _storage = catalog::StorageKind::kSearch;
  OpenWriter();
}

SearchTableShard::~SearchTableShard() {
  // Drop the writer first so iresearch can flush in-flight state and
  // release fds before the directory unique_ptr unwinds. This matches the
  // contract DropArtifacts relies on: by the time it wipes the directory
  // the writer must already have closed.
  _writer.reset();
  _dir.reset();
}

void SearchTableShard::OpenWriter() {
  auto path = GetPath(_db_id, _schema_id, GetTableId());

  std::error_code ec;
  bool path_exists = std::filesystem::exists(path, ec);
  if (ec) {
    SDB_THROW(ERROR_INTERNAL, "Failed to check existence of path '",
              path.string(),
              "' while initializing search table shard for table ",
              GetTableId().id(), ": ", ec.message());
  }
  if (!path_exists) {
    std::filesystem::create_directories(path, ec);
    if (ec) {
      SDB_THROW(ERROR_INTERNAL, "Failed to create directory '", path.string(),
                "' while initializing search table shard for table ",
                GetTableId().id(), ": ", ec.message());
    }
  }

  auto codec = irs::formats::Get("1_5simd");
  const auto open_mode =
    path_exists ? (irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate)
                : irs::OpenMode::kOmCreate;

  irs::ResourceManagementOptions resource_manager;
  resource_manager.cached_columns =
    &GetSearchEngine().getCachedColumnsManager();
  _dir = std::make_unique<irs::MMapDirectory>(path, irs::DirectoryAttributes{},
                                              resource_manager);

  irs::IndexWriterOptions writer_options;
  writer_options.segment_memory_max = 256 * (size_t{1} << 20);  // 256MB
  writer_options.lock_repository = false;  // RocksDB has its own lock
  writer_options.db = query::DuckDBEngine::Instance().GetDB().instance.get();
  writer_options.reader_options.db = writer_options.db;

  _writer = irs::IndexWriter::Make(*_dir, codec, open_mode, writer_options);

  if (_is_new) {
    // Force a commit so the directory contains a valid empty index --
    // otherwise a crash between CREATE TABLE and the first INSERT would
    // leave a half-initialised iresearch dir.
    _writer->RefreshCommit();
  }
}

}  // namespace sdb::search
