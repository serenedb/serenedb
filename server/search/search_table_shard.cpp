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

#include <absl/base/internal/endian.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/file_system.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <system_error>

#include "basics/duckdb_engine.h"
#include "basics/exceptions.h"
#include "storage_engine/search_engine.h"

namespace sdb::search {

std::filesystem::path SearchTableShard::GetPath(ObjectId db_id,
                                                ObjectId table_id) {
  SDB_ASSERT(db_id.isSet());
  SDB_ASSERT(table_id.isSet());
  auto path = GetSearchEngine().GetPersistedPath(db_id);
  path /= absl::StrCat(table_id);
  return path;
}

std::filesystem::path SearchTableShard::GetWalPath(ObjectId db_id) {
  SDB_ASSERT(db_id.isSet());
  // One `wal/` tree per database under the per-db engine root (WAL_DESIGN.md
  // §4.0): the central commit log + a per-shard chunks/ subtree.
  auto path = GetSearchEngine().GetPersistedPath(db_id);
  path /= "wal";
  return path;
}

std::filesystem::path SearchTableShard::GetChunkDir(ObjectId db_id,
                                                    ObjectId table_id) {
  SDB_ASSERT(table_id.isSet());
  // Must match SearchDbWal's internal chunk path (keyed by the uint64_t id):
  // chunks/<table_id>/. Use the numeric id form on both sides.
  auto path = GetWalPath(db_id);
  path /= "chunks";
  path /= std::to_string(table_id.id());
  return path;
}

Result SearchTableShard::DropArtifacts(ObjectId db_id, ObjectId table_id) {
  // remove_all returns 0 and leaves ec clear when the path doesn't exist --
  // treat that as success (idempotent drop, also covers the create-then-
  // rollback path where the dir may never have been made).
  auto path = GetPath(db_id, table_id);
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    return Result{ERROR_INTERNAL,
                  "Failed to remove search table shard directory '" +
                    path.string() + "': " + ec.message()};
  }
  // Wipe THIS shard's bulk chunk subtree (WAL_DESIGN.md §4.0). The central
  // commit log is per-DATABASE (shared), so it is NOT removed here -- the
  // dropped shard's central sections become orphans (skipped on recovery via
  // the catalog, GC'd with their segment).
  auto chunk_dir = GetChunkDir(db_id, table_id);
  std::filesystem::remove_all(chunk_dir, ec);
  if (ec) {
    return Result{ERROR_INTERNAL,
                  "Failed to remove search table chunk directory '" +
                    chunk_dir.string() + "': " + ec.message()};
  }
  // Deregister from the db WAL's flush-subscription so the dropped shard's
  // frozen committed tick can't pin GC (WAL_DESIGN.md §10.3).
  GetSearchEngine().GetDbWal(db_id).DeregisterShard(table_id);
  return {};
}

SearchTableShard::SearchTableShard(ObjectId db_id, ObjectId table_id,
                                   const catalog::TableStats& stats)
  : TableShard{table_id, stats}, _db_id{db_id}, _is_new{true} {
  _storage = catalog::StorageKind::kSearch;
  OpenWriter();
}

SearchTableShard::SearchTableShard(ObjectId db_id, ObjectId table_id,
                                   ObjectId shard_id,
                                   const catalog::TableStats& stats)
  : TableShard{shard_id, table_id, stats}, _db_id{db_id}, _is_new{false} {
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
  auto path = GetPath(_db_id, GetTableId());

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
  _dir = std::make_unique<irs::MMapDirectory>(path, irs::DirectoryAttributes{},
                                              resource_manager);

  irs::IndexWriterOptions writer_options;
  writer_options.segment_memory_max = 256 * (size_t{1} << 20);  // 256MB
  writer_options.lock_repository = false;  // RocksDB has its own lock
  writer_options.db = &sdb::DuckDBEngine::Instance().instance();
  writer_options.reader_options.db = writer_options.db;

  // Persist this shard's last-committed iresearch tick in the commit meta
  // payload so it survives restart (mirrors InvertedIndexShard). iresearch
  // invokes this at each RefreshCommit with the max committed tick -- for us
  // the WAL tick passed to trx.Commit(tick). The WAL GC (Commit) and the
  // recovery skip read it back from the snapshot's index_meta payload (§8).
  writer_options.meta_payload_provider = [this](uint64_t tick,
                                                irs::bstring& out) {
    _last_committed_tick = std::max(_last_committed_tick, tick);
    uint64_t tick_be = absl::big_endian::FromHost(_last_committed_tick);
    out.append(reinterpret_cast<const irs::byte_type*>(&tick_be),
               sizeof(tick_be));
    return true;
  };

  _writer = irs::IndexWriter::Make(*_dir, codec, open_mode, writer_options);

  if (path_exists) {
    // Restore the durable commit tick from the last commit's meta payload.
    auto reader = _writer->GetSnapshot();
    auto payload = irs::GetPayload(reader.Meta().index_meta);
    if (payload.size() >= sizeof(uint64_t)) {
      _last_committed_tick = absl::big_endian::Load64(payload.data());
    }
  }

  // The database's shared search WAL (WAL_DESIGN.md), owned by the search
  // engine (one per db, lazily created). Register this shard in the WAL's
  // flush-subscription with its durable iresearch tick; that also continues the
  // engine-global tick line past it so the next commit's tick is strictly
  // greater (iresearch monotonicity). Replay of records not yet published into
  // iresearch is wired in the recovery pass (later phase).
  _wal = &GetSearchEngine().GetDbWal(_db_id);
  _wal->RegisterShard(GetTableId(), _last_committed_tick);

  if (_is_new) {
    // Force a commit so the directory contains a valid empty index --
    // otherwise a crash between CREATE TABLE and the first INSERT would
    // leave a half-initialised iresearch dir.
    _writer->RefreshCommit();
  }
}

}  // namespace sdb::search
