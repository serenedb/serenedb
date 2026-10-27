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

#include "search/index_durability.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/memory_file_system.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/valid_checker.hpp>
#include <duckdb/storage/block_manager.hpp>
#include <duckdb/storage/storage_lock.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/store/index_files.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <mutex>
#include <tuple>

namespace sdb::search {
namespace {

constexpr duckdb::field_id_t kFieldWalGeneration = 1;
constexpr duckdb::field_id_t kFieldWalOffset = 2;
constexpr duckdb::field_id_t kFieldManifest = 3;
constexpr duckdb::field_id_t kFieldCheckpoint = 4;

std::string MemoryRoot(const duckdb::AttachedDatabase& database,
                       duckdb::idx_t oid) {
  return absl::StrCat(duckdb::MemoryFileSystem::PREFIX, database.oid, "/", oid);
}

std::optional<std::string> StoragePath(
  duckdb::AttachedDatabase& database,
  const std::optional<catalog::StorageDirectory>& parent, duckdb::idx_t oid,
  StorageOpen open) {
  if (database.GetStorageManager().InMemory()) {
    return std::nullopt;
  }
  SDB_ASSERT(parent);
  switch (open) {
    case StorageOpen::Create:
      return parent->CreateStorage(oid);
    case StorageOpen::Open:
      return parent->OpenStorage(oid);
    case StorageOpen::Defer:
      return parent->StoragePath(oid);
  }
  SDB_UNREACHABLE();
}

}  // namespace

StorageOpen StorageOpenOf(duckdb::AttachedDatabase& database, bool is_new) {
  if (is_new) {
    return StorageOpen::Create;
  }
  return database.IsReadOnly() || database.GetStorageManager().IsLoaded()
           ? StorageOpen::Open
           : StorageOpen::Defer;
}

IndexStamp IndexStamp::Read(duckdb::BinaryDeserializer& in) {
  IndexStamp stamp;
  stamp.position.generation =
    in.ReadProperty<uint64_t>(kFieldWalGeneration, "wal_generation");
  stamp.position.offset =
    in.ReadProperty<uint64_t>(kFieldWalOffset, "wal_offset");
  const bool has_manifest =
    in.OnOptionalPropertyBegin(kFieldManifest, "manifest");
  if (has_manifest) {
    stamp.manifest = FileManifest::Read(in);
  }
  in.OnOptionalPropertyEnd(has_manifest);
  stamp.checkpoint = in.ReadPropertyWithExplicitDefault<uint64_t>(
    kFieldCheckpoint, "checkpoint", 0);
  return stamp;
}

IndexDurability::IndexDurability(
  duckdb::AttachedDatabase& database, std::string name,
  const std::optional<catalog::StorageDirectory>& parent, duckdb::idx_t oid,
  StorageOpen open)
  : IndexDurability{database, std::move(name),
                    StoragePath(database, parent, oid, open), oid} {}

IndexDurability::IndexDurability(duckdb::AttachedDatabase& database,
                                 std::string name,
                                 std::optional<std::string> path,
                                 duckdb::idx_t oid)
  : _database{database.shared_from_this()},
    _name{std::move(name)},
    _files{irs::DuckDBEngine::Instance().instance(),
           path ? *path : MemoryRoot(database, oid)},
    _absent{!path && !database.GetStorageManager().InMemory()} {}

bool IndexDurability::Locate(const catalog::StorageDirectory& parent,
                             duckdb::idx_t oid) {
  _absent = !parent.OpenStorage(oid);
  return !_absent;
}

std::pair<std::shared_ptr<irs::IndexWriter>, IndexStamp>
IndexDurability::OpenWriter(bool is_new, irs::IndexWriterOptions options,
                            ManifestSource manifest) {
  options.meta_payload_writer = [this, manifest = std::move(manifest)](
                                  const irs::SourcePosition& position,
                                  duckdb::BinarySerializer& out) {
    const auto current = manifest ? manifest() : nullptr;
    WritePayload(position, current.get(), out);
  };
  IndexStamp stamp;
  options.meta_payload_reader = [&](duckdb::BinaryDeserializer& in) {
    stamp = IndexStamp::Read(in);
  };
  if (is_new || _files.Kind() == irs::StorageKind::Memory) {
    auto writer = irs::IndexWriter::Make(_files, irs::OpenMode::kOmCreate,
                                         std::move(options));
    writer->RefreshCommit();
    return {std::move(writer), std::move(stamp)};
  }
  ResolvePendingSave();
  auto writer = irs::IndexWriter::Make(
    _files, irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate,
    std::move(options));
  _position = stamp.position;
  absl::MutexLock lock{&_positions_mutex};
  _published = stamp.position;
  _durable = stamp.position;
  _staged = stamp.position;
  return {std::move(writer), std::move(stamp)};
}

std::pair<irs::DirectoryReader, IndexStamp> IndexDurability::OpenReader(
  const irs::IndexReaderOptions& options) const {
  IndexStamp stamp;
  irs::DirectoryReader reader{
    _files, options,
    [&](duckdb::BinaryDeserializer& in) { stamp = IndexStamp::Read(in); }};
  return {std::move(reader), std::move(stamp)};
}

void IndexDurability::WritePayload(const irs::SourcePosition& position,
                                   const FileManifest* manifest,
                                   duckdb::BinarySerializer& out) {
  _position = std::max(_position, position);
  out.WriteProperty<uint64_t>(kFieldWalGeneration, "wal_generation",
                              _position.generation);
  out.WriteProperty<uint64_t>(kFieldWalOffset, "wal_offset", _position.offset);
  out.OnOptionalPropertyBegin(kFieldManifest, "manifest", manifest != nullptr);
  if (manifest) {
    manifest->Write(out);
  }
  out.OnOptionalPropertyEnd(manifest != nullptr);
  _pending_checkpoint =
    _checkpoint != 0 ? _checkpoint : RunningCheckpoint(_position.generation);
  out.WritePropertyWithDefault<uint64_t>(kFieldCheckpoint, "checkpoint",
                                         _pending_checkpoint, 0);
}

void IndexDurability::ResolvePendingSave() const {
  const auto pending = irs::index_meta::LastPendingFile(_files);
  if (!pending) {
    return;
  }
  uint64_t checkpoint = 0;
  try {
    irs::index_meta::ReadPayload(_files, *pending,
                                 [&](duckdb::BinaryDeserializer& in) {
                                   checkpoint = IndexStamp::Read(in).checkpoint;
                                 });
  } catch (const irs::SqlException& e) {
    if (e.error().errcode != ERRCODE_INDEX_CORRUPTED) {
      throw;
    }
    checkpoint = 0;
  }
  if (checkpoint != 0) {
    const auto store = duckdb::AttachedDatabase::TryGetReference(_database);
    SDB_ENSURE(store, _name,
               ": the database of a pending checkpoint save is not attached");
    if (checkpoint <=
        store->GetStorageManager().GetBlockManager().GetCheckpointIteration()) {
      const auto committed = irs::index_meta::FileName(
        irs::index_meta::ParsePendingGeneration(*pending));
      _files.Rename(*pending, committed);
      return;
    }
  }
  std::ignore = _files.Remove(*pending);
}

void IndexDurability::MarkStaged(const irs::SourcePosition& position) {
  absl::MutexLock lock{&_positions_mutex};
  _staged = std::max(_staged, position);
}

std::optional<uint64_t> IndexDurability::NeededWAL() const {
  absl::MutexLock lock{&_positions_mutex};
  if (_staged <= _durable) {
    return std::nullopt;
  }
  return _durable.generation;
}

void IndexDurability::MarkPublished() {
  absl::MutexLock lock{&_positions_mutex};
  _published = _position;
}

absl::Status IndexDurability::Refresh(
  irs::IndexWriter& writer, bool wait,
  const irs::ProgressReportCallback& progress, RefreshResult& code,
  absl::FunctionRef<void()> unchanged, absl::FunctionRef<void()> published) {
  code = RefreshResult::NoChanges;

  try {
    std::unique_lock refresh_lock{_refresh_mutex, std::try_to_lock};
    if (!refresh_lock.owns_lock()) {
      if (!wait) {
        SDB_TRACE(SEARCH, "Refresh for ", _name,
                  " is already in progress, skipping");
        code = RefreshResult::InProgress;
        return absl::OkStatus();
      }
      SDB_TRACE(SEARCH, "Refresh for ", _name,
                " is already in progress, waiting");
      refresh_lock.lock();
    }
    if (!FinishUnfinished(writer)) {
      return absl::UnavailableError(
        absl::StrCat(_name, " cannot commit its last refresh yet"));
    }
    if (wait) {
      AwaitPendingSave(refresh_lock, writer);
    } else {
      FinishPendingSave(writer);
    }

    if (!CanPersist()) {
      return absl::OkStatus();
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_index_refresh");
    bool were_changes = false;
    try {
      were_changes = writer.RefreshBegin({
        .progress = progress,
        .reopen_reader = _reopen.exchange(false, std::memory_order_acq_rel),
      });
      if (were_changes) {
        _directory_dirty.store(true, std::memory_order_relaxed);
      }
      if (were_changes && IsOutOfSync()) {
        writer.RefreshAbort();
        _pending_checkpoint = 0;
        return absl::OkStatus();
      }
      if (were_changes && _pending_checkpoint == 0) {
        SDB_PARK_ONCE_ON_FAILURE("pause_index_refresh_begun");
        if (!FinishRefresh(writer)) {
          _unfinished = true;
          return absl::UnavailableError(
            absl::StrCat(_name, " cannot commit its refresh yet"));
        }
      }
    } catch (...) {
      _pending_checkpoint = 0;
      MarkOutOfSync();
      throw;
    }
    if (!were_changes) {
      SDB_TRACE(SEARCH, "Refresh for ", _name, " found no changes");
      unchanged();
      return absl::OkStatus();
    }
    if (_pending_checkpoint != 0) {
      code = RefreshResult::InProgress;
      if (wait) {
        AwaitPendingSave(refresh_lock, writer);
      }
      return absl::OkStatus();
    }
    code = RefreshResult::Done;
    published();
  } catch (const irs::SqlException& e) {
    return absl::InternalError(absl::StrCat(
      "caught exception while refreshing ", _name, ": ", e.message()));
  } catch (const std::exception& e) {
    return absl::InternalError(
      absl::StrCat("caught exception while refreshing ", _name, ": ",
                   duckdb::ErrorData{e}.RawMessage()));
  } catch (...) {
    return absl::InternalError(
      absl::StrCat("caught exception while refreshing ", _name));
  }
  return absl::OkStatus();
}

void IndexDurability::ReplaceSegments(
  irs::IndexWriter& writer, std::span<const std::string_view> replaced,
  std::span<const std::string_view> adopted_metas,
  absl::FunctionRef<bool(irs::IndexWriter::QueryContext::FilterPtr&)>
    removal_provider) {
  std::unique_lock lock{_refresh_mutex};
  RequireFinished(writer);
  AwaitPendingSave(lock, writer);
  writer.ReplaceSegments(replaced, adopted_metas, removal_provider);
}

void IndexDurability::Clear(irs::IndexWriter& writer,
                            const irs::SourcePosition& position) {
  absl::MutexLock lock{&_refresh_mutex};
  SDB_ASSERT(_pending_checkpoint == 0);
  writer.Clear(position);
  MarkPublished();
  _directory_dirty.store(true, std::memory_order_relaxed);
}

void IndexDurability::PrepareCheckpoint(irs::IndexWriter& writer,
                                        uint64_t iteration) {
  std::lock_guard lock{_refresh_mutex};
  SDB_ASSERT(_pending_checkpoint == 0 || _pending_checkpoint == iteration);
  RequireFinished(writer);
  _checkpoint = iteration;
  absl::Cleanup reset = [&]() noexcept { _checkpoint = 0; };
  try {
    if (writer.RefreshBegin()) {
      _directory_dirty.store(true, std::memory_order_relaxed);
    }
  } catch (...) {
    _pending_checkpoint = 0;
    MarkOutOfSync();
    throw;
  }
  SyncDirectory();
  SDB_IF_FAILURE("crash_after_index_checkpoint_prepare") {
    SDB_IMMEDIATE_ABORT();
  }
  if (IsOutOfSync()) {
    if (_pending_checkpoint != 0) {
      writer.RefreshAbort();
      _pending_checkpoint = 0;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG(_name,
                            " is out of sync with its write-ahead log; "
                            "refusing to checkpoint"));
  }
}

bool IndexDurability::FinishCheckpoint(irs::IndexWriter& writer) {
  std::lock_guard lock{_refresh_mutex};
  SDB_IF_FAILURE("crash_before_index_checkpoint_finish") {
    SDB_IMMEDIATE_ABORT();
  }
  return FinishPendingSave(writer);
}

void IndexDurability::FinishCheckpointIfIdle(irs::IndexWriter& writer) {
  std::unique_lock lock{_refresh_mutex, std::try_to_lock};
  if (lock.owns_lock()) {
    FinishPendingSave(writer);
  }
}

bool IndexDurability::FinishRefresh(irs::IndexWriter& writer) {
  try {
    writer.RefreshFinishRetryable();
  } catch (const std::exception& e) {
    SDB_WARN(SEARCH, _name, " cannot commit its refresh, it is retried: ",
             duckdb::ErrorData{e}.RawMessage());
    return false;
  }
  MarkPublished();
  _directory_dirty.store(true, std::memory_order_relaxed);
  return true;
}

bool IndexDurability::FinishUnfinished(irs::IndexWriter& writer) {
  if (!_unfinished) {
    return true;
  }
  if (!FinishRefresh(writer)) {
    return false;
  }
  _unfinished = false;
  return true;
}

void IndexDurability::RequireFinished(irs::IndexWriter& writer) {
  if (!FinishUnfinished(writer)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG(_name, " cannot commit its last refresh"));
  }
}

bool IndexDurability::FinishPendingSave(irs::IndexWriter& writer) {
  if (_pending_checkpoint == 0 || !CheckpointWritten(_pending_checkpoint)) {
    return false;
  }
  if (!FinishRefresh(writer)) {
    return false;
  }
  _pending_checkpoint = 0;
  return true;
}

void IndexDurability::AwaitPendingSave(std::unique_lock<absl::Mutex>& lock,
                                       irs::IndexWriter& writer) {
  if (FinishPendingSave(writer) || _pending_checkpoint == 0 || !CanPersist()) {
    return;
  }
  lock.unlock();
  WaitForCheckpoint();
  lock.lock();
  FinishPendingSave(writer);
}

std::pair<irs::DirectoryReader, irs::SourcePosition> IndexDurability::Snapshot(
  irs::IndexWriter& writer) {
  absl::MutexLock lock{&_refresh_mutex};
  return {writer.GetSnapshot(), _published};
}

void IndexDurability::SyncDirectory() {
  if (!_directory_dirty.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  irs::SourcePosition published;
  {
    absl::MutexLock lock{&_positions_mutex};
    published = _published;
  }
  try {
    _files.SyncDirectory();
  } catch (...) {
    _directory_dirty.store(true, std::memory_order_relaxed);
    throw;
  }
  absl::MutexLock lock{&_positions_mutex};
  _durable = std::max(_durable, published);
}

absl::Status IndexDurability::Cleanup() {
  try {
    SyncDirectory();
    if (_swept.load(std::memory_order_relaxed)) {
      irs::RemoveUnreferenced(_files);
    } else {
      irs::RemoveAllUnreferenced(_files);
      _swept.store(true, std::memory_order_relaxed);
    }
  } catch (const std::exception& e) {
    return absl::InternalError(
      absl::StrCat("caught exception while cleaning up ", _name, ": ",
                   duckdb::ErrorData{e}.RawMessage()));
  } catch (...) {
    return absl::InternalError(
      absl::StrCat("caught exception while cleaning up ", _name));
  }
  return absl::OkStatus();
}

absl::Status IndexDurability::CompactFiles(irs::IndexWriter& writer) {
  try {
    const auto reader = writer.GetSnapshot();
    for (const auto& segment : reader.Meta().index_meta.segments) {
      for (const auto& file : segment.meta.files) {
        _files.Compact(file);
      }
    }
  } catch (const std::exception& e) {
    return absl::InternalError(
      absl::StrCat("caught exception while compacting the files of ", _name,
                   ": ", duckdb::ErrorData{e}.RawMessage()));
  }
  _reopen.store(true, std::memory_order_release);
  return absl::OkStatus();
}

bool IndexDurability::CanPersist() const {
  if (IsOutOfSync()) {
    return false;
  }
  const auto store = duckdb::AttachedDatabase::TryGetReference(_database);
  return !store || (!duckdb::ValidChecker::IsInvalidated(*store) &&
                    !duckdb::ValidChecker::IsInvalidated(store->GetDatabase()));
}

uint64_t IndexDurability::RunningCheckpoint(uint64_t generation) const {
  const auto store = duckdb::AttachedDatabase::TryGetReference(_database);
  if (!store || !store->HasStorageManager()) {
    return 0;
  }
  const auto header =
    store->GetStorageManager().GetBlockManager().GetCheckpointIteration();
  return generation > header ? generation : 0;
}

bool IndexDurability::CheckpointWritten(uint64_t iteration) const {
  const auto store = duckdb::AttachedDatabase::TryGetReference(_database);
  return store && store->HasStorageManager() &&
         store->GetStorageManager()
             .GetBlockManager()
             .GetCheckpointIteration() >= iteration;
}

void IndexDurability::WaitForCheckpoint() const {
  if (const auto store = duckdb::AttachedDatabase::TryGetReference(_database);
      store && store->HasStorageManager()) {
    std::ignore =
      duckdb::DuckTransactionManager::Get(*store).SharedCheckpointLock();
  }
}

}  // namespace sdb::search
