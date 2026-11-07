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
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/valid_checker.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <mutex>

#include "catalog/database_directory.h"
#include "utils/file_utils.h"

namespace sdb::search {
namespace {

constexpr duckdb::field_id_t kFieldWalGeneration = 1;
constexpr duckdb::field_id_t kFieldWalOffset = 2;
constexpr duckdb::field_id_t kFieldManifest = 3;

std::unique_lock<absl::Mutex> LockRefresh(absl::Mutex& mutex) {
#ifdef SDB_FAULT_INJECTION
  std::unique_lock lock{mutex, std::try_to_lock};
  if (!lock.owns_lock()) {
    SDB_PARK_ONCE_ON_FAILURE("pause_index_refresh_lock_wait");
    lock.lock();
  }
  return lock;
#else
  return std::unique_lock{mutex};
#endif
}

duckdb::shared_ptr<duckdb::AttachedDatabase> AttachedDatabaseById(
  duckdb::idx_t id) {
  auto& manager =
    duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance());
  for (auto& attached : manager.GetDatabases()) {
    if (attached->oid == id) {
      return attached;
    }
  }
  return nullptr;
}

}  // namespace

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
  return stamp;
}

IndexDurability::IndexDurability(duckdb::idx_t db_id, std::string name)
  : _db_id{db_id}, _name{std::move(name)} {}

std::pair<std::shared_ptr<irs::IndexWriter>, IndexStamp>
IndexDurability::OpenWriter(irs::Directory& dir, std::filesystem::path path,
                            bool on_disk, bool is_new,
                            irs::IndexWriterOptions options,
                            ManifestSource manifest) {
  _path = std::move(path);
  _on_disk = on_disk;
  _manifest = std::move(manifest);
  options.meta_payload_writer = [this](const irs::SourcePosition& position,
                                       duckdb::BinarySerializer& out) {
    _written_manifest = _manifest ? _manifest() : nullptr;
    WritePayload(position, _written_manifest.get(), out);
  };
  IndexStamp stamp;
  options.meta_payload_reader = [&](duckdb::BinaryDeserializer& in) {
    stamp = IndexStamp::Read(in);
  };
  if (!on_disk || is_new) {
    auto writer =
      irs::IndexWriter::Make(dir, irs::OpenMode::kOmCreate, std::move(options));
    writer->RefreshCommit();
    return {std::move(writer), std::move(stamp)};
  }
  RemovePendingSave(dir);
  auto writer = irs::IndexWriter::Make(
    dir, irs::OpenMode::kOmAppend | irs::OpenMode::kOmCreate,
    std::move(options));
  _written_manifest = stamp.manifest;
  _position = stamp.position;
  absl::MutexLock lock{&_positions_mutex};
  _published = stamp.position;
  _durable = stamp.position;
  _staged = stamp.position;
  return {std::move(writer), std::move(stamp)};
}

std::pair<irs::DirectoryReader, IndexStamp> IndexDurability::OpenReader(
  irs::Directory& dir, const irs::IndexReaderOptions& options) {
  IndexStamp stamp;
  irs::DirectoryReader reader{
    dir, options,
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
}

void IndexDurability::RemovePendingSave(irs::Directory& dir) const {
  std::string pending;
  if (!irs::index_meta::LastPendingFile(dir, pending)) {
    return;
  }
  if (!dir.remove(pending)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("failed to remove '", pending, "'"));
  }
}

void IndexDurability::MarkStaged(const irs::SourcePosition& position) {
  absl::MutexLock lock{&_positions_mutex};
  _staged = std::max(_staged, position);
}

void IndexDurability::MarkPublished() {
  absl::MutexLock lock{&_positions_mutex};
  _published = _position;
}

absl::Status IndexDurability::Refresh(
  irs::IndexWriter& writer, bool wait,
  const irs::ProgressReportCallback& progress, RefreshResult& code,
  absl::FunctionRef<void()> unchanged, absl::FunctionRef<void()> published,
  const RefreshGate* gate) {
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

    if (!CanPersist()) {
      return absl::OkStatus();
    }
    SDB_PARK_ONCE_ON_FAILURE("pause_index_refresh");
    bool were_changes = false;
    try {
      std::optional<absl::MutexLock> gate_lock;
      if (gate) {
        gate_lock.emplace(&gate->mutex);
        if (!gate->open()) {
          return absl::OkStatus();
        }
      }
      were_changes = writer.RefreshBegin({
        .progress = progress,
        .reopen_reader = /* TODO(codeworse) */ false,
        .payload_changed = PayloadChanged(),
      });
      gate_lock.reset();
      if (were_changes) {
        _directory_dirty.store(true, std::memory_order_relaxed);
      }
      if (were_changes && IsOutOfSync()) {
        writer.RefreshAbort();
        return absl::OkStatus();
      }
      if (were_changes) {
        SDB_PARK_ONCE_ON_FAILURE("pause_index_refresh_begun");
        writer.RefreshFinish();
        MarkPublished();
        _directory_dirty.store(true, std::memory_order_relaxed);
      }
    } catch (...) {
      MarkOutOfSync();
      throw;
    }
    if (!were_changes) {
      SDB_TRACE(SEARCH, "Refresh for ", _name, " found no changes");
      unchanged();
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

void IndexDurability::Clear(irs::IndexWriter& writer,
                            const irs::SourcePosition& position) {
  const auto lock = LockRefresh(_refresh_mutex);
  writer.Clear(position);
  MarkPublished();
  _directory_dirty.store(true, std::memory_order_relaxed);
}

void IndexDurability::SyncDirectory() {
  if (!_on_disk ||
      !_directory_dirty.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  irs::SourcePosition published;
  {
    absl::MutexLock lock{&_positions_mutex};
    published = _published;
  }
  try {
    utils::file_utils::SyncDirectory(_path);
  } catch (...) {
    _directory_dirty.store(true, std::memory_order_relaxed);
    throw;
  }
  absl::MutexLock lock{&_positions_mutex};
  _durable = std::max(_durable, published);
}

absl::Status IndexDurability::Cleanup(irs::Directory& dir) {
  try {
    SyncDirectory();
    irs::directory_utils::RemoveAllUnreferenced(dir);
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

bool IndexDurability::PayloadChanged() const {
  return _manifest && _manifest() != _written_manifest;
}

bool IndexDurability::CanPersist() const {
  if (IsOutOfSync()) {
    return false;
  }
  const auto store = AttachedDatabaseById(_db_id);
  return !store || (!duckdb::ValidChecker::IsInvalidated(*store) &&
                    !duckdb::ValidChecker::IsInvalidated(store->GetDatabase()));
}

}  // namespace sdb::search
