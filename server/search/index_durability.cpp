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

#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/valid_checker.hpp>
#include <iresearch/index/index_meta.hpp>
#include <iresearch/utils/directory_utils.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "utils/file_utils.h"

namespace sdb::search {
namespace {

constexpr duckdb::field_id_t kFieldManifest = 1;

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

void WritePayload(const FileManifest* manifest, duckdb::BinarySerializer& out) {
  out.OnOptionalPropertyBegin(kFieldManifest, "manifest", manifest != nullptr);
  if (manifest) {
    manifest->Write(out);
  }
  out.OnOptionalPropertyEnd(manifest != nullptr);
}

}  // namespace

IndexStamp IndexStamp::Read(duckdb::BinaryDeserializer& in) {
  IndexStamp stamp;
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
  options.meta_payload_writer = [this](const irs::SourcePosition&,
                                       duckdb::BinarySerializer& out) {
    _written_manifest = _manifest ? _manifest() : nullptr;
    WritePayload(_written_manifest.get(), out);
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
  return {std::move(writer), std::move(stamp)};
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

void IndexDurability::Commit(irs::IndexWriter& writer,
                             const irs::ProgressReportCallback& progress) {
  absl::MutexLock lock{&_commit_mutex};
  if (!CanPersist()) {
    return;
  }
  try {
    if (!writer.RefreshBegin({
          .progress = progress,
          .reopen_reader = false,
          .payload_changed = PayloadChanged(),
        })) {
      return;
    }
    _directory_dirty.store(true, std::memory_order_relaxed);
    writer.RefreshFinish();
  } catch (...) {
    _out_of_sync.store(true, std::memory_order_relaxed);
    throw;
  }
}

void IndexDurability::SyncDirectory() {
  if (!_on_disk ||
      !_directory_dirty.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  try {
    utils::file_utils::SyncDirectory(_path);
  } catch (...) {
    _directory_dirty.store(true, std::memory_order_relaxed);
    throw;
  }
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
  if (_out_of_sync.load(std::memory_order_relaxed)) {
    return false;
  }
  const auto store = AttachedDatabaseById(_db_id);
  return !store || (!duckdb::ValidChecker::IsInvalidated(*store) &&
                    !duckdb::ValidChecker::IsInvalidated(store->GetDatabase()));
}

}  // namespace sdb::search
