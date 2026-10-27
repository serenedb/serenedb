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

#include "iresearch/store/directory.hpp"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/open_file_info.hpp>
#include <duckdb/common/path.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>

#include "iresearch/store/file_output.hpp"
#include "iresearch/utils/log.hpp"

namespace irs {
namespace {

duckdb::FileSystem& FileSystemOf(duckdb::DatabaseInstance& db,
                                 std::string_view root) {
  if (StorageKindOf(root) == StorageKind::Memory) {
    return db.GetMemoryFileSystem();
  }
  return duckdb::FileSystem::GetFileSystem(db);
}

std::string RootPath(std::string_view root) {
  if (root.ends_with('/')) {
    return std::string{root};
  }
  return absl::StrCat(root, "/");
}

}  // namespace

StorageKind StorageKindOf(std::string_view location) {
  if (location.starts_with(duckdb::MemoryFileSystem::PREFIX)) {
    return StorageKind::Memory;
  }
  if (duckdb::Path::FromString(location).IsRemote()) {
    return StorageKind::Remote;
  }
  return StorageKind::Local;
}

Directory::State::~State() {
  if (kind != StorageKind::Memory) {
    return;
  }
  try {
    fs.RemoveDirectory(root);
  } catch (const std::exception& e) {
    SDB_ERROR(IRESEARCH, "Failed to remove the in-memory index '", root,
              "': ", duckdb::ErrorData{e}.RawMessage());
  }
}

Directory::Directory(duckdb::DatabaseInstance& db, std::string_view root)
  : Directory{db, FileSystemOf(db, root), root} {}

Directory::Directory(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
                     std::string_view root)
  : _state{
      std::make_shared<State>(db, fs, RootPath(root), StorageKindOf(root))} {}

std::string Directory::Path(std::string_view name) const {
  return absl::StrCat(_state->root, name);
}

Directory Directory::Observed(Observer& observer) const {
  auto result = *this;
  result._observers.push_back(&observer);
  return result;
}

Directory Directory::Unobserved() const {
  auto result = *this;
  result._observers.clear();
  return result;
}

duckdb::BlockCache& Directory::Cache() const {
  return duckdb::BlockCache::Get(_state->db);
}

IndexOutput::ptr Directory::Create(std::string_view name) const {
  return Create(name, true);
}

IndexOutput::ptr Directory::CreateWhole(std::string_view name) const {
  return Create(name, false);
}

IndexOutput::ptr Directory::Create(std::string_view name, bool cached) const {
  const auto path = Path(name);
  for (auto* observer : _observers) {
    observer->Created(*this, name);
  }
  absl::Cleanup abandon = [&] {
    for (auto* observer : _observers) {
      observer->Removed(*this, name);
    }
  };
  const bool remote = Kind() == StorageKind::Remote;
  duckdb::BlockCache* cache = nullptr;
  if (remote) {
    auto& blocks = Cache();
    blocks.ForgetFile(path);
    if (cached) {
      cache = &blocks;
    }
  }
  auto output = FileOutput::Open(_state->fs, path, _observers, remote, cache);
  std::move(abandon).Cancel();
  return output;
}

std::unique_ptr<InputFile> Directory::Open(
  std::string_view name, duckdb::MappedFileAccess access) const {
  const auto path = Path(name);
  if (Kind() == StorageKind::Remote) {
    return OpenBlocks(path);
  }
  if (Kind() == StorageKind::Memory) {
    if (auto blocks = _state->fs.GetBlockFile(path)) {
      return std::make_unique<InputFile>(Cache(), std::move(blocks));
    }
  }
  return OpenMapped(path, access);
}

bstring Directory::ReadWhole(std::string_view name) const {
  duckdb::OpenFileInfo file{Path(name)};
  if (Kind() == StorageKind::Remote) {
    file.extended_info =
      duckdb::make_shared_ptr<duckdb::ExtendedOpenFileInfo>();
    file.extended_info->options.emplace("force_full_download",
                                        duckdb::Value::BOOLEAN(true));
  }
  auto handle = _state->fs.OpenFile(file, duckdb::FileFlags::FILE_FLAGS_READ);
  bstring bytes(duckdb::NumericCast<size_t>(handle->GetFileSize()),
                byte_type{0});
  if (!bytes.empty()) {
    handle->Read(duckdb::QueryContext{}, bytes.data(), bytes.size(), 0);
  }
  return bytes;
}

std::unique_ptr<InputFile> Directory::OpenMapped(
  const std::string& path, duckdb::MappedFileAccess access) const {
  auto mapping = _state->fs.MemoryMapFile(duckdb::OpenFileInfo{path},
                                          duckdb::FileFlags::FILE_FLAGS_READ,
                                          duckdb::MMapOptions{});
  if (access != duckdb::MappedFileAccess::NORMAL) {
    mapping->Advise(access);
  }
  return std::make_unique<InputFile>(std::move(mapping));
}

std::unique_ptr<InputFile> Directory::OpenBlocks(
  const std::string& path) const {
  auto& cache = Cache();
  if (duckdb::BlockCacheFileInfo known; cache.TryGetFile(path, known)) {
    return std::make_unique<InputFile>(
      cache, cache.OpenFile(path, known, false, nullptr));
  }
  duckdb::shared_ptr<duckdb::FileHandle> handle =
    _state->fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ |
                                duckdb::FileFlags::FILE_FLAGS_PARALLEL_ACCESS);
  duckdb::BlockCacheFileInfo info;
  info.validation.file_size =
    duckdb::NumericCast<duckdb::idx_t>(handle->GetFileSize());
  info.can_seek = handle->CanSeek();
  info.on_disk = handle->OnDiskFile();
  return std::make_unique<InputFile>(
    cache, cache.OpenFile(path, info, false, std::move(handle)));
}

bool Directory::Has(std::string_view name) const {
  return _state->fs.FileExists(Path(name));
}

bool Directory::Remove(std::string_view name) const {
  const auto path = Path(name);
  if (!_state->fs.TryRemoveFile(path)) {
    return false;
  }
  if (Kind() == StorageKind::Remote) {
    Cache().ForgetFile(path);
  }
  for (auto* observer : _observers) {
    observer->Removed(*this, name);
  }
  return true;
}

size_t Directory::Remove(std::span<const std::string> names) const {
  if (names.empty()) {
    return 0;
  }
  duckdb::vector<std::string> paths;
  paths.reserve(names.size());
  for (const auto& name : names) {
    paths.push_back(Path(name));
  }
  _state->fs.RemoveFiles(paths);
  if (Kind() == StorageKind::Remote) {
    auto& cache = Cache();
    for (const auto& path : paths) {
      cache.ForgetFile(path);
    }
  }
  for (const auto& name : names) {
    for (auto* observer : _observers) {
      observer->Removed(*this, name);
    }
  }
  return names.size();
}

void Directory::Rename(std::string_view src, std::string_view dst) const {
  const auto source = Path(src);
  const auto target = Path(dst);
  _state->fs.MoveFile(source, target);
  if (Kind() == StorageKind::Remote) {
    auto& cache = Cache();
    cache.ForgetFile(source);
    cache.ForgetFile(target);
  }
  for (auto* observer : _observers) {
    observer->Renamed(*this, src, dst);
  }
}

void Directory::Sync(std::span<const std::string_view> names) const {
  if (Kind() != StorageKind::Local) {
    return;
  }
  duckdb::TaskScheduler::BlockingScope blocking;
  for (const auto name : names) {
    auto handle =
      _state->fs.OpenFile(Path(name), duckdb::FileFlags::FILE_FLAGS_READ);
    handle->Sync();
  }
}

void Directory::SyncDirectory() const {
  duckdb::TaskScheduler::BlockingScope blocking;
  _state->fs.SyncDirectory(_state->root);
}

void Directory::Compact(std::string_view name) const {
  if (Kind() == StorageKind::Memory) {
    _state->fs.Compact(Path(name));
  }
}

void Directory::Visit(absl::FunctionRef<void(std::string_view)> visitor) const {
  _state->fs.ListFiles(
    _state->root, [&](const std::string& name, bool is_directory) {
      if (!is_directory && name.find('/') == std::string::npos) {
        visitor(name);
      }
    });
}

void ForEach(const Directory& dir, size_t count,
             absl::FunctionRef<void(size_t)> visit) {
  if (dir.Kind() != StorageKind::Remote || count < 2) {
    for (size_t i = 0; i != count; ++i) {
      visit(i);
    }
    return;
  }
  duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(dir.Db()),
                                duckdb::TaskSchedulerType::ASYNC};
  for (size_t i = 0; i != count; ++i) {
    executor.ScheduleTask(duckdb::make_uniq<duckdb::FunctionExecutorTask>(
      executor, [visit, i] { visit(i); }, "DirectoryForEach"));
  }
  executor.WorkOnTasks();
}

}  // namespace irs
