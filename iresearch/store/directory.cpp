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

#include <atomic>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/numeric_utils.hpp>
#include <duckdb/common/open_file_info.hpp>
#include <duckdb/common/path.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <exception>

#include "iresearch/store/block_input.hpp"
#include "iresearch/store/file_output.hpp"
#include "iresearch/store/mapped_input.hpp"
#include "iresearch/utils/log.hpp"

namespace irs {
namespace {

StorageKind KindOf(std::string_view root) {
  if (root.starts_with(duckdb::MemoryFileSystem::PREFIX)) {
    return StorageKind::Memory;
  }
  if (duckdb::Path::FromString(std::string{root}).IsRemote()) {
    return StorageKind::Remote;
  }
  return StorageKind::Local;
}

duckdb::FileSystem& FileSystemOf(duckdb::DatabaseInstance& db,
                                 std::string_view root) {
  if (KindOf(root) == StorageKind::Memory) {
    return db.GetMemoryFileSystem();
  }
  return duckdb::FileSystem::GetFileSystem(db);
}

duckdb::OpenFileInfo ObjectToRead(const std::string& path, const FileInfo* info,
                                  bool whole) {
  duckdb::OpenFileInfo file{path};
  if (!info && !whole) {
    return file;
  }
  file.extended_info = duckdb::make_shared_ptr<duckdb::ExtendedOpenFileInfo>();
  auto& options = file.extended_info->options;
  if (info) {
    options.emplace("file_size", duckdb::Value::UBIGINT(info->size));
    options.emplace("etag", duckdb::Value{info->etag});
    options.emplace("last_modified",
                    duckdb::Value::TIMESTAMP(duckdb::timestamp_t{0}));
  } else {
    options.emplace("force_full_download", duckdb::Value::BOOLEAN(true));
  }
  return file;
}

constinit std::atomic<uint64_t> gMemoryIndexCount{0};

std::string RootPath(std::string_view root) {
  if (root.ends_with('/')) {
    return std::string{root};
  }
  return absl::StrCat(root, "/");
}

class WholeFileInput final : public BytesViewInput {
 public:
  explicit WholeFileInput(std::shared_ptr<const byte_type[]> bytes,
                          uint64_t size) noexcept
    : BytesViewInput{bytes_view{bytes.get(), size}}, _bytes{std::move(bytes)} {}

  ptr Dup() const final { return std::make_unique<WholeFileInput>(*this); }

 private:
  std::shared_ptr<const byte_type[]> _bytes;
};

class ForEachTask final : public duckdb::BaseExecutorTask {
 public:
  ForEachTask(duckdb::TaskExecutor& executor,
              absl::FunctionRef<void(size_t)> visit, size_t index)
    : BaseExecutorTask{executor}, _visit{visit}, _index{index} {}

  void ExecuteTask() final { _visit(_index); }

  std::string TaskType() const final { return "DirectoryForEach"; }

 private:
  absl::FunctionRef<void(size_t)> _visit;
  size_t _index;
};

}  // namespace

void FileInfos::Remember(std::string_view name, FileInfo info) {
  absl::MutexLock lock{&_mutex};
  _infos.insert_or_assign(name, std::move(info));
}

void FileInfos::Listed(std::string_view name, uint64_t size, std::string etag) {
  absl::MutexLock lock{&_mutex};
  _infos.try_emplace(name, FileInfo{.size = size, .etag = std::move(etag)});
}

std::optional<FileInfo> FileInfos::Find(std::string_view name) const {
  absl::ReaderMutexLock lock{&_mutex};
  const auto it = _infos.find(name);
  if (it == _infos.end()) {
    return std::nullopt;
  }
  return it->second;
}

bool FileInfos::Contains(std::string_view name) const {
  absl::ReaderMutexLock lock{&_mutex};
  return _infos.contains(name);
}

bool FileInfos::Forget(std::string_view name) {
  absl::MutexLock lock{&_mutex};
  return _infos.erase(name) != 0;
}

void FileInfos::Rename(std::string_view src, std::string_view dst) {
  absl::MutexLock lock{&_mutex};
  const auto it = _infos.find(src);
  if (it == _infos.end()) {
    _infos.erase(dst);
    return;
  }
  auto info = std::move(it->second);
  _infos.erase(it);
  info.etag.clear();
  _infos.insert_or_assign(dst, std::move(info));
}

TrackedFiles FileTracker::Flush() {
  TrackedFiles flushed;
  flushed.files.reserve(_files.size());
  for (auto it = _files.begin(), end = _files.end(); it != end;) {
    flushed.files.emplace_back(std::move(_files.extract(it++).value()));
  }
  flushed.bytes = std::exchange(_files_size, uint64_t{0});
  return flushed;
}

void FileTracker::Clear() noexcept {
  _files_size = 0;
  _files.clear();
}

void RefTracker::Keep(IndexFileRefs::ref_t ref) {
  absl::MutexLock lock{&_mutex};
  _tracked.insert(std::move(ref));
}

void RefTracker::Forget(std::string_view name) {
  absl::MutexLock lock{&_mutex};
  _tracked.erase(name);
}

void RefTracker::Replace(std::string_view src, IndexFileRefs::ref_t dst) {
  absl::MutexLock lock{&_mutex};
  _tracked.erase(src);
  _tracked.emplace(std::move(dst));
}

std::vector<IndexFileRefs::ref_t> RefTracker::GetRefs() const {
  absl::MutexLock lock{&_mutex};
  return {_tracked.begin(), _tracked.end()};
}

void RefTracker::Clear() {
  absl::MutexLock lock{&_mutex};
  _tracked.clear();
}

Directory::State::~State() {
  if (kind != StorageKind::Memory) {
    return;
  }
  try {
    fs.RemoveDirectory(root);
  } catch (const std::exception& e) {
    SDB_ERROR(IRESEARCH, absl::StrCat("Failed to remove the in-memory index '",
                                      root, "': ", e.what()));
  }
}

Directory::Directory(duckdb::DatabaseInstance& db, std::string_view root,
                     const ResourceManagementOptions& rm)
  : Directory{db, FileSystemOf(db, root), root, rm} {}

Directory::Directory(duckdb::DatabaseInstance& db, duckdb::FileSystem& fs,
                     std::string_view root, const ResourceManagementOptions& rm)
  : _state{std::make_shared<State>(db, fs, RootPath(root), KindOf(root), rm)} {}

Directory Directory::PrivateMemory(duckdb::DatabaseInstance& db) {
  auto heap = std::make_unique<duckdb::MemoryFileSystem>();
  auto& fs = *heap;
  return Directory{std::make_shared<State>(
    db, fs, RootPath(MemoryIndexRoot()), StorageKind::Memory,
    ResourceManagementOptions::gDefault, std::move(heap))};
}

std::string Directory::Path(std::string_view name) const {
  return absl::StrCat(_state->root, name);
}

std::optional<FileInfo> Directory::Info(std::string_view name) const {
  if (Kind() != StorageKind::Remote) {
    return std::nullopt;
  }
  return _state->infos.Find(name);
}

void Directory::Remember(std::string_view name, FileInfo info) const {
  if (Kind() == StorageKind::Remote) {
    _state->infos.Remember(name, std::move(info));
  }
}

Directory Directory::Tracking(FileTracker& tracker) const noexcept {
  auto result = *this;
  result._tracker = &tracker;
  return result;
}

Directory Directory::Tracking(RefTracker& tracker) const noexcept {
  auto result = *this;
  result._ref_tracker = &tracker;
  return result;
}

Directory Directory::Untracked() const noexcept {
  auto result = *this;
  result._tracker = nullptr;
  result._ref_tracker = nullptr;
  return result;
}

duckdb::BlockCache& Directory::Cache() const {
  return duckdb::BlockCache::Get(_state->db);
}

duckdb::MemoryFileSystem& Directory::MemoryFiles() const {
  return _state->db.GetMemoryFileSystem();
}

IndexOutput::ptr Directory::Create(std::string_view name) const {
  const auto path = Path(name);
  IndexFileRefs::ref_t ref;
  if (_ref_tracker) {
    ref = Refs().add(name);
  }
  absl::Cleanup forget = [&] {
    if (_ref_tracker) {
      Refs().remove(name);
    }
  };
  std::optional<RemoteUpload> remote;
  if (Kind() == StorageKind::Remote) {
    auto& cache = Cache();
    cache.ForgetFile(path);
    _state->infos.Forget(name);
    remote.emplace(
      RemoteUpload{.infos = _state->infos, .cache = cache, .name = name});
  }
  auto output = FileOutput::Open(_state->fs, path, _state->rm, _tracker,
                                 remote ? &*remote : nullptr);
  if (_tracker) {
    _tracker->Add(name);
  }
  if (ref) {
    _ref_tracker->Keep(std::move(ref));
  }
  std::move(forget).Cancel();
  return output;
}

IndexInput::ptr Directory::Open(std::string_view name,
                                duckdb::MappedFileAccess access) const {
  const auto path = Path(name);
  if (InBlocks()) {
    return OpenBlocks(path, name, access);
  }
  return OpenMapped(path, access);
}

IndexInput::ptr Directory::ReadWhole(std::string_view name) const {
  const auto path = Path(name);
  duckdb::OpenFileInfo file{path};
  if (Kind() == StorageKind::Remote) {
    const auto info = _state->infos.Find(name);
    file = ObjectToRead(path, info ? &*info : nullptr, true);
  }
  auto handle = _state->fs.OpenFile(file, duckdb::FileFlags::FILE_FLAGS_READ);
  const auto size = duckdb::NumericCast<duckdb::idx_t>(handle->GetFileSize());
  auto bytes = std::make_shared_for_overwrite<byte_type[]>(size);
  if (size != 0) {
    handle->Read(duckdb::QueryContext{}, bytes.get(), size, 0);
  }
  return std::make_unique<WholeFileInput>(std::move(bytes), size);
}

IndexInput::ptr Directory::OpenMapped(const std::string& path,
                                      duckdb::MappedFileAccess access) const {
  auto mapping = _state->fs.MemoryMapFile(duckdb::OpenFileInfo{path},
                                          duckdb::FileFlags::FILE_FLAGS_READ,
                                          duckdb::MMapOptions{});
  if (access != duckdb::MappedFileAccess::NORMAL) {
    mapping->Advise(access);
  }
  return std::make_unique<MappedInput>(
    std::make_shared<MappedFile>(std::move(mapping)));
}

IndexInput::ptr Directory::OpenBlocks(const std::string& path,
                                      std::string_view name,
                                      duckdb::MappedFileAccess access) const {
  auto& cache = Cache();
  if (Kind() == StorageKind::Memory) {
    auto blocks = MemoryFiles().GetBlockFile(path);
    if (!blocks) {
      return OpenMapped(path, access);
    }
    return std::make_unique<BlockInput>(cache, std::move(blocks));
  }
  const auto known = Info(name);
  duckdb::shared_ptr<duckdb::FileHandle> handle =
    _state->fs.OpenFile(ObjectToRead(path, known ? &*known : nullptr, false),
                        duckdb::FileFlags::FILE_FLAGS_READ |
                          duckdb::FileFlags::FILE_FLAGS_PARALLEL_ACCESS);
  duckdb::BlockCacheFileInfo info;
  info.validation.file_size =
    duckdb::NumericCast<duckdb::idx_t>(handle->GetFileSize());
  info.can_seek = handle->CanSeek();
  info.on_disk = handle->OnDiskFile();
  return std::make_unique<BlockInput>(
    cache, cache.OpenFile(path, info, false, std::move(handle)),
    known ? known->tail : nullptr);
}

bool Directory::Has(std::string_view name) const {
  if (Kind() == StorageKind::Remote && _state->infos.Contains(name)) {
    return true;
  }
  return _state->fs.FileExists(Path(name));
}

bool Directory::RemoveFile(std::string_view name) const {
  const auto path = Path(name);
  const bool remote = Kind() == StorageKind::Remote;
  if (remote) {
    _state->infos.Forget(name);
  }
  if (!_state->fs.TryRemoveFile(path)) {
    return false;
  }
  if (remote) {
    Cache().ForgetFile(path);
  }
  return true;
}

bool Directory::RemoveUnusedFile(std::string_view name) const noexcept try {
  return RemoveFile(name);
} catch (...) {
  return false;
}

bool Directory::Remove(std::string_view name) const {
  if (!RemoveFile(name)) {
    return false;
  }
  if (_ref_tracker) {
    Refs().remove(name);
    _ref_tracker->Forget(name);
  }
  return true;
}

void Directory::Rename(std::string_view src, std::string_view dst) const {
  const auto target = Path(dst);
  _state->fs.MoveFile(Path(src), target);
  if (Kind() == StorageKind::Remote) {
    Cache().ForgetFile(target);
    _state->infos.Rename(src, dst);
  }
  if (_ref_tracker) {
    _ref_tracker->Replace(src, Refs().add(dst));
    Refs().remove(src);
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
  if (Kind() != StorageKind::Local) {
    return;
  }
  duckdb::TaskScheduler::BlockingScope blocking;
  _state->fs.SyncDirectory(_state->root);
}

void Directory::Compact(std::string_view name) const {
  if (Kind() != StorageKind::Memory || !InBlocks()) {
    return;
  }
  MemoryFiles().Compact(Path(name));
}

IndexFileRefs::ref_t Directory::Reference(std::string_view name) const {
  if (!Has(name)) {
    return nullptr;
  }
  auto ref = Refs().add(name);
  if (!Has(name)) {
    return nullptr;
  }
  return ref;
}

size_t Directory::RemoveUnreferenced() const {
  size_t removed = 0;
  std::vector<std::string> batch;
  Refs().RemoveUnused(
    [&](const std::string& name) {
      if (Kind() == StorageKind::Remote) {
        batch.emplace_back(name);
      } else if (RemoveUnusedFile(name)) {
        ++removed;
      }
    },
    [&] { removed += RemoveFiles(batch); });
  return removed;
}

size_t Directory::RemoveFiles(std::span<const std::string> names) const noexcept
  try {
  if (names.empty()) {
    return 0;
  }
  duckdb::vector<std::string> paths;
  paths.reserve(names.size());
  size_t known = 0;
  for (const auto& name : names) {
    known += _state->infos.Forget(name);
    paths.push_back(Path(name));
  }
  absl::Cleanup forget = [&] noexcept {
    auto& cache = Cache();
    for (const auto& path : paths) {
      cache.ForgetFile(path);
    }
  };
  _state->fs.RemoveFiles(paths);
  return known;
} catch (...) {
  return 0;
}

void Directory::RemoveAllUnreferenced() const {
  duckdb::TaskScheduler::BlockingScope blocking;
  auto& refs = Refs();
  Visit([&](std::string_view name) { refs.add(name); });
  RemoveUnreferenced();
}

std::string MemoryIndexRoot() {
  return absl::StrCat(
    duckdb::MemoryFileSystem::PREFIX, "index/",
    gMemoryIndexCount.fetch_add(1, std::memory_order_relaxed) + 1);
}

void Directory::ForEach(size_t count,
                        absl::FunctionRef<void(size_t)> visit) const {
  if (Kind() != StorageKind::Remote || count < 2) {
    for (size_t i = 0; i != count; ++i) {
      visit(i);
    }
    return;
  }
  auto& scheduler = duckdb::TaskScheduler::GetScheduler(_state->db);
  duckdb::TaskExecutor executor{scheduler,
                                scheduler.NumberOfAsyncThreads() == 0
                                  ? duckdb::TaskSchedulerType::REGULAR
                                  : duckdb::TaskSchedulerType::ASYNC};
  for (size_t i = 0; i != count; ++i) {
    executor.ScheduleTask(duckdb::make_uniq<ForEachTask>(executor, visit, i));
  }
  executor.WorkOnTasks();
}

void Directory::Visit(absl::FunctionRef<void(std::string_view)> visitor) const {
  if (Kind() != StorageKind::Remote) {
    _state->fs.ListFiles(_state->root,
                         [&](const std::string& name, bool is_directory) {
                           if (!is_directory) {
                             visitor(name);
                           }
                         });
    return;
  }
  _state->fs.ListFiles(
    _state->root,
    [&](duckdb::OpenFileInfo& file) {
      if (duckdb::FileSystem::IsDirectory(file) ||
          file.path.find('/') != std::string::npos) {
        return;
      }
      duckdb::idx_t size = 0;
      if (file.extended_info &&
          file.extended_info->TryGetOption("file_size", size)) {
        std::string etag;
        file.extended_info->TryGetOption("etag", etag);
        _state->infos.Listed(file.path, size, std::move(etag));
      }
      visitor(file.path);
    },
    nullptr);
}

}  // namespace irs
