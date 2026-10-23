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

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
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
#include "iresearch/utils/pg/file_error.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

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

class BytesMapping final : public duckdb::MemoryMappedFile {
 public:
  BytesMapping(std::string path, bytes_view bytes,
               std::unique_ptr<byte_type[]> owner = nullptr)
    : duckdb::MemoryMappedFile{std::move(path),
                               duckdb::FileFlags::FILE_FLAGS_READ,
                               const_cast<byte_type*>(bytes.data()),
                               bytes.size()},
      _owner{std::move(owner)} {}

  void Sync() final {}
  bool Trim(duckdb::idx_t, duckdb::idx_t) final { return false; }
  bool OnDiskFile() const final { return false; }
  void Close() final {}

 private:
  std::unique_ptr<byte_type[]> _owner;
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

std::vector<std::string> FileTracker::Flush(uint64_t& files_size) {
  std::vector<std::string> files;
  files.reserve(_files.size());
  for (auto it = _files.begin(), end = _files.end(); it != end;) {
    files.emplace_back(std::move(_files.extract(it++).value()));
  }
  files_size = std::exchange(_files_size, uint64_t{0});
  return files;
}

void FileTracker::Clear() noexcept {
  _files_size = 0;
  _files.clear();
}

void RefTracker::Keep(IndexFileRefs::ref_t ref) {
  absl::MutexLock lock{&_mutex};
  _tracked.insert(std::move(ref));
}

void RefTracker::Remove(std::string_view name) {
  _refs->remove(name);
  absl::MutexLock lock{&_mutex};
  _tracked.erase(name);
}

void RefTracker::Rename(std::string_view src, std::string_view dst) {
  auto ref = _refs->add(dst);
  {
    absl::MutexLock lock{&_mutex};
    _tracked.emplace(std::move(ref));
    _tracked.erase(src);
  }
  _refs->remove(src);
}

std::vector<IndexFileRefs::ref_t> RefTracker::GetRefs() const {
  absl::MutexLock lock{&_mutex};
  return {_tracked.begin(), _tracked.end()};
}

void RefTracker::Clear() const {
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
  : _state{std::make_shared<State>(db, fs, RootPath(root), KindOf(root), rm)} {
  if (_state->kind == StorageKind::Remote) {
    _state->infos = std::make_shared<FileInfos>();
  }
}

Directory::Directory(duckdb::DatabaseInstance& db,
                     std::vector<LinkedFile> files,
                     const ResourceManagementOptions& rm)
  : _state{std::make_shared<State>(db, duckdb::FileSystem::GetFileSystem(db),
                                   std::string{}, StorageKind::Linked, rm,
                                   std::move(files))} {}

Directory Directory::PrivateMemory(duckdb::DatabaseInstance& db,
                                   const ResourceManagementOptions& rm) {
  auto heap = std::make_unique<duckdb::MemoryFileSystem>();
  auto& fs = *heap;
  return Directory{std::make_shared<State>(
    db, fs, RootPath(MemoryIndexRoot()), StorageKind::Memory, rm,
    std::vector<LinkedFile>{}, std::move(heap))};
}

const LinkedFile* Directory::FindLinked(std::string_view name) const noexcept {
  const auto it = absl::c_find_if(
    _state->linked, [&](const LinkedFile& file) { return file.name == name; });
  return it == _state->linked.end() ? nullptr : &*it;
}

std::string Directory::Path(std::string_view name) const {
  return absl::StrCat(_state->root, name);
}

std::optional<FileInfo> Directory::Info(std::string_view name) const {
  if (!_state->infos) {
    return std::nullopt;
  }
  return _state->infos->Find(name);
}

void Directory::Remember(std::string_view name, FileInfo info) const {
  if (_state->infos) {
    _state->infos->Remember(name, std::move(info));
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

IndexOutput::ptr Directory::Create(std::string_view name) const try {
  if (Kind() == StorageKind::Linked) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("could not create file \"", name, "\": the index is read-only"));
  }
  const auto path = Path(name);
  IndexFileRefs::ref_t ref;
  if (_ref_tracker) {
    ref = _ref_tracker->Add(name);
  }
  absl::Cleanup forget = [&] {
    if (_ref_tracker) {
      _ref_tracker->Forget(name);
    }
  };
  if (Kind() == StorageKind::Remote) {
    Cache().ForgetFile(path);
    _state->infos->Forget(name);
  }
  auto output =
    FileOutput::Open(_state->fs, path, _state->rm, _state->infos, name,
                     Kind() == StorageKind::Remote ? &Cache() : nullptr);
  if (_tracker) {
    _tracker->Add(name);
    output->SetOnClose(
      {absl::FunctionValue{}, [tracker = _tracker](uint64_t size) noexcept {
         tracker->AddBytes(size);
       }});
  }
  if (ref) {
    _ref_tracker->Keep(std::move(ref));
  }
  std::move(forget).Cancel();
  return output;
} catch (const duckdb::IOException& e) {
  ThrowFileError(e);
}

IndexInput::ptr Directory::Open(std::string_view name, IOAdvice advice) const
  try {
  if (Kind() == StorageKind::Linked) {
    const auto* file = FindLinked(name);
    if (!file) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FILE),
                      ERR_MSG("could not open file \"", name,
                              "\" of the index: no such file"));
    }
    return std::make_unique<MappedInput>(std::make_shared<MappedFile>(
      std::make_unique<BytesMapping>(std::string{name}, file->bytes), nullptr));
  }
  const auto path = Path(name);
  if ((advice & IOAdvice::READONCE) == IOAdvice::READONCE) {
    return ReadWhole(path, name);
  }
  if (InBlocks()) {
    return OpenBlocks(path, name, advice);
  }
  return OpenMapped(path, advice);
} catch (const duckdb::IOException& e) {
  ThrowFileError(e);
}

IndexInput::ptr Directory::ReadWhole(const std::string& path,
                                     std::string_view name) const {
  const bool counted = Kind() == StorageKind::Local;
  if (counted) {
    _state->rm.file_descriptors->Increase(1);
  }
  absl::Cleanup release = [&] noexcept {
    if (counted) {
      _state->rm.file_descriptors->Decrease(1);
    }
  };
  duckdb::OpenFileInfo file{path};
  if (Kind() == StorageKind::Remote) {
    const auto info = _state->infos->Find(name);
    file = ObjectToRead(path, info ? &*info : nullptr, true);
  }
  auto handle = _state->fs.OpenFile(file, duckdb::FileFlags::FILE_FLAGS_READ);
  const auto size = duckdb::NumericCast<duckdb::idx_t>(handle->GetFileSize());
  auto bytes = std::make_unique_for_overwrite<byte_type[]>(size);
  if (size != 0) {
    handle->Read(duckdb::QueryContext{}, bytes.get(), size, 0);
  }
  handle.reset();
  const bytes_view view{bytes.get(), size};
  return std::make_unique<MappedInput>(std::make_shared<MappedFile>(
    std::make_unique<BytesMapping>(path, view, std::move(bytes)), nullptr));
}

IndexInput::ptr Directory::OpenMapped(const std::string& path,
                                      IOAdvice advice) const {
  auto* file_descriptors =
    Kind() == StorageKind::Local ? _state->rm.file_descriptors : nullptr;
  if (file_descriptors) {
    file_descriptors->Increase(1);
  }
  absl::Cleanup release = [&] noexcept {
    if (file_descriptors) {
      file_descriptors->Decrease(1);
    }
  };
  auto mapping = _state->fs.MemoryMapFile(duckdb::OpenFileInfo{path},
                                          duckdb::FileFlags::FILE_FLAGS_READ,
                                          duckdb::MMapOptions{});
  if (!mapping) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("Cannot map index file '", path, "'"));
  }
  const auto access = ToMappedFileAccess(advice);
  if (access != duckdb::MappedFileAccess::NORMAL) {
    mapping->Advise(access);
  }
  auto file =
    std::make_shared<MappedFile>(std::move(mapping), file_descriptors);
  std::move(release).Cancel();
  return std::make_unique<MappedInput>(std::move(file));
}

IndexInput::ptr Directory::OpenBlocks(const std::string& path,
                                      std::string_view name,
                                      IOAdvice advice) const {
  auto& cache = Cache();
  if (Kind() == StorageKind::Memory) {
    auto blocks = MemoryFiles().GetBlockFile(path);
    if (!blocks) {
      return OpenMapped(path, advice);
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

bool Directory::Exists(bool& result, std::string_view name) const noexcept try {
  if (Kind() == StorageKind::Linked) {
    result = FindLinked(name) != nullptr;
    return true;
  }
  if (_state->infos && _state->infos->Contains(name)) {
    result = true;
    return true;
  }
  result = _state->fs.FileExists(Path(name));
  return true;
} catch (...) {
  return false;
}

bool Directory::Has(std::string_view name) const {
  bool exists = false;
  if (!Exists(exists, name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                    ERR_MSG("could not check whether file \"", name,
                            "\" of the index exists"));
  }
  return exists;
}

bool Directory::RemoveFile(std::string_view name) const noexcept try {
  if (Kind() == StorageKind::Linked) {
    return false;
  }
  const auto path = Path(name);
  if (_state->infos) {
    _state->infos->Forget(name);
  }
  if (!_state->fs.TryRemoveFile(path)) {
    return false;
  }
  if (Kind() == StorageKind::Remote) {
    Cache().ForgetFile(path);
  }
  return true;
} catch (...) {
  return false;
}

bool Directory::Remove(std::string_view name) const noexcept try {
  if (!RemoveFile(name)) {
    return false;
  }
  if (_ref_tracker) {
    _ref_tracker->Remove(name);
  }
  return true;
} catch (...) {
  return false;
}

void Directory::Rename(std::string_view src, std::string_view dst) const try {
  if (Kind() == StorageKind::Linked) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("could not rename file \"", src, "\": the index is read-only"));
  }
  const auto target = Path(dst);
  _state->fs.MoveFile(Path(src), target);
  if (Kind() == StorageKind::Remote) {
    Cache().ForgetFile(target);
    _state->infos->Rename(src, dst);
  }
  if (_ref_tracker) {
    _ref_tracker->Rename(src, dst);
  }
} catch (const duckdb::IOException& e) {
  ThrowFileError(e);
}

void Directory::Sync(std::span<const std::string_view> names) const try {
  if (Kind() != StorageKind::Local) {
    return;
  }
  duckdb::TaskScheduler::BlockingScope blocking;
  for (const auto name : names) {
    auto handle =
      _state->fs.OpenFile(Path(name), duckdb::FileFlags::FILE_FLAGS_READ);
    handle->Sync();
  }
} catch (const duckdb::IOException& e) {
  ThrowFileError(e);
}

void Directory::SyncDirectory() const try {
  if (Kind() != StorageKind::Local) {
    return;
  }
  duckdb::TaskScheduler::BlockingScope blocking;
  _state->fs.SyncDirectory(_state->root);
} catch (const duckdb::IOException& e) {
  ThrowFileError(e);
}

void Directory::Compact(std::string_view name) const try {
  if (Kind() != StorageKind::Memory || !InBlocks()) {
    return;
  }
  MemoryFiles().Compact(Path(name));
} catch (const duckdb::IOException& e) {
  ThrowFileError(e);
}

IndexFileRefs::ref_t Directory::Reference(std::string_view name) const {
  bool exists;
  if (!Exists(exists, name) || !exists) {
    return nullptr;
  }
  auto ref = Refs().add(name);
  if (!Exists(exists, name) || !exists) {
    return nullptr;
  }
  return ref;
}

size_t Directory::RemoveUnreferenced(
  const std::function<bool(std::string_view)>& acceptor) const {
  size_t removed = 0;
  IndexFileRefs::ref_t retained;
  std::vector<std::string> batch;
  Refs().refs().visit(
    [&](const IndexFileRefs::ref_t& ref, size_t count) {
      const std::string_view name = *ref;
      if (!acceptor(name)) {
        retained = ref;
      } else if (count == 0) {
        if (Kind() == StorageKind::Remote) {
          batch.emplace_back(name);
        } else if (RemoveFile(name)) {
          ++removed;
        }
      }
      return true;
    },
    true, [&] { removed += RemoveFiles(batch); });
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
    known += _state->infos->Forget(name);
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
  Visit([&](std::string_view name) {
    refs.add(name);
    return true;
  });
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

bool Directory::Visit(const Visitor& visitor) const {
  if (Kind() == StorageKind::Linked) {
    return absl::c_all_of(_state->linked, [&](const LinkedFile& file) {
      return visitor(file.name);
    });
  }
  std::vector<std::string> names;
  try {
    if (_state->infos) {
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
            _state->infos->Listed(file.path, size, std::move(etag));
          }
          names.push_back(std::move(file.path));
        },
        nullptr);
    } else {
      _state->fs.ListFiles(_state->root,
                           [&](const std::string& name, bool is_directory) {
                             if (!is_directory) {
                               names.push_back(name);
                             }
                           });
    }
  } catch (const duckdb::IOException& e) {
    ThrowFileError(e);
  }
  for (const auto& name : names) {
    if (!visitor(name)) {
      return false;
    }
  }
  return true;
}

}  // namespace irs
