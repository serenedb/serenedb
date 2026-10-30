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

#include "hook_file_system.hpp"

#include <duckdb/common/helper.hpp>
#include <duckdb/common/query_context.hpp>

namespace tests {
namespace {

class HookFileHandle final : public duckdb::FileHandle {
 public:
  HookFileHandle(HookFileSystem& fs,
                 duckdb::unique_ptr<duckdb::FileHandle> inner,
                 const std::string& path, duckdb::FileOpenFlags flags)
    : duckdb::FileHandle{fs, path, flags}, inner{std::move(inner)} {}

  void Close() final {
    file_system.Cast<HookFileSystem>().OnClose(HookFileSystem::Name(path));
    inner->Close();
  }

  duckdb::unique_ptr<duckdb::FileHandle> inner;
};

duckdb::FileHandle& Inner(duckdb::FileHandle& handle) {
  return *handle.Cast<HookFileHandle>().inner;
}

}  // namespace

std::string_view HookFileSystem::Name(std::string_view path) noexcept {
  const auto slash = path.rfind('/');
  return slash == std::string_view::npos ? path : path.substr(slash + 1);
}

duckdb::unique_ptr<duckdb::FileHandle> HookFileSystem::OpenFile(
  const std::string& path, duckdb::FileOpenFlags flags,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  const bool create = flags.OpenForWriting();
  if (create) {
    OnCreate(Name(path));
  } else {
    OnOpen(Name(path));
  }
  auto inner = _inner.OpenFile(path, flags, opener);
  if (!inner) {
    return nullptr;
  }
  auto handle =
    duckdb::make_uniq<HookFileHandle>(*this, std::move(inner), path, flags);
  if (create) {
    OnCreated(Name(path));
  }
  return handle;
}

duckdb::unique_ptr<duckdb::MemoryMappedFile> HookFileSystem::MemoryMapFile(
  const duckdb::OpenFileInfo& path, duckdb::FileOpenFlags flags,
  const duckdb::MMapOptions& options,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  OnOpen(Name(path.path));
  return _inner.MemoryMapFile(path, flags, options, opener);
}

duckdb::shared_ptr<duckdb::BlockCacheFile> HookFileSystem::GetBlockFile(
  const std::string& path) {
  return _inner.GetBlockFile(path);
}

void HookFileSystem::Compact(const std::string& path) { _inner.Compact(path); }

void HookFileSystem::Read(duckdb::FileHandle& handle, void* buffer,
                          int64_t nr_bytes, duckdb::idx_t location) {
  Inner(handle).Read(buffer, nr_bytes, location);
}

void HookFileSystem::Write(duckdb::FileHandle& handle, void* buffer,
                           int64_t nr_bytes, duckdb::idx_t location) {
  Inner(handle).Write(duckdb::QueryContext{}, buffer, nr_bytes, location);
}

int64_t HookFileSystem::Read(duckdb::FileHandle& handle, void* buffer,
                             int64_t nr_bytes) {
  return Inner(handle).Read(buffer, nr_bytes);
}

int64_t HookFileSystem::Write(duckdb::FileHandle& handle, void* buffer,
                              int64_t nr_bytes) {
  return Inner(handle).Write(buffer, nr_bytes);
}

int64_t HookFileSystem::GetFileSize(duckdb::FileHandle& handle) {
  return Inner(handle).GetFileSize();
}

duckdb::timestamp_t HookFileSystem::GetLastModifiedTime(
  duckdb::FileHandle& handle) {
  return Inner(handle).Stats().last_modification_time;
}

duckdb::FileType HookFileSystem::GetFileType(duckdb::FileHandle& handle) {
  return Inner(handle).GetType();
}

duckdb::FileMetadata HookFileSystem::Stats(duckdb::FileHandle& handle) {
  return Inner(handle).Stats();
}

void HookFileSystem::Truncate(duckdb::FileHandle& handle, int64_t new_size) {
  Inner(handle).Truncate(new_size);
}

void HookFileSystem::FileSync(duckdb::FileHandle& handle) {
  OnSync(Name(handle.GetPath()));
  Inner(handle).Sync();
}

void HookFileSystem::HintWriteback(duckdb::FileHandle& handle,
                                   duckdb::idx_t location,
                                   duckdb::idx_t nr_bytes) {
  Inner(handle).HintWriteback(location, nr_bytes);
}

void HookFileSystem::Seek(duckdb::FileHandle& handle, duckdb::idx_t location) {
  Inner(handle).Seek(location);
}

duckdb::idx_t HookFileSystem::SeekPosition(duckdb::FileHandle& handle) {
  return Inner(handle).SeekPosition();
}

bool HookFileSystem::OnDiskFile(duckdb::FileHandle& handle) {
  return Inner(handle).OnDiskFile();
}

duckdb::FileExistence HookFileSystem::TryFileExists(
  const std::string& filename, duckdb::optional_ptr<duckdb::FileOpener> opener,
  duckdb::optional_ptr<duckdb::ErrorData> error) {
  OnExists(Name(filename));
  return _inner.TryFileExists(filename, opener, error);
}

duckdb::FileExistence HookFileSystem::TryDirectoryExists(
  const std::string& directory, duckdb::optional_ptr<duckdb::FileOpener> opener,
  duckdb::optional_ptr<duckdb::ErrorData> error) {
  return _inner.TryDirectoryExists(directory, opener, error);
}

void HookFileSystem::CreateDirectory(
  const std::string& directory,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  _inner.CreateDirectory(directory, opener);
}

void HookFileSystem::RemoveDirectory(
  const std::string& directory,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  _inner.RemoveDirectory(directory, opener);
}

bool HookFileSystem::ListFiles(
  const std::string& directory,
  const std::function<void(const std::string&, bool)>& callback,
  duckdb::FileOpener* opener) {
  return _inner.ListFiles(directory, callback, opener);
}

void HookFileSystem::MoveFile(const std::string& source,
                              const std::string& target,
                              duckdb::optional_ptr<duckdb::FileOpener> opener) {
  OnRename(Name(source), Name(target));
  _inner.MoveFile(source, target, opener);
}

void HookFileSystem::RemoveFile(
  const std::string& filename,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  OnRemove(Name(filename));
  _inner.RemoveFile(filename, opener);
}

bool HookFileSystem::TryRemoveFile(
  const std::string& filename,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  OnRemove(Name(filename));
  return _inner.TryRemoveFile(filename, opener);
}

void HookFileSystem::SyncDirectory(
  const std::string& directory,
  duckdb::optional_ptr<duckdb::FileOpener> opener) {
  OnSyncDirectory();
  _inner.SyncDirectory(directory, opener);
}

}  // namespace tests
