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

#pragma once

#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <string>
#include <string_view>

namespace tests {

class HookFileSystem : public duckdb::FileSystem {
 public:
  explicit HookFileSystem(duckdb::FileSystem& inner) noexcept : _inner{inner} {}

  virtual void OnCreate(std::string_view name) {}
  virtual void OnCreated(std::string_view name) {}
  virtual void OnOpen(std::string_view name) {}
  virtual void OnExists(std::string_view name) {}
  virtual void OnRemove(std::string_view name) {}
  virtual void OnRename(std::string_view src, std::string_view dst) {}
  virtual void OnClose(std::string_view name) {}
  virtual void OnSync(std::string_view name) {}
  virtual void OnSyncDirectory() {}

  static std::string_view Name(std::string_view path) noexcept;

  duckdb::unique_ptr<duckdb::FileHandle> OpenFile(
    const std::string& path, duckdb::FileOpenFlags flags,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override;
  duckdb::unique_ptr<duckdb::MemoryMappedFile> MemoryMapFile(
    const duckdb::OpenFileInfo& path, duckdb::FileOpenFlags flags,
    const duckdb::MMapOptions& options,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override;

  void Read(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
            duckdb::idx_t location) override;
  void Write(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
             duckdb::idx_t location) override;
  int64_t Read(duckdb::FileHandle& handle, void* buffer,
               int64_t nr_bytes) override;
  int64_t Write(duckdb::FileHandle& handle, void* buffer,
                int64_t nr_bytes) override;
  int64_t GetFileSize(duckdb::FileHandle& handle) override;
  duckdb::timestamp_t GetLastModifiedTime(duckdb::FileHandle& handle) override;
  duckdb::FileType GetFileType(duckdb::FileHandle& handle) override;
  duckdb::FileMetadata Stats(duckdb::FileHandle& handle) override;
  void Truncate(duckdb::FileHandle& handle, int64_t new_size) override;
  void FileSync(duckdb::FileHandle& handle) override;
  void HintWriteback(duckdb::FileHandle& handle, duckdb::idx_t location,
                     duckdb::idx_t nr_bytes) override;
  void Seek(duckdb::FileHandle& handle, duckdb::idx_t location) override;
  duckdb::idx_t SeekPosition(duckdb::FileHandle& handle) override;
  bool CanSeek() override { return true; }
  bool OnDiskFile(duckdb::FileHandle& handle) override;

  duckdb::FileExistence TryFileExists(
    const std::string& filename,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr,
    duckdb::optional_ptr<duckdb::ErrorData> error = nullptr) override;
  duckdb::FileExistence TryDirectoryExists(
    const std::string& directory,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr,
    duckdb::optional_ptr<duckdb::ErrorData> error = nullptr) override;
  void CreateDirectory(
    const std::string& directory,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override;
  void RemoveDirectory(
    const std::string& directory,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override;
  bool ListFiles(const std::string& directory,
                 const std::function<void(const std::string&, bool)>& callback,
                 duckdb::FileOpener* opener = nullptr) override;
  void MoveFile(
    const std::string& source, const std::string& target,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override;
  void RemoveFile(
    const std::string& filename,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override;
  bool TryRemoveFile(
    const std::string& filename,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override;
  void SyncDirectory(
    const std::string& directory,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) override;

  std::string GetName() const override { return "HookFileSystem"; }

 private:
  duckdb::FileSystem& _inner;
};

}  // namespace tests
