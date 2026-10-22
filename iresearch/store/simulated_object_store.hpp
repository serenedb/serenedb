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

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/open_file_info.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <memory>
#include <string>
#include <string_view>

namespace irs {

struct ObjectStoreRequests {
  uint64_t get = 0;
  uint64_t put = 0;
  uint64_t head = 0;
  uint64_t list = 0;
  uint64_t remove = 0;
  uint64_t copy = 0;
  uint64_t bytes_read = 0;
  uint64_t bytes_written = 0;
  uint64_t max_in_flight = 0;
};

class SimulatedObjectStore final : public duckdb::FileSystem {
 public:
  static constexpr std::string_view kPrefix = "sim://";
  static constexpr size_t kDeleteBatch = 1000;

  struct Object {
    std::string bytes;
    uint64_t version = 0;
    duckdb::timestamp_t written;
  };

  static SimulatedObjectStore& Register(duckdb::DatabaseInstance& db);

  ObjectStoreRequests Requests() const noexcept;
  void ResetRequests() noexcept;
  void SetLatency(std::chrono::microseconds latency) noexcept {
    _latency_us.store(latency.count(), std::memory_order_relaxed);
  }

  std::shared_ptr<const Object> Find(std::string_view path) const;
  uint64_t Publish(const std::string& path, std::string bytes);

  void Read(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
            duckdb::idx_t location) final;
  int64_t Read(duckdb::FileHandle& handle, void* buffer,
               int64_t nr_bytes) final;
  void Write(duckdb::FileHandle& handle, void* buffer, int64_t nr_bytes,
             duckdb::idx_t location) final;
  int64_t Write(duckdb::FileHandle& handle, void* buffer,
                int64_t nr_bytes) final;
  int64_t GetFileSize(duckdb::FileHandle& handle) final;
  duckdb::timestamp_t GetLastModifiedTime(duckdb::FileHandle& handle) final;
  std::string GetVersionTag(duckdb::FileHandle& handle) final;
  duckdb::FileType GetFileType(duckdb::FileHandle& handle) final {
    return duckdb::FileType::FILE_TYPE_REGULAR;
  }
  void Truncate(duckdb::FileHandle& handle, int64_t new_size) final;
  void FileSync(duckdb::FileHandle& handle) final;

  duckdb::FileExistence TryFileExists(
    const std::string& filename,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr,
    duckdb::optional_ptr<duckdb::ErrorData> error = nullptr) final;
  duckdb::FileExistence TryDirectoryExists(
    const std::string& directory,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr,
    duckdb::optional_ptr<duckdb::ErrorData> error = nullptr) final;
  void CreateDirectory(
    const std::string& directory,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) final {}
  void RemoveDirectory(
    const std::string& directory,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) final;
  void RemoveFiles(
    const duckdb::vector<std::string>& filenames,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) final;
  void MoveFile(
    const std::string& source, const std::string& target,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) final;
  void RemoveFile(
    const std::string& filename,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) final;
  bool TryRemoveFile(
    const std::string& filename,
    duckdb::optional_ptr<duckdb::FileOpener> opener = nullptr) final;

  void Seek(duckdb::FileHandle& handle, duckdb::idx_t location) final;
  duckdb::idx_t SeekPosition(duckdb::FileHandle& handle) final;
  bool CanSeek() final { return true; }
  bool OnDiskFile(duckdb::FileHandle& handle) final { return false; }
  bool CanHandleFile(const std::string& fpath) final;
  std::string GetName() const final { return "SimulatedObjectStore"; }
  std::string PathSeparator(const std::string& path) final { return "/"; }

 protected:
  duckdb::unique_ptr<duckdb::FileHandle> OpenFileExtended(
    const duckdb::OpenFileInfo& file, duckdb::FileOpenFlags flags,
    duckdb::optional_ptr<duckdb::FileOpener> opener) final;
  bool SupportsOpenFileExtended() const final { return true; }
  bool ListFilesExtended(
    const std::string& directory,
    const std::function<void(duckdb::OpenFileInfo& info)>& callback,
    duckdb::optional_ptr<duckdb::FileOpener> opener) final;
  bool SupportsListFilesExtended() const final { return true; }

 private:
  void Request(std::atomic<uint64_t>& counter) const;
  std::shared_ptr<const Object> Fetch(std::string_view path, uint64_t version);

  mutable absl::Mutex _mutex;
  absl::flat_hash_map<std::string, std::shared_ptr<const Object>> _objects
    ABSL_GUARDED_BY(_mutex);
  uint64_t _versions ABSL_GUARDED_BY(_mutex) = 0;
  std::atomic<int64_t> _latency_us{0};
  mutable std::atomic<uint64_t> _get{0};
  mutable std::atomic<uint64_t> _put{0};
  mutable std::atomic<uint64_t> _head{0};
  mutable std::atomic<uint64_t> _list{0};
  mutable std::atomic<uint64_t> _remove{0};
  mutable std::atomic<uint64_t> _copy{0};
  mutable std::atomic<uint64_t> _bytes_read{0};
  mutable std::atomic<uint64_t> _bytes_written{0};
  mutable std::atomic<uint64_t> _in_flight{0};
  mutable std::atomic<uint64_t> _max_in_flight{0};
};

}  // namespace irs
