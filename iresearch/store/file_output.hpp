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
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <limits>
#include <memory>
#include <string>
#include <string_view>

#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/crc.hpp"
#include "iresearch/utils/resource_manager.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct PendingUploads {
  uint64_t files = 0;
  uint64_t bytes = 0;
};

PendingUploads CurrentUploads() noexcept;

struct RemoteUpload {
  FileInfos& infos;
  duckdb::BlockCache& cache;
  std::string_view name;
};

class FileOutput final : public IndexOutput {
 public:
  static constexpr size_t kMaxTail = 16 << 10;

  static ptr Open(duckdb::FileSystem& fs, const std::string& path,
                  const ResourceManagementOptions& rm, FileTracker* tracker,
                  const RemoteUpload* remote);

  ~FileOutput() final;

 private:
  FileOutput(std::unique_ptr<duckdb::FileHandle> handle,
             const ResourceManagementOptions& rm, FileTracker* tracker,
             const RemoteUpload* remote);

  void Flush() final;
  uint32_t Checksum() final;
  void BeginTail() final;
  void CloseImpl() final;
  void WriteDirect(const byte_type* b, size_t len) final;
  void WriteHandle(const byte_type* b, size_t len);
  void KeepTail(const byte_type* b, size_t len);
  void CacheBytes(const byte_type* b, size_t len) noexcept;
  void FinishCache(uint64_t size, bool published) noexcept;

  static constexpr size_t kWritebackHintBytes = 8 << 20;

  byte_type _buf[64 * 1024];
  const ResourceManagementOptions& _rm;
  std::unique_ptr<duckdb::FileHandle> _handle;
  Crc32c _crc;
  uint64_t _unhinted = 0;
  FileTracker* _tracker = nullptr;
  FileInfos* _infos = nullptr;
  std::string _name;
  uint64_t _tail_start = std::numeric_limits<uint64_t>::max();
  bstring _tail;
  duckdb::BlockCache* _cache = nullptr;
  duckdb::shared_ptr<duckdb::BlockCacheFile> _cached;
  duckdb::BlockCachePin _block;
  uint64_t _block_index = std::numeric_limits<uint64_t>::max();
};

}  // namespace irs
