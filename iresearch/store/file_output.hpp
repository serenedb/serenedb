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
#include <memory>
#include <optional>
#include <string>

#include "iresearch/store/data_output.hpp"
#include "iresearch/store/directory.hpp"
#include "iresearch/utils/crc.hpp"

namespace irs {

struct PendingUploads {
  uint64_t files = 0;
  uint64_t bytes = 0;
};

PendingUploads CurrentUploads() noexcept;

class FileOutput final : public IndexOutput {
 public:
  static ptr Open(duckdb::FileSystem& fs, const std::string& path,
                  const Directory::Observers& observers, bool remote,
                  duckdb::BlockCache* cache);

  ~FileOutput() final;

 private:
  FileOutput(std::unique_ptr<duckdb::FileHandle> handle,
             const Directory::Observers& observers, bool remote,
             duckdb::BlockCache* cache);

  void Flush() final;
  uint32_t Checksum() final;
  void CloseImpl() final;
  void Release() noexcept;
  void Overflow(const byte_type* data, size_t size) final;
  void WriteHandle(const byte_type* b, size_t len);
  void CacheBytes(const byte_type* b, size_t len) noexcept;
  void FinishCache(bool published) noexcept;

  static constexpr size_t kWritebackHintBytes = 8 << 20;

  byte_type _buf[64 * 1024];
  std::unique_ptr<duckdb::FileHandle> _handle;
  Crc32c _crc;
  uint64_t _unhinted = 0;
  Directory::Observers _observers;
  bool _remote;
  std::optional<duckdb::BlockCacheAppender> _cached;
};

}  // namespace irs
