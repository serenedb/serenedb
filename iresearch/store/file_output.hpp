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
#include <string_view>

#include "iresearch/store/data_output.hpp"
#include "iresearch/store/index_files.hpp"
#include "iresearch/utils/crc.hpp"

namespace irs {

struct PendingUploads {
  uint64_t files = 0;
  uint64_t bytes = 0;
};

PendingUploads CurrentUploads() noexcept;

class FileOutput final : public DataOutput {
 public:
  static std::unique_ptr<FileOutput> Open(duckdb::FileSystem& fs,
                                          std::string_view root,
                                          std::string_view name,
                                          const FileObservers& observers,
                                          duckdb::BlockCache* cache);

  ~FileOutput();

  uint32_t Checksum();

  void Close();

 private:
  FileOutput(std::unique_ptr<duckdb::FileHandle> handle,
             const FileObservers& observers, bool remote,
             duckdb::BlockCache* cache);

  void Flush();
  void Release() noexcept;
  void Overflow(const byte_type* data, size_t size) final;
  void SetWindow(duckdb::FileWriteWindow window) noexcept;
  void NextWindow();
  void UseBuffer();
  void CommitWindow();
  void WriteHandle(const byte_type* b, size_t len);
  void FinishCache(bool published) noexcept;

  static constexpr size_t kBufferSize = 64 * 1024;
  static constexpr size_t kWritebackHintBytes = 8 << 20;

  std::unique_ptr<byte_type[]> _buf;
  std::unique_ptr<duckdb::FileHandle> _handle;
  Crc32c _crc;
  uint64_t _unhinted = 0;
  FileObservers _observers;
  bool _remote;
  bool _in_place = false;
  std::optional<duckdb::BlockCacheAppender> _cached;
};

}  // namespace irs
