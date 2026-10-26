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

#include "iresearch/store/file_output.hpp"

#include <absl/cleanup/cleanup.h>

#include <atomic>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/query_context.hpp>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs {
namespace {

constinit std::atomic<uint64_t> gUploadingFiles{0};
constinit std::atomic<uint64_t> gUploadingBytes{0};

}  // namespace

PendingUploads CurrentUploads() noexcept {
  return {.files = gUploadingFiles.load(std::memory_order_relaxed),
          .bytes = gUploadingBytes.load(std::memory_order_relaxed)};
}

IndexOutput::ptr FileOutput::Open(duckdb::FileSystem& fs,
                                  const std::string& path,
                                  const Directory::Observers& observers,
                                  bool remote, duckdb::BlockCache* cache) {
  auto handle =
    fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                        duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
  return ptr{new FileOutput{std::move(handle), observers, remote, cache}};
}

FileOutput::FileOutput(std::unique_ptr<duckdb::FileHandle> handle,
                       const Directory::Observers& observers, bool remote,
                       duckdb::BlockCache* cache)
  : IndexOutput{_buf, std::end(_buf)},
    _handle{std::move(handle)},
    _observers{observers},
    _remote{remote} {
  SDB_ASSERT(_handle);
  if (!_remote) {
    return;
  }
  gUploadingFiles.fetch_add(1, std::memory_order_relaxed);
  if (cache && cache->IsEnabled()) {
    _cached.emplace(*cache, cache->CreateWrittenFile(_handle->GetPath()));
  }
}

FileOutput::~FileOutput() {
  if (!_handle) {
    return;
  }
  try {
    _handle->AbortWrite();
  } catch (const std::exception& e) {
    SDB_WARN(IRESEARCH, "Failed to abandon the write of '", _handle->GetPath(),
             "': ", duckdb::ErrorData{e}.RawMessage());
  }
  Release();
}

void FileOutput::Flush() {
  if (Length() != 0) {
    WriteHandle(_buf, Length());
  }
  _pos = _buf;
}

uint32_t FileOutput::Checksum() {
  Flush();
  return std::exchange(_crc, Crc32c{}).checksum();
}

void FileOutput::Release() noexcept {
  if (_cached) {
    FinishCache(false);
  }
  _handle.reset();
  if (_remote) {
    gUploadingFiles.fetch_sub(1, std::memory_order_relaxed);
    gUploadingBytes.fetch_sub(_offset, std::memory_order_relaxed);
  }
  _crc = Crc32c{};
  _offset = 0;
}

void FileOutput::CloseImpl() {
  const auto size = Position();
  absl::Cleanup release = [&] noexcept { Release(); };
  Flush();
  _handle->Close();
  if (_cached) {
    FinishCache(true);
  }
  for (auto* observer : _observers) {
    observer->Written(size);
  }
}

void FileOutput::WriteDirect(const byte_type* b, size_t len) {
  auto remain = Remain();
  SDB_ASSERT(len >= remain);
  const auto option =
    static_cast<size_t>(Length() != 0) * 2 + static_cast<size_t>(remain != 0);
  switch (option) {
    case 2 + 1:
      WriteBuffer(b, remain);
      b += remain;
      len -= remain;
      [[fallthrough]];
    case 2 + 0:
      Flush();
      remain = Remain();
      [[fallthrough]];
    case 0 + 1:
      if (remain <= len) {
        const auto buffer_len = len % remain;
        const auto direct_len = len - buffer_len;
        WriteHandle(b, direct_len);
        if (buffer_len == 0) {
          return;
        }
        b += direct_len;
        len = buffer_len;
      }
      WriteBuffer(b, len);
      break;
    default:
      SDB_UNREACHABLE();
  }
}

void FileOutput::WriteHandle(const byte_type* b, size_t len) {
  SDB_ASSERT(_handle);
  _handle->Write(duckdb::QueryContext{}, const_cast<byte_type*>(b), len,
                 _offset);
  _crc.process_bytes(b, len);
  if (_cached) {
    CacheBytes(b, len);
  }
  if (_remote) {
    gUploadingBytes.fetch_add(len, std::memory_order_relaxed);
  }
  _offset += len;
  _unhinted += len;
  if (_unhinted >= kWritebackHintBytes) {
    _handle->HintWriteback(_offset - _unhinted, _unhinted);
    _unhinted = 0;
  }
}

void FileOutput::CacheBytes(const byte_type* b, size_t len) noexcept try {
  _cached->Append(b, len);
} catch (...) {
  FinishCache(false);
}

void FileOutput::FinishCache(bool published) noexcept {
  try {
    if (published) {
      _cached->Finish();
    } else {
      _cached->Abandon();
    }
  } catch (...) {
  }
  _cached.reset();
}

}  // namespace irs
