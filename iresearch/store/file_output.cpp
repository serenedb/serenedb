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

#include <algorithm>
#include <atomic>
#include <cstring>
#include <duckdb/common/query_context.hpp>

#include "iresearch/utils/assert.hpp"
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
                                  const ResourceManagementOptions& rm,
                                  FileTracker* tracker,
                                  const RemoteUpload* remote) {
  auto handle =
    fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                        duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
  rm.transactions->Increase(sizeof(FileOutput));
  absl::Cleanup untrack = [&] noexcept {
    rm.transactions->Decrease(sizeof(FileOutput));
  };
  ptr output{new FileOutput{std::move(handle), rm, tracker, remote}};
  std::move(untrack).Cancel();
  return output;
}

FileOutput::FileOutput(std::unique_ptr<duckdb::FileHandle> handle,
                       const ResourceManagementOptions& rm,
                       FileTracker* tracker, const RemoteUpload* remote)
  : IndexOutput{_buf, std::end(_buf)},
    _rm{rm},
    _handle{std::move(handle)},
    _tracker{tracker} {
  SDB_ASSERT(_handle);
  if (!remote) {
    return;
  }
  _infos = &remote->infos;
  _name = remote->name;
  gUploadingFiles.fetch_add(1, std::memory_order_relaxed);
  if (remote->cache.IsEnabled()) {
    _cache = &remote->cache;
    _cached = _cache->CreateWrittenFile(_handle->GetPath());
  }
}

FileOutput::~FileOutput() {
  SDB_ASSERT(!_handle);
  _rm.transactions->Decrease(sizeof(FileOutput));
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

void FileOutput::BeginTail() {
  if (_infos) {
    _tail.clear();
    _tail_start = Position();
  }
}

void FileOutput::CloseImpl() {
  const auto size = Position();
  absl::Cleanup release = [&] noexcept {
    if (_cached) {
      FinishCache(size, false);
    }
    _handle.reset();
    if (_infos) {
      gUploadingFiles.fetch_sub(1, std::memory_order_relaxed);
      gUploadingBytes.fetch_sub(_offset, std::memory_order_relaxed);
    }
    _crc = Crc32c{};
    _offset = 0;
  };
  Flush();
  _handle->Close();
  if (_cached) {
    FinishCache(size, true);
  }
  if (_infos) {
    _infos->Remember(
      _name, {.size = size,
              .tail = _tail.empty()
                        ? nullptr
                        : std::make_shared<const bstring>(std::move(_tail)),
              .etag = _handle->file_system.GetVersionTag(*_handle)});
  }
  if (_tracker) {
    _tracker->AddBytes(size);
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
  if (_offset + len > _tail_start) [[unlikely]] {
    KeepTail(b, len);
  }
  if (_cached) {
    CacheBytes(b, len);
  }
  if (_infos) {
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
  constexpr uint64_t kBlock = duckdb::BlockCache::BLOCK_SIZE;
  for (auto offset = _offset; len != 0;) {
    const auto block = offset / kBlock;
    if (block != _block_index) {
      _block = _cache->CreateBlock(*_cached, block);
      _block_index = block;
    }
    const auto in_block = offset % kBlock;
    const auto chunk = std::min<uint64_t>(len, kBlock - in_block);
    std::memcpy(_block.GetDataMutable() + in_block, b, chunk);
    b += chunk;
    len -= chunk;
    offset += chunk;
  }
} catch (...) {
  FinishCache(_offset, false);
}

void FileOutput::FinishCache(uint64_t size, bool published) noexcept {
  _block.Reset();
  try {
    _cache->FinishWrittenFile(
      *_cached,
      published || _block_index == std::numeric_limits<uint64_t>::max()
        ? size
        : (_block_index + 1) * duckdb::BlockCache::BLOCK_SIZE,
      published);
  } catch (...) {
  }
  _cached.reset();
}

void FileOutput::KeepTail(const byte_type* b, size_t len) {
  const auto skip =
    _tail_start > _offset ? static_cast<size_t>(_tail_start - _offset) : 0;
  if (_tail.size() + len - skip > kMaxTail) {
    _tail_start = std::numeric_limits<uint64_t>::max();
    bstring{}.swap(_tail);
    return;
  }
  _tail.append(b + skip, len - skip);
}

}  // namespace irs
