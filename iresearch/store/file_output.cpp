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
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/query_context.hpp>
#include <new>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/log.hpp"

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
                                  duckdb::MemoryFileSystem* memory,
                                  const std::string& path,
                                  const Directory::Observers& observers,
                                  bool remote, duckdb::BlockCache* cache) {
  auto handle =
    fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                        duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
  return ptr{
    new FileOutput{std::move(handle), memory, observers, remote, cache}};
}

FileOutput::FileOutput(std::unique_ptr<duckdb::FileHandle> handle,
                       duckdb::MemoryFileSystem* memory,
                       const Directory::Observers& observers, bool remote,
                       duckdb::BlockCache* cache)
  : _handle{std::move(handle)}, _observers{observers}, _remote{remote} {
  SDB_ASSERT(_handle);
  if (memory && &_handle->file_system == memory) {
    _memory = memory;
  }
  if (_remote) {
    gUploadingFiles.fetch_add(1, std::memory_order_relaxed);
    if (cache && cache->IsEnabled()) {
      _cached.emplace(*cache, cache->CreateWrittenFile(_handle->GetPath()));
    }
  }
  if (_memory || _cached) {
    NextWindow();
  } else {
    UseBuffer();
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
  if (!_buf) {
    CommitWindow();
    _begin = _pos = _end = nullptr;
    return;
  }
  if (_pos != _begin) {
    WriteHandle(_begin, static_cast<size_t>(_pos - _begin));
  }
  _pos = _begin;
}

void FileOutput::SetWindow(duckdb::FileWriteWindow window) noexcept {
  _begin = _pos = window.data;
  _end = window.data + window.size;
}

void FileOutput::NextWindow() {
  if (_memory) {
    SetWindow(_memory->GetWriteWindow(*_handle));
    return;
  }
  try {
    SDB_IF_FAILURE("FileOutput::CacheWindow") { throw std::bad_alloc{}; }
    SetWindow(_cached->Window());
    return;
  } catch (...) {
    FinishCache(false);
  }
  UseBuffer();
}

void FileOutput::UseBuffer() {
  _buf = std::make_unique_for_overwrite<byte_type[]>(kBufferSize);
  _begin = _pos = _buf.get();
  _end = _begin + kBufferSize;
}

void FileOutput::CommitWindow() {
  const auto size = static_cast<size_t>(_pos - _begin);
  if (size == 0) {
    return;
  }
  if (_memory) {
    _crc.process_bytes(_begin, size);
    _memory->CommitWriteWindow(*_handle, size);
    _offset += size;
  } else {
    WriteHandle(_begin, size);
    _cached->Commit(size);
  }
  _begin = _pos;
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

void FileOutput::Overflow(const byte_type* data, size_t size) {
  if (!_buf) {
    for (;;) {
      const auto chunk = std::min(size, Remain());
      if (chunk != 0) {
        std::memcpy(_pos, data, chunk);
        _pos += chunk;
        data += chunk;
        size -= chunk;
      }
      if (size == 0) {
        return;
      }
      CommitWindow();
      NextWindow();
      if (_buf) {
        return WriteData(data, size);
      }
      SDB_ASSERT(_begin != _end);
    }
  }
  SDB_ASSERT(size >= Remain());
  if (_pos != _begin) {
    const auto fill = Remain();
    std::memcpy(_pos, data, fill);
    _pos += fill;
    data += fill;
    size -= fill;
    Flush();
  }
  if (const auto direct = size - size % kBufferSize; direct != 0) {
    WriteHandle(data, direct);
    data += direct;
    size -= direct;
  }
  if (size != 0) {
    std::memcpy(_pos, data, size);
    _pos += size;
  }
}

void FileOutput::WriteHandle(const byte_type* b, size_t len) {
  SDB_ASSERT(_handle);
  _handle->Write(duckdb::QueryContext{}, const_cast<byte_type*>(b), len,
                 _offset);
  _crc.process_bytes(b, len);
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

void FileOutput::FinishCache(bool published) noexcept {
  try {
    if (published) {
      auto& fs = _handle->file_system;
      _cached->Finish(fs.GetVersionTag(*_handle),
                      fs.GetLastModifiedTime(*_handle));
    } else {
      _cached->Abandon();
    }
  } catch (...) {
  }
  _cached.reset();
}

}  // namespace irs
