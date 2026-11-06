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

#include "iresearch/store/storage.hpp"
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

std::unique_ptr<FileOutput> FileOutput::Open(duckdb::FileSystem& fs,
                                             std::string_view root,
                                             std::string_view name,
                                             const FileObservers& observers,
                                             bool keep_cached) {
  for (auto* observer : observers) {
    observer->Created(name);
  }
  absl::Cleanup abandon = [&] {
    for (auto* observer : observers) {
      observer->Removed(name);
    }
  };
  const auto path = PathOf(root, name);
  const bool remote = StorageKindOf(root) == StorageKind::Remote;
  std::unique_ptr<FileOutput> output;
  if (auto writer = fs.OpenFileWriter(path, keep_cached)) {
    output.reset(new FileOutput{std::move(writer), observers, remote});
  } else {
    output.reset(new FileOutput{
      fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_WRITE |
                          duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW),
      observers, remote});
  }
  std::move(abandon).Cancel();
  return output;
}

FileOutput::FileOutput(std::unique_ptr<duckdb::FileWriter> writer,
                       const FileObservers& observers, bool remote)
  : _writer{std::move(writer)}, _observers{observers}, _remote{remote} {
  NextWindow();
  if (_remote) {
    gUploadingFiles.fetch_add(1, std::memory_order_relaxed);
  }
}

FileOutput::FileOutput(std::unique_ptr<duckdb::FileHandle> handle,
                       const FileObservers& observers, bool remote)
  : _handle{std::move(handle)},
    _buf{std::make_unique_for_overwrite<byte_type[]>(kBufferSize)},
    _observers{observers},
    _remote{remote} {
  SDB_ASSERT(_handle);
  _begin = _pos = _buf.get();
  _end = _begin + kBufferSize;
  if (_remote) {
    gUploadingFiles.fetch_add(1, std::memory_order_relaxed);
  }
}

FileOutput::~FileOutput() {
  if (!_writer && !_handle) {
    return;
  }
  if (_writer) {
    _writer->Abort();
  } else {
    try {
      _handle->AbortWrite();
    } catch (const std::exception& e) {
      SDB_WARN(IRESEARCH, "Failed to abandon the write of '",
               _handle->GetPath(), "': ", duckdb::ErrorData{e}.RawMessage());
    }
  }
  Release();
}

void FileOutput::Flush() {
  if (_writer) {
    CommitWindow();
    _begin = _pos = _end = nullptr;
    return;
  }
  if (_pos != _begin) {
    WriteHandle(_begin, static_cast<size_t>(_pos - _begin));
  }
  _pos = _begin;
}

void FileOutput::NextWindow() {
  const auto window = _writer->Window();
  _begin = _pos = window.data;
  _end = _begin + window.size;
}

void FileOutput::CommitWindow() {
  const auto size = static_cast<size_t>(_pos - _begin);
  if (size == 0) {
    return;
  }
  _crc.process_bytes(_begin, size);
  _writer->Commit(size);
  Advance(size);
  _begin = _pos;
}

uint32_t FileOutput::Checksum() {
  Flush();
  return std::exchange(_crc, Crc32c{}).checksum();
}

void FileOutput::Release() noexcept {
  _writer.reset();
  _handle.reset();
  if (_remote) {
    gUploadingFiles.fetch_sub(1, std::memory_order_relaxed);
    gUploadingBytes.fetch_sub(_offset, std::memory_order_relaxed);
  }
}

void FileOutput::Close() {
  if (!_writer && !_handle) {
    return;
  }
  const auto size = Position();
  absl::Cleanup release = [&] noexcept { Release(); };
  Flush();
  if (_writer) {
    _writer->Close();
  } else {
    _handle->Close();
  }
  for (auto* observer : _observers) {
    observer->Written(size);
  }
}

void FileOutput::Overflow(const byte_type* data, size_t size) {
  if (_writer) {
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
  Advance(len);
  _unhinted += len;
  if (_unhinted >= kWritebackHintBytes) {
    _handle->HintWriteback(_offset - _unhinted, _unhinted);
    _unhinted = 0;
  }
}

void FileOutput::Advance(size_t size) noexcept {
  if (_remote) {
    gUploadingBytes.fetch_add(size, std::memory_order_relaxed);
  }
  _offset += size;
}

}  // namespace irs
