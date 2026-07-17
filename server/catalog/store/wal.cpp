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

#include "catalog/store/wal.h"

#include <fcntl.h>
#include <unistd.h>

#include <duckdb/common/checksum.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/buffered_file_writer.hpp>
#include <filesystem>
#include <vector>

#include "basics/assert.h"
#include "basics/log.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kWalFile = "catalog.wal";
constexpr std::string_view kWalTmpFile = "catalog.wal.tmp";

constexpr duckdb::FileOpenFlags kAppendFlags =
  duckdb::FileFlags::FILE_FLAGS_WRITE |
  duckdb::FileFlags::FILE_FLAGS_FILE_CREATE |
  duckdb::FileFlags::FILE_FLAGS_APPEND;

bool ReadFrame(duckdb::BufferedFileReader& reader,
               std::vector<uint8_t>& payload) {
  if (reader.FileSize() - reader.CurrentOffset() < 2 * sizeof(uint64_t)) {
    return false;
  }
  auto size = reader.Read<uint64_t>();
  auto checksum = reader.Read<uint64_t>();
  if (reader.FileSize() - reader.CurrentOffset() < size) {
    return false;
  }
  payload.resize(size);
  reader.ReadData(payload.data(), size);
  return duckdb::Checksum(payload.data(), size) == checksum;
}

void WriteFrame(duckdb::BufferedFileWriter& writer,
                std::span<const uint8_t> payload) {
  writer.Write<uint64_t>(payload.size());
  writer.Write<uint64_t>(duckdb::Checksum(payload.data(), payload.size()));
  writer.WriteData(payload.data(), payload.size());
}

// The rename is only durable once the directory entry is; fsync the dir.
void SyncDirectory(const std::string& dir) {
  int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    SDB_FATAL(STARTUP, "catalog wal: open dir '", dir, "': errno=", errno);
  }
  if (::fsync(fd) != 0) {
    ::close(fd);
    SDB_FATAL(STARTUP, "catalog wal: fsync dir '", dir, "': errno=", errno);
  }
  ::close(fd);
}

}  // namespace

CatalogWal::CatalogWal() : _fs{duckdb::FileSystem::CreateLocal()} {}

CatalogWal::~CatalogWal() = default;

uint64_t CatalogWal::ReplayAndTruncate(FrameVisitor replay) {
  if (!_fs->FileExists(_path)) {
    return 0;
  }
  uint64_t valid_end = 0;
  uint64_t replayed = 0;
  {
    duckdb::BufferedFileReader reader{*_fs, _path.c_str()};
    std::vector<uint8_t> payload;
    while (true) {
      const auto frame_start = reader.CurrentOffset();
      if (!ReadFrame(reader, payload)) {
        valid_end = frame_start;
        break;
      }
      replay({payload.data(), payload.size()});
      ++replayed;
      valid_end = reader.CurrentOffset();
    }
    if (valid_end < reader.FileSize()) {
      SDB_WARN(STARTUP, "catalog wal: torn tail, truncating '", _path,
               "' from ", reader.FileSize(), " to ", valid_end, " bytes");
    } else {
      return replayed;
    }
  }
  auto handle = _fs->OpenFile(_path, duckdb::FileFlags::FILE_FLAGS_WRITE);
  _fs->Truncate(*handle, static_cast<int64_t>(valid_end));
  handle->Sync();
  return replayed;
}

void CatalogWal::Open(std::string_view directory, FrameVisitor replay) {
  _dir = std::string{directory};
  std::error_code ec;
  std::filesystem::create_directories(_dir, ec);
  if (ec) {
    SDB_FATAL(STARTUP, "catalog wal: cannot create directory '", _dir,
              "': ", ec.message());
  }
  _path = _fs->JoinPath(_dir, std::string{kWalFile});
  _tmp_path = _fs->JoinPath(_dir, std::string{kWalTmpFile});
  // A leftover temp is an aborted compaction; the live file is intact.
  _fs->TryRemoveFile(_tmp_path);

  const auto replayed = ReplayAndTruncate(replay);

  absl::MutexLock lock{&_mutex};
  _writer =
    std::make_unique<duckdb::BufferedFileWriter>(*_fs, _path, kAppendFlags);
  _size_on_disk.store(_writer->GetFileSize(), std::memory_order_relaxed);
  SDB_INFO(STARTUP, "catalog wal: opened '", _path, "', replayed ", replayed,
           " frame(s), ", _size_on_disk.load(std::memory_order_relaxed),
           " bytes");
}

void CatalogWal::SyncLocked(uint64_t my_seq) {
  struct Wait {
    const uint64_t* synced;
    uint64_t seq;
    const bool* in_progress;
  };
  while (_synced_seq < my_seq) {
    if (_sync_in_progress) {
      Wait wait{&_synced_seq, my_seq, &_sync_in_progress};
      _mutex.Await(absl::Condition(
        +[](Wait* w) { return *w->synced >= w->seq || !*w->in_progress; },
        &wait));
      continue;
    }
    _sync_in_progress = true;
    const auto covers = _written_seq;
    _writer->Flush();
    auto* handle = _writer->handle.get();
    _mutex.Unlock();
    handle->Sync();
    _mutex.Lock();
    _synced_seq = covers;
    _sync_in_progress = false;
    _sync_batches.fetch_add(1, std::memory_order_relaxed);
  }
}

void CatalogWal::Append(std::span<const uint8_t> payload) {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(_writer);
  WriteFrame(*_writer, payload);
  const auto my_seq = ++_written_seq;
  _frames.fetch_add(1, std::memory_order_relaxed);
  _appended_bytes.fetch_add(payload.size(), std::memory_order_relaxed);
  SyncLocked(my_seq);
  _size_on_disk.store(_writer->GetTotalWritten(), std::memory_order_relaxed);
}

void CatalogWal::Compact(absl::FunctionRef<void(FrameSink)> fill) {
  absl::MutexLock lock{&_mutex};
  _mutex.Await(absl::Condition(
    +[](bool* in_progress) { return !*in_progress; }, &_sync_in_progress));

  auto tmp = std::make_unique<duckdb::BufferedFileWriter>(
    *_fs, _tmp_path,
    duckdb::FileFlags::FILE_FLAGS_WRITE |
      duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
  fill([&](std::span<const uint8_t> payload) { WriteFrame(*tmp, payload); });
  tmp->Sync();
  const auto new_size = tmp->GetTotalWritten();
  tmp->Close();
  tmp.reset();

  _writer->Close();
  _writer.reset();
  _fs->MoveFile(_tmp_path, _path);
  SyncDirectory(_dir);
  _writer =
    std::make_unique<duckdb::BufferedFileWriter>(*_fs, _path, kAppendFlags);
  _size_on_disk.store(new_size, std::memory_order_relaxed);
}

void CatalogWal::Close() {
  absl::MutexLock lock{&_mutex};
  if (!_writer) {
    return;
  }
  _mutex.Await(absl::Condition(
    +[](bool* in_progress) { return !*in_progress; }, &_sync_in_progress));
  _writer->Sync();
  _writer->Close();
  _writer.reset();
}

CatalogWal::Stats CatalogWal::GetStats() const {
  return {
    .frames = _frames.load(std::memory_order_relaxed),
    .sync_batches = _sync_batches.load(std::memory_order_relaxed),
    .appended_bytes = _appended_bytes.load(std::memory_order_relaxed),
    .size_on_disk = _size_on_disk.load(std::memory_order_relaxed),
  };
}

}  // namespace sdb::catalog
