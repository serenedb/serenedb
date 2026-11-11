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

#include "catalog/log.h"

#include <absl/cleanup/cleanup.h>
#include <absl/crc/crc32c.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <array>
#include <cstring>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/query_context.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <optional>
#include <string_view>
#include <vector>

#include "utils/file_utils.h"

namespace sdb::catalog {
namespace {

struct Header {
  uint32_t crc = 0;
  uint32_t size = 0;
};

struct SnapshotHeader {
  Lsn covered = 0;
  uint64_t number = 0;
  Lsn begin = 0;
};

struct LinkedChunk {
  uint64_t number = 0;
  Lsn begin = 0;
};

constexpr std::string_view kChunkSuffix = ".log";
constexpr std::string_view kSnapshotFile = "snapshot";
constexpr std::string_view kSnapshotTemp = "snapshot.tmp";
constexpr size_t kIoBytes = size_t{1} << 20;
constexpr uint64_t kMaxRecordBytes = std::numeric_limits<uint32_t>::max();

std::string_view View(const void* data, size_t size) {
  return {static_cast<const char*>(data), size};
}

absl::crc32c_t RecordSeed(Lsn lsn, uint32_t size) {
  std::array<char, sizeof(Lsn) + sizeof(uint32_t)> seed{};
  std::memcpy(seed.data(), &lsn, sizeof(lsn));
  std::memcpy(seed.data() + sizeof(lsn), &size, sizeof(size));
  return absl::ComputeCrc32c(View(seed.data(), seed.size()));
}

template<typename T>
T Load(std::span<const uint8_t> bytes, size_t at) {
  T value;
  std::memcpy(&value, bytes.data() + at, sizeof(T));
  return value;
}

void ZeroFill(duckdb::FileHandle& file, uint64_t from, uint64_t to) {
  std::vector<uint8_t> zeros(
    static_cast<size_t>(std::min<uint64_t>(to - from, kIoBytes)));
  for (auto at = from; at < to; at += zeros.size()) {
    file.Write(duckdb::QueryContext{}, zeros.data(),
               std::min<uint64_t>(zeros.size(), to - at), at);
  }
}

std::optional<std::vector<uint8_t>> ReadFile(
  duckdb::FileSystem& fs, const std::filesystem::path& path) {
  const auto file = fs.OpenFile(
    path.string(), duckdb::FileFlags::FILE_FLAGS_READ |
                     duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
  if (!file) {
    return std::nullopt;
  }
  std::vector<uint8_t> bytes(static_cast<size_t>(file->GetFileSize()));
  if (!bytes.empty()) {
    file->Read(bytes.data(), bytes.size(), 0);
  }
  return bytes;
}

template<typename Apply>
size_t ParseRecords(std::span<uint8_t> bytes, Lsn base, Apply&& apply) {
  size_t end = 0;
  while (end + sizeof(Header) <= bytes.size()) {
    const auto header = Load<Header>(bytes, end);
    const auto begin = end + sizeof(Header);
    if (header.size == 0 || header.size > bytes.size() - begin) {
      break;
    }
    const auto payload = bytes.subspan(begin, header.size);
    if (static_cast<uint32_t>(absl::ExtendCrc32c(
          RecordSeed(base + end, header.size),
          View(payload.data(), payload.size()))) != header.crc) {
      break;
    }
    apply(end, begin + header.size, payload);
    end = begin + header.size;
  }
  return end;
}

struct SnapshotFile {
  SnapshotHeader header;
  std::vector<uint8_t> bytes;
  std::vector<std::span<uint8_t>> records;
};

SnapshotFile ReadSnapshot(duckdb::FileSystem& fs,
                          const std::filesystem::path& path) {
  SnapshotFile snapshot;
  auto bytes = ReadFile(fs, path);
  if (!bytes) {
    return snapshot;
  }
  snapshot.bytes = std::move(*bytes);
  const auto parsed = ParseRecords(
    snapshot.bytes, 0, [&](size_t, size_t, std::span<uint8_t> payload) {
      snapshot.records.push_back(payload);
    });
  if (parsed != snapshot.bytes.size() || snapshot.records.empty() ||
      snapshot.records.front().size() != sizeof(SnapshotHeader)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("log snapshot '", path.string(), "' is damaged"));
  }
  snapshot.header = Load<SnapshotHeader>(snapshot.records.front(), 0);
  snapshot.records.erase(snapshot.records.begin());
  return snapshot;
}

class SnapshotWriter {
 public:
  explicit SnapshotWriter(duckdb::FileHandle& file) : _file{file} {}

  void Record(std::span<const uint8_t> payload) {
    SDB_ASSERT(!payload.empty() && payload.size() <= kMaxRecordBytes);
    Header header{.size = static_cast<uint32_t>(payload.size())};
    header.crc = static_cast<uint32_t>(
      absl::ExtendCrc32c(RecordSeed(_position, header.size),
                         View(payload.data(), payload.size())));
    Put(&header, sizeof(header));
    Put(payload.data(), payload.size());
  }

  uint64_t Finish() {
    Flush();
    return _position;
  }

 private:
  void Put(const void* data, size_t size) {
    const auto* from = static_cast<const uint8_t*>(data);
    _position += size;
    while (size != 0) {
      if (_used == kIoBytes) {
        Flush();
      }
      const auto take = std::min(size, kIoBytes - _used);
      std::memcpy(_buffer.get() + _used, from, take);
      _used += take;
      from += take;
      size -= take;
    }
  }

  void Flush() {
    if (_used == 0) {
      return;
    }
    _file.Write(duckdb::QueryContext{}, _buffer.get(), _used, _offset);
    _offset += _used;
    _used = 0;
  }

  duckdb::FileHandle& _file;
  std::unique_ptr<uint8_t[]> _buffer =
    std::make_unique_for_overwrite<uint8_t[]>(kIoBytes);
  size_t _used = 0;
  uint64_t _offset = 0;
  uint64_t _position = 0;
};

[[noreturn]] void ThrowDamaged(const std::filesystem::path& dir,
                               std::string_view why) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("log '", dir.string(), "' is damaged: ", why));
}

[[noreturn]] void ThrowIncomplete(const std::filesystem::path& dir, Lsn reached,
                                  Lsn end) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("the copy is incomplete: the log in '", dir.string(),
                          "' is shorter than the backup: it ends at LSN ",
                          reached, ", before the cut at LSN ", end));
}

uint64_t TotalBytes(std::span<const std::vector<uint8_t>> payloads) {
  uint64_t bytes = 0;
  for (const auto& payload : payloads) {
    bytes += payload.size();
  }
  return bytes;
}

void RemoveFrom(const std::filesystem::path& dir, uint64_t number) {
  auto last = number;
  for (std::error_code ec;
       std::filesystem::exists(dir / Log::ChunkName(last), ec);) {
    ++last;
  }
  while (last != number) {
    std::filesystem::remove(dir / Log::ChunkName(--last));
    utils::file_utils::SyncDirectory(dir);
  }
}

struct Scanned {
  std::vector<LinkedChunk> chain;
  uint64_t first = 0;
  uint64_t next = 0;
  Lsn end = 0;
  Lsn covered = 0;
  uint64_t snapshot_bytes = 0;
  size_t torn_from = 0;
  size_t torn_to = 0;
};

Scanned ScanLog(duckdb::FileSystem& fs, const std::filesystem::path& dir,
                Log::Apply snapshot_record, Log::Apply log_record) {
  auto past = ReadSnapshot(fs, dir / kSnapshotFile);
  for (const auto record : past.records) {
    snapshot_record(record);
  }
  const auto& snapshot = past.header;
  Scanned scanned{
    .first = snapshot.number,
    .end = snapshot.covered,
    .covered = snapshot.covered,
    .snapshot_bytes = past.bytes.size(),
  };
  std::vector<uint8_t> last;
  size_t last_parsed = 0;
  for (auto number = snapshot.number;; ++number) {
    const bool first = scanned.chain.empty();
    const auto begin = first ? snapshot.begin : scanned.end;
    const auto from = static_cast<size_t>(scanned.end - begin);
    auto bytes = ReadFile(fs, dir / Log::ChunkName(number));
    if (first && (bytes ? bytes->size() < from : from != 0)) {
      ThrowDamaged(dir, absl::StrCat("the snapshot continues in chunk ", number,
                                     ", which is missing or ends before LSN ",
                                     snapshot.covered));
    }
    if (!bytes) {
      scanned.next = number;
      break;
    }
    const auto base = begin + from;
    const auto parsed =
      from + ParseRecords(std::span{*bytes}.subspan(from), base,
                          [&](size_t, size_t, std::span<uint8_t> payload) {
                            log_record(payload);
                          });
    if (!first && parsed == 0) {
      scanned.next = number;
      break;
    }
    scanned.end = begin + parsed;
    scanned.chain.push_back({.number = number, .begin = begin});
    last = std::move(*bytes);
    last_parsed = parsed;
  }
  auto dirty = last.size();
  while (dirty > last_parsed && last[dirty - 1] == 0) {
    --dirty;
  }
  scanned.torn_from = last_parsed;
  scanned.torn_to = std::max(dirty, last_parsed);
  return scanned;
}

}  // namespace

std::string Log::ChunkName(uint64_t number) {
  return absl::StrFormat("%016x%s", number, kChunkSuffix);
}

std::vector<duckdb::unique_ptr<duckdb::FileHandle>> Log::LockChunks(
  duckdb::FileSystem& fs, const std::filesystem::path& dir) {
  const auto snapshot = ReadSnapshot(fs, dir / kSnapshotFile);
  std::vector<duckdb::unique_ptr<duckdb::FileHandle>> locks;
  for (auto number = snapshot.header.number;; ++number) {
    auto lock = fs.OpenFile((dir / ChunkName(number)).string(),
                            duckdb::FileFlags::FILE_FLAGS_READ |
                              duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS |
                              duckdb::FileLockType::READ_LOCK);
    if (!lock) {
      return locks;
    }
    locks.push_back(std::move(lock));
  }
}

void Log::Read(duckdb::FileSystem& fs, const std::filesystem::path& dir,
               Apply apply) {
  ScanLog(fs, dir, apply, apply);
}

void Log::Move(const std::filesystem::path& from,
               const std::filesystem::path& to) {
  const auto fs = duckdb::FileSystem::CreateLocal();
  const auto snapshot = ReadSnapshot(*fs, from / kSnapshotFile);
  const auto move = [&](std::string_view name) {
    std::error_code ec;
    std::filesystem::rename(from / name, to / name, ec);
    if (ec) {
      utils::file_utils::ThrowFileError("move", (from / name).string(), ec);
    }
  };
  std::vector<std::string> chunks;
  for (auto number = snapshot.header.number;; ++number) {
    auto name = ChunkName(number);
    std::error_code ec;
    if (!std::filesystem::exists(from / name, ec)) {
      break;
    }
    chunks.push_back(std::move(name));
  }
  for (auto it = chunks.rbegin(); it != chunks.rend(); ++it) {
    move(*it);
  }
  if (!snapshot.bytes.empty()) {
    move(kSnapshotFile);
  }
  utils::file_utils::SyncDirectory(to);
}

void Log::Cut(const std::filesystem::path& dir, Lsn end) {
  const auto fs = duckdb::FileSystem::CreateLocal();
  const auto snapshot = ReadSnapshot(*fs, dir / kSnapshotFile).header;
  if (snapshot.covered > end) {
    ThrowDamaged(dir, absl::StrCat("its snapshot covers LSN ", snapshot.covered,
                                   ", past the cut at LSN ", end));
  }
  auto begin = snapshot.begin;
  auto reached = snapshot.covered;
  for (auto number = snapshot.number;; ++number) {
    const auto path = dir / ChunkName(number);
    auto bytes = ReadFile(*fs, path);
    const auto from = static_cast<size_t>(reached - begin);
    if (!bytes && reached == end) {
      RemoveFrom(dir, number);
      return;
    }
    if (!bytes || bytes->size() < from) {
      ThrowIncomplete(dir, reached, end);
    }
    const auto limit = std::min<size_t>(bytes->size(), end - begin);
    reached =
      begin + from +
      ParseRecords(std::span{*bytes}.subspan(from, limit - from), begin + from,
                   [](size_t, size_t, std::span<uint8_t>) {});
    if (reached == end) {
      const auto file =
        fs->OpenFile(path.string(), duckdb::FileFlags::FILE_FLAGS_WRITE);
      file->Truncate(static_cast<int64_t>(end - begin));
      file->Sync();
      RemoveFrom(dir, number + 1);
      return;
    }
    if (reached == begin && number != snapshot.number) {
      ThrowIncomplete(dir, reached, end);
    }
    begin = reached;
  }
}

Log::Log(duckdb::FileSystem& fs, std::filesystem::path dir)
  : _fs{fs}, _dir{std::move(dir)} {
  if (std::filesystem::create_directories(_dir)) {
    utils::file_utils::SyncDirectory(_dir.parent_path());
  }
}

std::filesystem::path Log::ChunkPath(uint64_t number) const {
  return _dir / ChunkName(number);
}

duckdb::unique_ptr<duckdb::FileHandle> Log::OpenChunk(uint64_t number,
                                                      bool create) const {
  const auto flags = duckdb::FileFlags::FILE_FLAGS_READ |
                     duckdb::FileFlags::FILE_FLAGS_WRITE |
                     duckdb::FileLockType::WRITE_LOCK |
                     (create ? duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW
                             : duckdb::FileOpenFlags{});
  return _fs.OpenFile(ChunkPath(number).string(), flags);
}

void Log::SyncDirectory() const { utils::file_utils::SyncDirectory(_dir); }

void Log::Replay(Apply apply, CatalogPart part) {
  absl::MutexLock files{&_files_mutex};
  std::vector<std::vector<uint8_t>> history;
  const auto scanned = ScanLog(
    _fs, _dir,
    [&](std::span<uint8_t> payload) {
      apply(payload);
      if (!part(payload).empty()) {
        history.emplace_back(payload.begin(), payload.end());
      }
    },
    apply);
  bool removed = false;
  for (auto number = scanned.first;
       number != 0 && std::filesystem::remove(ChunkPath(number - 1));
       --number) {
    removed = true;
  }
  RemoveFrom(_dir, scanned.next);
  absl::btree_map<Lsn, std::shared_ptr<Chunk>> chunks;
  for (const auto& linked : scanned.chain) {
    auto file = OpenChunk(linked.number, false);
    file->Sync();
    chunks.emplace(linked.begin, std::make_shared<Chunk>(Chunk{
                                   .number = linked.number,
                                   .begin = linked.begin,
                                   .file = std::move(file),
                                 }));
  }
  const auto end = scanned.end;
  auto next = scanned.next;
  if (chunks.empty()) {
    chunks.emplace(end, std::make_shared<Chunk>(Chunk{
                          .number = next,
                          .begin = end,
                          .file = OpenChunk(next, true),
                        }));
    ++next;
    removed = true;
  }
  auto& last = *chunks.rbegin()->second->file;
  const auto size = static_cast<uint64_t>(last.GetFileSize());
  if (scanned.torn_to > scanned.torn_from) {
    ZeroFill(last, scanned.torn_from, scanned.torn_to);
  }
  if (size < kChunkBytes) {
    ZeroFill(last, size, kChunkBytes);
  }
  if (scanned.torn_to > scanned.torn_from || size < kChunkBytes) {
    last.Sync();
  }
  std::filesystem::remove(_dir / kSnapshotTemp);
  _covered.store(scanned.covered, std::memory_order_release);
  _covered_number.store(scanned.first, std::memory_order_relaxed);
  _snapshot_bytes.store(scanned.snapshot_bytes, std::memory_order_relaxed);
  _history_bytes.store(TotalBytes(history), std::memory_order_relaxed);
  _history = std::move(history);
  if (removed) {
    SyncDirectory();
  }
  absl::MutexLock lock{&_mutex};
  _next_number = next;
  _oldest.store(chunks.begin()->first, std::memory_order_relaxed);
  _newest.store(chunks.rbegin()->first, std::memory_order_relaxed);
  _chunks = std::move(chunks);
  _replayed = end;
  _reserved = end;
  _written.store(end, std::memory_order_release);
  _durable.store(end, std::memory_order_release);
}

Log::Range Log::Reserve(size_t payload_size) {
  SDB_ASSERT(payload_size != 0);
  if (payload_size > kMaxRecordBytes) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    ERR_MSG("the log record of this commit would take ",
                            payload_size, " bytes, more than the ",
                            kMaxRecordBytes, " a log record can hold"));
  }
  absl::MutexLock lock{&_mutex};
  const auto bytes = sizeof(Header) + payload_size;
  SDB_ASSERT(!_chunks.empty());
  for (;;) {
    const auto current = _chunks.rbegin()->first;
    if (_reserved == current || _reserved - current + bytes <= kChunkBytes) {
      break;
    }
    if (_preparing) {
      _mutex.Await(absl::Condition{this, &Log::NotPreparing});
      continue;
    }
    StartChunk();
  }
  SDB_ASSERT(_reserved_seq - _written_seq.load(std::memory_order_relaxed) <
             kWrittenRing);
  Range range{.begin = _reserved,
              .end = _reserved + bytes,
              .seq = _reserved_seq++,
              .chunk = _chunks.rbegin()->second};
  _reserved = range.end;
  return range;
}

void Log::StartChunk() {
  const auto number = _next_number;
  if (_directory_changes != _directory_synced) {
    SyncDirectory();
    _directory_synced = _directory_changes;
  }
  auto file = std::exchange(_next, nullptr);
  _has_next.store(false, std::memory_order_relaxed);
  if (!file) {
    file = OpenChunk(number, true);
    ++_directory_changes;
  }
  _chunks.emplace(_reserved, std::make_shared<Chunk>(Chunk{
                               .number = number,
                               .begin = _reserved,
                               .file = std::move(file),
                             }));
  _next_number = number + 1;
  _newest.store(_reserved, std::memory_order_relaxed);
}

void Log::PrepareNext() {
  uint64_t number = 0;
  {
    absl::MutexLock lock{&_mutex};
    if (_next || _preparing) {
      return;
    }
    _preparing = true;
    number = _next_number;
  }
  duckdb::unique_ptr<duckdb::FileHandle> file;
  bool ready = false;
  absl::Cleanup done = [&] {
    if (!ready && file) {
      file.reset();
      std::error_code ec;
      std::filesystem::remove(ChunkPath(number), ec);
    }
    absl::MutexLock lock{&_mutex};
    _preparing = false;
    if (ready) {
      _next = std::move(file);
      _has_next.store(true, std::memory_order_relaxed);
    }
  };
  file = OpenChunk(number, true);
  ZeroFill(*file, 0, kChunkBytes);
  file->Sync();
  SyncDirectory();
  ready = true;
}

void Log::Write(const Range& range, std::span<const uint8_t> payload) {
  SDB_ASSERT(range.end - range.begin == sizeof(Header) + payload.size());
  const auto bytes = sizeof(Header) + payload.size();
  const auto record = std::make_unique_for_overwrite<uint8_t[]>(bytes);
  Header header{.size = static_cast<uint32_t>(payload.size())};
  header.crc = static_cast<uint32_t>(
    absl::MemcpyCrc32c(record.get() + sizeof(Header), payload.data(),
                       payload.size(), RecordSeed(range.begin, header.size)));
  std::memcpy(record.get(), &header, sizeof(Header));
  try {
    range.chunk->file->Write(duckdb::QueryContext{}, record.get(), bytes,
                             range.begin - range.chunk->begin);
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL, "log '", _dir.string(),
              "': a committed record could not be written: ",
              duckdb::ErrorData{e}.RawMessage());
  }
  auto& slot = _written_ring[range.seq % kWrittenRing];
  slot.end.store(range.end, std::memory_order_relaxed);
  slot.seq.store(range.seq + 1, std::memory_order_seq_cst);
  AdvanceWritten();
}

void Log::AdvanceWritten() noexcept {
  auto seq = _written_seq.load(std::memory_order_seq_cst);
  bool advanced = false;
  for (;;) {
    const auto& slot = _written_ring[seq % kWrittenRing];
    if (slot.seq.load(std::memory_order_seq_cst) != seq + 1) {
      break;
    }
    const auto end = slot.end.load(std::memory_order_relaxed);
    if (!_written_seq.compare_exchange_weak(seq, seq + 1,
                                            std::memory_order_seq_cst)) {
      continue;
    }
    ++seq;
    auto written = _written.load(std::memory_order_relaxed);
    while (written < end && !_written.compare_exchange_weak(
                              written, end, std::memory_order_release,
                              std::memory_order_relaxed)) {
    }
    advanced = true;
  }
  if (advanced) {
    _written.notify_all();
  }
}

void Log::Sync(Lsn end) {
  for (;;) {
    if (_durable.load(std::memory_order_acquire) >= end) {
      return;
    }
    const auto syncs = _syncs.load(std::memory_order_acquire);
    if (_syncing.exchange(true, std::memory_order_acq_rel)) {
      _syncs.wait(syncs, std::memory_order_acquire);
      continue;
    }
    absl::Cleanup done = [this] {
      _syncing.store(false, std::memory_order_release);
      _syncs.fetch_add(1, std::memory_order_release);
      _syncs.notify_all();
    };
    for (auto written = _written.load(std::memory_order_acquire); written < end;
         written = _written.load(std::memory_order_acquire)) {
      _written.wait(written, std::memory_order_acquire);
    }
    SyncTo(_written.load(std::memory_order_acquire));
  }
}

void Log::SyncTo(Lsn target) {
  const auto durable = _durable.load(std::memory_order_relaxed);
  if (target <= durable) {
    return;
  }
  std::vector<std::shared_ptr<Chunk>> chunks;
  uint64_t directory_changes = 0;
  bool directory = false;
  {
    absl::MutexLock lock{&_mutex};
    auto it = _chunks.upper_bound(durable);
    if (it != _chunks.begin()) {
      --it;
    }
    for (; it != _chunks.end() && it->first < target; ++it) {
      chunks.push_back(it->second);
    }
    directory_changes = _directory_changes;
    directory = directory_changes != _directory_synced;
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_wal_sync");
  try {
    if (directory) {
      SyncDirectory();
    }
    for (const auto& chunk : chunks) {
      chunk->file->Sync();
    }
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL, "log '", _dir.string(),
              "': committed records could not be synced: ",
              duckdb::ErrorData{e}.RawMessage());
  }
  {
    absl::MutexLock lock{&_mutex};
    _directory_synced = std::max(_directory_synced, directory_changes);
  }
  _durable.store(std::max(durable, target), std::memory_order_release);
}

Lsn Log::Append(std::span<const uint8_t> payload) {
  const auto range = Reserve(payload.size());
  Write(range, payload);
  Sync(range.end);
  return range.end;
}

void Log::WriteSnapshot(Lsn covered,
                        std::span<const std::vector<uint8_t>> records,
                        CatalogPart part, Compact compact) {
  struct Scan {
    std::shared_ptr<Chunk> chunk;
    Lsn end = 0;
  };
  absl::MutexLock files{&_files_mutex};
  std::vector<Scan> scans;
  SnapshotHeader header{.covered = covered};
  {
    absl::MutexLock lock{&_mutex};
    SDB_ASSERT(covered <= _durable.load(std::memory_order_acquire));
    for (auto it = _chunks.begin(); it != _chunks.end() && it->first <= covered;
         ++it) {
      const auto next = std::next(it);
      header.number = it->second->number;
      header.begin = it->first;
      scans.push_back({.chunk = it->second,
                       .end = next == _chunks.end()
                                ? covered
                                : std::min(next->first, covered)});
    }
  }
  const auto past = _covered.load(std::memory_order_acquire);
  std::vector<std::vector<uint8_t>> logged;
  for (const auto& [chunk, end] : scans) {
    const auto from = std::max(chunk->begin, past);
    if (end <= from) {
      continue;
    }
    std::vector<uint8_t> bytes(static_cast<size_t>(end - from));
    chunk->file->Read(bytes.data(), bytes.size(), from - chunk->begin);
    const auto parsed = ParseRecords(
      bytes, from, [&](size_t, size_t, std::span<uint8_t> payload) {
        if (const auto catalog = part(payload); !catalog.empty()) {
          logged.emplace_back(catalog.begin(), catalog.end());
        }
      });
    SDB_ENSURE(parsed == bytes.size(), "log '", _dir.string(), "': chunk ",
               chunk->number, " ends early");
  }
  std::vector<std::span<uint8_t>> parts;
  parts.reserve(_history.size() + logged.size());
  for (auto& payload : _history) {
    parts.push_back(part(payload));
  }
  for (auto& catalog : logged) {
    parts.emplace_back(catalog);
  }
  auto history = compact(parts);
  const auto temp = _dir / kSnapshotTemp;
  std::filesystem::remove(temp);
  uint64_t size = 0;
  {
    const auto file = _fs.OpenFile(
      temp.string(), duckdb::FileFlags::FILE_FLAGS_WRITE |
                       duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
    SnapshotWriter out{*file};
    out.Record({reinterpret_cast<const uint8_t*>(&header), sizeof(header)});
    for (const auto& payload : history) {
      out.Record(payload);
    }
    for (const auto& record : records) {
      out.Record(record);
    }
    size = out.Finish();
    file->Sync();
  }
  std::filesystem::rename(temp, _dir / kSnapshotFile);
  SyncDirectory();
  _history_bytes.store(TotalBytes(history), std::memory_order_relaxed);
  _history = std::move(history);
  _snapshot_bytes.store(size, std::memory_order_relaxed);
  _covered_number.store(header.number, std::memory_order_relaxed);
  _covered.store(covered, std::memory_order_release);
}

void Log::DropCovered() {
  absl::MutexLock files{&_files_mutex};
  const auto first = _covered_number.load(std::memory_order_relaxed);
  std::vector<std::shared_ptr<Chunk>> dropped;
  {
    absl::MutexLock lock{&_mutex};
    for (const auto& [begin, chunk] : _chunks) {
      if (chunk->number >= first) {
        break;
      }
      dropped.push_back(chunk);
    }
    for (const auto& chunk : dropped) {
      _chunks.erase(chunk->begin);
    }
    _oldest.store(_chunks.begin()->first, std::memory_order_relaxed);
  }
  if (dropped.empty()) {
    return;
  }
  for (const auto& chunk : dropped) {
    std::filesystem::remove(ChunkPath(chunk->number));
  }
  SyncDirectory();
}

Lsn Log::Link(const std::filesystem::path& dir) {
  absl::MutexLock files{&_files_mutex};
  Lsn durable = 0;
  std::vector<std::string> names;
  if (SnapshotBytes() != 0) {
    names.emplace_back(kSnapshotFile);
  }
  {
    absl::MutexLock lock{&_mutex};
    durable = _durable.load(std::memory_order_acquire);
    for (const auto& [_, chunk] : _chunks) {
      names.push_back(ChunkName(chunk->number));
    }
  }
  std::filesystem::create_directories(dir);
  for (const auto& name : names) {
    std::error_code ec;
    std::filesystem::create_hard_link(_dir / name, dir / name, ec);
    if (ec) {
      utils::file_utils::ThrowFileError("link", (dir / name).string(), ec);
    }
  }
  utils::file_utils::SyncDirectory(dir);
  return durable;
}

}  // namespace sdb::catalog
