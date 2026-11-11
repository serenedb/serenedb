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
#include <absl/random/random.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <array>
#include <cstddef>
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

struct ChunkHeader {
  uint64_t crc = 0;
  uint64_t number = 0;
  Lsn begin = 0;
  uint64_t id = 0;
  uint64_t previous = 0;
  uint64_t sealed = 0;
};

struct SnapshotHeader {
  uint64_t crc = 0;
  Lsn covered = 0;
  uint64_t number = 0;
  uint64_t id = 0;
  uint64_t oldest = 0;
  uint64_t history_bytes = 0;
};

constexpr std::string_view kChunkSuffix = ".log";
constexpr std::string_view kSnapshotFile = "snapshot";
constexpr std::string_view kSnapshotTemp = "snapshot.tmp";
constexpr std::string_view kSpareFile = "spare";
constexpr std::string_view kSpareTemp = "spare.tmp";
constexpr size_t kChunkHeader = sizeof(ChunkHeader);

template<typename T>
std::span<const uint8_t> Bytes(const T& value) {
  return {reinterpret_cast<const uint8_t*>(&value), sizeof(T)};
}

uint32_t Checksum(std::span<const uint8_t> bytes, uint32_t crc = 0) {
  return static_cast<uint32_t>(absl::ExtendCrc32c(
    absl::crc32c_t{crc},
    std::string_view{reinterpret_cast<const char*>(bytes.data()),
                     bytes.size()}));
}

uint32_t RecordChecksum(uint64_t id, Lsn lsn, std::span<const uint8_t> sized) {
  const std::array<uint64_t, 2> seed{id, lsn};
  return Checksum(sized, Checksum(Bytes(seed)));
}

uint32_t HeaderChecksum(ChunkHeader header) {
  header.crc = 0;
  header.sealed = 0;
  return Checksum(Bytes(header));
}

template<typename T>
T Load(std::span<const uint8_t> bytes, size_t at) {
  T value;
  std::memcpy(&value, bytes.data() + at, sizeof(T));
  return value;
}

template<typename T>
void Store(std::vector<uint8_t>& bytes, T value) {
  const auto at = bytes.size();
  bytes.resize(at + sizeof(T));
  std::memcpy(bytes.data() + at, &value, sizeof(T));
}

ChunkHeader NewChunkHeader(uint64_t number, Lsn begin, uint64_t previous) {
  absl::BitGen random;
  ChunkHeader header{
    .number = number,
    .begin = begin,
    .id = absl::Uniform(absl::IntervalClosed, random, uint64_t{1},
                        std::numeric_limits<uint64_t>::max()),
    .previous = previous,
  };
  header.crc = HeaderChecksum(header);
  return header;
}

std::optional<ChunkHeader> LoadChunkHeader(std::span<const uint8_t> bytes,
                                           uint64_t number) {
  if (bytes.size() < kChunkHeader) {
    return std::nullopt;
  }
  const auto header = Load<ChunkHeader>(bytes, 0);
  if (header.crc != HeaderChecksum(header) || header.number != number ||
      header.id == 0) {
    return std::nullopt;
  }
  return header;
}

std::optional<ChunkHeader> ReadChunkHeader(duckdb::FileHandle& file,
                                           uint64_t number) {
  std::array<uint8_t, kChunkHeader> bytes{};
  if (static_cast<uint64_t>(file.GetFileSize()) < bytes.size()) {
    return std::nullopt;
  }
  file.Read(bytes.data(), bytes.size(), 0);
  return LoadChunkHeader(bytes, number);
}

void WriteChunkHeader(duckdb::FileHandle& file, ChunkHeader header) {
  file.Write(duckdb::QueryContext{}, &header, kChunkHeader, 0);
}

void ZeroFill(duckdb::FileHandle& file, uint64_t from, uint64_t to) {
  std::vector<uint8_t> zeros(
    static_cast<size_t>(std::min<uint64_t>(to - from, uint64_t{1} << 20)));
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
size_t ParseRecords(std::span<uint8_t> bytes, uint64_t id, Lsn base,
                    Apply&& apply) {
  size_t end = 0;
  while (end + sizeof(Header) <= bytes.size()) {
    const auto header = Load<Header>(bytes, end);
    const auto begin = end + sizeof(Header);
    if (header.size == 0 || header.size > bytes.size() - begin ||
        RecordChecksum(id, base + end,
                       bytes.subspan(end + sizeof(uint32_t),
                                     sizeof(uint32_t) + header.size)) !=
          header.crc) {
      break;
    }
    apply(end, begin + header.size, bytes.subspan(begin, header.size));
    end = begin + header.size;
  }
  return end;
}

struct SnapshotFile {
  SnapshotHeader header;
  std::vector<uint8_t> bytes;
};

SnapshotFile ReadSnapshot(duckdb::FileSystem& fs,
                          const std::filesystem::path& path) {
  SnapshotFile snapshot;
  auto bytes = ReadFile(fs, path);
  if (!bytes) {
    return snapshot;
  }
  snapshot.bytes = std::move(*bytes);
  const std::span<const uint8_t> view{snapshot.bytes};
  if (view.size() < sizeof(SnapshotHeader)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("log snapshot '", path.string(), "' is damaged"));
  }
  snapshot.header = Load<SnapshotHeader>(view, 0);
  if (snapshot.header.crc != Checksum(view.subspan(sizeof(uint64_t))) ||
      snapshot.header.history_bytes > view.size() - sizeof(SnapshotHeader)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("log snapshot '", path.string(), "' is damaged"));
  }
  return snapshot;
}

template<typename Apply>
void ForEachHistoryEntry(SnapshotFile& snapshot, Apply&& apply) {
  const std::span<uint8_t> bytes{snapshot.bytes};
  const auto records = sizeof(SnapshotHeader) + snapshot.header.history_bytes;
  for (size_t at = sizeof(SnapshotHeader); at < records;) {
    const auto lsn = Load<Lsn>(bytes, at);
    const auto size = Load<uint32_t>(bytes, at + sizeof(Lsn));
    at += sizeof(Lsn) + sizeof(uint32_t);
    apply(lsn, bytes.subspan(at, size));
    at += size;
  }
}

template<typename Apply>
void ForEachSnapshotRecord(SnapshotFile& snapshot, Apply&& apply) {
  const std::span<uint8_t> bytes{snapshot.bytes};
  for (size_t at = sizeof(SnapshotHeader) + snapshot.header.history_bytes;
       at < bytes.size();) {
    const auto size = Load<uint32_t>(bytes, at);
    at += sizeof(uint32_t);
    apply(bytes.subspan(at, size));
    at += size;
  }
}

std::vector<uint8_t> EncodeSnapshot(
  SnapshotHeader header, std::span<const Log::HistoryEntry> history,
  std::span<const std::vector<uint8_t>> records) {
  std::vector<uint8_t> bytes(sizeof(SnapshotHeader));
  for (const auto& entry : history) {
    Store<Lsn>(bytes, entry.lsn);
    Store<uint32_t>(bytes, static_cast<uint32_t>(entry.catalog.size()));
    bytes.insert(bytes.end(), entry.catalog.begin(), entry.catalog.end());
  }
  header.history_bytes = bytes.size() - sizeof(SnapshotHeader);
  for (const auto& record : records) {
    Store<uint32_t>(bytes, static_cast<uint32_t>(record.size()));
    bytes.insert(bytes.end(), record.begin(), record.end());
  }
  std::memcpy(bytes.data(), &header, sizeof(SnapshotHeader));
  header.crc =
    Checksum(std::span<const uint8_t>{bytes}.subspan(sizeof(uint64_t)));
  std::memcpy(bytes.data(), &header.crc, sizeof(uint64_t));
  return bytes;
}

[[noreturn]] void ThrowDamaged(const std::filesystem::path& dir,
                               std::string_view why) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("log '", dir.string(), "' is damaged: ", why));
}

struct Scanned {
  std::vector<ChunkHeader> chain;
  uint64_t oldest = 0;
  uint64_t first = 0;
  uint64_t next = 0;
  Lsn end = 0;
  Lsn covered = 0;
  uint64_t snapshot_bytes = 0;
  size_t torn_from = 0;
  size_t torn_to = 0;
};

Scanned ScanLog(
  duckdb::FileSystem& fs, const std::filesystem::path& dir,
  absl::FunctionRef<void(Lsn, std::span<uint8_t>)> history,
  absl::FunctionRef<void(const Log::Range&, std::span<uint8_t>)> apply) {
  auto past = ReadSnapshot(fs, dir / kSnapshotFile);
  const auto& snapshot = past.header;
  if (!past.bytes.empty()) {
    ForEachHistoryEntry(past, history);
    ForEachSnapshotRecord(past, [&](std::span<uint8_t> payload) {
      apply(Log::Range{.begin = snapshot.covered, .end = snapshot.covered},
            payload);
    });
  }
  Scanned scanned{
    .oldest = snapshot.oldest,
    .first = snapshot.number,
    .end = snapshot.covered,
    .covered = snapshot.covered,
    .snapshot_bytes = past.bytes.size(),
  };
  std::vector<uint8_t> last;
  size_t last_parsed = 0;
  for (auto number = snapshot.number;; ++number) {
    auto bytes = ReadFile(fs, dir / Log::ChunkName(number));
    const auto header = bytes ? LoadChunkHeader(*bytes, number) : std::nullopt;
    size_t from = 0;
    if (scanned.chain.empty()) {
      if (!past.bytes.empty()) {
        if (!header || header->id != snapshot.id ||
            header->begin > snapshot.covered ||
            bytes->size() - kChunkHeader < snapshot.covered - header->begin) {
          ThrowDamaged(dir,
                       absl::StrCat("the snapshot continues in chunk ", number,
                                    ", which is missing or damaged"));
        }
        from = static_cast<size_t>(snapshot.covered - header->begin);
      } else if (header && (header->begin != 0 || header->previous != 0)) {
        ThrowDamaged(dir, absl::StrCat("chunk ", number, " starts at LSN ",
                                       header->begin, " without a snapshot"));
      }
    } else if (header && header->previous == scanned.chain.back().id &&
               header->begin != scanned.end && header->sealed == header->id) {
      ThrowDamaged(
        dir, absl::StrCat("chunk ", number - 1, " ends at LSN ", scanned.end,
                          ", but chunk ", number, " starts at the synced LSN ",
                          header->begin));
    }
    if (!header || (!scanned.chain.empty() &&
                    (header->previous != scanned.chain.back().id ||
                     header->begin != scanned.end))) {
      scanned.next = number;
      break;
    }
    const auto area = std::span{*bytes}.subspan(kChunkHeader);
    const auto base = header->begin + from;
    const auto parsed =
      from +
      ParseRecords(area.subspan(from), header->id, base,
                   [&](size_t at, size_t to, std::span<uint8_t> payload) {
                     apply(Log::Range{.begin = base + at, .end = base + to},
                           payload);
                   });
    scanned.end = header->begin + parsed;
    scanned.chain.push_back(*header);
    last = std::move(*bytes);
    last_parsed = parsed;
  }
  if (!last.empty()) {
    const auto area = std::span{last}.subspan(kChunkHeader);
    auto dirty = area.size();
    while (dirty > last_parsed && area[dirty - 1] == 0) {
      --dirty;
    }
    scanned.torn_from = last_parsed;
    scanned.torn_to = dirty;
  }
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

void Log::Read(
  duckdb::FileSystem& fs, const std::filesystem::path& dir,
  absl::FunctionRef<void(Lsn, std::span<uint8_t>)> history,
  absl::FunctionRef<void(const Range&, std::span<uint8_t>)> apply) {
  ScanLog(fs, dir, history, apply);
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
  auto number = snapshot.number;
  for (;; ++number) {
    const auto path = dir / ChunkName(number);
    const auto file = fs->OpenFile(
      path.string(), duckdb::FileFlags::FILE_FLAGS_READ |
                       duckdb::FileFlags::FILE_FLAGS_WRITE |
                       duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
    const auto header = file ? ReadChunkHeader(*file, number) : std::nullopt;
    if (!header) {
      ThrowDamaged(dir, absl::StrCat("the cut at LSN ", end, " needs chunk ",
                                     number, ", which is missing or damaged"));
    }
    const auto size = static_cast<uint64_t>(file->GetFileSize());
    const auto next_file =
      fs->OpenFile((dir / ChunkName(number + 1)).string(),
                   duckdb::FileFlags::FILE_FLAGS_READ |
                     duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
    const auto next =
      next_file ? ReadChunkHeader(*next_file, number + 1) : std::nullopt;
    if (next && next->begin <= end) {
      if (const auto needed = kChunkHeader + (next->begin - header->begin);
          size < needed) {
        ThrowDamaged(dir, absl::StrCat("chunk \"", path.string(), "\" has ",
                                       size, " bytes, the log needs ", needed));
      }
      continue;
    }
    const auto needed = kChunkHeader + (end - header->begin);
    if (size < needed) {
      ThrowDamaged(dir, absl::StrCat("chunk \"", path.string(), "\" has ", size,
                                     " bytes, the cut needs ", needed));
    }
    file->Truncate(static_cast<int64_t>(needed));
    file->Sync();
    break;
  }
  while (std::filesystem::remove(dir / ChunkName(++number))) {
  }
  utils::file_utils::SyncDirectory(dir);
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

void Log::Replay(
  absl::FunctionRef<void(Lsn, std::span<uint8_t>)> history,
  absl::FunctionRef<void(const Range&, std::span<uint8_t>)> apply) {
  absl::MutexLock files{&_files_mutex};
  const auto scanned = ScanLog(_fs, _dir, history, apply);
  bool removed = false;
  for (auto number = scanned.oldest; number < scanned.first; ++number) {
    removed |= std::filesystem::remove(ChunkPath(number));
  }
  for (auto number = scanned.next; std::filesystem::remove(ChunkPath(number));
       ++number) {
    removed = true;
  }
  absl::btree_map<Lsn, std::shared_ptr<Chunk>> chunks;
  for (const auto& header : scanned.chain) {
    auto file = OpenChunk(header.number, false);
    file->Sync();
    chunks.emplace(header.begin, std::make_shared<Chunk>(Chunk{
                                   .number = header.number,
                                   .begin = header.begin,
                                   .id = header.id,
                                   .sealed = header.previous == 0 ||
                                             header.sealed == header.id,
                                   .file = std::move(file),
                                 }));
  }
  const auto end = scanned.end;
  if (chunks.empty()) {
    const auto header = NewChunkHeader(scanned.next, end, 0);
    auto file = OpenChunk(header.number, true);
    WriteChunkHeader(*file, header);
    chunks.emplace(end, std::make_shared<Chunk>(Chunk{
                          .number = header.number,
                          .begin = end,
                          .id = header.id,
                          .sealed = true,
                          .file = std::move(file),
                        }));
    removed = true;
  }
  auto& last = *chunks.rbegin()->second->file;
  const auto size = static_cast<uint64_t>(last.GetFileSize());
  if (scanned.torn_to > scanned.torn_from) {
    ZeroFill(last, kChunkHeader + scanned.torn_from,
             kChunkHeader + scanned.torn_to);
  }
  if (size < kChunkBytes) {
    ZeroFill(last, size, kChunkBytes);
  }
  if (scanned.torn_to > scanned.torn_from || size < kChunkBytes) {
    last.Sync();
  }
  std::filesystem::remove(_dir / kSpareTemp);
  std::filesystem::remove(_dir / kSnapshotTemp);
  _covered.store(scanned.covered, std::memory_order_release);
  _snapshot_bytes.store(scanned.snapshot_bytes, std::memory_order_relaxed);
  auto spare = _fs.OpenFile((_dir / kSpareFile).string(),
                            duckdb::FileFlags::FILE_FLAGS_READ |
                              duckdb::FileFlags::FILE_FLAGS_WRITE |
                              duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS |
                              duckdb::FileLockType::WRITE_LOCK);
  if (removed) {
    SyncDirectory();
  }
  absl::MutexLock lock{&_mutex};
  _chunks = std::move(chunks);
  _oldest.store(_chunks.begin()->first, std::memory_order_relaxed);
  _newest.store(_chunks.rbegin()->first, std::memory_order_relaxed);
  _spare = std::move(spare);
  _has_spare.store(_spare != nullptr, std::memory_order_relaxed);
  _replayed = end;
  _reserved = end;
  _written.store(end, std::memory_order_release);
  _durable.store(end, std::memory_order_release);
}

Log::Range Log::Reserve(size_t payload_size) {
  absl::MutexLock lock{&_mutex};
  const auto bytes = sizeof(Header) + payload_size;
  SDB_ASSERT(!_chunks.empty());
  const auto current = _chunks.rbegin()->first;
  if (_reserved != current &&
      _reserved - current + bytes > kChunkBytes - kChunkHeader) {
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

Lsn Log::Switch() {
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(!_chunks.empty());
  if (_reserved != _chunks.rbegin()->first) {
    StartChunk();
  }
  return _reserved;
}

void Log::StartChunk() {
  const auto& last = *_chunks.rbegin()->second;
  const auto header = NewChunkHeader(last.number + 1, _reserved, last.id);
  duckdb::unique_ptr<duckdb::FileHandle> file;
  if (auto spare = std::exchange(_spare, nullptr)) {
    _has_spare.store(false, std::memory_order_relaxed);
    std::error_code ec;
    std::filesystem::rename(_dir / kSpareFile, ChunkPath(header.number), ec);
    if (!ec) {
      file = std::move(spare);
    }
  }
  if (!file) {
    file = OpenChunk(header.number, true);
  }
  WriteChunkHeader(*file, header);
  _chunks.emplace(_reserved, std::make_shared<Chunk>(Chunk{
                               .number = header.number,
                               .begin = _reserved,
                               .id = header.id,
                               .file = std::move(file),
                             }));
  _newest.store(_reserved, std::memory_order_relaxed);
  ++_directory_changes;
}

void Log::PrepareSpare() {
  if (HasSpare()) {
    return;
  }
  const auto temp = _dir / kSpareTemp;
  std::filesystem::remove(temp);
  auto file = _fs.OpenFile(temp.string(),
                           duckdb::FileFlags::FILE_FLAGS_READ |
                             duckdb::FileFlags::FILE_FLAGS_WRITE |
                             duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW |
                             duckdb::FileLockType::WRITE_LOCK);
  ZeroFill(*file, 0, kChunkBytes);
  file->Sync();
  std::filesystem::rename(temp, _dir / kSpareFile);
  SyncDirectory();
  absl::MutexLock lock{&_mutex};
  _spare = std::move(file);
  _has_spare.store(true, std::memory_order_relaxed);
}

void Log::Write(const Range& range, std::span<const uint8_t> payload) {
  SDB_ASSERT(range.end - range.begin == sizeof(Header) + payload.size());
  std::vector<uint8_t> record(sizeof(Header) + payload.size());
  const auto size = static_cast<uint32_t>(payload.size());
  std::memcpy(record.data() + sizeof(uint32_t), &size, sizeof(size));
  std::memcpy(record.data() + sizeof(Header), payload.data(), payload.size());
  const auto crc = RecordChecksum(range.chunk->id, range.begin,
                                  std::span{record}.subspan(sizeof(uint32_t)));
  std::memcpy(record.data(), &crc, sizeof(crc));
  try {
    range.chunk->file->Write(duckdb::QueryContext{}, record.data(),
                             record.size(),
                             kChunkHeader + (range.begin - range.chunk->begin));
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
      if (!chunk->sealed) {
        auto sealed = chunk->id;
        chunk->file->Write(duckdb::QueryContext{}, &sealed, sizeof(sealed),
                           offsetof(ChunkHeader, sealed));
        chunk->sealed = true;
      }
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
    header.oldest = _chunks.begin()->second->number;
    for (auto it = _chunks.begin(); it != _chunks.end() && it->first <= covered;
         ++it) {
      const auto next = std::next(it);
      header.number = it->second->number;
      header.id = it->second->id;
      scans.push_back({.chunk = it->second,
                       .end = next == _chunks.end()
                                ? covered
                                : std::min(next->first, covered)});
    }
  }
  auto past = ReadSnapshot(_fs, _dir / kSnapshotFile);
  std::vector<HistoryEntry> entries;
  if (!past.bytes.empty()) {
    ForEachHistoryEntry(past, [&](Lsn lsn, std::span<uint8_t> catalog) {
      entries.push_back({.lsn = lsn, .catalog{catalog.begin(), catalog.end()}});
    });
  }
  for (const auto& [chunk, end] : scans) {
    const auto from = std::max(chunk->begin, past.header.covered);
    if (end <= from) {
      continue;
    }
    std::vector<uint8_t> bytes(static_cast<size_t>(end - from));
    chunk->file->Read(bytes.data(), bytes.size(),
                      kChunkHeader + (from - chunk->begin));
    const auto parsed = ParseRecords(
      bytes, chunk->id, from,
      [&](size_t, size_t to, std::span<uint8_t> payload) {
        const auto catalog = part(payload);
        if (!catalog.empty()) {
          entries.push_back(
            {.lsn = from + to, .catalog{catalog.begin(), catalog.end()}});
        }
      });
    SDB_ENSURE(parsed == bytes.size(), "log '", _dir.string(), "': chunk ",
               chunk->number, " ends early");
  }
  const auto history = compact(std::move(entries));
  auto bytes = EncodeSnapshot(header, history, records);
  const auto temp = _dir / kSnapshotTemp;
  std::filesystem::remove(temp);
  {
    const auto file = _fs.OpenFile(
      temp.string(), duckdb::FileFlags::FILE_FLAGS_WRITE |
                       duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
    file->Write(duckdb::QueryContext{}, bytes.data(), bytes.size(), 0);
    file->Sync();
  }
  std::filesystem::rename(temp, _dir / kSnapshotFile);
  SyncDirectory();
  _snapshot_bytes.store(bytes.size(), std::memory_order_relaxed);
  _covered.store(covered, std::memory_order_release);
}

void Log::DropCovered() {
  absl::MutexLock files{&_files_mutex};
  const auto covered = _covered.load(std::memory_order_acquire);
  std::vector<std::shared_ptr<Chunk>> dropped;
  {
    absl::MutexLock lock{&_mutex};
    for (auto it = _chunks.begin(); it != _chunks.end(); ++it) {
      const auto next = std::next(it);
      if (next == _chunks.end() || next->first > covered) {
        break;
      }
      dropped.push_back(it->second);
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
