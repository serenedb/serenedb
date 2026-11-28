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

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/crc/crc32c.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <array>
#include <cstring>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/path.hpp>
#include <duckdb/common/query_context.hpp>
#include <duckdb_versions/storage_version.hpp>
#include <filesystem>
#include <iresearch/store/storage.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <optional>
#include <string_view>
#include <system_error>
#include <vector>

namespace sdb::catalog {
namespace {

struct Header {
  uint32_t crc = 0;
  uint32_t size = 0;
};

struct SnapshotHeader {
  uint64_t version = 0;
  Lsn covered = 0;
  uint64_t number = 0;
  Lsn begin = 0;
  uint64_t clean = 0;
  std::array<uint8_t, UniqueId::kBytes> server_id{};
  std::array<uint8_t, 32> mock_nonce{};
};

struct LinkedChunk {
  uint64_t number = 0;
  Lsn begin = 0;
};

static_assert(sizeof(Header) == RecordBuffer::kFrameBytes);

constexpr size_t kRecordMinGrowth = size_t{4} << 10;
constexpr size_t kRecordMaxGrowth = size_t{64} << 20;

constexpr size_t kChunkNameBytes = 16;
constexpr std::string_view kReplacingSuffix = ".tmp";
constexpr size_t kIoBytes = size_t{1} << 20;
constexpr auto kSnapshotVersion =
  static_cast<uint64_t>(duckdb::StorageVersion::SDB_LATEST);
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

std::optional<std::vector<uint8_t>> ReadFile(duckdb::FileSystem& fs,
                                             const std::string& path) {
  const auto file =
    fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ |
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
  bool exists = false;
  SnapshotHeader header;
  std::vector<uint8_t> bytes;
  std::vector<std::span<uint8_t>> records;
};

Log::Identity IdentityOf(const SnapshotHeader& header) {
  const std::string_view id{reinterpret_cast<const char*>(header.server_id.data()),
                            header.server_id.size()};
  return {.server_id = *UniqueId::FromBytes(id),
          .mock_nonce = header.mock_nonce};
}

SnapshotHeader CheckHeader(const std::string& path,
                           std::span<const uint8_t> record) {
  if (record.size() >= sizeof(uint64_t)) {
    const auto version = Load<uint64_t>(record, 0);
    if (version != kSnapshotVersion) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("snapshot '", path, "' was written in storage version ",
                version, ", and this build reads only version ",
                kSnapshotVersion));
    }
  }
  if (record.size() != sizeof(SnapshotHeader)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("snapshot '", path, "' is damaged"));
  }
  return Load<SnapshotHeader>(record, 0);
}

SnapshotFile ReadSnapshot(duckdb::FileSystem& fs, const std::string& path) {
  SnapshotFile snapshot;
  auto bytes = ReadFile(fs, path);
  if (!bytes) {
    return snapshot;
  }
  snapshot.exists = true;
  snapshot.bytes = std::move(*bytes);
  const auto parsed = ParseRecords(
    snapshot.bytes, 0, [&](size_t, size_t, std::span<uint8_t> payload) {
      snapshot.records.push_back(payload);
    });
  if (parsed != snapshot.bytes.size() || snapshot.records.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("snapshot '", path, "' is damaged"));
  }
  snapshot.header = CheckHeader(path, snapshot.records.front());
  snapshot.records.erase(snapshot.records.begin());
  return snapshot;
}

class SnapshotWriter {
 public:
  explicit SnapshotWriter(duckdb::FileHandle& file) : _file{file} {}

  void Record(std::span<const uint8_t> payload) {
    SDB_ASSERT(!payload.empty());
    const auto size = Size(payload.size());
    Put(Frame(size, absl::ExtendCrc32c(RecordSeed(_position, size),
                                       View(payload.data(), payload.size()))));
    Put(payload.data(), payload.size());
  }

  void Record(const RecordBuffer& record) {
    const auto size = Size(record.PayloadBytes());
    auto crc = RecordSeed(_position, size);
    record.ForEachPayload([&](std::span<const uint8_t> part) {
      crc = absl::ExtendCrc32c(crc, View(part.data(), part.size()));
    });
    Put(Frame(size, crc));
    record.ForEachPayload(
      [&](std::span<const uint8_t> part) { Put(part.data(), part.size()); });
  }

  uint64_t Finish() {
    Flush();
    return _position;
  }

 private:
  static uint32_t Size(size_t payload) {
    if (payload > kMaxRecordBytes) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                      ERR_MSG("a snapshot record would take ", payload,
                              " bytes, more than the ", kMaxRecordBytes,
                              " a WAL record can hold"));
    }
    return static_cast<uint32_t>(payload);
  }

  static Header Frame(uint32_t size, absl::crc32c_t crc) {
    return {.crc = static_cast<uint32_t>(crc), .size = size};
  }

  void Put(const Header& header) { Put(&header, sizeof(header)); }

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

[[noreturn]] void ThrowDamaged(const std::string& dir, std::string_view why) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("WAL '", dir, "' is damaged: ", why));
}

uint64_t TotalBytes(std::span<const std::vector<uint8_t>> payloads) {
  uint64_t bytes = 0;
  for (const auto& payload : payloads) {
    bytes += payload.size();
  }
  return bytes;
}

std::vector<std::string> LogFiles(duckdb::FileSystem& fs,
                                  const std::string& dir) {
  std::vector<std::string> files;
  fs.ListFiles(dir,
               [&](duckdb::OpenFileInfo& info) {
                 if (!duckdb::FileSystem::IsDirectory(info) &&
                     info.path.size() == kChunkNameBytes &&
                     absl::c_all_of(info.path, absl::ascii_isxdigit)) {
                   files.push_back(std::move(info.path));
                 }
               },
               nullptr, {.strict = true});
  return files;
}

void RemoveFrom(duckdb::FileSystem& fs, const std::string& dir,
                uint64_t number) {
  auto last = number;
  while (fs.FileExists(fs.JoinPath(dir, Log::ChunkName(last)))) {
    ++last;
  }
  while (last != number) {
    fs.TryRemoveFile(fs.JoinPath(dir, Log::ChunkName(--last)));
    fs.SyncDirectory(dir);
  }
}

struct Scanned {
  std::vector<LinkedChunk> chain;
  uint64_t first = 0;
  Lsn first_begin = 0;
  uint64_t next = 0;
  Lsn end = 0;
  Lsn covered = 0;
  bool snapshot = false;
  bool clean = false;
  Log::Identity identity;
  uint64_t snapshot_bytes = 0;
  size_t torn_from = 0;
  size_t torn_to = 0;
};

using ApplyAt = absl::FunctionRef<void(Lsn, Lsn, std::span<uint8_t>)>;

Scanned ScanLog(duckdb::FileSystem& fs, const std::string& dir,
                const std::string& snapshot_path, Log::Apply snapshot_record,
                ApplyAt log_record) {
  auto past = ReadSnapshot(fs, snapshot_path);
  for (const auto record : past.records) {
    snapshot_record(record);
  }
  const auto& snapshot = past.header;
  Scanned scanned{
    .first = snapshot.number,
    .first_begin = snapshot.begin,
    .end = snapshot.covered,
    .covered = snapshot.covered,
    .snapshot = past.exists,
    .identity = IdentityOf(snapshot),
    .snapshot_bytes = past.bytes.size(),
  };
  std::vector<uint8_t> last;
  size_t last_parsed = 0;
  for (auto number = snapshot.number;; ++number) {
    const bool first = scanned.chain.empty();
    const auto begin = first ? snapshot.begin : scanned.end;
    const auto from = static_cast<size_t>(scanned.end - begin);
    auto bytes = ReadFile(fs, fs.JoinPath(dir, Log::ChunkName(number)));
    if (first && !bytes && past.exists && snapshot.clean == 0) {
      ThrowDamaged(dir,
                   absl::StrCat("the snapshot continues in WAL file ", number,
                                ", which is missing: the commits after "
                                "LSN ",
                                snapshot.covered, " are not here"));
    }
    if (first && bytes && bytes->size() < from) {
      ThrowDamaged(
        dir, absl::StrCat("WAL file ", number, " ends before LSN ",
                          snapshot.covered, ", which the snapshot covers"));
    }
    if (!bytes) {
      scanned.next = number;
      break;
    }
    const auto base = begin + from;
    const auto parsed =
      from + ParseRecords(std::span{*bytes}.subspan(from), base,
                          [&](size_t record_begin, size_t record_end,
                              std::span<uint8_t> payload) {
                            log_record(base + record_begin, base + record_end,
                                       payload);
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
  scanned.clean = snapshot.clean != 0 && scanned.end == snapshot.covered;
  return scanned;
}

}  // namespace

std::string Log::ChunkName(uint64_t number) {
  return absl::StrFormat("%016x", number);
}

bool Log::HasSnapshot(duckdb::FileSystem& fs, const std::string& snapshot) {
  return fs.FileExists(snapshot);
}

std::optional<Log::Identity> Log::ReadIdentity(duckdb::FileSystem& fs,
                                               const std::string& snapshot) {
  const auto file =
    fs.OpenFile(snapshot, duckdb::FileFlags::FILE_FLAGS_READ |
                            duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS);
  if (!file) {
    return std::nullopt;
  }
  std::vector<uint8_t> bytes(sizeof(Header) + sizeof(SnapshotHeader));
  std::optional<SnapshotHeader> header;
  if (file->GetFileSize() >= bytes.size()) {
    file->Read(bytes.data(), bytes.size(), 0);
    ParseRecords(bytes, 0, [&](size_t, size_t, std::span<uint8_t> payload) {
      header = CheckHeader(snapshot, payload);
    });
  }
  if (!header) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("snapshot '", snapshot, "' is damaged"));
  }
  return IdentityOf(*header);
}

void Log::Read(duckdb::FileSystem& fs, const std::string& dir,
               const std::string& snapshot, Apply apply) {
  ScanLog(fs, dir, snapshot, apply,
          [&](Lsn, Lsn, std::span<uint8_t> payload) { apply(payload); });
}

void Log::Copy(duckdb::FileSystem& fs, const std::string& from,
               const std::string& to) {
  if (!duckdb::Path::IsRemote(from)) {
    std::error_code ec;
    if (std::filesystem::equivalent(from, to, ec)) {
      return;
    }
    const auto replacing = absl::StrCat(to, kReplacingSuffix);
    fs.TryRemoveFile(replacing);
    std::filesystem::create_hard_link(from, replacing, ec);
    if (ec) {
      THROW_SQL_ERROR(ERR_CODE(irs::pg::ErrcodeForFileAccess(ec.value())),
                      ERR_MSG("could not link \"", from, "\" to \"", replacing,
                              "\": ", ec.message()));
    }
    fs.MoveFile(replacing, to);
    fs.SyncDirectory(fs.JoinPath(to, ".."));
    return;
  }
  auto bytes = ReadFile(fs, from);
  if (!bytes) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_FILE),
      ERR_MSG("could not copy \"", from, "\": it does not exist"));
  }
  irs::WriteWhole(fs, to,
                  std::string_view{reinterpret_cast<const char*>(bytes->data()),
                                   bytes->size()},
                  true);
}

void Log::Discard(duckdb::FileSystem& fs, const std::string& dir,
                  const std::string& snapshot) {
  const auto past = ReadSnapshot(fs, snapshot);
  if (!past.exists) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FILE),
                    ERR_MSG("could not discard the WAL after snapshot \"",
                            snapshot, "\": it does not exist"));
  }
  fs.CreateDirectoryExtended(
    dir, {.mode = duckdb::CreateDirectoryMode::RECURSIVE, .sync = true});
  for (const auto& name : LogFiles(fs, dir)) {
    fs.RemoveFile(fs.JoinPath(dir, name));
  }
  {
    const auto file =
      fs.OpenFile(fs.JoinPath(dir, ChunkName(past.header.number)),
                  duckdb::FileFlags::FILE_FLAGS_WRITE |
                    duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
    ZeroFill(*file, 0, kChunkBytes);
    file->Sync();
  }
  fs.SyncDirectory(dir);
}

Log::Log(duckdb::FileSystem& fs, std::string dir, std::string snapshot)
  : _fs{fs}, _dir{std::move(dir)}, _snapshot_path{std::move(snapshot)} {
  _fs.CreateDirectoryExtended(
    _dir, {.mode = duckdb::CreateDirectoryMode::RECURSIVE, .sync = true});
}

std::string Log::ChunkPath(uint64_t number) const {
  return _fs.JoinPath(_dir, ChunkName(number));
}

duckdb::unique_ptr<duckdb::FileHandle> Log::OpenChunk(uint64_t number,
                                                      bool create) const {
  const auto flags = duckdb::FileFlags::FILE_FLAGS_READ |
                     duckdb::FileFlags::FILE_FLAGS_WRITE |
                     (create ? duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW
                             : duckdb::FileOpenFlags{});
  return _fs.OpenFile(ChunkPath(number), flags);
}

void Log::SyncDirectory() const { _fs.SyncDirectory(_dir); }

Log::Replayed Log::Replay(Apply apply, CatalogPart part) {
  absl::MutexLock files{&_files_mutex};
  std::vector<std::vector<uint8_t>> history;
  const auto scanned = ScanLog(
    _fs, _dir, _snapshot_path,
    [&](std::span<uint8_t> payload) {
      apply(payload);
      if (!part(payload).empty()) {
        history.emplace_back(payload.begin(), payload.end());
      }
    },
    [&](Lsn begin, Lsn end, std::span<uint8_t> payload) {
      apply(payload);
      if (const auto catalog = part(payload); !catalog.empty()) {
        AddTail(begin, end, {catalog.begin(), catalog.end()});
      }
    });
  bool removed = false;
  for (auto number = scanned.first;
       number != 0 && _fs.TryRemoveFile(ChunkPath(number - 1)); --number) {
    removed = true;
  }
  RemoveFrom(_fs, _dir, scanned.next);
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
    chunks.emplace(scanned.first_begin, std::make_shared<Chunk>(Chunk{
                                          .number = next,
                                          .begin = scanned.first_begin,
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
  if (!duckdb::Path::IsRemote(_snapshot_path)) {
    _fs.TryRemoveFile(absl::StrCat(_snapshot_path, kReplacingSuffix));
  }
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
  return {.snapshot = scanned.snapshot,
          .clean = scanned.clean,
          .identity = scanned.identity};
}

Log::Range Log::Reserve(size_t payload_size) {
  SDB_ASSERT(payload_size != 0);
  if (payload_size > kMaxRecordBytes) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                    ERR_MSG("the WAL record of this commit would take ",
                            payload_size, " bytes, more than the ",
                            kMaxRecordBytes, " a WAL record can hold"));
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
      try {
        _fs.TryRemoveFile(ChunkPath(number));
      } catch (const std::exception& e) {
        SDB_WARN(GENERAL, "could not remove '", ChunkPath(number),
                 "': ", duckdb::ErrorData{e}.RawMessage());
      }
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

RecordBuffer::RecordBuffer()
  : _buffer{std::make_unique<message::Buffer>(kRecordMinGrowth,
                                              kRecordMaxGrowth)},
    _writer{std::make_unique<message::Writer>(*_buffer)},
    _frame{_writer->Alloc(kFrameBytes)} {}

RecordBuffer::~RecordBuffer() {
  if (_writer) {
    _writer->Commit(false);
  }
}

void Log::Write(const Range& range, RecordBuffer& record) {
  const auto payload = record.PayloadBytes();
  SDB_ASSERT(range.end - range.begin == sizeof(Header) + payload);
  Header header{.size = static_cast<uint32_t>(payload)};
  auto crc = RecordSeed(range.begin, header.size);
  record.ForEachPayload([&](std::span<const uint8_t> part) {
    crc = absl::ExtendCrc32c(crc, View(part.data(), part.size()));
  });
  header.crc = static_cast<uint32_t>(crc);
  record.SetFrame(std::span<const uint8_t, sizeof(Header)>{
    reinterpret_cast<const uint8_t*>(&header), sizeof(header)});
  try {
    auto offset = range.begin - range.chunk->begin;
    for (const auto part : record.Framed()) {
      range.chunk->file->Write(duckdb::QueryContext{},
                               const_cast<void*>(part.data()), part.size(),
                               offset);
      offset += part.size();
    }
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL, "WAL '", _dir,
              "': a committed record could not be written: ",
              duckdb::ErrorData{e}.RawMessage());
  }
  if (record.CatalogBytes() != 0) {
    AddTail(range.begin, range.end, record.CopyCatalog());
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
  if (_durable.load(std::memory_order_acquire) >= end) {
    return;
  }
  _syncs.Submit(end, [this](auto batch) {
    Lsn target = 0;
    for (const auto* requested : batch) {
      target = std::max(target, *requested);
    }
    for (auto written = _written.load(std::memory_order_acquire);
         written < target; written = _written.load(std::memory_order_acquire)) {
      _written.wait(written, std::memory_order_acquire);
    }
    SyncTo(_written.load(std::memory_order_acquire));
  });
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
    SDB_FATAL(GENERAL, "WAL '", _dir,
              "': committed records could not be synced: ",
              duckdb::ErrorData{e}.RawMessage());
  }
  {
    absl::MutexLock lock{&_mutex};
    _directory_synced = std::max(_directory_synced, directory_changes);
  }
  _durable.store(std::max(durable, target), std::memory_order_release);
}

Lsn Log::Append(RecordBuffer record) {
  const auto range = Reserve(record.PayloadBytes());
  Write(range, record);
  Sync(range.end);
  return range.end;
}

void Log::AddTail(Lsn begin, Lsn end, std::vector<uint8_t> bytes) {
  const auto size = bytes.size();
  {
    absl::MutexLock tail{&_tail_mutex};
    _tail.emplace(begin, TailPart{.end = end, .bytes = std::move(bytes)});
  }
  _tail_bytes.fetch_add(size, std::memory_order_relaxed);
}

std::vector<std::vector<uint8_t>> Log::TakeTail(Lsn bound) {
  std::vector<std::vector<uint8_t>> taken;
  uint64_t bytes = 0;
  absl::MutexLock tail{&_tail_mutex};
  auto it = _tail.begin();
  for (; it != _tail.end() && it->second.end <= bound; ++it) {
    bytes += it->second.bytes.size();
    taken.push_back(std::move(it->second.bytes));
  }
  SDB_ASSERT(it == _tail.end() || it->first >= bound);
  _tail.erase(_tail.begin(), it);
  _tail_bytes.fetch_sub(bytes, std::memory_order_relaxed);
  return taken;
}

void Log::FoldTail(Lsn bound, CatalogPart part, Compact compact) {
  absl::MutexLock files{&_files_mutex};
  auto folded =
    TakeTail(std::min(bound, _written.load(std::memory_order_acquire)));
  if (folded.empty()) {
    return;
  }
  std::vector<std::span<uint8_t>> parts;
  parts.reserve(_history.size() + folded.size());
  for (auto& payload : _history) {
    parts.push_back(part(payload));
  }
  for (auto& catalog : folded) {
    parts.emplace_back(catalog);
  }
  _history = compact(parts);
  _history_bytes.store(TotalBytes(_history), std::memory_order_relaxed);
}

void Log::WriteSnapshot(Lsn covered, std::span<const RecordBuffer> records,
                        CatalogPart part, Compact compact, bool clean) {
  absl::MutexLock files{&_files_mutex};
  SnapshotHeader header{.version = kSnapshotVersion,
                        .covered = covered,
                        .clean = clean ? uint64_t{1} : uint64_t{0},
                        .mock_nonce = _identity.mock_nonce};
  absl::c_copy(_identity.server_id.Bytes(), header.server_id.begin());
  {
    absl::MutexLock lock{&_mutex};
    SDB_ASSERT(covered <= _durable.load(std::memory_order_acquire));
    for (auto it = _chunks.begin(); it != _chunks.end() && it->first <= covered;
         ++it) {
      header.number = it->second->number;
      header.begin = it->first;
    }
  }
  auto logged = TakeTail(covered);
  std::vector<std::span<uint8_t>> parts;
  parts.reserve(_history.size() + logged.size());
  for (auto& payload : _history) {
    parts.push_back(part(payload));
  }
  for (auto& catalog : logged) {
    parts.emplace_back(catalog);
  }
  auto history = compact(parts);
  uint64_t size = 0;
  irs::WriteWhole(
    _fs, _snapshot_path,
    [&](duckdb::FileHandle& file) {
      SnapshotWriter out{file};
      out.Record({reinterpret_cast<const uint8_t*>(&header), sizeof(header)});
      for (const auto& payload : history) {
        out.Record(payload);
      }
      for (const auto& record : records) {
        out.Record(record);
      }
      size = out.Finish();
    },
    true);
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
    _fs.TryRemoveFile(ChunkPath(chunk->number));
  }
  SyncDirectory();
}

void Log::Pin(const std::string& to) {
  absl::MutexLock files{&_files_mutex};
  Copy(_fs, _snapshot_path, to);
}

void Log::Clear() {
  absl::MutexLock files{&_files_mutex};
  {
    absl::MutexLock lock{&_mutex};
    _chunks.clear();
    _next.reset();
    _has_next.store(false, std::memory_order_relaxed);
  }
  for (const auto& name : LogFiles(_fs, _dir)) {
    _fs.TryRemoveFile(_fs.JoinPath(_dir, name));
  }
  SyncDirectory();
}

}  // namespace sdb::catalog
