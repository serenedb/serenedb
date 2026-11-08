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
#include <absl/crc/crc32c.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>
#include <absl/strings/strip.h>

#include <cstring>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/query_context.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
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

constexpr std::string_view kChunkSuffix = ".log";
constexpr std::string_view kHistoryTemp = "history.tmp";
constexpr std::string_view kSpareFile = "spare";
constexpr std::string_view kSpareTemp = "spare.tmp";
constexpr size_t kHistoryHeader = sizeof(uint32_t) + sizeof(Lsn);

uint32_t Checksum(std::span<const uint8_t> bytes) {
  return static_cast<uint32_t>(absl::ComputeCrc32c(std::string_view{
    reinterpret_cast<const char*>(bytes.data()), bytes.size()}));
}

struct Awaited {
  const Lsn* position = nullptr;
  Lsn end = 0;
};

bool Reached(Awaited* awaited) { return *awaited->position >= awaited->end; }

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

void ZeroFill(duckdb::FileHandle& file, uint64_t from, uint64_t to) {
  std::vector<uint8_t> zeros(
    static_cast<size_t>(std::min<uint64_t>(to - from, uint64_t{1} << 20)));
  for (auto at = from; at < to; at += zeros.size()) {
    file.Write(duckdb::QueryContext{}, zeros.data(),
               std::min<uint64_t>(zeros.size(), to - at), at);
  }
}

std::vector<uint8_t> ReadAll(duckdb::FileSystem& fs,
                             const std::filesystem::path& path) {
  const auto file =
    fs.OpenFile(path.string(), duckdb::FileFlags::FILE_FLAGS_READ);
  std::vector<uint8_t> bytes(static_cast<size_t>(file->GetFileSize()));
  if (!bytes.empty()) {
    file->Read(bytes.data(), bytes.size(), 0);
  }
  return bytes;
}

template<typename Apply>
size_t ParseRecords(std::span<uint8_t> bytes, Apply&& apply) {
  size_t end = 0;
  while (end + sizeof(Header) <= bytes.size()) {
    const auto header = Load<Header>(bytes, end);
    const auto begin = end + sizeof(Header);
    if (header.size == 0 || header.size > bytes.size() - begin) {
      break;
    }
    const auto payload = bytes.subspan(begin, header.size);
    if (Checksum(payload) != header.crc) {
      break;
    }
    apply(end, begin + header.size, payload);
    end = begin + header.size;
  }
  return end;
}

struct History {
  Lsn cut = 0;
  std::vector<uint8_t> bytes;
};

History ReadHistory(duckdb::FileSystem& fs, const std::filesystem::path& path) {
  History history;
  std::error_code ec;
  if (!std::filesystem::exists(path, ec)) {
    return history;
  }
  history.bytes = ReadAll(fs, path);
  const std::span<const uint8_t> bytes{history.bytes};
  if (bytes.size() < kHistoryHeader ||
      Load<uint32_t>(bytes, 0) != Checksum(bytes.subspan(sizeof(uint32_t)))) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                    ERR_MSG("log history '", path.string(), "' is damaged"));
  }
  history.cut = Load<Lsn>(bytes, sizeof(uint32_t));
  return history;
}

template<typename Apply>
void ForEachHistoryEntry(std::span<uint8_t> bytes, Apply&& apply) {
  for (size_t at = kHistoryHeader; at < bytes.size();) {
    const auto lsn = Load<Lsn>(bytes, at);
    const auto size = Load<uint32_t>(bytes, at + sizeof(Lsn));
    at += sizeof(Lsn) + sizeof(uint32_t);
    apply(lsn, bytes.subspan(at, size));
    at += size;
  }
}

struct Scanned {
  std::vector<Lsn> obsolete;
  std::vector<Lsn> live;
  Lsn end = 0;
  Lsn torn = 0;
  size_t torn_from = 0;
  size_t torn_to = 0;
};

Scanned ScanLog(
  duckdb::FileSystem& fs, const std::filesystem::path& dir,
  absl::FunctionRef<void(Lsn, std::span<uint8_t>)> history,
  absl::FunctionRef<void(const Log::Range&, std::span<uint8_t>)> apply) {
  Scanned scanned;
  auto past = ReadHistory(fs, dir / Log::kHistoryFile);
  if (!past.bytes.empty()) {
    ForEachHistoryEntry(past.bytes, history);
  }
  std::vector<Lsn> begins;
  for (const auto& entry : std::filesystem::directory_iterator{dir}) {
    if (const auto begin = Log::ChunkBegin(entry.path().filename().string())) {
      begins.push_back(*begin);
    }
  }
  absl::c_sort(begins);
  size_t first = 0;
  while (first + 1 < begins.size() && begins[first + 1] <= past.cut) {
    scanned.obsolete.push_back(begins[first++]);
  }
  scanned.end = first < begins.size() ? begins[first] : past.cut;
  SDB_ENSURE(scanned.end <= past.cut || past.bytes.empty(), "log '",
             dir.string(), "': the chunk at ", scanned.end, " is missing");
  bool stop = false;
  for (auto i = first; i < begins.size(); ++i) {
    const auto begin = begins[i];
    if (stop || begin != scanned.end) {
      scanned.obsolete.push_back(begin);
      stop = true;
      continue;
    }
    auto bytes = ReadAll(fs, dir / Log::ChunkName(begin));
    const bool last = i + 1 == begins.size();
    const auto content =
      last ? bytes.size()
           : std::min<size_t>(bytes.size(),
                              static_cast<size_t>(begins[i + 1] - begin));
    const auto parsed = ParseRecords(
      std::span{bytes}.first(content),
      [&](size_t from, size_t to, std::span<uint8_t> payload) {
        apply(Log::Range{.begin = begin + from, .end = begin + to}, payload);
      });
    scanned.end = begin + parsed;
    scanned.live.push_back(begin);
    if (last || parsed != content) {
      auto dirty = bytes.size();
      while (dirty > parsed && bytes[dirty - 1] == 0) {
        --dirty;
      }
      if (dirty > parsed) {
        scanned.torn = begin;
        scanned.torn_from = parsed;
        scanned.torn_to = dirty;
      }
      stop = true;
    }
  }
  return scanned;
}

}  // namespace

std::optional<Lsn> Log::ChunkBegin(std::string_view name) {
  Lsn begin = 0;
  if (!absl::ConsumeSuffix(&name, kChunkSuffix) || name.size() != 16 ||
      !absl::SimpleHexAtoi(name, &begin)) {
    return std::nullopt;
  }
  return begin;
}

std::string Log::ChunkName(Lsn begin) {
  return absl::StrFormat("%016x%s", begin, kChunkSuffix);
}

std::optional<Lsn> Log::HistoryCut(const std::filesystem::path& dir) {
  auto fs = duckdb::FileSystem::CreateLocal();
  const auto history = ReadHistory(*fs, dir / kHistoryFile);
  if (history.bytes.empty()) {
    return std::nullopt;
  }
  return history.cut;
}

Log::Log(duckdb::FileSystem& fs, std::filesystem::path dir)
  : _fs{fs}, _dir{std::move(dir)} {
  if (std::filesystem::create_directories(_dir)) {
    utils::file_utils::SyncDirectory(_dir.parent_path());
  }
}

std::filesystem::path Log::ChunkPath(Lsn begin) const {
  return _dir / ChunkName(begin);
}

std::shared_ptr<Log::Chunk> Log::OpenChunk(Lsn begin, bool create) const {
  const auto flags = duckdb::FileFlags::FILE_FLAGS_READ |
                     duckdb::FileFlags::FILE_FLAGS_WRITE |
                     duckdb::FileLockType::WRITE_LOCK |
                     (create ? duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW
                             : duckdb::FileOpenFlags{});
  return std::make_shared<Chunk>(Chunk{
    .begin = begin, .file = _fs.OpenFile(ChunkPath(begin).string(), flags)});
}

void Log::SyncDirectory() const { utils::file_utils::SyncDirectory(_dir); }

void Log::Replay(
  absl::FunctionRef<void(Lsn, std::span<uint8_t>)> history,
  absl::FunctionRef<void(const Range&, std::span<uint8_t>)> apply) {
  absl::MutexLock files{&_files_mutex};
  const auto scanned = ScanLog(_fs, _dir, history, apply);
  for (const auto begin : scanned.obsolete) {
    std::filesystem::remove(ChunkPath(begin));
  }
  bool removed = !scanned.obsolete.empty();
  absl::btree_map<Lsn, std::shared_ptr<Chunk>> chunks;
  for (const auto begin : scanned.live) {
    chunks.emplace(begin, OpenChunk(begin, false));
  }
  if (scanned.torn_to > scanned.torn_from) {
    auto& torn = *chunks.at(scanned.torn)->file;
    ZeroFill(torn, scanned.torn_from, scanned.torn_to);
    torn.Sync();
  }
  const auto end = scanned.end;
  if (chunks.empty()) {
    chunks.emplace(end, OpenChunk(end, true));
    removed = true;
  }
  if (auto& last = *chunks.rbegin()->second->file;
      static_cast<uint64_t>(last.GetFileSize()) < kChunkBytes) {
    ZeroFill(last, static_cast<uint64_t>(last.GetFileSize()), kChunkBytes);
    last.Sync();
  }
  std::filesystem::remove(_dir / kSpareTemp);
  std::shared_ptr<Chunk> spare;
  if (std::filesystem::exists(_dir / kSpareFile)) {
    spare = std::make_shared<Chunk>(
      Chunk{.file = _fs.OpenFile((_dir / kSpareFile).string(),
                                 duckdb::FileFlags::FILE_FLAGS_READ |
                                   duckdb::FileFlags::FILE_FLAGS_WRITE |
                                   duckdb::FileLockType::WRITE_LOCK)});
  }
  if (removed) {
    SyncDirectory();
  }
  auto& first = *chunks.begin()->second->file;
  const bool parallel_syncs = first.file_system.SyncParallelism(first) ==
                              duckdb::FileSyncParallelism::PARALLEL;
  absl::MutexLock lock{&_mutex};
  _chunks = std::move(chunks);
  _parallel_syncs = parallel_syncs;
  _oldest.store(_chunks.begin()->first, std::memory_order_relaxed);
  _newest.store(_chunks.rbegin()->first, std::memory_order_relaxed);
  _spare = std::move(spare);
  _has_spare.store(_spare != nullptr, std::memory_order_relaxed);
  _replayed = end;
  _reserved = end;
  _written = end;
  _durable = end;
}

Log::Range Log::Reserve(size_t payload_size) {
  absl::MutexLock lock{&_mutex};
  const auto bytes = sizeof(Header) + payload_size;
  SDB_ASSERT(!_chunks.empty());
  const auto current = _chunks.rbegin()->first;
  if (_reserved != current && _reserved - current + bytes > kChunkBytes) {
    StartChunk();
  }
  const Range range{.begin = _reserved, .end = _reserved + bytes};
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
  std::shared_ptr<Chunk> chunk;
  if (auto spare = std::exchange(_spare, nullptr)) {
    _has_spare.store(false, std::memory_order_relaxed);
    std::error_code ec;
    std::filesystem::rename(_dir / kSpareFile, ChunkPath(_reserved), ec);
    if (!ec) {
      spare->begin = _reserved;
      chunk = std::move(spare);
    }
  }
  if (!chunk) {
    chunk = OpenChunk(_reserved, true);
  }
  _chunks.emplace(_reserved, std::move(chunk));
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
  _spare = std::make_shared<Chunk>(Chunk{.file = std::move(file)});
  _has_spare.store(true, std::memory_order_relaxed);
}

void Log::Write(const Range& range, std::span<const uint8_t> payload) {
  SDB_ASSERT(range.end - range.begin == sizeof(Header) + payload.size());
  std::vector<uint8_t> record(sizeof(Header) + payload.size());
  const Header header{.crc = Checksum(payload),
                      .size = static_cast<uint32_t>(payload.size())};
  std::memcpy(record.data(), &header, sizeof(Header));
  std::memcpy(record.data() + sizeof(Header), payload.data(), payload.size());
  std::shared_ptr<Chunk> chunk;
  {
    absl::MutexLock lock{&_mutex};
    const auto it = _chunks.upper_bound(range.begin);
    SDB_ASSERT(it != _chunks.begin());
    chunk = std::prev(it)->second;
  }
  try {
    chunk->file->Write(duckdb::QueryContext{}, record.data(), record.size(),
                       range.begin - chunk->begin);
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL, "log '", _dir.string(),
              "': a committed record could not be written: ",
              duckdb::ErrorData{e}.RawMessage());
  }
  absl::MutexLock lock{&_mutex};
  if (range.begin != _written) {
    _out_of_order.emplace(range.begin, range.end);
    return;
  }
  _written = range.end;
  for (auto it = _out_of_order.begin();
       it != _out_of_order.end() && it->first == _written;
       it = _out_of_order.erase(it)) {
    _written = it->second;
  }
}

void Log::Sync(Lsn end) {
  _mutex.Lock();
  Awaited written{.position = &_written, .end = end};
  Awaited durable{.position = &_durable, .end = end};
  while (_durable < end) {
    _mutex.Await(absl::Condition(&Reached, &written));
    if (_durable >= end) {
      break;
    }
    if (_syncing_to >= end) {
      _mutex.Await(absl::Condition(&Reached, &durable));
      continue;
    }
    if (_syncs != 0 && !_parallel_syncs) {
      _mutex.Await(absl::Condition{this, &Log::SyncIdle});
      continue;
    }
    ++_syncs;
    const auto target = _written;
    _syncing_to = std::max(_syncing_to, target);
    std::vector<std::shared_ptr<Chunk>> chunks;
    auto it = _chunks.upper_bound(_durable);
    if (it != _chunks.begin()) {
      --it;
    }
    for (; it != _chunks.end() && it->first < target; ++it) {
      chunks.push_back(it->second);
    }
    const auto directory_changes = _directory_changes;
    const bool directory = directory_changes != _directory_synced;
    _mutex.Unlock();
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
    _mutex.Lock();
    _durable = std::max(_durable, target);
    _directory_synced = std::max(_directory_synced, directory_changes);
    --_syncs;
  }
  _mutex.Unlock();
}

Lsn Log::Append(std::span<const uint8_t> payload) {
  const auto range = Reserve(payload.size());
  Write(range, payload);
  Sync(range.end);
  return range.end;
}

void Log::Truncate(Lsn below, CatalogPart part) {
  absl::MutexLock files{&_files_mutex};
  std::vector<std::shared_ptr<Chunk>> dropped;
  Lsn cut = 0;
  {
    absl::MutexLock lock{&_mutex};
    below = std::min(below, _durable);
    for (auto it = _chunks.begin(); it != _chunks.end(); ++it) {
      const auto next = std::next(it);
      if (next == _chunks.end() || next->first > below) {
        break;
      }
      dropped.push_back(it->second);
      cut = next->first;
    }
  }
  if (dropped.empty()) {
    return;
  }
  auto past = ReadHistory(_fs, _dir / kHistoryFile);
  std::vector<uint8_t> history(kHistoryHeader);
  if (!past.bytes.empty()) {
    history.insert(history.end(), past.bytes.begin() + kHistoryHeader,
                   past.bytes.end());
  }
  for (size_t i = 0; i < dropped.size(); ++i) {
    const auto& chunk = *dropped[i];
    const auto chunk_end = i + 1 < dropped.size() ? dropped[i + 1]->begin : cut;
    std::vector<uint8_t> bytes(static_cast<size_t>(chunk_end - chunk.begin));
    if (!bytes.empty()) {
      chunk.file->Read(bytes.data(), bytes.size(), 0);
    }
    const auto parsed =
      ParseRecords(bytes, [&](size_t, size_t to, std::span<uint8_t> payload) {
        const auto catalog = part(payload);
        if (catalog.empty()) {
          return;
        }
        Store<Lsn>(history, chunk.begin + to);
        Store<uint32_t>(history, static_cast<uint32_t>(catalog.size()));
        history.insert(history.end(), catalog.begin(), catalog.end());
      });
    SDB_ENSURE(parsed == bytes.size(), "log '", _dir.string(),
               "': the chunk at ", chunk.begin, " ends early");
  }
  std::memcpy(history.data() + sizeof(uint32_t), &cut, sizeof(Lsn));
  const auto crc =
    Checksum(std::span<const uint8_t>{history}.subspan(sizeof(uint32_t)));
  std::memcpy(history.data(), &crc, sizeof(uint32_t));
  const auto temp = _dir / kHistoryTemp;
  {
    const auto file = _fs.OpenFile(
      temp.string(), duckdb::FileFlags::FILE_FLAGS_WRITE |
                       duckdb::FileFlags::FILE_FLAGS_FILE_CREATE_NEW);
    file->Write(duckdb::QueryContext{}, history.data(), history.size(), 0);
    file->Sync();
  }
  std::filesystem::rename(temp, _dir / kHistoryFile);
  SyncDirectory();
  {
    absl::MutexLock lock{&_mutex};
    for (const auto& chunk : dropped) {
      _chunks.erase(chunk->begin);
    }
    _oldest.store(cut, std::memory_order_relaxed);
  }
  for (const auto& chunk : dropped) {
    std::filesystem::remove(ChunkPath(chunk->begin));
  }
  SyncDirectory();
}

Lsn Log::Link(const std::filesystem::path& dir) {
  absl::MutexLock files{&_files_mutex};
  Lsn durable = 0;
  Lsn first = 0;
  {
    absl::MutexLock lock{&_mutex};
    durable = _durable;
    first = _chunks.begin()->first;
  }
  const auto history = _dir / kHistoryFile;
  std::error_code ec;
  if (!std::filesystem::exists(history, ec)) {
    std::vector<uint8_t> empty(kHistoryHeader);
    std::memcpy(empty.data() + sizeof(uint32_t), &first, sizeof(Lsn));
    const auto crc =
      Checksum(std::span<const uint8_t>{empty}.subspan(sizeof(uint32_t)));
    std::memcpy(empty.data(), &crc, sizeof(uint32_t));
    utils::file_utils::Spit(
      history.string(),
      std::string_view{reinterpret_cast<const char*>(empty.data()),
                       empty.size()},
      true);
    SyncDirectory();
  }
  std::filesystem::create_directories(dir);
  for (const auto& entry : std::filesystem::directory_iterator{_dir}) {
    const auto name = entry.path().filename();
    if (name != kHistoryTemp &&
        (name == kHistoryFile || ChunkBegin(name.string()))) {
      std::filesystem::create_hard_link(entry.path(), dir / name, ec);
      if (ec) {
        utils::file_utils::ThrowFileError("link", (dir / name).string(), ec);
      }
    }
  }
  utils::file_utils::SyncDirectory(dir);
  return durable;
}

}  // namespace sdb::catalog
