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

#include "search/search_db_wal.h"

#include <absl/strings/str_format.h>
#include <zstd.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/checksum.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/buffered_file_writer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/log.h"

namespace sdb::search {
namespace {

constexpr uint8_t kKindInline = 0;
constexpr uint8_t kKindReference = 1;
constexpr uint8_t kKindDelete = 2;
constexpr uint8_t kKindTruncate = 3;

constexpr uint8_t kChunkCodecNone = 0;
constexpr uint8_t kChunkCodecZstd = 1;
constexpr int kZstdLevel = 1;

constexpr std::string_view kSegSuffix = ".swal";
constexpr std::string_view kChunkSuffix = ".swchunk";

constexpr duckdb::FileOpenFlags kAppendFlags =
  duckdb::FileFlags::FILE_FLAGS_WRITE |
  duckdb::FileFlags::FILE_FLAGS_FILE_CREATE |
  duckdb::FileFlags::FILE_FLAGS_APPEND |
  duckdb::FileFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS;

// PostgreSQL-style fixed-width 16-hex names: lexicographic order == numeric.
std::string SegmentName(uint64_t first_tick) {
  return absl::StrFormat("%016x%s", first_tick, kSegSuffix);
}
std::string ChunkName(uint64_t seg_id) {
  return absl::StrFormat("%016x%s", seg_id, kChunkSuffix);
}

bool ParseHex(std::string_view s, uint64_t& out) {
  if (s.empty() || s.size() > 16) {
    return false;
  }
  uint64_t v = 0;
  for (char c : s) {
    v <<= 4;
    if (c >= '0' && c <= '9') {
      v |= static_cast<uint64_t>(c - '0');
    } else if (c >= 'a' && c <= 'f') {
      v |= static_cast<uint64_t>(c - 'a' + 10);
    } else if (c >= 'A' && c <= 'F') {
      v |= static_cast<uint64_t>(c - 'A' + 10);
    } else {
      return false;
    }
  }
  out = v;
  return true;
}

// "<016x>.swal" -> first_tick; "<016x>.swchunk" -> seg_id.
bool ParseName(std::string_view name, std::string_view suffix, uint64_t& out) {
  if (name.size() <= suffix.size() || !name.ends_with(suffix)) {
    return false;
  }
  return ParseHex(name.substr(0, name.size() - suffix.size()), out);
}

// Central segments under `wal_dir`, sorted by first_tick (== tick order).
std::vector<std::pair<uint64_t, std::filesystem::path>> EnumerateSegments(
  const std::filesystem::path& wal_dir) {
  std::vector<std::pair<uint64_t, std::filesystem::path>> out;
  std::error_code ec;
  if (!std::filesystem::exists(wal_dir, ec)) {
    return out;
  }
  for (const auto& entry : std::filesystem::directory_iterator(wal_dir, ec)) {
    if (ec || !entry.is_regular_file(ec)) {
      continue;
    }
    uint64_t first_tick = 0;
    if (ParseName(entry.path().filename().string(), kSegSuffix, first_tick)) {
      out.emplace_back(first_tick, entry.path());
    }
  }
  std::sort(out.begin(), out.end(),
            [](const auto& a, const auto& b) { return a.first < b.first; });
  return out;
}

// Max seg_id among one shard's chunk dir (non-recursive).
uint64_t MaxChunkSegId(const std::filesystem::path& chunk_dir) {
  uint64_t mx = 0;
  std::error_code ec;
  if (!std::filesystem::exists(chunk_dir, ec)) {
    return 0;
  }
  for (const auto& entry : std::filesystem::directory_iterator(chunk_dir, ec)) {
    if (ec || !entry.is_regular_file(ec)) {
      continue;
    }
    uint64_t seg_id = 0;
    if (ParseName(entry.path().filename().string(), kChunkSuffix, seg_id)) {
      mx = std::max(mx, seg_id);
    }
  }
  return mx;
}

// Read one [u64 size][u64 checksum][payload] frame into `payload`. Returns
// false at EOF or on a torn/corrupt tail -- the caller stops the segment there.
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

// Forward cursor over a record payload (mixed fixed fields + length-prefixed
// blobs). Bounds-checked via SDB_ASSERT (payloads are checksum-verified).
struct Cursor {
  const uint8_t* p;
  const uint8_t* end;
  explicit Cursor(const std::vector<uint8_t>& buf)
    : p(buf.data()), end(buf.data() + buf.size()) {}
  template<typename T>
  T Read() {
    SDB_ASSERT(p + sizeof(T) <= end);
    T v;
    std::memcpy(&v, p, sizeof(T));
    p += sizeof(T);
    return v;
  }
  const uint8_t* ReadBlob(uint64_t len) {
    SDB_ASSERT(p + len <= end);
    const uint8_t* b = p;
    p += len;
    return b;
  }
  bool AtEnd() const { return p >= end; }
};

// One parsed shard-section header
struct SectionHeader {
  uint64_t table_id;
};
SectionHeader ReadSectionHeader(Cursor& c) {
  SectionHeader h;
  h.table_id = c.Read<uint64_t>();
  return h;
}

struct ParsedOp {
  uint8_t kind = 0;
  // INLINE
  uint32_t seg_count = 0;  // # of (u64 base, u64 count) InlinePk pairs
  const uint8_t* pk_blob = nullptr;  // seg_count * 2 * u64, packed
  const uint8_t* inline_blob = nullptr;
  uint64_t inline_blob_len = 0;
  // REFERENCE (into seg_scratch -- alignment-safe)
  std::span<const uint64_t> seg_ids;
  // DELETE (views into the payload, into pk_scratch)
  std::span<const std::string_view> delete_pks;
};

ParsedOp ParseOp(Cursor& c, std::vector<uint64_t>& seg_scratch,
                 std::vector<std::string_view>& pk_scratch) {
  ParsedOp op;
  op.kind = c.Read<uint8_t>();
  switch (op.kind) {
    case kKindInline:
      op.seg_count = c.Read<uint32_t>();
      op.pk_blob = c.ReadBlob(op.seg_count * 2 * sizeof(uint64_t));
      op.inline_blob_len = c.Read<uint64_t>();
      op.inline_blob = c.ReadBlob(op.inline_blob_len);
      break;
    case kKindReference: {
      const auto n = c.Read<uint32_t>();
      seg_scratch.clear();
      seg_scratch.reserve(n);
      for (uint32_t k = 0; k < n; ++k) {
        seg_scratch.push_back(c.Read<uint64_t>());
      }
      op.seg_ids = seg_scratch;
      break;
    }
    case kKindDelete: {
      const auto n = c.Read<uint32_t>();
      pk_scratch.clear();
      pk_scratch.reserve(n);
      for (uint32_t i = 0; i < n; ++i) {
        const auto len = c.Read<uint32_t>();
        const uint8_t* bytes = c.ReadBlob(len);
        pk_scratch.emplace_back(reinterpret_cast<const char*>(bytes), len);
      }
      op.delete_pks = pk_scratch;
      break;
    }
    case kKindTruncate:
      break;  // bodyless
    default:
      SDB_ENSURE(false, ERROR_INTERNAL,
                 "unknown search WAL op kind: ", static_cast<int>(op.kind));
  }
  return op;
}

template<typename OpHandler>
void VisitSectionsOps(Cursor& c, std::vector<uint64_t>& seg_scratch,
                      std::vector<std::string_view>& pk_scratch,
                      const OpHandler& on_op) {
  const auto shard_count = c.Read<uint32_t>();
  for (uint32_t s = 0; s < shard_count; ++s) {
    const auto h = ReadSectionHeader(c);
    const auto op_count = c.Read<uint32_t>();
    for (uint32_t o = 0; o < op_count; ++o) {
      const ParsedOp op = ParseOp(c, seg_scratch, pk_scratch);
      on_op(h.table_id, op);
    }
  }
}

struct ZstdDCtxDeleter {
  void operator()(ZSTD_DCtx* d) const noexcept { ZSTD_freeDCtx(d); }
};
using ZstdDCtxPtr = std::unique_ptr<ZSTD_DCtx, ZstdDCtxDeleter>;

void ReplayChunkFile(
  duckdb::FileSystem& fs, const std::string& chunk_path, ZSTD_DCtx* dctx,
  const absl::AnyInvocable<void(duckdb::DataChunk&, uint64_t pk_base) const>&
    emit) {
  SDB_ASSERT(dctx);
  duckdb::BufferedFileReader reader(fs, chunk_path.c_str());
  std::vector<uint8_t> comp_buf;
  std::vector<uint8_t> raw_buf;
  constexpr uint64_t kChunkHdr =
    sizeof(uint8_t) + 2 * sizeof(uint32_t) + sizeof(uint64_t);
  while (reader.CurrentOffset() < reader.FileSize()) {
    uint64_t remaining = reader.FileSize() - reader.CurrentOffset();
    SDB_ENSURE(remaining >= kChunkHdr, ERROR_INTERNAL,
               "corrupt search chunk file '", chunk_path, "': trailing ",
               remaining, " bytes < frame header");
    auto codec = reader.Read<uint8_t>();
    auto raw_len = reader.Read<uint32_t>();
    auto comp_len = reader.Read<uint32_t>();
    auto pk_base = reader.Read<uint64_t>();
    SDB_ENSURE(reader.FileSize() - reader.CurrentOffset() >= comp_len,
               ERROR_INTERNAL, "corrupt search chunk file '", chunk_path,
               "': frame of ", comp_len, " bytes exceeds remaining file");
    comp_buf.resize(comp_len);
    reader.ReadData(comp_buf.data(), comp_len);

    uint8_t* src = nullptr;
    uint32_t src_len = 0;
    if (codec == kChunkCodecZstd) {
      raw_buf.resize(raw_len);
      const size_t got = ZSTD_decompressDCtx(dctx, raw_buf.data(), raw_len,
                                             comp_buf.data(), comp_len);
      SDB_ENSURE(!ZSTD_isError(got) && got == raw_len, ERROR_INTERNAL,
                 "corrupt search chunk file '", chunk_path,
                 "': zstd decompress failed (", raw_len, " expected)");
      src = raw_buf.data();
      src_len = raw_len;
    } else {
      SDB_ENSURE(codec == kChunkCodecNone && comp_len == raw_len,
                 ERROR_INTERNAL, "corrupt search chunk file '", chunk_path,
                 "': unknown codec ", codec);
      src = comp_buf.data();
      src_len = comp_len;
    }
    duckdb::MemoryStream ms(src, src_len);
    duckdb::BinaryDeserializer cdeser{ms};
    cdeser.Begin();
    duckdb::DataChunk chunk;
    chunk.Deserialize(cdeser);
    cdeser.End();
    emit(chunk, pk_base);
  }
}

}  // namespace

SearchDbWal::PendingChunk::PendingChunk(uint64_t seg_id,
                                        std::filesystem::path path)
  : _seg_id(seg_id), _path(std::move(path)) {}

SearchDbWal::PendingChunk::PendingChunk(PendingChunk&& o) noexcept
  : _seg_id(o._seg_id), _path(std::move(o._path)), _committed(o._committed) {
  o._committed = true;  // the moved-from husk must not reclaim the file
}

SearchDbWal::PendingChunk& SearchDbWal::PendingChunk::operator=(
  PendingChunk&& o) noexcept {
  if (this != &o) {
    ReclaimIfUncommitted();
    _seg_id = o._seg_id;
    _path = std::move(o._path);
    _committed = o._committed;
    o._committed = true;
  }
  return *this;
}

SearchDbWal::PendingChunk::~PendingChunk() { ReclaimIfUncommitted(); }

void SearchDbWal::PendingChunk::ReclaimIfUncommitted() noexcept {
  if (_committed || _path.empty()) {
    return;
  }
  std::error_code ec;
  std::filesystem::remove(_path, ec);
  if (ec) {
    SDB_WARN(SEARCH, "search WAL: failed to remove uncommitted chunk file '",
             _path.string(), "': ", ec.message());
  }
}

void SearchDbWal::ChunkWriter::CCtxDeleter::operator()(
  ZSTD_CCtx_s* cctx) const noexcept {
  ZSTD_freeCCtx(cctx);  // no-op on nullptr
}

SearchDbWal::ChunkWriter::ChunkWriter(
  PendingChunk pending, std::unique_ptr<duckdb::BufferedFileWriter> writer)
  : _pending(std::move(pending)),
    _writer(std::move(writer)),
    _stream(std::make_unique<duckdb::MemoryStream>()),
    _cctx(ZSTD_createCCtx()) {}
SearchDbWal::ChunkWriter::ChunkWriter(ChunkWriter&&) noexcept = default;
SearchDbWal::ChunkWriter& SearchDbWal::ChunkWriter::operator=(
  ChunkWriter&&) noexcept = default;
SearchDbWal::ChunkWriter::~ChunkWriter() = default;

void SearchDbWal::ChunkWriter::Append(duckdb::DataChunk& chunk,
                                      uint64_t pk_base) {
  SDB_ASSERT(_writer);
  _stream->Rewind();
  duckdb::BinarySerializer serializer{*_stream};
  serializer.Begin();
  chunk.Serialize(serializer);
  serializer.End();
  const auto raw_len = static_cast<uint32_t>(_stream->GetPosition());

  const auto bound = ZSTD_compressBound(raw_len);
  if (_comp.size() < bound) {
    _comp.resize(bound);
  }
  // Reuse this writer's context (ZSTD_compressCCtx resets it per call) so each
  // chunk doesn't allocate + free its own.
  SDB_ASSERT(_cctx);
  const size_t comp =
    ZSTD_compressCCtx(_cctx.get(), _comp.data(), _comp.size(),
                      _stream->GetData(), raw_len, kZstdLevel);
  uint8_t codec = kChunkCodecZstd;
  const uint8_t* payload = _comp.data();
  auto payload_len = static_cast<uint32_t>(comp);
  if (ZSTD_isError(comp) || comp >= raw_len) {
    codec = kChunkCodecNone;
    payload = _stream->GetData();
    payload_len = raw_len;
  }
  _writer->Write<uint8_t>(codec);
  _writer->Write<uint32_t>(raw_len);
  _writer->Write<uint32_t>(payload_len);
  _writer->Write<uint64_t>(pk_base);
  _writer->WriteData(payload, payload_len);
}

SearchDbWal::PendingChunk SearchDbWal::ChunkWriter::Finish() {
  SDB_ASSERT(_writer);
  _writer->Sync();
  _writer.reset();
  return std::move(_pending);
}

//
// SearchDbWal
//

SearchDbWal::SearchDbWal(duckdb::FileSystem& fs, std::filesystem::path wal_dir,
                         uint64_t seal_threshold)
  : _fs(fs),
    _wal_dir(std::move(wal_dir)),
    _chunks_root(_wal_dir / "chunks"),
    _seal_threshold(seal_threshold) {
  const auto segments = EnumerateSegments(_wal_dir);
  uint64_t max_tick = 0;
  for (size_t i = segments.size(); i-- > 0;) {
    const auto& path = segments[i].second;
    uint64_t last_tick = 0;
    bool any = false;
    {
      duckdb::BufferedFileReader reader(_fs, path.string().c_str());
      std::vector<uint8_t> payload;
      while (ReadFrame(reader, payload)) {
        if (payload.size() >= sizeof(uint64_t)) {
          Cursor c(payload);
          last_tick = c.Read<uint64_t>();
          any = true;
        }
      }
    }
    if (any) {
      max_tick = last_tick;
      break;
    }
    std::error_code ec;
    std::filesystem::remove(path, ec);
    SDB_ENSURE(!ec, ERROR_INTERNAL, "remove corrupted wal file '",
               path.string(), "': ", ec.message());
  }
  _tick.store(max_tick, std::memory_order_relaxed);
}

SearchDbWal::~SearchDbWal() = default;

std::filesystem::path SearchDbWal::ChunkDir(uint64_t table_id) const {
  return _chunks_root / std::to_string(table_id);
}

void SearchDbWal::EnsureActiveSegmentLocked(uint64_t first_tick) {
  if (_active) {
    return;
  }
  std::error_code ec;
  std::filesystem::create_directories(_wal_dir, ec);
  SDB_ENSURE(!ec, ERROR_INTERNAL, "create wal dir '", _wal_dir.string(),
             "': ", ec.message());
  auto seg_path = _wal_dir / SegmentName(first_tick);
  std::error_code exists_ec;
  SDB_ENSURE(!std::filesystem::exists(seg_path, exists_ec), ERROR_INTERNAL,
             "search WAL: new active segment '", seg_path.string(),
             "' already exists -- tick seed regressed");
  _active = std::make_unique<duckdb::BufferedFileWriter>(_fs, seg_path.string(),
                                                         kAppendFlags);
  _active_first_tick = first_tick;
  _active_chunk_bytes = 0;  // fresh segment owns no chunks yet
}

void SearchDbWal::WriteFrameLocked(const uint8_t* payload, uint64_t size) {
  SDB_ASSERT(_active);
  auto checksum = duckdb::Checksum(payload, size);
  _active->Write<uint64_t>(size);
  _active->Write<uint64_t>(checksum);
  _active->WriteData(payload, size);
  _active->Sync();  // commit point

  if (_active->GetTotalWritten() + _active_chunk_bytes > _seal_threshold) {
    _active->Close();
    _active.reset();
    _active_first_tick = 0;
  }
}

uint64_t SearchDbWal::AppendCommit(std::span<const ShardSection> sections,
                                   uint64_t tick_span) {
  SDB_ASSERT(!sections.empty(), "AppendCommit with no shard sections");
  SDB_ASSERT(tick_span >= 1, "every commit advances the tick by at least 1");
  std::lock_guard<std::mutex> lock(_append_mu);
  // Reserve `tick_span` ticks atomically with the append; the record's tick is
  // the top of the band so file-order == tick-order is preserved. The per-shard
  // iresearch bands live within (base, tick] and are assigned by the caller.
  uint64_t base = _tick.fetch_add(tick_span, std::memory_order_relaxed);
  uint64_t tick = base + tick_span;
  EnsureActiveSegmentLocked(tick);

  duckdb::MemoryStream payload;
  payload.Write<uint64_t>(tick);
  payload.Write<uint32_t>(static_cast<uint32_t>(sections.size()));
  // Reused inline-CDC scratch across every INLINE op (Rewind keeps the buffer).
  duckdb::MemoryStream tmp;
  for (const auto& s : sections) {
    payload.Write<uint64_t>(s.table_id.id());
    SDB_ASSERT(!s.ops.empty(), "shard section with no ops");
    payload.Write<uint32_t>(static_cast<uint32_t>(s.ops.size()));
    for (const auto& op : s.ops) {
      SDB_ASSERT((op.inline_data != nullptr) + (!op.seg_ids.empty()) +
                     (!op.delete_pks.empty()) + op.truncate ==
                   1,
                 "op must be exactly one of INLINE / REFERENCE / DELETE / "
                 "TRUNCATE");
      const uint8_t kind = op.truncate           ? kKindTruncate
                           : op.inline_data      ? kKindInline
                           : !op.seg_ids.empty() ? kKindReference
                                                 : kKindDelete;
      payload.Write<uint8_t>(kind);
      if (kind == kKindInline) {
        payload.Write<uint32_t>(static_cast<uint32_t>(op.inline_pks.size()));
        for (const auto& pk : op.inline_pks) {
          payload.Write<uint64_t>(pk.base);
          payload.Write<uint64_t>(pk.count);
        }
        tmp.Rewind();
        duckdb::BinarySerializer serializer{tmp};
        serializer.Begin();
        op.inline_data->Serialize(serializer);
        serializer.End();
        auto len = static_cast<uint64_t>(tmp.GetPosition());
        payload.Write<uint64_t>(len);
        payload.WriteData(tmp.GetData(), len);
      } else if (kind == kKindReference) {
        payload.Write<uint32_t>(static_cast<uint32_t>(op.seg_ids.size()));
        for (uint64_t sid : op.seg_ids) {
          payload.Write<uint64_t>(sid);
          auto chunk_path = ChunkDir(s.table_id.id()) / ChunkName(sid);
          std::error_code se;
          auto sz = std::filesystem::file_size(chunk_path, se);
          if (se) {
            SDB_WARN(SEARCH, "search WAL: file_size('", chunk_path.string(),
                     "') failed: ", se.message(),
                     " -- chunk excluded from seal accounting");
          } else {
            _active_chunk_bytes += sz;
          }
        }
      } else if (kind == kKindDelete) {
        payload.Write<uint32_t>(static_cast<uint32_t>(op.delete_pks.size()));
        for (const auto& pk : op.delete_pks) {
          payload.Write<uint32_t>(static_cast<uint32_t>(pk.size()));
          payload.WriteData(reinterpret_cast<const uint8_t*>(pk.data()),
                            pk.size());
        }
      }
    }
  }
  WriteFrameLocked(payload.GetData(), payload.GetPosition());
  return tick;
}

SearchDbWal::ChunkWriter SearchDbWal::NewChunkWriter(ObjectId table_id) {
  auto dir = ChunkDir(table_id.id());
  std::error_code ec;
  std::filesystem::create_directories(dir, ec);
  SDB_ENSURE(!ec, ERROR_INTERNAL, "create chunk dir '", dir.string(),
             "': ", ec.message());

  uint64_t seg_id;
  {
    std::lock_guard<std::mutex> lock(_seg_mu);
    auto it = _seg_ids.find(table_id.id());
    if (it == _seg_ids.end()) {
      it = _seg_ids.emplace(table_id.id(), MaxChunkSegId(dir)).first;
    }
    seg_id = ++it->second;
  }
  auto path = dir / ChunkName(seg_id);
  auto writer = std::make_unique<duckdb::BufferedFileWriter>(_fs, path.string(),
                                                             kAppendFlags);
  return ChunkWriter{PendingChunk{seg_id, std::move(path)}, std::move(writer)};
}

void SearchDbWal::RegisterShard(ObjectId table_id, uint64_t committed_tick) {
  {
    std::lock_guard<std::mutex> lock(_sub_mu);
    auto& cur = _committed[table_id.id()];
    cur = std::max(cur, committed_tick);
  }
  // Continue the tick line past every shard's durable tick: a shard's committed
  // tick can exceed the WAL max if consumed records were already GC'd.
  std::lock_guard<std::mutex> lock(_append_mu);
  if (_tick.load(std::memory_order_relaxed) < committed_tick) {
    _tick.store(committed_tick, std::memory_order_relaxed);
  }
}

void SearchDbWal::OnShardCommit(ObjectId table_id, uint64_t committed_tick) {
  {
    std::lock_guard<std::mutex> lock(_sub_mu);
    auto& cur = _committed[table_id.id()];
    cur = std::max(cur, committed_tick);
  }
  {
    std::lock_guard<std::mutex> lock(_append_mu);
    if (_tick.load(std::memory_order_relaxed) < committed_tick) {
      _tick.store(committed_tick, std::memory_order_relaxed);
    }
  }
  RunGc();
}

void SearchDbWal::DeregisterShard(ObjectId table_id) {
  {
    std::lock_guard<std::mutex> lock(_sub_mu);
    _committed.erase(table_id.id());
  }
  RunGc();
}

uint64_t SearchDbWal::MinCommittedTick() {
  std::lock_guard<std::mutex> lock(_sub_mu);
  if (_committed.empty()) {
    return 0;
  }
  uint64_t mn = std::numeric_limits<uint64_t>::max();
  for (const auto& [table_id, tick] : _committed) {
    mn = std::min(mn, tick);
  }
  return mn;
}

void SearchDbWal::RunGc() {
  uint64_t min_tick = MinCommittedTick();
  if (min_tick == 0) {
    return;  // nothing durable everywhere yet
  }
  // Snapshot the active segment (the only mutated file) so we never GC it even
  // if a concurrent AppendCommit rolls it.
  uint64_t active_first_tick;
  {
    std::lock_guard<std::mutex> lock(_append_mu);
    active_first_tick = _active_first_tick;
  }

  std::vector<uint64_t> seg_scratch;         // reused by ParseOp across records
  std::vector<std::string_view> pk_scratch;  // (RunGc only consumes REFERENCE)
  for (const auto& [first_tick, path] : EnumerateSegments(_wal_dir)) {
    if (active_first_tick != 0 && first_tick == active_first_tick) {
      continue;  // the live, still-appended segment
    }
    bool consumed = true;
    std::vector<std::filesystem::path> chunk_paths;
    {
      duckdb::BufferedFileReader reader(_fs, path.string().c_str());
      std::vector<uint8_t> payload;
      while (ReadFrame(reader, payload)) {
        Cursor c(payload);
        if (c.Read<uint64_t>() > min_tick) {  // tick (records are ascending)
          consumed = false;
          break;
        }
        VisitSectionsOps(
          c, seg_scratch, pk_scratch,
          [&](uint64_t table_id, const ParsedOp& op) {
            if (op.kind != kKindReference) {
              return;
            }
            for (uint64_t sid : op.seg_ids) {
              chunk_paths.push_back(ChunkDir(table_id) / ChunkName(sid));
            }
          });
      }
    }
    if (!consumed) {
      break;  // this + every later (higher-tick) segment still un-published
    }
    for (const auto& cp : chunk_paths) {
      std::error_code ec;
      std::filesystem::remove(cp, ec);
    }
    std::error_code ec;
    std::filesystem::remove(path, ec);
  }
}

uint64_t SearchDbWal::Recover(const ShardExistsFn& exists_of,
                              const ShardCommittedFn& committed_of,
                              const ReplayCallback& insert_cb,
                              const DeleteReplayCallback& delete_cb,
                              const TruncateReplayCallback& truncate_cb) {
  std::lock_guard<std::mutex> lock(_append_mu);
  uint64_t max_tick = 0;
  std::unordered_set<std::string> referenced;  // surviving chunk-file paths
  std::vector<uint64_t> seg_scratch;           // reused by ParseOp across
  std::vector<std::string_view> pk_scratch;    // records
  ZstdDCtxPtr dctx{ZSTD_createDCtx()};  // reused across all chunk-file frames

  for (const auto& [first_tick, path] : EnumerateSegments(_wal_dir)) {
    duckdb::BufferedFileReader reader(_fs, path.string().c_str());
    std::vector<uint8_t> payload;
    while (ReadFrame(reader, payload)) {
      Cursor c(payload);
      const uint64_t tick = c.Read<uint64_t>();
      max_tick = std::max(max_tick, tick);
      VisitSectionsOps(
        c, seg_scratch, pk_scratch, [&](uint64_t table_id, const ParsedOp& op) {
          const ObjectId tid{table_id};
          const bool live = exists_of(tid) && tick > committed_of(tid);
          switch (op.kind) {
            case kKindInline: {
              if (!live) {
                return;
              }
              std::vector<InlinePk> segments(op.seg_count);
              for (uint32_t i = 0; i < op.seg_count; ++i) {
                std::memcpy(&segments[i].base,
                            op.pk_blob + (2 * i) * sizeof(uint64_t),
                            sizeof(uint64_t));
                std::memcpy(&segments[i].count,
                            op.pk_blob + (2 * i + 1) * sizeof(uint64_t),
                            sizeof(uint64_t));
              }
              duckdb::MemoryStream ms(const_cast<uint8_t*>(op.inline_blob),
                                      op.inline_blob_len);
              duckdb::BinaryDeserializer deser{ms};
              deser.Begin();
              auto cdc = duckdb::ColumnDataCollection::Deserialize(deser);
              deser.End();
              VisitInlineSegments(
                *cdc, segments,
                [&](duckdb::DataChunk& chunk, uint64_t pk_base) {
                  insert_cb(tick, tid, pk_base, chunk);
                });
              break;
            }
            case kKindReference:
              for (uint64_t sid : op.seg_ids) {
                auto chunk_path =
                  (ChunkDir(table_id) / ChunkName(sid)).string();
                referenced.insert(chunk_path);  // survives the orphan sweep
                if (live) {
                  ReplayChunkFile(
                    _fs, chunk_path, dctx.get(),
                    [&](duckdb::DataChunk& chunk, uint64_t pk_base) {
                      insert_cb(tick, tid, pk_base, chunk);
                    });
                }
              }
              break;
            case kKindDelete:
              if (live) {
                delete_cb(tick, tid, op.delete_pks);
              }
              break;
            case kKindTruncate:
              if (live) {
                truncate_cb(tick, tid);
              }
              break;
          }
        });
    }
  }

  std::error_code ec;
  if (std::filesystem::exists(_chunks_root, ec)) {
    for (const auto& entry :
         std::filesystem::recursive_directory_iterator(_chunks_root, ec)) {
      if (ec) {
        break;
      }
      if (!entry.is_regular_file(ec)) {
        continue;
      }
      uint64_t sid = 0;
      if (ParseName(entry.path().filename().string(), kChunkSuffix, sid) &&
          !referenced.contains(entry.path().string())) {
        std::error_code remove_ec;
        std::filesystem::remove(entry.path(), remove_ec);
      }
    }
  }

  if (_tick.load(std::memory_order_relaxed) < max_tick) {
    _tick.store(max_tick, std::memory_order_relaxed);
  }
  return max_tick;
}

void VisitInlineSegments(
  const duckdb::ColumnDataCollection& cdc,
  std::span<const SearchDbWal::InlinePk> segments,
  const absl::AnyInvocable<void(duckdb::DataChunk&, uint64_t base) const>&
    emit) {
  if (segments.empty()) {
    for (auto& chunk : cdc.Chunks()) {
      emit(chunk, 0);
    }
    return;
  }
  size_t seg = 0;
  uint64_t seg_off = 0;  // rows of the current segment already emitted
  for (auto& chunk : cdc.Chunks()) {
    const uint64_t n = chunk.size();
    uint64_t off = 0;  // rows of this (coalesced) chunk already consumed
    while (off < n && seg < segments.size()) {
      const auto take = static_cast<duckdb::idx_t>(
        std::min<uint64_t>(segments[seg].count - seg_off, n - off));
      if (off == 0 && seg_off == 0 && take == n) {
        emit(chunk, segments[seg].base);
      } else {
        duckdb::SelectionVector sel(take);
        for (duckdb::idx_t r = 0; r < take; ++r) {
          sel.set_index(r, off + r);
        }
        duckdb::DataChunk slice;
        slice.InitializeEmpty(cdc.Types());
        slice.Slice(chunk, sel, take);
        emit(slice, segments[seg].base + seg_off);
      }
      off += take;
      seg_off += take;
      if (seg_off == segments[seg].count) {
        ++seg;
        seg_off = 0;
      }
    }
  }
}

}  // namespace sdb::search
