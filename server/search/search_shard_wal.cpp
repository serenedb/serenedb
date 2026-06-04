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

#include "search/search_shard_wal.h"

#include <absl/strings/str_format.h>
#include <zstd.h>

#include <algorithm>
#include <duckdb/common/checksum.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/buffered_file_writer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_set>
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"

namespace sdb::search {
namespace {

constexpr uint8_t kKindInline = 0;
constexpr uint8_t kKindReference = 1;

// Per-chunk frame codec (chunk files only): [u8 codec][u32 raw_len]
// [u32 comp_len][payload]. The chunk-file write is I/O-bound on the data
// volume (WAL_DESIGN.md §3.1/§14.2); serialized search data compresses ~5-8x,
// and zstd-1 decompresses at ~GB/s so recovery stays fast. kNone stores raw
// (tiny/incompressible chunks) so a chunk never expands.
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

// Read one [u64 size][u64 checksum][payload] frame into `payload`. Returns
// false at EOF or on a torn/corrupt tail (incomplete header, payload past EOF,
// or checksum mismatch) -- the caller stops reading the segment there.
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

// Total bytes of all chunk files. Counted toward the central-segment seal
// threshold so a bulk insert (tiny central record, large chunks) rolls promptly
// (WAL_DESIGN.md §10.2). Cheap for the inline path: chunks/ usually doesn't
// exist (no NewChunkWriter), so this is a single existence check.
uint64_t TotalChunkBytes(const std::filesystem::path& chunks_dir) {
  uint64_t total = 0;
  std::error_code ec;
  if (!std::filesystem::exists(chunks_dir, ec)) {
    return 0;
  }
  for (const auto& entry :
       std::filesystem::directory_iterator(chunks_dir, ec)) {
    if (ec || !entry.is_regular_file(ec)) {
      continue;
    }
    uint64_t sz = 0;
    if (ParseName(entry.path().filename().string(), kChunkSuffix, sz)) {
      total += entry.file_size(ec);
    }
  }
  return total;
}

uint64_t MaxChunkSegId(const std::filesystem::path& chunks_dir) {
  uint64_t mx = 0;
  std::error_code ec;
  if (!std::filesystem::exists(chunks_dir, ec)) {
    return 0;
  }
  for (const auto& entry :
       std::filesystem::directory_iterator(chunks_dir, ec)) {
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

// Per-record header (after the frame): [u64 tick][u8 kind][u32 col_count]
// [u64 col_id x N]. Reads it from `ps`, leaving `ps` positioned at the body.
struct RecordHeader {
  uint64_t tick;
  uint8_t kind;
  std::vector<uint64_t> column_ids;
};
RecordHeader ReadHeader(duckdb::MemoryStream& ps) {
  RecordHeader h;
  h.tick = ps.Read<uint64_t>();
  h.kind = ps.Read<uint8_t>();
  auto col_count = ps.Read<uint32_t>();
  h.column_ids.resize(col_count);
  for (uint32_t i = 0; i < col_count; ++i) {
    h.column_ids[i] = ps.Read<uint64_t>();
  }
  return h;
}

// Read the seg_ids of a REFERENCE body (ps positioned right after the header).
std::vector<uint64_t> ReadSegIds(duckdb::MemoryStream& ps) {
  auto seg_count = ps.Read<uint32_t>();
  std::vector<uint64_t> seg_ids(seg_count);
  for (uint32_t i = 0; i < seg_count; ++i) {
    seg_ids[i] = ps.Read<uint64_t>();
  }
  return seg_ids;
}

}  // namespace

//
// ChunkWriter
//

SearchShardWal::ChunkWriter::ChunkWriter(
  uint64_t seg_id, std::unique_ptr<duckdb::BufferedFileWriter> writer)
  : _seg_id(seg_id),
    _writer(std::move(writer)),
    _stream(std::make_unique<duckdb::MemoryStream>()) {}
SearchShardWal::ChunkWriter::ChunkWriter(ChunkWriter&&) noexcept = default;
SearchShardWal::ChunkWriter& SearchShardWal::ChunkWriter::operator=(
  ChunkWriter&&) noexcept = default;
SearchShardWal::ChunkWriter::~ChunkWriter() = default;

void SearchShardWal::ChunkWriter::Append(duckdb::DataChunk& chunk) {
  SDB_ASSERT(_writer);
  // Serialize the chunk into the reused stream, then block-compress it and
  // write one framed record: [u8 codec][u32 raw_len][u32 comp_len][payload].
  // The reader iterates frames and deserialises each from a bounded buffer
  // without depending on DataChunk::Deserialize consuming byte-for-byte. (The
  // chunk must be a materialised pipeline chunk, where per-vector size() ==
  // cardinality, which is what Sink delivers; a hand-built
  // Initialize+SetCardinality chunk would not round-trip through Serialize.)
  //
  // Rewind() reuses the stream's backing buffer across chunks (the serializer
  // holds a reference to _stream, so it stays per-call -- a sibling-member
  // serializer would dangle on ChunkWriter move).
  _stream->Rewind();
  duckdb::BinarySerializer serializer{*_stream};
  serializer.Begin();
  chunk.Serialize(serializer);
  serializer.End();
  const auto raw_len = static_cast<uint32_t>(_stream->GetPosition());

  // Compress (zstd-1) into the reused output buffer. The chunk-file write is
  // I/O-bound on this second copy of the data, and it compresses ~5-8x, so
  // this is the dominant insert-phase win. Store raw if it doesn't shrink
  // (incompressible/tiny chunk) so we never expand on disk.
  const auto bound = ZSTD_compressBound(raw_len);
  if (_comp.size() < bound) {
    _comp.resize(bound);
  }
  const size_t comp = ZSTD_compress(_comp.data(), _comp.size(),
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
  _writer->WriteData(payload, payload_len);
}

void SearchShardWal::ChunkWriter::Finish() {
  SDB_ASSERT(_writer);
  _writer->Sync();
}

//
// SearchShardWal
//

SearchShardWal::SearchShardWal(duckdb::FileSystem& fs,
                               std::filesystem::path wal_dir,
                               uint64_t seal_threshold)
  : _fs(fs),
    _wal_dir(std::move(wal_dir)),
    _chunks_dir(_wal_dir / "chunks"),
    _seal_threshold(seal_threshold) {}

SearchShardWal::~SearchShardWal() = default;

void SearchShardWal::EnsureActiveSegmentLocked(uint64_t first_tick) {
  if (_active) {
    return;
  }
  std::error_code ec;
  std::filesystem::create_directories(_wal_dir, ec);
  SDB_ASSERT(!ec, "create wal dir '", _wal_dir.string(), "': ", ec.message());
  _active = std::make_unique<duckdb::BufferedFileWriter>(
    _fs, (_wal_dir / SegmentName(first_tick)).string(), kAppendFlags);
  _active_first_tick = first_tick;
}

void SearchShardWal::WriteFrameLocked(const uint8_t* payload, uint64_t size) {
  SDB_ASSERT(_active);
  auto checksum = duckdb::Checksum(payload, size);
  _active->Write<uint64_t>(size);
  _active->Write<uint64_t>(checksum);
  _active->WriteData(payload, size);
  _active->Sync();  // commit point

  // Size-based rotation (WAL_DESIGN.md §10.2): seal once the active segment plus
  // the outstanding chunk files exceed the threshold. Counting chunk bytes rolls
  // a bulk insert promptly (tiny central record, GB of chunks) so GC can reclaim
  // its chunks; small INLINE inserts accumulate to the threshold before one
  // roll. The next AppendCommit opens a fresh segment named by its tick.
  if (_active->GetTotalWritten() + TotalChunkBytes(_chunks_dir) >
      _seal_threshold) {
    _active->Close();
    _active.reset();
    _active_first_tick = 0;
  }
}

uint64_t SearchShardWal::AppendCommit(const InlineCommit& rec) {
  std::lock_guard<std::mutex> lock(_append_mu);
  uint64_t tick = ++_tick;
  EnsureActiveSegmentLocked(tick);
  duckdb::MemoryStream payload;
  payload.Write<uint64_t>(tick);
  payload.Write<uint8_t>(kKindInline);
  payload.Write<uint32_t>(static_cast<uint32_t>(rec.column_ids.size()));
  for (uint64_t cid : rec.column_ids) {
    payload.Write<uint64_t>(cid);
  }
  duckdb::BinarySerializer serializer{payload};
  serializer.Begin();
  rec.data.Serialize(serializer);
  serializer.End();
  WriteFrameLocked(payload.GetData(), payload.GetPosition());
  return tick;
}

uint64_t SearchShardWal::AppendCommit(const ReferenceCommit& rec) {
  std::lock_guard<std::mutex> lock(_append_mu);
  uint64_t tick = ++_tick;
  EnsureActiveSegmentLocked(tick);
  duckdb::MemoryStream payload;
  payload.Write<uint64_t>(tick);
  payload.Write<uint8_t>(kKindReference);
  payload.Write<uint32_t>(static_cast<uint32_t>(rec.column_ids.size()));
  for (uint64_t cid : rec.column_ids) {
    payload.Write<uint64_t>(cid);
  }
  payload.Write<uint32_t>(static_cast<uint32_t>(rec.seg_ids.size()));
  for (uint64_t sid : rec.seg_ids) {
    payload.Write<uint64_t>(sid);
  }
  WriteFrameLocked(payload.GetData(), payload.GetPosition());
  return tick;
}

SearchShardWal::ChunkWriter SearchShardWal::NewChunkWriter() {
  std::error_code ec;
  std::filesystem::create_directories(_chunks_dir, ec);
  SDB_ASSERT(!ec, "create chunks dir '", _chunks_dir.string(),
             "': ", ec.message());
  uint64_t seg_id = _seg_id.fetch_add(1, std::memory_order_relaxed) + 1;
  auto writer = std::make_unique<duckdb::BufferedFileWriter>(
    _fs, (_chunks_dir / ChunkName(seg_id)).string(), kAppendFlags);
  return ChunkWriter{seg_id, std::move(writer)};
}

uint64_t SearchShardWal::Recover(uint64_t committed_tick,
                                 const ReplayCallback& cb) {
  std::lock_guard<std::mutex> lock(_append_mu);
  uint64_t max_tick = 0;
  std::unordered_set<uint64_t> referenced;

  for (const auto& [first_tick, path] : EnumerateSegments(_wal_dir)) {
    duckdb::BufferedFileReader reader(_fs, path.string().c_str());
    std::vector<uint8_t> payload;
    while (ReadFrame(reader, payload)) {
      duckdb::MemoryStream ps(payload.data(), payload.size());
      auto header = ReadHeader(ps);
      max_tick = std::max(max_tick, header.tick);
      ColumnIds column_ids{header.column_ids};

      if (header.kind == kKindReference) {
        auto seg_ids = ReadSegIds(ps);
        referenced.insert(seg_ids.begin(), seg_ids.end());
        if (header.tick <= committed_tick) {
          continue;  // consumed; seg_ids collected for the orphan sweep
        }
        std::vector<uint8_t> comp_buf;
        std::vector<uint8_t> raw_buf;
        constexpr uint64_t kChunkHdr = sizeof(uint8_t) + 2 * sizeof(uint32_t);
        for (uint64_t sid : seg_ids) {
          auto chunk_path = (_chunks_dir / ChunkName(sid)).string();
          duckdb::BufferedFileReader creader(_fs, chunk_path.c_str());
          // A committed reference guarantees its chunk files are complete
          // (fsynced at Combine BEFORE the commit, WAL_DESIGN.md §9). Unlike
          // the central segment's tolerated torn tail, a truncated/garbled
          // frame here means corruption -- fail loudly rather than silently
          // drop committed rows. Frame: [u8 codec][u32 raw_len][u32
          // comp_len][payload].
          while (creader.CurrentOffset() < creader.FileSize()) {
            uint64_t remaining = creader.FileSize() - creader.CurrentOffset();
            SDB_ENSURE(remaining >= kChunkHdr, ERROR_INTERNAL,
                       "corrupt search chunk file '", chunk_path,
                       "': trailing ", remaining, " bytes < frame header");
            auto codec = creader.Read<uint8_t>();
            auto raw_len = creader.Read<uint32_t>();
            auto comp_len = creader.Read<uint32_t>();
            SDB_ENSURE(creader.FileSize() - creader.CurrentOffset() >= comp_len,
                       ERROR_INTERNAL, "corrupt search chunk file '",
                       chunk_path, "': frame of ", comp_len,
                       " bytes exceeds remaining file");
            comp_buf.resize(comp_len);
            creader.ReadData(comp_buf.data(), comp_len);

            uint8_t* src = nullptr;
            uint32_t src_len = 0;
            if (codec == kChunkCodecZstd) {
              raw_buf.resize(raw_len);
              const size_t got = ZSTD_decompress(raw_buf.data(), raw_len,
                                                 comp_buf.data(), comp_len);
              SDB_ENSURE(!ZSTD_isError(got) && got == raw_len, ERROR_INTERNAL,
                         "corrupt search chunk file '", chunk_path,
                         "': zstd decompress failed (", raw_len, " expected)");
              src = raw_buf.data();
              src_len = raw_len;
            } else {
              SDB_ENSURE(codec == kChunkCodecNone && comp_len == raw_len,
                         ERROR_INTERNAL, "corrupt search chunk file '",
                         chunk_path, "': unknown codec ", codec);
              src = comp_buf.data();
              src_len = comp_len;
            }
            duckdb::MemoryStream ms(src, src_len);
            duckdb::BinaryDeserializer cdeser{ms};
            cdeser.Begin();
            duckdb::DataChunk chunk;
            chunk.Deserialize(cdeser);
            cdeser.End();
            cb(header.tick, column_ids, chunk);
          }
        }
      } else {  // INLINE
        if (header.tick <= committed_tick) {
          continue;
        }
        duckdb::BinaryDeserializer deser{ps};
        deser.Begin();
        auto cdc = duckdb::ColumnDataCollection::Deserialize(deser);
        deser.End();
        for (auto& chunk : cdc->Chunks()) {
          cb(header.tick, column_ids, chunk);
        }
      }
    }
  }

  // Orphan chunk-file sweep: a crashed bulk txn leaves chunk files not
  // referenced by any committed record. Delete them (best-effort);
  // MaxChunkSegId scans the *remaining* files so seg_id stays collision-free
  // either way.
  std::error_code ec;
  if (std::filesystem::exists(_chunks_dir, ec)) {
    for (const auto& entry :
         std::filesystem::directory_iterator(_chunks_dir, ec)) {
      if (ec || !entry.is_regular_file(ec)) {
        continue;
      }
      uint64_t sid = 0;
      if (ParseName(entry.path().filename().string(), kChunkSuffix, sid) &&
          !referenced.contains(sid)) {
        std::error_code remove_ec;
        std::filesystem::remove(entry.path(), remove_ec);
      }
    }
  }

  // Seed the counters from durable state. `_active` stays null: the next commit
  // opens a fresh segment named by its tick, so we never append after a torn
  // tail in an old segment.
  _tick = std::max(committed_tick, max_tick);
  _seg_id.store(MaxChunkSegId(_chunks_dir), std::memory_order_relaxed);
  return max_tick;
}

void SearchShardWal::SeedCounters(uint64_t committed_tick) {
  std::lock_guard<std::mutex> lock(_append_mu);
  uint64_t max_tick = 0;
  for (const auto& [first_tick, path] : EnumerateSegments(_wal_dir)) {
    duckdb::BufferedFileReader reader(_fs, path.string().c_str());
    std::vector<uint8_t> payload;
    while (ReadFrame(reader, payload)) {
      // The record's tick is the first u64 of the payload (§5.1) -- read it
      // without deserialising the rest of the record.
      duckdb::MemoryStream ps(payload.data(), payload.size());
      max_tick = std::max(max_tick, ps.Read<uint64_t>());
    }
  }
  _tick = std::max(committed_tick, max_tick);
  _seg_id.store(MaxChunkSegId(_chunks_dir), std::memory_order_relaxed);
}

void SearchShardWal::OnRefreshCommit(uint64_t committed_tick) {
  // Snapshot which segment is active (the only file AppendCommit mutates). The
  // active segment is sealed by size in AppendCommit, NOT here -- so we never
  // seal or touch it during GC, and the sweep below runs lock-free over the
  // immutable sealed segments + dead chunk files.
  uint64_t active_first_tick;
  {
    std::lock_guard<std::mutex> lock(_append_mu);
    active_first_tick = _active_first_tick;
  }

  // Delete fully-consumed SEALED segments (max tick <= committed_tick) and the
  // chunk files they reference. Skip the active segment: a concurrent
  // AppendCommit may roll it (so active_first_tick goes stale), but the worst
  // case is leaving the just-rolled segment for the next GC, and any freshly
  // opened active holds only ticks > committed_tick (newer than this commit),
  // so it is never eligible anyway.
  for (const auto& [first_tick, path] : EnumerateSegments(_wal_dir)) {
    if (active_first_tick != 0 && first_tick == active_first_tick) {
      continue;  // the live, still-appended segment
    }
    uint64_t seg_max_tick = 0;
    std::vector<uint64_t> seg_ids;
    {
      duckdb::BufferedFileReader reader(_fs, path.string().c_str());
      std::vector<uint8_t> payload;
      while (ReadFrame(reader, payload)) {
        duckdb::MemoryStream ps(payload.data(), payload.size());
        auto header = ReadHeader(ps);
        seg_max_tick = std::max(seg_max_tick, header.tick);
        if (header.kind == kKindReference) {
          auto ids = ReadSegIds(ps);
          seg_ids.insert(seg_ids.end(), ids.begin(), ids.end());
        }
      }
    }
    if (seg_max_tick > committed_tick) {
      continue;  // still has live records
    }
    for (uint64_t sid : seg_ids) {
      std::error_code ec;
      std::filesystem::remove(_chunks_dir / ChunkName(sid), ec);
    }
    std::error_code ec;
    std::filesystem::remove(path, ec);
  }
}

}  // namespace sdb::search
