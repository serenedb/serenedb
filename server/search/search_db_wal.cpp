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
#include <utility>
#include <vector>

#include "basics/assert.h"
#include "basics/log.h"
#include "basics/serialization.h"
#include "basics/serializer.h"
#include "iresearch/formats/formats.hpp"
#include "pg/sql_exception_macro.h"

namespace sdb::search {
namespace {

constexpr uint8_t kKindInline = 0;
constexpr uint8_t kKindDelete = 2;
constexpr uint8_t kKindTruncate = 3;
constexpr uint8_t kKindSegment = 4;

constexpr std::string_view kSegSuffix = ".swal";

constexpr duckdb::FileOpenFlags kAppendFlags =
  duckdb::FileFlags::FILE_FLAGS_WRITE |
  duckdb::FileFlags::FILE_FLAGS_FILE_CREATE |
  duckdb::FileFlags::FILE_FLAGS_APPEND |
  duckdb::FileFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS;

// PostgreSQL-style fixed-width 16-hex names: lexicographic order == numeric.
std::string SegmentName(uint64_t first_tick) {
  return absl::StrFormat("%016x%s", first_tick, kSegSuffix);
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

// "<016x>.swal" -> first_tick.
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
  absl::c_sort(out,
               [](const auto& a, const auto& b) { return a.first < b.first; });
  return out;
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

// Scratch reused across every record of a sweep, so parsing a WAL allocates
// once rather than per op.
struct ParseScratch {
  std::vector<std::string_view> pks;
  std::vector<SearchDbWal::SegmentRef> segments;
};

struct ParsedOp {
  uint8_t kind = 0;
  // INLINE
  uint32_t seg_count = 0;  // # of (u64 base, u64 count) InlinePk pairs
  const uint8_t* pk_blob = nullptr;  // seg_count * 2 * u64, packed
  const uint8_t* inline_blob = nullptr;
  uint64_t inline_blob_len = 0;
  // DELETE (views into the payload, into scratch.pks)
  std::span<const std::string_view> delete_pks;
  // SEGMENT (into scratch.segments; owns its strings, unlike the views above)
  std::span<const SearchDbWal::SegmentRef> segments;
};

ParsedOp ParseOp(Cursor& c, ParseScratch& scratch) {
  auto& pk_scratch = scratch.pks;
  ParsedOp op;
  op.kind = c.Read<uint8_t>();
  switch (op.kind) {
    case kKindInline:
      op.seg_count = c.Read<uint32_t>();
      op.pk_blob = c.ReadBlob(op.seg_count * 2 * sizeof(uint64_t));
      op.inline_blob_len = c.Read<uint64_t>();
      op.inline_blob = c.ReadBlob(op.inline_blob_len);
      break;
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
    case kKindSegment: {
      const auto len = c.Read<uint64_t>();
      const auto* blob = c.ReadBlob(len);
      duckdb::MemoryStream ms(const_cast<uint8_t*>(blob), len);
      duckdb::BinaryDeserializer deser{ms};
      deser.Begin();
      // Resizes the scratch and reads each element via SerdeRead on SegmentRef.
      basics::ReadTuple(deser, scratch.segments);
      deser.End();
      op.segments = scratch.segments;
      break;
    }
    case kKindTruncate:
      break;  // bodyless
    default:
      SDB_ENSURE(false,
                 "unknown search WAL op kind: ", static_cast<int>(op.kind));
  }
  return op;
}

template<typename OpHandler>
void VisitSectionsOps(Cursor& c, ParseScratch& scratch,
                      const OpHandler& on_op) {
  const auto shard_count = c.Read<uint32_t>();
  for (uint32_t s = 0; s < shard_count; ++s) {
    const auto h = ReadSectionHeader(c);
    const auto op_count = c.Read<uint32_t>();
    for (uint32_t o = 0; o < op_count; ++o) {
      ParsedOp op = ParseOp(c, scratch);
      on_op(h.table_id, op);
    }
  }
}

}  // namespace

SearchDbWal::SearchDbWal(duckdb::FileSystem& fs, std::filesystem::path wal_dir,
                         uint64_t seal_threshold)
  : _fs(fs), _wal_dir(std::move(wal_dir)), _seal_threshold(seal_threshold) {
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
    SDB_ENSURE(!ec, "remove corrupted wal file '", path.string(),
               "': ", ec.message());
  }
  _tick.store(max_tick, std::memory_order_relaxed);
}

SearchDbWal::~SearchDbWal() = default;

void SearchDbWal::EnsureActiveSegmentLocked(uint64_t first_tick) {
  if (_active) {
    return;
  }
  std::error_code ec;
  std::filesystem::create_directories(_wal_dir, ec);
  SDB_ENSURE(!ec, "create wal dir '", _wal_dir.string(), "': ", ec.message());
  auto seg_path = _wal_dir / SegmentName(first_tick);
  std::error_code exists_ec;
  SDB_ENSURE(!std::filesystem::exists(seg_path, exists_ec),
             "search WAL: new active segment '", seg_path.string(),
             "' already exists -- tick seed regressed");
  _active = std::make_unique<duckdb::BufferedFileWriter>(_fs, seg_path.string(),
                                                         kAppendFlags);
  _active_first_tick = first_tick;
}

void SearchDbWal::WriteFrameLocked(const uint8_t* payload, uint64_t size) {
  SDB_ASSERT(_active);
  auto checksum = duckdb::Checksum(payload, size);
  _active->Write<uint64_t>(size);
  _active->Write<uint64_t>(checksum);
  _active->WriteData(payload, size);
  _active->Sync();  // commit point

  if (_active->GetTotalWritten() > _seal_threshold) {
    _active->Close();
    _active.reset();
    _active_first_tick = 0;
  }
}

uint64_t SearchDbWal::AppendCommit(std::span<const ShardSection> sections,
                                   uint64_t tick_span) {
  SDB_ASSERT(!sections.empty(), "AppendCommit with no shard sections");
  SDB_ASSERT(tick_span >= 1, "every commit advances the tick by at least 1");
  absl::MutexLock lock(&_append_mu);

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
      SDB_ASSERT((op.inline_data != nullptr) + (!op.segments.empty()) +
                     (!op.delete_pks.empty()) + op.truncate ==
                   1,
                 "op must be exactly one of INLINE / SEGMENT / DELETE / "
                 "TRUNCATE");
      const uint8_t kind = op.truncate            ? kKindTruncate
                           : op.inline_data       ? kKindInline
                           : !op.segments.empty() ? kKindSegment
                                                  : kKindDelete;
      payload.Write<uint8_t>(kind);
      if (kind == kKindInline) {
        payload.Write<uint32_t>(static_cast<uint32_t>(op.inline_pks.size()));
        for (const auto& pk : op.inline_pks) {
          payload.Write<uint64_t>(pk.base);
          payload.Write<uint64_t>(pk.count);
        }
        tmp.Rewind();
        duckdb::BinarySerializer serializer{tmp,
                                            duckdb::VersionStorageOptions()};
        serializer.Begin();
        op.inline_data->Serialize(serializer);
        serializer.End();
        auto len = static_cast<uint64_t>(tmp.GetPosition());
        payload.Write<uint64_t>(len);
        payload.WriteData(tmp.GetData(), len);
      } else if (kind == kKindSegment) {
        // The list, its count and each segment's fields all go through the
        // serializer, so only the blob's length is framed here.
        tmp.Rewind();
        duckdb::BinarySerializer serializer{tmp,
                                            duckdb::VersionStorageOptions()};
        serializer.Begin();
        basics::WriteTuple(serializer, op.segments);
        serializer.End();
        const auto len = static_cast<uint64_t>(tmp.GetPosition());
        payload.Write<uint64_t>(len);
        payload.WriteData(tmp.GetData(), len);
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

void SearchDbWal::RegisterShard(ObjectId table_id, uint64_t committed_tick) {
  {
    absl::MutexLock lock(&_sub_mu);
    auto& cur = _committed[table_id.id()];
    cur = std::max(cur, committed_tick);
  }
  // Continue the tick line past every shard's durable tick: a shard's committed
  // tick can exceed the WAL max if consumed records were already GC'd.
  absl::MutexLock lock(&_append_mu);
  if (_tick.load(std::memory_order_relaxed) < committed_tick) {
    _tick.store(committed_tick, std::memory_order_relaxed);
  }
}

void SearchDbWal::OnShardCommit(ObjectId table_id, uint64_t committed_tick) {
  {
    absl::MutexLock lock(&_sub_mu);
    auto& cur = _committed[table_id.id()];
    cur = std::max(cur, committed_tick);
  }
  {
    absl::MutexLock lock(&_append_mu);
    if (_tick.load(std::memory_order_relaxed) < committed_tick) {
      _tick.store(committed_tick, std::memory_order_relaxed);
    }
  }
  RunGc();
}

void SearchDbWal::DeregisterShard(ObjectId table_id) {
  {
    absl::MutexLock lock(&_sub_mu);
    _committed.erase(table_id.id());
  }
  RunGc();
}

uint64_t SearchDbWal::MinCommittedTick() {
  absl::MutexLock lock(&_sub_mu);
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
    absl::MutexLock lock(&_append_mu);
    active_first_tick = _active_first_tick;
  }

  // Only the frame headers matter: a record owns no other files, and a SEGMENT
  // op points at the index's own segments, which iresearch reclaims itself.
  for (const auto& [first_tick, path] : EnumerateSegments(_wal_dir)) {
    if (active_first_tick != 0 && first_tick == active_first_tick) {
      continue;  // the live, still-appended segment
    }
    bool consumed = true;
    {
      duckdb::BufferedFileReader reader(_fs, path.string().c_str());
      std::vector<uint8_t> payload;
      while (ReadFrame(reader, payload)) {
        Cursor c(payload);
        if (c.Read<uint64_t>() > min_tick) {  // tick (records are ascending)
          consumed = false;
          break;
        }
      }
    }
    if (!consumed) {
      break;  // this + every later (higher-tick) segment still un-published
    }
    std::error_code ec;
    std::filesystem::remove(path, ec);
  }
}

uint64_t SearchDbWal::Recover(const ShardExistsFn& exists_of,
                              const ShardCommittedFn& committed_of,
                              const ReplayCallback& insert_cb,
                              const DeleteReplayCallback& delete_cb,
                              const TruncateReplayCallback& truncate_cb,
                              const AdoptReplayCallback& adopt_cb) {
  absl::MutexLock lock(&_append_mu);
  uint64_t max_tick = 0;
  ParseScratch scratch;  // reused across records

  for (const auto& [first_tick, path] : EnumerateSegments(_wal_dir)) {
    duckdb::BufferedFileReader reader(_fs, path.string().c_str());
    std::vector<uint8_t> payload;
    while (ReadFrame(reader, payload)) {
      Cursor c(payload);
      const uint64_t tick = c.Read<uint64_t>();
      max_tick = std::max(max_tick, tick);
      VisitSectionsOps(c, scratch, [&](uint64_t table_id, ParsedOp& op) {
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
              *cdc, segments, [&](duckdb::DataChunk& chunk, uint64_t pk_base) {
                insert_cb(tick, tid, pk_base, chunk);
              });
            break;
          }
          case kKindSegment:
            if (live) {
              // In manifest order, so the host can place each segment
              // relative to the deletes it has replayed so far.
              for (const auto& ref : op.segments) {
                adopt_cb(tick, tid, ref);
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
