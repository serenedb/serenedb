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
#include <iresearch/formats/formats.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/serialization.hpp>
#include <iresearch/utils/serializer.hpp>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

namespace sdb::search {
namespace {

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

// Serialise bands [first_band, last_band) of `cdc` as a self-describing run:
// a slice count, then each slice's rowid base and its rows chunk-major. Chunk
// at a time, so nothing materialises the collection as Values the way
// ColumnDataCollection::Serialize would.
void EncodeRows(duckdb::MemoryStream& out,
                const duckdb::ColumnDataCollection& cdc,
                std::span<const SearchDbWal::InlinePk> bands, size_t first_band,
                size_t last_band) {
  uint32_t slices = 0;
  VisitInlineSegments(cdc, bands, first_band, last_band,
                      [&](duckdb::DataChunk&, uint64_t) { ++slices; });
  out.Write<uint32_t>(slices);
  duckdb::BinarySerializer serializer{out, duckdb::VersionStorageOptions()};
  VisitInlineSegments(cdc, bands, first_band, last_band,
                      [&](duckdb::DataChunk& chunk, uint64_t base) {
                        out.Write<uint64_t>(base);
                        serializer.Begin();
                        chunk.Serialize(serializer);
                        serializer.End();
                      });
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

// Scratch reused across every record of a sweep, so parsing a WAL allocates
// once rather than per section.
struct ParseScratch {
  std::vector<SearchDbWal::SegmentRef> segments;
};

// One parsed entry. Entries are stored in issue order, so walking them in
// order is the ordering -- nothing else has to be reconstructed.
struct ParsedEntry {
  SearchDbWal::Entry::Kind kind = SearchDbWal::Entry::Kind::kRows;
  // kRows: the run blob, self-describing (slice count, then base + chunk).
  const uint8_t* rows_blob = nullptr;
  uint64_t rows_blob_len = 0;
  // kDelete: a view into the payload.
  std::span<const int64_t> delete_rows;
  // kSegments: into scratch.segments, which owns its strings.
  std::span<const SearchDbWal::SegmentRef> segments;
};

ParsedEntry ParseEntry(Cursor& c, ParseScratch& scratch) {
  ParsedEntry e;
  e.kind = static_cast<SearchDbWal::Entry::Kind>(c.Read<uint8_t>());
  switch (e.kind) {
    case SearchDbWal::Entry::Kind::kRows:
      e.rows_blob_len = c.Read<uint64_t>();
      e.rows_blob = c.ReadBlob(e.rows_blob_len);
      break;
    case SearchDbWal::Entry::Kind::kDelete: {
      const auto n = c.Read<uint32_t>();
      const auto* blob = c.ReadBlob(uint64_t{n} * sizeof(int64_t));
      // Fixed width, written in host order, so the payload is the array.
      e.delete_rows =
        std::span<const int64_t>{reinterpret_cast<const int64_t*>(blob), n};
      break;
    }
    case SearchDbWal::Entry::Kind::kTruncate:
      break;  // bodyless
    case SearchDbWal::Entry::Kind::kSegments: {
      const auto len = c.Read<uint64_t>();
      const auto* blob = c.ReadBlob(len);
      duckdb::MemoryStream ms(const_cast<uint8_t*>(blob), len);
      duckdb::BinaryDeserializer deser{ms};
      deser.Begin();
      // Resizes the scratch and reads each element via SerdeRead on SegmentRef.
      irs::utils::ReadTuple(deser, scratch.segments);
      deser.End();
      e.segments = scratch.segments;
      break;
    }
    default:
      SDB_ENSURE(false,
                 "unknown search WAL entry kind: ", static_cast<int>(e.kind));
  }
  return e;
}

// Replays a kRows blob: slice count, then each slice's rowid base and chunk.
template<typename RowHandler>
void DecodeRows(const uint8_t* blob, uint64_t len, const RowHandler& on_rows) {
  duckdb::MemoryStream ms(const_cast<uint8_t*>(blob), len);
  const auto slices = ms.Read<uint32_t>();
  duckdb::BinaryDeserializer deser{ms};
  for (uint32_t i = 0; i < slices; ++i) {
    const auto base = ms.Read<uint64_t>();
    deser.Begin();
    duckdb::DataChunk chunk;
    chunk.Deserialize(deser);
    deser.End();
    on_rows(base, chunk);
  }
}

template<typename SectionHandler>
void VisitSections(Cursor& c, ParseScratch& scratch,
                   const SectionHandler& on_section) {
  const auto shard_count = c.Read<uint32_t>();
  for (uint32_t s = 0; s < shard_count; ++s) {
    const auto table_id = c.Read<uint64_t>();
    const auto entry_count = c.Read<uint32_t>();
    on_section(table_id, entry_count, c, scratch);
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
    SDB_ASSERT(!s.entries.empty(), "shard section with no entries");
    payload.Write<uint64_t>(s.table_id.id());
    payload.Write<uint32_t>(static_cast<uint32_t>(s.entries.size()));
    for (const auto& e : s.entries) {
      payload.Write<uint8_t>(static_cast<uint8_t>(e.kind));
      switch (e.kind) {
        case SearchDbWal::Entry::Kind::kRows: {
          SDB_ASSERT(s.inline_data != nullptr);
          tmp.Rewind();
          EncodeRows(tmp, *s.inline_data, s.inline_pks, e.first_band,
                     e.last_band);
          const auto len = static_cast<uint64_t>(tmp.GetPosition());
          payload.Write<uint64_t>(len);
          payload.WriteData(tmp.GetData(), len);
          break;
        }
        case SearchDbWal::Entry::Kind::kDelete:
          // Fixed width, so no per-entry framing.
          payload.Write<uint32_t>(static_cast<uint32_t>(e.delete_rows.size()));
          payload.WriteData(
            reinterpret_cast<const uint8_t*>(e.delete_rows.data()),
            e.delete_rows.size() * sizeof(int64_t));
          break;
        case SearchDbWal::Entry::Kind::kTruncate:
          break;  // bodyless
        case SearchDbWal::Entry::Kind::kSegments: {
          // The list, its count and each segment's fields all go through the
          // serializer, so only the blob's length is framed here.
          tmp.Rewind();
          duckdb::BinarySerializer serializer{tmp,
                                              duckdb::VersionStorageOptions()};
          serializer.Begin();
          irs::utils::WriteTuple(serializer, e.segments);
          serializer.End();
          const auto len = static_cast<uint64_t>(tmp.GetPosition());
          payload.Write<uint64_t>(len);
          payload.WriteData(tmp.GetData(), len);
          break;
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
      VisitSections(
        c, scratch,
        [&](uint64_t table_id, uint32_t entry_count, Cursor& cur,
            ParseScratch& sc) {
          const ObjectId tid{table_id};
          // Entries have to be consumed either way to keep the cursor in step,
          // even for a shard this replay skips.
          const bool live = exists_of(tid) && tick > committed_of(tid);
          for (uint32_t i = 0; i < entry_count; ++i) {
            const auto e = ParseEntry(cur, sc);
            if (!live) {
              continue;
            }
            switch (e.kind) {
              case SearchDbWal::Entry::Kind::kRows:
                DecodeRows(e.rows_blob, e.rows_blob_len,
                           [&](uint64_t base, duckdb::DataChunk& chunk) {
                             insert_cb(tick, tid, base, chunk);
                           });
                break;
              case SearchDbWal::Entry::Kind::kDelete:
                delete_cb(tick, tid, e.delete_rows);
                break;
              case SearchDbWal::Entry::Kind::kTruncate:
                truncate_cb(tick, tid);
                break;
              case SearchDbWal::Entry::Kind::kSegments:
                for (const auto& ref : e.segments) {
                  adopt_cb(tick, tid, ref);
                }
                break;
            }
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
  std::span<const SearchDbWal::InlinePk> segments, size_t first_band,
  size_t last_band,
  const absl::AnyInvocable<void(duckdb::DataChunk&, uint64_t base) const>&
    emit) {
  if (segments.empty()) {
    SDB_ASSERT(first_band == 0);
    for (auto& chunk : cdc.Chunks()) {
      emit(chunk, 0);
    }
    return;
  }
  SDB_ASSERT(first_band <= last_band && last_band <= segments.size());
  if (first_band == last_band) {
    return;
  }
  // Rows the bands before `first_band` hold: the collection is one run of rows,
  // so an entry that starts mid-way has to walk past them.
  uint64_t skip = 0;
  for (size_t i = 0; i < first_band; ++i) {
    skip += segments[i].count;
  }

  size_t seg = first_band;
  uint64_t seg_off = 0;  // rows of the current band already emitted
  for (auto& chunk : cdc.Chunks()) {
    const uint64_t n = chunk.size();
    if (skip >= n) {
      skip -= n;
      continue;
    }
    uint64_t off = skip;  // rows of this (coalesced) chunk already consumed
    skip = 0;
    while (off < n && seg < last_band) {
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
    if (seg >= last_band) {
      return;
    }
  }
}

}  // namespace sdb::search
