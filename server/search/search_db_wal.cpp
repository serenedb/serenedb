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

#include <absl/cleanup/cleanup.h>
#include <absl/crc/crc32c.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/buffered_file_reader.hpp>
#include <duckdb/common/serializer/buffered_file_writer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/serializer.hpp>
#include <limits>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "catalog/database_directory.h"

namespace sdb::search {
namespace {

constexpr size_t kFrameSizeOffset = 0;
constexpr size_t kFrameCrcOffset = kFrameSizeOffset + sizeof(uint64_t);
constexpr size_t kFrameTickOffset = kFrameCrcOffset + sizeof(uint32_t);
constexpr size_t kFrameHeaderSize = kFrameTickOffset + sizeof(uint64_t);
constexpr std::array<uint8_t, kFrameHeaderSize> kEmptyFrameHeader{};

constexpr duckdb::field_id_t kBodySections = 0;

constexpr duckdb::field_id_t kSectionTableId = 0;
constexpr duckdb::field_id_t kSectionOps = 1;

constexpr duckdb::field_id_t kOpKind = 0;
constexpr duckdb::field_id_t kOpSegments = 3;
constexpr duckdb::field_id_t kOpRows = 5;
constexpr duckdb::field_id_t kOpDeleteRows = 6;

constexpr duckdb::field_id_t kRowsSlices = 0;

constexpr duckdb::field_id_t kSliceBase = 0;
constexpr duckdb::field_id_t kSliceChunk = 1;

constexpr std::string_view kSegPrefix = "search.wal.";

constexpr duckdb::idx_t kAppendFlags =
  duckdb::FileOpenFlags::FILE_FLAGS_WRITE |
  duckdb::FileOpenFlags::FILE_FLAGS_FILE_CREATE |
  duckdb::FileOpenFlags::FILE_FLAGS_APPEND |
  duckdb::FileOpenFlags::FILE_FLAGS_MULTI_CLIENT_ACCESS;

// PostgreSQL-style fixed-width 16-hex names: lexicographic order == numeric.
std::string SegmentName(uint64_t first_tick) {
  return absl::StrFormat("%s%016x", kSegPrefix, first_tick);
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

bool ParseName(std::string_view name, uint64_t& out) {
  return name.starts_with(kSegPrefix) &&
         ParseHex(name.substr(kSegPrefix.size()), out);
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
    if (ParseName(entry.path().filename().string(), first_tick)) {
      out.emplace_back(first_tick, entry.path());
    }
  }
  absl::c_sort(out,
               [](const auto& a, const auto& b) { return a.first < b.first; });
  return out;
}

// Serialise bands [first_band, last_band) of `cdc` as an object holding the
// list of slices, each its rowid base and its rows chunk-major. Chunk at a
// time, so nothing materialises the collection as Values the way
// ColumnDataCollection::Serialize would.
void EncodeRows(duckdb::MemoryStream& out,
                const duckdb::ColumnDataCollection& cdc,
                std::span<const SearchDbWal::InlinePk> bands, size_t first_band,
                size_t last_band) {
  duckdb::idx_t slices = 0;
  VisitInlineSegments(cdc, bands, first_band, last_band,
                      [&](duckdb::DataChunk&, uint64_t) { ++slices; });
  duckdb::BinarySerializer serializer{out};
  serializer.Begin();
  serializer.OnPropertyBegin(kRowsSlices, "slices");
  serializer.OnListBegin(slices);
  VisitInlineSegments(
    cdc, bands, first_band, last_band,
    [&](duckdb::DataChunk& chunk, uint64_t base) {
      serializer.OnObjectBegin();
      serializer.WriteProperty<uint64_t>(kSliceBase, "base", base);
      serializer.WriteObject(
        kSliceChunk, "chunk",
        [&](duckdb::BinarySerializer& obj) { chunk.Serialize(obj); });
      serializer.OnObjectEnd();
    });
  serializer.OnListEnd();
  serializer.OnPropertyEnd();
  serializer.End();
}

absl::crc32c_t TickCrc(uint64_t tick) {
  return absl::ComputeCrc32c(
    std::string_view{reinterpret_cast<const char*>(&tick), sizeof(tick)});
}

// Read one [u64 size][u32 crc32c][u64 tick][body] frame. Returns false at EOF
// or on a torn/corrupt tail -- the caller stops the segment there.
bool ReadFrame(duckdb::BufferedFileReader& reader, uint64_t& tick,
               std::vector<uint8_t>& body) {
  if (reader.FileSize() - reader.CurrentOffset() < kFrameHeaderSize) {
    return false;
  }
  const auto size = reader.Read<uint64_t>();
  const auto crc = reader.Read<uint32_t>();
  tick = reader.Read<uint64_t>();
  if (reader.FileSize() - reader.CurrentOffset() < size) {
    return false;
  }
  body.resize(size);
  reader.ReadData(body.data(), size);
  const auto actual = absl::ExtendCrc32c(
    TickCrc(tick),
    std::string_view{reinterpret_cast<const char*>(body.data()), size});
  return static_cast<uint32_t>(actual) == crc;
}

[[noreturn]] void ThrowUnreadable(const std::filesystem::path& path,
                                  std::string_view reason) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("search WAL segment '", path.string(),
                          "' cannot be read: ", reason));
}

std::span<const uint8_t> ViewBytes(duckdb::MemoryStream& stream, uint64_t size,
                                   const std::filesystem::path& path) {
  const auto position = stream.GetPosition();
  if (size > stream.GetCapacity() - position) {
    ThrowUnreadable(path, "a length runs past the end of the record");
  }
  stream.SetPosition(position + size);
  return {stream.GetData() + position, size};
}

// Scratch reused across every record of a sweep, so parsing a WAL allocates
// once rather than per section.
struct ParseScratch {
  std::vector<SearchDbWal::SegmentRef> segments;
  std::vector<int64_t> delete_rows;
};

// One parsed op. Ops are stored in issue order, so walking them in order is
// the ordering -- nothing else has to be reconstructed.
struct ParsedOp {
  SearchDbWal::Op::Kind kind = SearchDbWal::Op::Kind::kRows;
  // kRows: a view into the payload, an object holding the slices.
  std::span<const uint8_t> rows;
  // kDelete: into scratch.delete_rows.
  std::span<const int64_t> delete_rows;
  // kSegments: into scratch.segments, which owns its strings.
  std::span<const SearchDbWal::SegmentRef> segments;
};

ParsedOp ReadOp(duckdb::BinaryDeserializer& in, duckdb::MemoryStream& stream,
                ParseScratch& scratch, const std::filesystem::path& path) {
  ParsedOp op;
  const auto kind = in.ReadProperty<uint8_t>(kOpKind, "kind");
  op.kind = static_cast<SearchDbWal::Op::Kind>(kind);
  switch (op.kind) {
    case SearchDbWal::Op::Kind::kRows:
      in.OnPropertyBegin(kOpRows, "rows");
      op.rows = ViewBytes(stream, in.ReadUnsignedInt64(), path);
      in.OnPropertyEnd();
      break;
    case SearchDbWal::Op::Kind::kDelete: {
      in.OnPropertyBegin(kOpDeleteRows, "delete_rows");
      const auto bytes = ViewBytes(stream, in.ReadUnsignedInt64(), path);
      in.OnPropertyEnd();
      if (bytes.size() % sizeof(int64_t) != 0) {
        ThrowUnreadable(path, "delete rows are not a whole number of rowids");
      }
      // Fixed width, written in host order; copied out since the payload
      // gives no alignment.
      scratch.delete_rows.resize(bytes.size() / sizeof(int64_t));
      std::memcpy(scratch.delete_rows.data(), bytes.data(), bytes.size());
      op.delete_rows = scratch.delete_rows;
      break;
    }
    case SearchDbWal::Op::Kind::kTruncate:
      break;
    case SearchDbWal::Op::Kind::kSegments:
      in.OnPropertyBegin(kOpSegments, "segments");
      irs::utils::ReadTuple(in, scratch.segments);
      in.OnPropertyEnd();
      op.segments = scratch.segments;
      break;
    default:
      ThrowUnreadable(path,
                      absl::StrCat("unknown op kind ", static_cast<int>(kind)));
  }
  return op;
}

// Replays a kRows object: each slice's rowid base and chunk.
template<typename RowHandler>
void DecodeRows(std::span<const uint8_t> rows,
                const std::filesystem::path& path, const RowHandler& on_rows) {
  duckdb::MemoryStream stream{const_cast<uint8_t*>(rows.data()), rows.size()};
  duckdb::BinaryDeserializer in{stream};
  in.Begin();
  in.ReadList(
    kRowsSlices, "slices",
    [&](duckdb::BinaryDeserializer::List& slices, duckdb::idx_t) {
      slices.ReadObject([&](duckdb::BinaryDeserializer& slice) {
        const auto base = slice.ReadProperty<uint64_t>(kSliceBase, "base");
        duckdb::DataChunk chunk;
        slice.ReadObject(
          kSliceChunk, "chunk",
          [&](duckdb::BinaryDeserializer& obj) { chunk.Deserialize(obj); });
        on_rows(base, chunk);
      });
    });
  in.End();
  if (stream.GetPosition() != rows.size()) {
    ThrowUnreadable(path, "unexpected bytes after the end of the rows");
  }
}

template<typename OpHandler>
void VisitRecord(uint64_t tick, std::span<const uint8_t> body_bytes,
                 const std::filesystem::path& path, ParseScratch& scratch,
                 const OpHandler& on_op) {
  duckdb::MemoryStream body{const_cast<uint8_t*>(body_bytes.data()),
                            body_bytes.size()};
  duckdb::BinaryDeserializer body_in{body};
  body_in.Begin();
  body_in.ReadList(
    kBodySections, "sections",
    [&](duckdb::BinaryDeserializer::List& sections, duckdb::idx_t) {
      sections.ReadObject([&](duckdb::BinaryDeserializer& section) {
        const auto table_id =
          section.ReadProperty<uint64_t>(kSectionTableId, "table_id");
        section.ReadList(
          kSectionOps, "ops",
          [&](duckdb::BinaryDeserializer::List& ops, duckdb::idx_t) {
            ops.ReadObject([&](duckdb::BinaryDeserializer& op) {
              on_op(tick, table_id, ReadOp(op, body, scratch, path));
            });
          });
      });
    });
  body_in.End();
  if (body.GetPosition() != body_bytes.size()) {
    ThrowUnreadable(path, "unexpected bytes after the end of the record body");
  }
}

void WriteOp(duckdb::BinarySerializer& out, const SearchDbWal::ShardSection& s,
             const SearchDbWal::Op& op, duckdb::MemoryStream& data) {
  out.WriteProperty<uint8_t>(kOpKind, "kind", static_cast<uint8_t>(op.kind));
  switch (op.kind) {
    case SearchDbWal::Op::Kind::kRows:
      SDB_ASSERT(s.inline_data != nullptr);
      data.Rewind();
      EncodeRows(data, *s.inline_data, s.inline_pks, op.first_band,
                 op.last_band);
      out.WriteProperty(kOpRows, "rows", data.GetData(), data.GetPosition());
      break;
    case SearchDbWal::Op::Kind::kDelete:
      out.WriteProperty(
        kOpDeleteRows, "delete_rows",
        reinterpret_cast<duckdb::const_data_ptr_t>(op.delete_rows.data()),
        op.delete_rows.size() * sizeof(int64_t));
      break;
    case SearchDbWal::Op::Kind::kTruncate:
      break;
    case SearchDbWal::Op::Kind::kSegments:
      out.OnPropertyBegin(kOpSegments, "segments");
      irs::utils::WriteTuple(out, op.segments);
      out.OnPropertyEnd();
      break;
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
      uint64_t tick = 0;
      std::vector<uint8_t> body;
      while (ReadFrame(reader, tick, body)) {
        last_tick = tick;
        any = true;
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
  auto seg_path = _wal_dir / SegmentName(first_tick);
  std::error_code exists_ec;
  SDB_ENSURE(!std::filesystem::exists(seg_path, exists_ec),
             "search WAL: new active segment '", seg_path.string(),
             "' already exists -- tick seed regressed");
  _active = std::make_unique<duckdb::BufferedFileWriter>(_fs, seg_path.string(),
                                                         kAppendFlags);
  catalog::SyncDirectory(_wal_dir);
  _active_first_tick = first_tick;
}

void SearchDbWal::WriteFrameLocked(const uint8_t* frame, uint64_t size) {
  SDB_ASSERT(_active);
  _active->WriteData(frame, size);
  try {
    _active->Sync();  // commit point
  } catch (const std::exception& e) {
    SDB_FATAL(SEARCH, "search WAL: failed to sync a commit record: ", e.what());
  }

  if (_active->GetTotalWritten() > _seal_threshold) {
    _active->Close();
    _active.reset();
    _active_first_tick = 0;
  }
}

uint64_t SearchDbWal::AppendCommit(std::span<const ShardSection> sections,
                                   uint64_t tick_span) {
  return AppendCommit(sections, tick_span, [](uint64_t) noexcept {});
}

uint64_t SearchDbWal::AppendCommit(
  std::span<const ShardSection> sections, uint64_t tick_span,
  absl::AnyInvocable<void(uint64_t) noexcept> on_durable) {
  SDB_ASSERT(!sections.empty(), "AppendCommit with no shard sections");
  SDB_ASSERT(tick_span >= 1, "every commit advances the tick by at least 1");

  duckdb::MemoryStream frame;
  frame.WriteData(kEmptyFrameHeader.data(), kEmptyFrameHeader.size());
  // Reused inline-CDC scratch across every INLINE op (Rewind keeps the buffer).
  duckdb::MemoryStream tmp;
  duckdb::BinarySerializer body_record{frame};
  body_record.Begin();
  body_record.WriteList(
    kBodySections, "sections", sections.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
      const auto& s = sections[i];
      SDB_ASSERT(!s.ops.empty(), "shard section with no ops");
      list.WriteObject([&](duckdb::BinarySerializer& section) {
        section.WriteProperty<uint64_t>(kSectionTableId, "table_id",
                                        s.table_id);
        section.WriteList(
          kSectionOps, "ops", s.ops.size(),
          [&](duckdb::BinarySerializer::List& ops, duckdb::idx_t j) {
            ops.WriteObject([&](duckdb::BinarySerializer& out) {
              WriteOp(out, s, s.ops[j], tmp);
            });
          });
      });
    });
  body_record.End();

  const uint64_t body_size = frame.GetPosition() - kFrameHeaderSize;
  const auto body_crc = absl::ComputeCrc32c(std::string_view{
    reinterpret_cast<const char*>(frame.GetData()) + kFrameHeaderSize,
    body_size});

  absl::MutexLock lock(&_append_mu);

  const uint64_t tick = _tick.load(std::memory_order_relaxed) + tick_span;
  // Always burn tick, on error we do not want this value to be reused.
  absl::Cleanup publish = [&] { _tick.store(tick, std::memory_order_release); };
  EnsureActiveSegmentLocked(tick);

  const auto crc = static_cast<uint32_t>(
    absl::ConcatCrc32c(TickCrc(tick), body_crc, body_size));
  std::memcpy(frame.GetData() + kFrameSizeOffset, &body_size,
              sizeof(body_size));
  std::memcpy(frame.GetData() + kFrameCrcOffset, &crc, sizeof(crc));
  std::memcpy(frame.GetData() + kFrameTickOffset, &tick, sizeof(tick));

  WriteFrameLocked(frame.GetData(), frame.GetPosition());
  on_durable(tick);
  return tick;
}

void SearchDbWal::RegisterShard(duckdb::idx_t table_id,
                                uint64_t committed_tick) {
  {
    absl::MutexLock lock(&_sub_mu);
    auto& cur = _committed[table_id];
    cur = std::max(cur, committed_tick);
  }
  // Continue the tick line past every shard's durable tick: a shard's committed
  // tick can exceed the WAL max if consumed records were already GC'd.
  absl::MutexLock lock(&_append_mu);
  if (_tick.load(std::memory_order_relaxed) < committed_tick) {
    _tick.store(committed_tick, std::memory_order_relaxed);
  }
}

void SearchDbWal::OnShardCommit(duckdb::idx_t table_id,
                                uint64_t committed_tick) {
  {
    absl::MutexLock lock(&_sub_mu);
    auto& cur = _committed[table_id];
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

void SearchDbWal::DeregisterShard(duckdb::idx_t table_id) {
  {
    absl::MutexLock lock(&_sub_mu);
    _committed.erase(table_id);
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
      uint64_t tick = 0;
      std::vector<uint8_t> body;
      while (consumed && ReadFrame(reader, tick, body)) {
        consumed = tick <= min_tick;
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
    uint64_t frame_tick = 0;
    std::vector<uint8_t> body;
    while (ReadFrame(reader, frame_tick, body)) {
      auto on_op = [&](uint64_t tick, uint64_t table_id, const ParsedOp& op) {
        const duckdb::idx_t tid{table_id};
        if (!exists_of(tid) || tick <= committed_of(tid)) {
          return;
        }
        switch (op.kind) {
          case Op::Kind::kRows:
            DecodeRows(op.rows, path,
                       [&](uint64_t base, duckdb::DataChunk& chunk) {
                         insert_cb(tick, tid, base, chunk);
                       });
            break;
          case Op::Kind::kDelete:
            delete_cb(tick, tid, op.delete_rows);
            break;
          case Op::Kind::kTruncate:
            truncate_cb(tick, tid);
            break;
          case Op::Kind::kSegments:
            for (const auto& ref : op.segments) {
              adopt_cb(tick, tid, ref);
            }
            break;
        }
      };
      try {
        VisitRecord(frame_tick, body, path, scratch, on_op);
      } catch (const duckdb::SerializationException& e) {
        ThrowUnreadable(path, duckdb::ErrorData{e}.RawMessage());
      }
      max_tick = std::max(max_tick, frame_tick);
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
  // so an op that starts mid-way has to walk past them.
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
