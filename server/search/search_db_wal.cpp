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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_format.h>

#include <algorithm>
#include <cstring>
#include <duckdb/common/checksum.hpp>
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

namespace sdb::search {
namespace {

constexpr uint8_t kKindInline = 0;
constexpr uint8_t kKindDelete = 2;
constexpr uint8_t kKindTruncate = 3;
constexpr uint8_t kKindSegment = 4;

constexpr duckdb::field_id_t kRecordTick = 0;
constexpr duckdb::field_id_t kRecordSections = 1;

constexpr duckdb::field_id_t kSectionTableId = 0;
constexpr duckdb::field_id_t kSectionOps = 1;

constexpr duckdb::field_id_t kOpKind = 0;
constexpr duckdb::field_id_t kOpInlinePks = 1;
constexpr duckdb::field_id_t kOpInlineData = 2;
constexpr duckdb::field_id_t kOpSegments = 3;
constexpr duckdb::field_id_t kOpDeletePks = 4;

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

[[noreturn]] void ThrowUnreadable(const std::filesystem::path& path,
                                  std::string_view reason) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("search WAL segment '", path.string(),
                          "' cannot be read: ", reason));
}

uint64_t ReadRecordTick(duckdb::BinaryDeserializer& in) {
  in.Begin();
  return in.ReadProperty<uint64_t>(kRecordTick, "tick");
}

uint64_t RecordTick(std::span<const uint8_t> payload,
                    const std::filesystem::path& path) {
  duckdb::MemoryStream stream{const_cast<uint8_t*>(payload.data()),
                              payload.size()};
  duckdb::BinaryDeserializer in{stream};
  try {
    return ReadRecordTick(in);
  } catch (const duckdb::SerializationException& e) {
    ThrowUnreadable(path, duckdb::ErrorData{e}.RawMessage());
  }
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
// once rather than per op.
struct ParseScratch {
  std::vector<std::string_view> pks;
  std::vector<SearchDbWal::SegmentRef> segments;
  std::vector<SearchDbWal::InlinePk> inline_pks;
};

struct ParsedOp {
  uint8_t kind = 0;
  // INLINE
  std::span<const SearchDbWal::InlinePk> inline_pks;
  std::span<const uint8_t> inline_data;
  // DELETE (views into the payload, into scratch.pks)
  std::span<const std::string_view> delete_pks;
  // SEGMENT (into scratch.segments; owns its strings, unlike the views above)
  std::span<const SearchDbWal::SegmentRef> segments;
};

ParsedOp ReadOp(duckdb::BinaryDeserializer& in, duckdb::MemoryStream& stream,
                ParseScratch& scratch, const std::filesystem::path& path) {
  ParsedOp op;
  op.kind = in.ReadProperty<uint8_t>(kOpKind, "kind");
  switch (op.kind) {
    case kKindInline: {
      scratch.inline_pks.clear();
      const bool has_pks =
        in.OnOptionalPropertyBegin(kOpInlinePks, "inline_pks");
      if (has_pks) {
        irs::utils::ReadTuple(in, scratch.inline_pks);
      }
      in.OnOptionalPropertyEnd(has_pks);
      op.inline_pks = scratch.inline_pks;
      in.OnPropertyBegin(kOpInlineData, "inline_data");
      op.inline_data = ViewBytes(stream, in.ReadUnsignedInt64(), path);
      in.OnPropertyEnd();
      break;
    }
    case kKindDelete: {
      in.OnPropertyBegin(kOpDeletePks, "delete_pks");
      const auto count = in.OnListBegin();
      scratch.pks.clear();
      scratch.pks.reserve(count);
      for (duckdb::idx_t i = 0; i < count; ++i) {
        const auto bytes = ViewBytes(stream, in.ReadUnsignedInt32(), path);
        scratch.pks.emplace_back(reinterpret_cast<const char*>(bytes.data()),
                                 bytes.size());
      }
      in.OnListEnd();
      in.OnPropertyEnd();
      op.delete_pks = scratch.pks;
      break;
    }
    case kKindSegment:
      in.OnPropertyBegin(kOpSegments, "segments");
      irs::utils::ReadTuple(in, scratch.segments);
      in.OnPropertyEnd();
      op.segments = scratch.segments;
      break;
    case kKindTruncate:
      break;
    default:
      ThrowUnreadable(
        path, absl::StrCat("unknown op kind ", static_cast<int>(op.kind)));
  }
  return op;
}

template<typename OpHandler>
uint64_t VisitRecord(std::span<const uint8_t> payload,
                     const std::filesystem::path& path, ParseScratch& scratch,
                     const OpHandler& on_op) {
  duckdb::MemoryStream stream{const_cast<uint8_t*>(payload.data()),
                              payload.size()};
  duckdb::BinaryDeserializer in{stream};
  const auto tick = ReadRecordTick(in);
  in.ReadList(kRecordSections, "sections",
              [&](duckdb::BinaryDeserializer::List& sections, duckdb::idx_t) {
                sections.ReadObject([&](duckdb::BinaryDeserializer& section) {
                  const auto table_id =
                    section.ReadProperty<uint64_t>(kSectionTableId, "table_id");
                  section.ReadList(
                    kSectionOps, "ops",
                    [&](duckdb::BinaryDeserializer::List& ops, duckdb::idx_t) {
                      ops.ReadObject([&](duckdb::BinaryDeserializer& op) {
                        on_op(tick, table_id,
                              ReadOp(op, stream, scratch, path));
                      });
                    });
                });
              });
  in.End();
  if (stream.GetPosition() != payload.size()) {
    ThrowUnreadable(path, "unexpected bytes after the end of the record");
  }
  return tick;
}

void WriteOp(duckdb::BinarySerializer& out, const SearchDbWal::Op& op,
             uint8_t kind, duckdb::MemoryStream& data) {
  out.WriteProperty<uint8_t>(kOpKind, "kind", kind);
  switch (kind) {
    case kKindInline:
      if (!op.inline_pks.empty()) {
        out.OnPropertyBegin(kOpInlinePks, "inline_pks");
        irs::utils::WriteTuple(out, op.inline_pks);
        out.OnPropertyEnd();
      }
      data.Rewind();
      {
        duckdb::BinarySerializer collection{data};
        collection.Begin();
        op.inline_data->Serialize(collection);
        collection.End();
      }
      out.WriteProperty(kOpInlineData, "inline_data", data.GetData(),
                        data.GetPosition());
      break;
    case kKindDelete:
      out.WriteList(kOpDeletePks, "delete_pks", op.delete_pks.size(),
                    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
                      list.WriteElement(op.delete_pks[i]);
                    });
      break;
    case kKindSegment:
      out.OnPropertyBegin(kOpSegments, "segments");
      irs::utils::WriteTuple(out, op.segments);
      out.OnPropertyEnd();
      break;
    case kKindTruncate:
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
      std::vector<uint8_t> payload;
      while (ReadFrame(reader, payload)) {
        last_tick = RecordTick(payload, path);
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
  // Reused inline-CDC scratch across every INLINE op (Rewind keeps the buffer).
  duckdb::MemoryStream tmp;
  duckdb::BinarySerializer record{payload};
  record.Begin();
  record.WriteProperty<uint64_t>(kRecordTick, "tick", tick);
  record.WriteList(
    kRecordSections, "sections", sections.size(),
    [&](duckdb::BinarySerializer::List& list, duckdb::idx_t i) {
      const auto& s = sections[i];
      SDB_ASSERT(!s.ops.empty(), "shard section with no ops");
      list.WriteObject([&](duckdb::BinarySerializer& section) {
        section.WriteProperty<uint64_t>(kSectionTableId, "table_id",
                                        s.table_id);
        section.WriteList(
          kSectionOps, "ops", s.ops.size(),
          [&](duckdb::BinarySerializer::List& ops, duckdb::idx_t j) {
            const auto& op = s.ops[j];
            SDB_ASSERT((op.inline_data != nullptr) + (!op.segments.empty()) +
                           (!op.delete_pks.empty()) + op.truncate ==
                         1,
                       "op must be exactly one of INLINE / SEGMENT / DELETE / "
                       "TRUNCATE");
            const uint8_t kind = op.truncate            ? kKindTruncate
                                 : op.inline_data       ? kKindInline
                                 : !op.segments.empty() ? kKindSegment
                                                        : kKindDelete;
            ops.WriteObject([&](duckdb::BinarySerializer& out) {
              WriteOp(out, op, kind, tmp);
            });
          });
      });
    });
  record.End();
  WriteFrameLocked(payload.GetData(), payload.GetPosition());
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
      std::vector<uint8_t> payload;
      while (consumed && ReadFrame(reader, payload)) {
        try {
          consumed = RecordTick(payload, path) <= min_tick;
        } catch (const std::exception&) {
          consumed = false;
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
      auto on_op = [&](uint64_t tick, uint64_t table_id, const ParsedOp& op) {
        const duckdb::idx_t tid{table_id};
        const bool live = exists_of(tid) && tick > committed_of(tid);
        switch (op.kind) {
          case kKindInline: {
            if (!live) {
              return;
            }
            duckdb::MemoryStream ms(const_cast<uint8_t*>(op.inline_data.data()),
                                    op.inline_data.size());
            duckdb::BinaryDeserializer deser{ms};
            deser.Begin();
            auto cdc = duckdb::ColumnDataCollection::Deserialize(deser);
            deser.End();
            VisitInlineSegments(
              *cdc, op.inline_pks,
              [&](duckdb::DataChunk& chunk, uint64_t pk_base) {
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
      };
      uint64_t tick = 0;
      try {
        tick = VisitRecord(payload, path, scratch, on_op);
      } catch (const duckdb::SerializationException& e) {
        ThrowUnreadable(path, duckdb::ErrorData{e}.RawMessage());
      }
      max_tick = std::max(max_tick, tick);
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
