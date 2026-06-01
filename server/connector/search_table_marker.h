////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <absl/base/internal/endian.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/transaction.h>

#include <cstdint>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <span>
#include <string>
#include <string_view>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "query/transaction.h"
#include "rocksdb_engine_catalog/wal_log_data_magics.h"

// WAL markers for search-backed table writes (StorageKind::kSearch). Ride
// in the same rocksdb WriteBatch as anything else the txn touches so they
// share its fsync and sequence number; recovery decodes them and re-feeds
// the payload into the iresearch writer.
//
// Two insert-marker shapes (Appendix A of search_table_shard_sink.md):
//
//   [InsertCdc]   [magic=kSearchTableInsertCdc] [table_id u64 BE]
//                 [column_count u32 BE] [column_ids u64 BE x N]
//                 [blob = ColumnDataCollection::Serialize]
//
//   [InsertChunk] [magic=kSearchTableInsertChunk] [table_id u64 BE]
//                 [column_count u32 BE] [column_ids u64 BE x N]
//                 [blob = DataChunk::Serialize]
//
//   [Delete]      [magic=kSearchTableDelete] [table_id u64 BE]
//                 ([pk_len u32 BE] [pk_bytes])*  -- iterate until slice end
//
// Dispatch (EmitInsertsForBuffer): if the buffer's AllocationSize is below
// kSplitThreshold, emit a single CDC marker (one WriteBatch entry per
// commit, ideal for small/medium INSERTs). Otherwise iterate the buffer's
// chunks and emit one Chunk marker per chunk -- bounds the per-marker
// serialise memory and avoids the block-to-block copy a CDC accumulator
// would need. Recovery handles both shapes; the choice is invisible to
// callers above EmitInsertsForBuffer.
//
// Delete payload is raw PK byte strings (already-encoded by the writer).
// Both omit explicit row/PK counts where they can be derived (Insert: from
// the embedded serialiser; Delete: iterate until slice exhausted).

namespace sdb::connector::search_table_marker {

// Above this AllocationSize, switch from one CDC marker to per-chunk
// markers. Arbitrary upper bound on transient commit-time memory per
// marker; may become a session/instance setting later.
inline constexpr size_t kSplitThreshold = 2 * 1024 * 1024;

namespace detail {

// Writes the shared header bytes (magic + table_id + column_count +
// column_ids) into `out`. Used by both insert encoders.
inline void AppendInsertHeader(
  std::string& out, uint8_t magic, ObjectId table_id,
  std::span<const catalog::Column::Id> column_ids) {
  out.push_back(static_cast<char>(magic));
  char be8[sizeof(uint64_t)];
  absl::big_endian::Store64(be8, table_id.id());
  out.append(be8, sizeof(be8));
  char be4[sizeof(uint32_t)];
  absl::big_endian::Store32(be4, static_cast<uint32_t>(column_ids.size()));
  out.append(be4, sizeof(be4));
  for (auto col_id : column_ids) {
    absl::big_endian::Store64(be8, col_id.id());
    out.append(be8, sizeof(be8));
  }
}

// Serialises an object via DuckDB's BinarySerializer and appends the raw
// bytes onto `out`. The object's class must expose
// `void Serialize(Serializer&) const`.
template<typename T>
void AppendSerialized(std::string& out, const T& obj) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  obj.Serialize(serializer);
  serializer.End();
  out.append(reinterpret_cast<const char*>(stream.GetData()),
             stream.GetPosition());
}

inline size_t HeaderReserve(std::span<const catalog::Column::Id> column_ids) {
  return 1 + sizeof(uint64_t) + sizeof(uint32_t) +
         column_ids.size() * sizeof(uint64_t);
}

}  // namespace detail

// --- encoders -------------------------------------------------------------

// CDC-payload insert: a single marker that carries the whole buffer.
// Used when the buffer fits under kSplitThreshold.
inline std::string EncodeInsertCdc(
  ObjectId table_id, std::span<const catalog::Column::Id> column_ids,
  const duckdb::ColumnDataCollection& cdc) {
  std::string out;
  out.reserve(detail::HeaderReserve(column_ids));
  detail::AppendInsertHeader(out, wal_log_data::kSearchTableInsertCdc, table_id,
                             column_ids);
  detail::AppendSerialized(out, cdc);
  return out;
}

// DataChunk-payload insert: one marker per chunk. Used when splitting a
// large buffer to bound per-marker memory.
inline std::string EncodeInsertChunk(
  ObjectId table_id, std::span<const catalog::Column::Id> column_ids,
  const duckdb::DataChunk& chunk) {
  std::string out;
  out.reserve(detail::HeaderReserve(column_ids));
  detail::AppendInsertHeader(out, wal_log_data::kSearchTableInsertChunk,
                             table_id, column_ids);
  detail::AppendSerialized(out, chunk);
  return out;
}

inline std::string EncodeDelete(ObjectId table_id,
                                std::span<const std::string_view> pks) {
  size_t total = 1 + sizeof(uint64_t);
  for (auto pk : pks) {
    total += sizeof(uint32_t) + pk.size();
  }

  std::string out;
  out.reserve(total);
  out.push_back(static_cast<char>(wal_log_data::kSearchTableDelete));

  char be8[sizeof(uint64_t)];
  absl::big_endian::Store64(be8, table_id.id());
  out.append(be8, sizeof(be8));

  char be4[sizeof(uint32_t)];
  for (auto pk : pks) {
    absl::big_endian::Store32(be4, static_cast<uint32_t>(pk.size()));
    out.append(be4, sizeof(be4));
    out.append(pk.data(), pk.size());
  }
  return out;
}

// --- thin emit helpers ----------------------------------------------------
//
// Same pattern as indexonly_marker::EmitCP/RD: take the sdb-side
// Transaction so marker emission and the marker counter (which keeps the
// commit gate honest -- rocksdb's own counters ignore PutLogData) stay
// paired and callers can't forget one.

inline void EmitInsertCdc(query::Transaction& sdb_txn, ObjectId table_id,
                          std::span<const catalog::Column::Id> column_ids,
                          const duckdb::ColumnDataCollection& cdc) {
  auto blob = EncodeInsertCdc(table_id, column_ids, cdc);
  sdb_txn.GetRocksDBTransaction().PutLogData(
    rocksdb::Slice{blob.data(), blob.size()});
  sdb_txn.RegisterLogDataMarker();
}

inline void EmitInsertChunk(query::Transaction& sdb_txn, ObjectId table_id,
                            std::span<const catalog::Column::Id> column_ids,
                            const duckdb::DataChunk& chunk) {
  auto blob = EncodeInsertChunk(table_id, column_ids, chunk);
  sdb_txn.GetRocksDBTransaction().PutLogData(
    rocksdb::Slice{blob.data(), blob.size()});
  sdb_txn.RegisterLogDataMarker();
}

inline void EmitDelete(query::Transaction& sdb_txn, ObjectId table_id,
                       std::span<const std::string_view> pks) {
  auto blob = EncodeDelete(table_id, pks);
  sdb_txn.GetRocksDBTransaction().PutLogData(
    rocksdb::Slice{blob.data(), blob.size()});
  sdb_txn.RegisterLogDataMarker();
}

// High-level dispatch: under kSplitThreshold, emit the buffer as one CDC
// marker (zero copies, one WriteBatch entry). Otherwise stream chunks
// through per-chunk markers (zero copies, more entries but bounded peak
// memory). Used at commit-time by the SereneDBSearchInsert operator.
inline void EmitInsertsForBuffer(
  query::Transaction& sdb_txn, ObjectId table_id,
  std::span<const catalog::Column::Id> column_ids,
  const duckdb::ColumnDataCollection& buffer) {
  if (buffer.AllocationSize() < kSplitThreshold) {
    EmitInsertCdc(sdb_txn, table_id, column_ids, buffer);
    return;
  }
  for (auto& chunk : buffer.Chunks()) {
    EmitInsertChunk(sdb_txn, table_id, column_ids, chunk);
  }
}

}  // namespace sdb::connector::search_table_marker
