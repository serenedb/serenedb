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

#pragma once

#include <absl/base/internal/endian.h>
#include <rocksdb/slice.h>
#include <rocksdb/utilities/transaction.h>

#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>

#include "query/transaction.h"
#include "rocksdb_engine_catalog/wal_log_data_magics.h"

// WAL markers used to make sdb_indexonly column writes crash-safe.
//
// Why: IndexOnly columns are not written to main RocksDB storage -- their
// values live only inside the inverted index. Without an extra durability
// hop, a crash between the rocksdb transaction commit and the iresearch
// segment commit would lose the IndexOnly value entirely.
//
// Mechanism: alongside the rocksdb writes, emit a small log-data record
// via rocksdb::Transaction::PutLogData. The record rides in the same
// WriteBatch as the regular puts, so it inherits the WAL fsync and the
// per-batch sequence number wal_recovery uses to decide whether replay
// should pick it up. On recovery, wal_recovery's per-batch handler decodes
// these markers and feeds them into the same Row reconstruction path that
// the existing PutCF records use.
//
// Format (each marker is one PutLogData blob; first byte is the magic, see
// rocksdb_engine_catalog/wal_log_data_magics.h for the registry):
//
//   CP  [0x01] [u32 key_len] [full_key_bytes] [value_bytes]
//   RD  [0x02] [full_key_bytes]
//
// `full_key_bytes` is exactly the row-key encoding the rocksdb writer
// would have used: [ObjectId table_id (8B big-endian)]
//                  [Column::Id col_id (8B big-endian)]
//                  [PK bytes (variable)]
// We re-use it verbatim so encode/decode is just a memcpy and the on-wire
// byte order matches what wal_recovery already understands. For [RD],
// only the table_id portion of the key is meaningful -- col_id is
// irrelevant because the inverted index deletes by PK across all fields.
//
// Forward compatibility: a future record class can pick a different magic
// byte (see the registry). Decoders skip unrecognised magics.

namespace sdb::connector::indexonly_marker {

enum class MarkerKind : uint8_t { Unknown, CP, RD };

// Decoded form. Lifetime of `key` and `value` is the underlying slice the
// LogData callback was invoked with -- copy out before that slice is
// reused.
struct Decoded {
  MarkerKind kind = MarkerKind::Unknown;
  // Full row key: [table_id 8B][col_id 8B][PK bytes].
  std::string_view key;
  // CP only: cell value bytes (may be empty for NULL). Empty for RD.
  std::string_view value;
};

// --- encoders -------------------------------------------------------------

inline std::string EncodeCP(std::string_view full_key,
                            std::span<const rocksdb::Slice> value_slices) {
  size_t value_len = 0;
  for (const auto& s : value_slices) {
    value_len += s.size();
  }
  std::string out;
  out.reserve(1 + sizeof(uint32_t) + full_key.size() + value_len);
  out.push_back(static_cast<char>(wal_log_data::kIndexOnlyCp));
  char buf[sizeof(uint32_t)];
  absl::big_endian::Store32(buf, static_cast<uint32_t>(full_key.size()));
  out.append(buf, sizeof(buf));
  out.append(full_key);
  for (const auto& s : value_slices) {
    out.append(s.data(), s.size());
  }
  return out;
}

inline std::string EncodeRD(std::string_view full_key) {
  std::string out;
  out.reserve(1 + full_key.size());
  out.push_back(static_cast<char>(wal_log_data::kIndexOnlyRd));
  out.append(full_key);
  return out;
}

// --- decoder --------------------------------------------------------------

// Returns std::nullopt for blobs we don't recognise (callers should ignore
// them -- they may belong to an unrelated PutLogData consumer or a future
// marker version) and for malformed-but-claimed-by-us blobs (the caller
// should treat that as a recovery error).
inline std::optional<Decoded> Decode(rocksdb::Slice blob) {
  std::string_view in{blob.data(), blob.size()};
  if (in.empty()) {
    return std::nullopt;
  }
  auto magic = static_cast<uint8_t>(in.front());
  std::string_view rest = in.substr(1);

  Decoded d;
  if (magic == wal_log_data::kIndexOnlyCp) {
    if (rest.size() < sizeof(uint32_t)) {
      return std::nullopt;
    }
    auto key_len = absl::big_endian::Load32(rest.data());
    rest.remove_prefix(sizeof(uint32_t));
    if (rest.size() < key_len) {
      return std::nullopt;
    }
    d.key = rest.substr(0, key_len);
    d.value = rest.substr(key_len);
    d.kind = MarkerKind::CP;
    return d;
  }
  if (magic == wal_log_data::kIndexOnlyRd) {
    d.key = rest;
    d.kind = MarkerKind::RD;
    return d;
  }
  return std::nullopt;
}

// --- thin emit helpers ----------------------------------------------------
//
// Each helper takes the sdb-side Transaction so it can both look up the
// rocksdb txn and call RegisterLogDataMarker -- this keeps the Commit-gate
// (num_ops > 0) honest. Callers that go through these helpers cannot
// forget to bump the marker counter.

// Emits a [CP] marker into the txn's WriteBatch. `full_key` is the same
// row key the main writer would Put for this cell. Empty `slices` = NULL.
inline void EmitCP(query::Transaction& sdb_txn, std::string_view full_key,
                   std::span<const rocksdb::Slice> slices) {
  auto blob = EncodeCP(full_key, slices);
  sdb_txn.GetRocksDBTransaction().PutLogData(
    rocksdb::Slice{blob.data(), blob.size()});
  sdb_txn.RegisterLogDataMarker();
}

// Emits an [RD] marker into the txn's WriteBatch. `full_key` is any row
// key for the deleted row -- only the table_id and PK portion are read on
// replay (col_id is ignored for row-level delete).
inline void EmitRD(query::Transaction& sdb_txn, std::string_view full_key) {
  auto blob = EncodeRD(full_key);
  sdb_txn.GetRocksDBTransaction().PutLogData(
    rocksdb::Slice{blob.data(), blob.size()});
  sdb_txn.RegisterLogDataMarker();
}

}  // namespace sdb::connector::indexonly_marker
