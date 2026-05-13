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

// WAL markers that make sdb_indexonly column writes crash-safe: ride in the
// same rocksdb WriteBatch as the surrounding puts so they share its fsync
// and sequence number, and carry enough information for recovery to
// re-feed the value into the inverted index.
//
// Marker layouts:
//
//   CP  [magic] [u32 key_len] [full_key_bytes] [value_bytes]
//   RD  [magic] [full_key_bytes]
//
// `full_key_bytes` is the same row-key encoding the main writer uses:
//   [ObjectId table_id (8B BE)][Column::Id col_id (8B BE)][PK bytes].
// Reused verbatim so encode/decode is a memcpy. For [RD] only the
// table_id + PK portion matters.

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

// Returns std::nullopt for blobs whose magic is not in our registry, and
// for malformed-but-claimed-by-us blobs (callers should ignore the former
// and treat the latter as a recovery error).
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
// Take the sdb-side Transaction so marker emission and the counter that
// keeps the commit gate honest stay paired -- direct callers cannot forget
// one or the other.

// Emit a [CP] marker. Empty `slices` = NULL.
inline void EmitCP(query::Transaction& sdb_txn, std::string_view full_key,
                   std::span<const rocksdb::Slice> slices) {
  auto blob = EncodeCP(full_key, slices);
  sdb_txn.GetRocksDBTransaction().PutLogData(
    rocksdb::Slice{blob.data(), blob.size()});
  sdb_txn.RegisterLogDataMarker();
}

// Emit an [RD] marker. Any row key for the deleted row works -- replay
// uses only the table_id + PK portion.
inline void EmitRD(query::Transaction& sdb_txn, std::string_view full_key) {
  auto blob = EncodeRD(full_key);
  sdb_txn.GetRocksDBTransaction().PutLogData(
    rocksdb::Slice{blob.data(), blob.size()});
  sdb_txn.RegisterLogDataMarker();
}

}  // namespace sdb::connector::indexonly_marker
