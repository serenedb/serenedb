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

#include "common.h"
#include "key_utils.hpp"
#include "pg/sql_exception_macro.h"
#include "primary_key.hpp"
#include "rocksdb/utilities/transaction.h"
#include "sink_writer_base.hpp"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "utils/errcodes.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::connector {

// Utilities for building secondary index keys.
//
// Secondary index key layout:
//
// Every key starts with <shard (8B)> <dummy column_id=0 (8B)> to satisfy
// CappedPrefixTransform(16).  After the 16-byte prefix, each SK column is
// encoded as a 1-byte null/not-null marker followed by the encoded value
// (omitted when NULL).
//
// 1) Unique, non-NULL:
//    Key:   <shard (8B)> <dummy (8B)> <1B markers + encoded SK values>
//    Value: <PK bytes>
//    One entry per SK value. Uniqueness enforced via GetForUpdate.
//
// 2) Unique, NULL:
//    Key:   <shard (8B)> <dummy (8B)> <1B markers + encoded SK values>
//           <PK bytes>
//    Value: empty
//    PK in key -- multiple NULLs allowed (PostgreSQL semantics).
//
// 3) Non-unique (any value):
//    Key:   <shard (8B)> <dummy (8B)> <1B markers + encoded SK values>
//           <PK bytes>
//    Value: empty
//    PK always in key. Multiple rows per SK value.
//
// Marker values (1 byte): 0x01 = NULL, 0x02 = NOT NULL (nulls-first).
namespace secondary_key {

inline void AppendShardPrefix(std::string& key, ObjectId shard_id) {
  const auto base = key.size();
  basics::StrAppend(key, sizeof(ObjectId));
  absl::big_endian::Store64(key.data() + base, shard_id.id());
}

// Appends a dummy Column::Id (0) so that the key reaches the 16-byte prefix
// required by CappedPrefixTransform(16) on the default column family.
inline void AppendDummyColumnId(std::string& key) {
  const auto base = key.size();
  basics::StrAppend(key, sizeof(catalog::Column::Id));
  absl::big_endian::Store64(key.data() + base, 0);
}

inline void AppendNullMarker(std::string& key) { key.push_back('\1'); }

inline void AppendNotNullMarker(std::string& key) { key.push_back('\2'); }

// Build secondary index key + value.
//
// Non-unique (any value):
//   key = [shard][marker][SK values][PK]   value = empty
//
// Unique + NULL:
//   key = [shard][marker][PK]              value = empty
//   (PK in key -- multiple NULLs allowed, like non-unique)
//
// Unique + non-NULL:
//   key = [shard][marker][SK values]       value = [PK]
//   (PK as value -- uniqueness enforced)
inline constexpr char kPKInKey = '\x00';
inline constexpr char kPKInValue = '\x01';

}  // namespace secondary_key
}  // namespace sdb::connector
