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

#include <rocksdb/slice.h>

#include <utility>

#include "catalog/identifiers/index_id.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/types.h"
#include "rocksdb_engine_catalog/concat.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"

namespace sdb::wal {

struct Database {
  ObjectId database_id;
  vpack::Slice data;
};

struct ObjectPut {
  ObjectId database_id;
  ObjectId object_id;
  vpack::Slice data;
};

struct ObjectDrop {
  ObjectId database_id;
  ObjectId object_id;
  std::string_view uuid;
};

struct SchemaObjectPut {
  ObjectId database_id;
  ObjectId schema_id;
  ObjectId object_id;
  vpack::Slice data;
};

struct SchemaObjectDrop {
  ObjectId database_id;
  ObjectId schema_id;
  ObjectId object_id;
  std::string_view uuid;
};

struct TableTruncate {
  ObjectId database_id;
  ObjectId schema_id;
  ObjectId object_id;
};

// TODO(gnusi): fix
struct IndexCreate {
  ObjectId database_id;
  ObjectId object_id;
  vpack::Slice data;
};

// TODO(gnusi): fix
struct IndexDrop {
  ObjectId database_id;
  ObjectId object_id;
  IndexId index_id;
};

struct SingleOp {
  ObjectId database_id;
  ObjectId object_id;
};

template<typename T>
std::string Write(RocksDBLogType type, const T& in) {
  std::string out;
  basics::StrResize(out, sizeof(RocksDBLogType) + rocksutils::GetByteSize(in));
  out[0] = std::to_underlying(type);
  rocksutils::Write(in, out.data() + 1);
  return out;
}

inline RocksDBLogType GetType(rocksdb::Slice slice) noexcept {
  SDB_ASSERT(slice.size() >= sizeof(RocksDBLogType));
  return static_cast<RocksDBLogType>(*slice.data());
}

}  // namespace sdb::wal
