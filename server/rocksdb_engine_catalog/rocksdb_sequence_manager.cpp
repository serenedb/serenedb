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

#include "rocksdb_engine_catalog/rocksdb_sequence_manager.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <bit>
#include <cstring>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "basics/result.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_format.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"

namespace sdb {
namespace {

// 8-byte little-endian encoding compatible with rocksdb::UInt64AddOperator
// (which uses PutFixed64 / DecodeFixed64).
void EncodeFixed64Le(std::string& dst, uint64_t value) {
  if constexpr (std::endian::native == std::endian::big) {
    value = std::byteswap(value);
  }
  dst.append(reinterpret_cast<const char*>(&value), sizeof(value));
}

uint64_t DecodeFixed64Le(const char* src) {
  uint64_t value;
  std::memcpy(&value, src, sizeof(value));
  if constexpr (std::endian::native == std::endian::big) {
    value = std::byteswap(value);
  }
  return value;
}

}  // namespace

TableSequence::TableSequence(rocksdb::DB* db, rocksdb::ColumnFamilyHandle* cf,
                             ObjectId table_id)
  : _db{db}, _cf{cf} {
  rocksutils::Uint64ToPersistent(_key, table_id.id());
}

uint64_t TableSequence::Reserve(uint64_t count) {
  SDB_ASSERT(count > 0);

  std::string operand;
  EncodeFixed64Le(operand, count);

  absl::MutexLock lock{&_mu};

  rocksdb::WriteOptions wo;
  rocksdb::Status s = _db->Merge(wo, _cf, _key, operand);
  if (!s.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(s));
  }

  std::string raw;
  s = _db->Get(rocksdb::ReadOptions{}, _cf, _key, &raw);
  if (!s.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(s));
  }
  SDB_ASSERT(raw.size() == sizeof(uint64_t));
  uint64_t high_water = DecodeFixed64Le(raw.data());
  SDB_ASSERT(high_water >= count);

  return high_water - count + 1;
}

RocksDBSequenceManager::RocksDBSequenceManager(RocksDBEngineCatalog& engine)
  : _engine{engine},
    _cf{RocksDBColumnFamilyManager::get(
      RocksDBColumnFamilyManager::Family::Sequences)} {
  SDB_ASSERT(_cf != nullptr);
}

TableSequence& RocksDBSequenceManager::GetForTable(ObjectId table_id) {
  {
    absl::ReaderMutexLock lock{&_map_mu};
    auto it = _by_table_id.find(table_id.id());
    if (it != _by_table_id.end()) {
      return *it->second;
    }
  }

  absl::MutexLock lock{&_map_mu};
  auto [it, inserted] = _by_table_id.try_emplace(table_id.id(), nullptr);
  if (inserted) {
    it->second =
      std::make_unique<TableSequence>(_engine.db(), _cf, table_id);
  }
  return *it->second;
}

}  // namespace sdb
