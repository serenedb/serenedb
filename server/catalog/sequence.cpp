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

#include "catalog/sequence.h"

#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <vpack/builder.h>
#include <vpack/slice.h>
#include <vpack/vpack_helper.h>

#include <bit>
#include <cstring>
#include <string>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "basics/static_strings.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_format.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"
#include "storage_engine/engine_feature.h"

namespace sdb::catalog {
namespace {

// Wire-compatible with rocksdb::UInt64AddOperator (PutFixed64). Hand-rolled
// because rocksdb's util/coding.h depends on port::kLittleEndian which is
// not surfaced to consumers.
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

std::string CounterKey(ObjectId id) {
  std::string key;
  rocksutils::Uint64ToPersistent(key, id.id());
  return key;
}

rocksdb::ColumnFamilyHandle* CounterCF() {
  return RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Sequences);
}

}  // namespace

Sequence::Sequence(ObjectId database_id, ObjectId schema_id, ObjectId id,
                   std::string_view name, SequenceOptions opts)
  : SchemaObject{{}, database_id,       schema_id,
                 id, std::string{name}, ObjectType::Sequence},
    _options{opts} {}

Sequence::~Sequence() = default;

std::shared_ptr<Sequence> Sequence::ReadInternal(vpack::Slice slice,
                                                 ReadContext ctx) {
  auto name =
    basics::VPackHelper::getString(slice, StaticStrings::kDataSourceName, {});

  SequenceOptions opts;
  opts.start_value = basics::VPackHelper::getNumber<int64_t>(slice, "start", 1);
  opts.increment =
    basics::VPackHelper::getNumber<int64_t>(slice, "increment", 1);
  opts.min_value = basics::VPackHelper::getNumber<int64_t>(slice, "min", 1);
  opts.max_value = basics::VPackHelper::getNumber<int64_t>(
    slice, "max", std::numeric_limits<int64_t>::max());
  opts.cache_size = basics::VPackHelper::getNumber<int64_t>(slice, "cache", 1);
  opts.cycle = basics::VPackHelper::getBool(slice, "cycle", false);

  auto seq = std::make_shared<Sequence>(ctx.database_id, ctx.schema_id, ctx.id,
                                        name, opts);
  seq->_live.store(seq->LoadFromDb(), std::memory_order_release);
  return seq;
}

void Sequence::WriteInternal(vpack::Builder& builder) const {
  builder.openObject();
  builder.add(StaticStrings::kDataSourceName, GetName());
  builder.add("start", _options.start_value);
  builder.add("increment", _options.increment);
  builder.add("min", _options.min_value);
  builder.add("max", _options.max_value);
  builder.add("cache", _options.cache_size);
  builder.add("cycle", _options.cycle);
  builder.close();
}

std::shared_ptr<Object> Sequence::Clone() const {
  return std::make_shared<Sequence>(GetDatabaseId(), GetSchemaId(), GetId(),
                                    GetName(), _options);
}

uint64_t Sequence::LoadFromDb() const {
  auto* db = GetServerEngine().db();
  auto* cf = CounterCF();
  auto key = CounterKey(GetId());
  std::string raw;
  auto s = db->Get(rocksdb::ReadOptions{}, cf, key, &raw);
  if (s.IsNotFound()) {
    return 0;
  }
  if (!s.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(s));
  }
  SDB_ASSERT(raw.size() == sizeof(uint64_t));
  return DecodeFixed64Le(raw.data());
}

uint64_t Sequence::ReserveWriteUnsafe(uint64_t count) {
  SDB_ASSERT(count > 0);

  // Merge persists before fetch_add, so the WAL high-water always covers
  // any base we've handed out. A crash between the two burns IDs but does
  // not reuse them.
  std::string operand;
  EncodeFixed64Le(operand, count);
  auto* db = GetServerEngine().db();
  auto* cf = CounterCF();
  auto s = db->Merge(rocksdb::WriteOptions{}, cf, CounterKey(GetId()), operand);
  if (!s.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(s));
  }
  return _live.fetch_add(count, std::memory_order_acq_rel) + 1;
}

uint64_t Sequence::Reserve(uint64_t count) {
  absl::ReaderMutexLock lock{&_setval_mu};
  return ReserveWriteUnsafe(count);
}

uint64_t Sequence::Read() const {
  return _live.load(std::memory_order_acquire);
}

void Sequence::Write(uint64_t value) {
  std::string encoded;
  EncodeFixed64Le(encoded, value);
  auto* db = GetServerEngine().db();
  auto* cf = CounterCF();
  auto key = CounterKey(GetId());

  // Writer lock: blocks all in-flight Reserves and serialises Writes.
  // ReserveWriteUnsafe (auto-PK) bypasses this -- those sequences never
  // see setval.
  absl::MutexLock lock{&_setval_mu};
  auto s = db->Put(rocksdb::WriteOptions{}, cf, key, encoded);
  if (!s.ok()) {
    SDB_THROW(rocksutils::ConvertStatus(s));
  }
  _live.store(value, std::memory_order_release);
}

}  // namespace sdb::catalog
