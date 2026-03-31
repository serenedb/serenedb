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

#include <rocksdb/slice.h>
#include <velox/common/memory/MemoryPool.h>
#include <velox/type/Type.h>
#include <velox/type/Variant.h>
#include <velox/vector/BaseVector.h>
#include <velox/vector/FlatVector.h>

#include <span>

#include "basics/containers/bitset.hpp"
#include "basics/fwd.h"
#include "connector/common.h"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/rocksdb_materializer.hpp"
#include "connector/secondary_sink_writer.hpp"

namespace sdb::connector {

// ---- Key builders ----

class PrimaryKeyBuilder {
 public:
  PrimaryKeyBuilder(ObjectId table_id, velox::RowTypePtr pk_type)
    : _table_id{table_id}, _pk_type{std::move(pk_type)} {}

  void BuildFullKey(std::string& key, catalog::Column::Id column_id,
                    std::span<const velox::variant> values) const {
    key_utils::AppendTableKey(key, _table_id);
    key_utils::AppendColumnKey(key, column_id);
    primary_key::Create(values, *_pk_type, key);
  }

  ObjectId _table_id;
  velox::RowTypePtr _pk_type;
};

class SecondaryKeyBuilder {
 public:
  SecondaryKeyBuilder(ObjectId shard_id, velox::RowTypePtr sk_type)
    : _shard_id{shard_id}, _sk_type{std::move(sk_type)} {}

  void BuildFullKey(std::string& key, catalog::Column::Id column_id,
                    std::span<const velox::variant> values) const {
    secondary_key::AppendShardPrefix(key, _shard_id);
    secondary_key::AppendNotNullMarker(key);
    primary_key::Create(values, *_sk_type, key);
  }

  ObjectId _shard_id;
  velox::RowTypePtr _sk_type;
};

// ---- Result collectors ----

template<typename T>
void SetResultValue(std::string_view value, size_t idx,
                    velox::FlatVector<T>& result) {
  if constexpr (std::is_same_v<T, velox::StringView>) {
    const size_t offset = value[0] == 0 ? 1 : 0;
    result.set(idx,
               velox::StringView(value.data() + offset, value.size() - offset));
  } else if constexpr (std::is_same_v<T, bool>) {
    SDB_ASSERT(value.size() == kTrueValue.size(),
               "unexpected value size for bool column");
    result.set(idx, value == kTrueValue);
  } else {
    SDB_ASSERT(value.size() == sizeof(T),
               "unexpected value size for scalar column");
    T tmp;
    memcpy(&tmp, value.data(), sizeof(T));
    result.set(idx, tmp);
  }
}

template<velox::TypeKind Kind>
velox::VectorPtr CreateColumnVector(size_t count,
                                    velox::memory::MemoryPool& pool) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  return velox::BaseVector::create<velox::FlatVector<T>>(
    velox::Type::create<Kind>(), count, &pool);
}

template<velox::TypeKind Kind>
void FillColumnValues(velox::BaseVector& result, size_t offset,
                      std::span<const rocksdb::PinnableSlice> values) {
  using T = typename velox::TypeTraits<Kind>::NativeType;
  auto& flat = static_cast<velox::FlatVector<T>&>(result);
  for (size_t i = 0; i < values.size(); ++i) {
    if (values[i].empty()) {
      flat.setNull(offset + i, true);
    } else {
      SetResultValue(values[i].ToStringView(), offset + i, flat);
    }
  }
}

// Collects column values directly into velox vectors (primary key path).
// Also maintains presence mask for Step 2 (remaining columns).
class ColumnCollector {
 public:
  static constexpr bool kHasMaterializer = false;

  void Init(const velox::TypePtr& type, size_t capacity,
            velox::memory::MemoryPool& pool) {
    _type_kind = type->kind();
    _vec = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(CreateColumnVector, _type_kind,
                                              capacity, pool);
    _present_rows.reset(capacity);
  }

  void Fill(size_t batch_idx, size_t found_idx,
            std::span<const rocksdb::PinnableSlice> values) {
    _present_rows.set(batch_idx);
    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(FillColumnValues, _type_kind, *_vec,
                                       found_idx, values);
  }

  velox::VectorPtr Finish(size_t found_count) {
    SDB_ASSERT(_present_rows.count() == found_count);
    _vec->resize(found_count);
    return std::move(_vec);
  }

  const irs::bitset& PresentRows() const { return _present_rows; }

 private:
  velox::TypeKind _type_kind;
  velox::VectorPtr _vec;
  irs::bitset _present_rows;
};

// Collects PK bytes from index values, then delegates to a Materializer.
class MaterializerCollector {
 public:
  static constexpr bool kHasMaterializer = true;

  MaterializerCollector(RocksDBMaterializer materializer,
                        velox::memory::MemoryPool& pool)
    : _materializer{std::move(materializer)}, _row_keys{pool} {}

  void Init(const velox::TypePtr& /*type*/, size_t capacity,
            velox::memory::MemoryPool& /*pool*/) {
    _row_keys.reserve(capacity);
  }

  void Fill(size_t /*batch_idx*/, size_t /*found_idx*/,
            std::span<const rocksdb::PinnableSlice> values) {
    for (const auto& val : values) {
      _row_keys.emplace_back(val.data(), val.size());
    }
  }

  velox::RowVectorPtr Finish(size_t /*found_count*/) {
    return _materializer.ReadRows(_row_keys, nullptr);
  }

 private:
  RocksDBMaterializer _materializer;
  primary_key::Keys _row_keys;
};

// ---- Lookup policies ----

template<bool ReadYourOwnWrites>
struct PrimaryLookupPolicy {
  using Source =
    std::conditional_t<ReadYourOwnWrites, rocksdb::Transaction, rocksdb::DB>;
  using KeyBuilder = PrimaryKeyBuilder;
  using ResultCollector = ColumnCollector;
};

template<bool ReadYourOwnWrites>
struct SecondaryLookupPolicy {
  using Source =
    std::conditional_t<ReadYourOwnWrites, rocksdb::Transaction, rocksdb::DB>;
  using KeyBuilder = SecondaryKeyBuilder;
  using ResultCollector = MaterializerCollector;
};

}  // namespace sdb::connector
