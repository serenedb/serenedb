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

#include <velox/vector/tests/utils/VectorTestBase.h>

#include "connector/common.h"
#include "connector/data_sink.hpp"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "gtest/gtest.h"
#include "iresearch/utils/bytes_utils.hpp"
#include "rocksdb/utilities/transaction_db.h"

using namespace sdb::connector;

namespace {

constexpr sdb::ObjectId kObjectKey{123456};

class DataSinkTest : public ::testing::Test,
                     public velox::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance({});
  }

  void SetUp() final {
    rocksdb::TransactionDBOptions transaction_options;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_families;
    rocksdb::Options db_options;
    db_options.OptimizeForSmallDb();
    db_options.create_if_missing = true;
    cf_families.emplace_back(rocksdb::kDefaultColumnFamilyName, db_options);
    _path = testing::TempDir() + "/" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name() +
            "_XXXXXX";
    ASSERT_NE(mkdtemp(_path.data()), nullptr);
    db_options.wal_dir = _path + "/journals";
    rocksdb::Status status = rocksdb::TransactionDB::Open(
      db_options, transaction_options, _path, cf_families, &_cf_handles, &_db);
    ASSERT_TRUE(status.ok());
  }

  void TearDown() final {
    if (_db) {
      for (auto h : _cf_handles) {
        _db->DestroyColumnFamilyHandle(h);
      }
      _db->Close();
      delete _db;
      _db = nullptr;
    }
    std::filesystem::remove_all(_path);
  }

  velox::BufferPtr MakeInMap(const std::vector<bool>& in_map) {
    auto buff = velox::AlignedBuffer::allocate<uint8_t>(
      velox::bits::nbytes(in_map.size()), pool());
    auto raw_in_maps = buff->asMutable<uint8_t>();
    for (size_t i = 0; i < in_map.size(); i++) {
      velox::bits::setBit(raw_in_maps, i, in_map[i]);
    }
    return buff;
  }

  void PrepareRocksDBWrite(
    const velox::RowVectorPtr& data, sdb::ObjectId object_key,
    const std::vector<velox::column_index_t>& pk,
    std::unique_ptr<rocksdb::Transaction>& transaction,
    sdb::connector::primary_key::Keys& written_row_keys) {
    rocksdb::TransactionOptions trx_opts;
    trx_opts.skip_concurrency_control = true;
    trx_opts.lock_timeout = 100;
    rocksdb::WriteOptions wo;
    transaction.reset(_db->BeginTransaction(wo, trx_opts, nullptr));
    ASSERT_NE(transaction, nullptr);
    std::vector<sdb::catalog::Column::Id> column_oids;
    column_oids.reserve(data->childrenSize());
    for (velox::column_index_t i = 0; i < data->childrenSize(); ++i) {
      column_oids.push_back(static_cast<sdb::catalog::Column::Id>(i));
    }
    sdb::connector::primary_key::Create(*data, pk, written_row_keys);
    sdb::connector::RocksDBDataSink sink(*transaction, *_cf_handles.front(),
                                         *pool_.get(), object_key, pk,
                                         column_oids, false);
    sink.appendData(data);
    while (!sink.finish()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  }

  void MakeRocksDBWrite(std::vector<std::string> names,
                        std::vector<velox::VectorPtr> data,
                        sdb::ObjectId& object_key,
                        sdb::connector::primary_key::Keys& written_row_keys) {
    object_key = kObjectKey;
    // creating  PK column
    names.push_back("id");
    std::vector<int32_t> idx;
    idx.resize(data.front()->size());
    std::iota(idx.begin(), idx.end(), 0);
    data.push_back(makeFlatVector<int32_t>(idx));
    auto row_data = makeRowVector(names, {data});
    std::unique_ptr<rocksdb::Transaction> transaction;
    std::vector<velox::column_index_t> pk = {
      static_cast<velox::column_index_t>(names.size() - 1)};
    PrepareRocksDBWrite(row_data, object_key, pk, transaction,
                        written_row_keys);
    ASSERT_TRUE(transaction->Commit().ok());
  }

  template<velox::TypeKind Kind>
  velox::VectorPtr ReadFlatScalarVector(irs::bytes_view value) {
    using T = typename velox::TypeTraits<Kind>::NativeType;
    auto read_data = value.data();
    auto vector_size = irs::vread<uint32_t>(read_data);
    if (vector_size == 0) {
      EXPECT_EQ(read_data, value.data() + value.size());
      return makeFlatVector<T>({});
    }
    const auto flags = std::bit_cast<ValueFlags>(*(read_data++));
    uint32_t length_array_size = 0;
    const auto same_width = velox::TypeTraits<Kind>::isFixedWidth;
    const bool have_length_array =
      (flags & ValueFlags::HaveLength) == ValueFlags::HaveLength;
    const bool is_constant =
      (flags & ValueFlags::Constant) == ValueFlags::Constant;
    if (!is_constant) {
      EXPECT_EQ(same_width, have_length_array ? false : true);
      if (have_length_array) {
        length_array_size = irs::vread<uint32_t>(read_data);
      }
    }
    velox::BufferPtr nulls_data = nullptr;
    size_t nulls_mask_size = 0;
    if ((flags & ValueFlags::HaveNulls) == ValueFlags::HaveNulls) {
      nulls_mask_size = velox::bits::nbytes(vector_size);
      nulls_data = velox::AlignedBuffer::allocate<uint64_t>(
        velox::bits::nwords(vector_size), pool_.get());
      memcpy(nulls_data->asMutable<uint8_t>(), read_data, nulls_mask_size);
      read_data += nulls_mask_size;
    }
    if (is_constant) {
      // length array make no sense for single constant value
      EXPECT_FALSE(have_length_array);
      if (read_data == value.data() + value.size()) {
        // constant null vector
        return makeConstant<T>(std::nullopt, vector_size);
      }
      auto result = velox::BaseVector::wrapInConstant(
        vector_size, 0,
        ReadSingleScalarValue<Kind>(irs::bytes_view(
          read_data, value.size() - (read_data - value.data()))));
      if (nulls_data) {
        // apply nulls mask
        auto indices = makeIndices(vector_size, [](auto i) { return i; });
        result = velox::BaseVector::wrapInDictionary(nulls_data, indices,
                                                     vector_size, result);
      }
      return result;
    }
    if (have_length_array) {
      std::vector<uint32_t> length;
      length.resize(vector_size);
      auto string_data = read_data + length_array_size;
      auto values = velox::AlignedBuffer::allocate<velox::StringView>(
        vector_size, pool_.get(), velox::StringView());
      auto string_view = values->asMutable<velox::StringView>();
      for (size_t i = 0; i < vector_size; ++i) {
        auto len = irs::vread<uint32_t>(read_data);
        if (len) {
          *string_view =
            velox::StringView(reinterpret_cast<const char*>(string_data), len);
          string_data += len;
        }
        ++string_view;
      }
      return std::make_shared<velox::FlatVector<velox::StringView>>(
        pool_.get(), velox::Type::create<Kind>(), nulls_data, vector_size,
        values, std::vector<velox::BufferPtr>{});
    }
    auto data = velox::AlignedBuffer::allocate<T>(vector_size, pool_.get());
    memcpy(data->template asMutable<uint8_t>(), read_data,
           value.size() - (read_data - value.data()));
    return std::make_shared<velox::FlatVector<T>>(
      pool_.get(), velox::Type::create<Kind>(), nulls_data, vector_size, data,
      std::vector<velox::BufferPtr>{});
  }

  velox::VectorPtr ReadFlatMapValue(irs::bytes_view value,
                                    velox::TypePtr type) {
    EXPECT_TRUE(type->isMap());
    auto map_type = type->asMap();
    auto* read_data = value.data();
    const auto flags = std::bit_cast<ValueFlags>(*(read_data++));
    EXPECT_TRUE((flags & ValueFlags::FlatMap) == ValueFlags::FlatMap);
    const auto length_array_size = irs::vread<uint32_t>(read_data);
    const auto* vectors_data_start = read_data + length_array_size;
    auto* vectors_data = vectors_data_start;
    std::vector<velox::VectorPtr> vectors;
    while (read_data != vectors_data_start) {
      EXPECT_LT(read_data, vectors_data_start);
      const auto element_length = irs::vread<uint32_t>(read_data);
      irs::bytes_view element_value(vectors_data, element_length);
      vectors_data += element_length;
      if (vectors.empty()) {
        vectors.push_back(ReadVectorValue(element_value, map_type.keyType()));
        continue;
      }
      vectors.push_back(ReadSingleValue(element_value, map_type.valueType()));
    }
    // number of keys should match number of value vectors
    EXPECT_GT(vectors.size(), 1);
    EXPECT_EQ(vectors[0]->size(), vectors.size() - 1);
    // no inmaps bitset as for value we store only what is actually present.
    return std::make_shared<velox::FlatMapVector>(
      pool_.get(), type, nullptr, 1, vectors[0],
      std::vector<velox::VectorPtr>(vectors.begin() + 1, vectors.end()),
      std::vector<velox::BufferPtr>{});
  }

  velox::VectorPtr ReadMapValue(irs::bytes_view value, velox::TypePtr type) {
    EXPECT_TRUE(type->isMap());
    auto map_type = type->asMap();
    auto read_data = value.data();
    const auto flags = std::bit_cast<ValueFlags>(*(read_data++));
    if ((flags & ValueFlags::FlatMap) == ValueFlags::FlatMap) {
      // FlatMapValue
      return ReadFlatMapValue(value, type);
    }

    const auto keys_size = irs::vread<uint32_t>(read_data);
    EXPECT_LT(keys_size, value.size());
    auto offsets = velox::allocateOffsets(1, pool_.get());
    auto sizes = velox::allocateSizes(1, pool_.get());
    *offsets->asMutable<uint32_t>() = 0;
    auto keys = ReadVectorValue(irs::bytes_view(read_data, keys_size),
                                map_type.keyType());
    auto values = ReadVectorValue(
      irs::bytes_view(read_data + keys_size,
                      value.size() - (read_data - value.data()) - keys_size),
      map_type.valueType());
    EXPECT_EQ(keys->size(), values->size());
    *sizes->asMutable<uint32_t>() = keys->size();
    return std::make_shared<velox::MapVector>(pool_.get(), type, nullptr, 1,
                                              offsets, sizes, keys, values);
  }

  velox::VectorPtr ReadFlatMapVector(irs::bytes_view value,
                                     velox::TypePtr type) {
    EXPECT_TRUE(type->isMap());
    auto map_type = type->asMap();
    auto* read_data = value.data();
    const auto vector_size = irs::vread<uint32_t>(read_data);
    EXPECT_GT(vector_size, 0);
    const auto flags = std::bit_cast<ValueFlags>(*(read_data++));
    EXPECT_TRUE((flags & ValueFlags::FlatMap) == ValueFlags::FlatMap);
    const auto length_array_size = irs::vread<uint32_t>(read_data);
    auto* lengths_data = read_data;
    const auto* lengths_data_end = read_data + length_array_size;
    velox::BufferPtr nulls_data = nullptr;
    size_t nulls_mask_size = 0;

    if ((flags & ValueFlags::HaveNulls) == ValueFlags::HaveNulls) {
      nulls_mask_size = velox::bits::nbytes(vector_size);
      nulls_data = velox::AlignedBuffer::allocate<uint64_t>(
        velox::bits::nwords(vector_size), pool_.get());
      memcpy(nulls_data->asMutable<uint8_t>(), lengths_data_end,
             nulls_mask_size);
      read_data += nulls_mask_size + length_array_size;
    } else {
      read_data += length_array_size;
    }

    std::vector<velox::VectorPtr> vectors;
    std::vector<velox::BufferPtr> inmaps_buffers;
    size_t values_vectors_count = 0;
    while (lengths_data != lengths_data_end) {
      const auto element_length = irs::vread<uint32_t>(lengths_data);
      irs::bytes_view element_value(read_data, element_length);
      read_data += element_length;
      if (vectors.empty()) {
        vectors.push_back(ReadVectorValue(element_value, map_type.keyType()));
        values_vectors_count = vectors.back()->size();
        continue;
      }
      if (values_vectors_count > 0) {
        vectors.push_back(ReadVectorValue(element_value, map_type.valueType()));
        --values_vectors_count;
        continue;
      }
      if (!element_length) {
        inmaps_buffers.push_back(nullptr);
        continue;
      }
      auto tmp = velox::AlignedBuffer::allocate<uint64_t>(
        velox::bits::roundUp(element_length, sizeof(uint64_t)), pool_.get());
      memcpy(tmp->asMutable<uint8_t>(), element_value.data(), element_length);
      inmaps_buffers.push_back(std::move(tmp));
    }
    // number of keys should match number of value vectors
    EXPECT_GT(vectors.size(), 1);
    EXPECT_EQ(vectors[0]->size(), vectors.size() - 1);
    return std::make_shared<velox::FlatMapVector>(
      pool_.get(), type, nulls_data, vector_size, vectors[0],
      std::vector<velox::VectorPtr>(vectors.begin() + 1, vectors.end()),
      inmaps_buffers);
  }

  velox::VectorPtr ReadMapVector(irs::bytes_view value,
                                 const velox::TypePtr& type) {
    EXPECT_TRUE(type->isMap());
    auto* read_data = value.data();
    velox::BufferPtr nulls_data = nullptr;
    size_t nulls_mask_size = 0;
    const auto vector_size = irs::vread<uint32_t>(read_data);
    if (vector_size == 0) {
      EXPECT_EQ(read_data, value.data() + value.size());
      return velox::BaseVector::getOrCreateEmpty(nullptr, type, pool_.get());
    }
    const auto flags = std::bit_cast<ValueFlags>(*(read_data++));
    if ((flags & ValueFlags::FlatMap) == ValueFlags::FlatMap) {
      return ReadFlatMapVector(value, type);
    }
    const bool is_constant =
      (flags & ValueFlags::Constant) == ValueFlags::Constant;
    const bool have_nulls =
      (flags & ValueFlags::HaveNulls) == ValueFlags::HaveNulls;
    uint32_t keys_size = 0;
    if (!is_constant) {
      keys_size = irs::vread<uint32_t>(read_data);
    }
    if (have_nulls) {
      nulls_mask_size = velox::bits::nbytes(vector_size);
      nulls_data = velox::AlignedBuffer::allocate<uint64_t>(
        velox::bits::nwords(vector_size), pool_.get());
      memcpy(nulls_data->asMutable<uint8_t>(), read_data, nulls_mask_size);
      read_data += nulls_mask_size;
    }
    if (is_constant) {
      velox::VectorPtr result;
      if (read_data == value.data() + value.size()) {
        result = velox::BaseVector::create(type, 1, pool_.get());
        result->setNull(0, true);
      } else {
        result = ReadMapValue(
          irs::bytes_view(read_data, value.size() - (read_data - value.data())),
          type);
      }
      result = velox::BaseVector::wrapInConstant(vector_size, 0, result);
      if (have_nulls) {
        // apply nulls mask
        auto indices = makeIndices(vector_size, [](auto i) { return i; });
        result = velox::BaseVector::wrapInDictionary(nulls_data, indices,
                                                     vector_size, result);
      }
      return result;
    }
    const auto indexes_size = sizeof(velox::vector_size_t) * vector_size;
    auto offsets = velox::AlignedBuffer::allocate<velox::vector_size_t>(
      vector_size, pool_.get());
    memcpy(offsets->asMutable<uint8_t>(), read_data, indexes_size);
    read_data += indexes_size;
    auto sizes = velox::AlignedBuffer::allocate<velox::vector_size_t>(
      vector_size, pool_.get());
    memcpy(sizes->asMutable<uint8_t>(), read_data, indexes_size);
    read_data += indexes_size;
    auto keys = ReadVectorValue(irs::bytes_view(read_data, keys_size),
                                type->asMap().keyType());
    read_data += keys_size;
    auto values = ReadVectorValue(
      irs::bytes_view(read_data, value.size() - (read_data - value.data())),
      type->asMap().valueType());
    // validate read vector offsets/sizes
    for (size_t i = 0; i < vector_size; ++i) {
      if (nulls_data && !velox::bits::isBitSet(nulls_data->as<uint64_t>(), i)) {
        // nulls could contain garbage offsets/sizes
        continue;
      }
      const auto offset = offsets->as<velox::vector_size_t>()[i];
      const auto size = sizes->as<velox::vector_size_t>()[i];
      EXPECT_GE(offset, 0);
      EXPECT_LE(offset + size, keys->size());
      EXPECT_LE(offset + size, values->size());
    }
    EXPECT_EQ(keys->size(), values->size());
    return std::make_shared<velox::MapVector>(
      pool_.get(), type, nulls_data, vector_size, offsets, sizes, keys, values);
  }

  template<velox::TypeKind Kind>
  velox::VectorPtr ReadSingleScalarValue(irs::bytes_view value) {
    using T = typename velox::TypeTraits<Kind>::NativeType;
    if (value.size() == 0) {
      return makeAllNullFlatVector<T>(1);
    }
    if constexpr (Kind == velox::TypeKind::BOOLEAN) {
      bool val = value[0] == 0x01;
      return makeFlatVector<bool>({val});
    } else if constexpr (velox::TypeTraits<Kind>::isFixedWidth) {
      T val;
      memcpy(&val, value.data(), sizeof(T));
      return makeFlatVector<T>({val});
    } else if constexpr (std::is_same_v<T, velox::StringView>) {
      velox::StringView val;
      const size_t offset = value[0] == 0 ? 1 : 0;
      EXPECT_GE(value.size(), offset);
      val =
        velox::StringView(reinterpret_cast<const char*>(value.data()) + offset,
                          value.size() - offset);
      return makeFlatVector<T>({val});
    }
    // some yet not supported type?
    EXPECT_TRUE(false);
    return nullptr;
  }

  template<>
  velox::VectorPtr ReadSingleScalarValue<velox::TypeKind::UNKNOWN>(
    irs::bytes_view value) {
    EXPECT_TRUE(value.empty());
    return velox::BaseVector::createNullConstant(velox::UNKNOWN(), 1,
                                                 pool_.get());
  }

  velox::VectorPtr ReadVectorValue(irs::bytes_view value,
                                   velox::TypePtr element_type) {
    velox::VectorPtr elements_vector;
    if (element_type->isRow()) {
      elements_vector = ReadRowValue<false>(value, element_type);
    } else if (element_type->isArray()) {
      elements_vector = ReadArrayVector(value, element_type);
    } else if (element_type->isMap()) {
      elements_vector = ReadMapVector(value, element_type);
    } else {
      const auto element_kind = element_type->kind();
      if (element_kind == velox::TypeKind::UNKNOWN) {
        auto read_data = value.data();
        auto vector_size = irs::vread<uint32_t>(read_data);
        if (vector_size == 0) {
          EXPECT_EQ(read_data, value.data() + value.size());
          return velox::BaseVector::createNullConstant(velox::UNKNOWN(), 0,
                                                       pool_.get());
        }
        const auto flags = std::bit_cast<ValueFlags>(*(read_data++));
        EXPECT_TRUE((flags & ValueFlags::Constant) == ValueFlags::Constant);
        // makes no sense to store nulls bitmap for UNKNOWN
        EXPECT_FALSE((flags & ValueFlags::HaveNulls) == ValueFlags::HaveNulls);
        EXPECT_EQ(read_data, value.data() + value.size());
        return velox::BaseVector::createNullConstant(velox::UNKNOWN(),
                                                     vector_size, pool_.get());
      }
      elements_vector = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(ReadFlatScalarVector,
                                                           element_kind, value);
    }
    return elements_vector;
  }

  velox::VectorPtr ReadArrayVector(irs::bytes_view value,
                                   const velox::TypePtr& type) {
    EXPECT_TRUE(type->isArray());
    auto* read_data = value.data();
    velox::BufferPtr nulls_data = nullptr;
    size_t nulls_mask_size = 0;
    const auto vector_size = irs::vread<uint32_t>(read_data);
    if (vector_size == 0) {
      EXPECT_EQ(read_data, value.data() + value.size());
      return velox::BaseVector::getOrCreateEmpty(nullptr, type, pool_.get());
    }
    const auto flags = std::bit_cast<ValueFlags>(*(read_data++));
    const bool is_constant =
      (flags & ValueFlags::Constant) == ValueFlags::Constant;
    const bool have_nulls =
      (flags & ValueFlags::HaveNulls) == ValueFlags::HaveNulls;
    if (have_nulls) {
      nulls_mask_size = velox::bits::nbytes(vector_size);
      nulls_data = velox::AlignedBuffer::allocate<uint64_t>(
        velox::bits::nwords(vector_size), pool_.get());
      memcpy(nulls_data->asMutable<uint8_t>(), read_data, nulls_mask_size);
      read_data += nulls_mask_size;
    }
    if (is_constant) {
      velox::VectorPtr result;
      if (read_data == value.data() + value.size()) {
        result = velox::BaseVector::create(type, 1, pool_.get());
        result->setNull(0, true);
      } else {
        result = ReadArrayValue(
          irs::bytes_view(read_data, value.size() - (read_data - value.data())),
          type->asArray().elementType());
      }
      result = velox::BaseVector::wrapInConstant(vector_size, 0, result);
      if (have_nulls) {
        // apply nulls mask
        auto indices = makeIndices(vector_size, [](auto i) { return i; });
        result = velox::BaseVector::wrapInDictionary(nulls_data, indices,
                                                     vector_size, result);
      }
      return result;
    }

    const auto indexes_size = sizeof(velox::vector_size_t) * vector_size;
    auto offsets = velox::AlignedBuffer::allocate<velox::vector_size_t>(
      vector_size, pool_.get());
    memcpy(offsets->asMutable<uint8_t>(), read_data, indexes_size);
    read_data += indexes_size;
    auto sizes = velox::AlignedBuffer::allocate<velox::vector_size_t>(
      vector_size, pool_.get());
    memcpy(sizes->asMutable<uint8_t>(), read_data, indexes_size);
    read_data += indexes_size;
    auto elements = ReadVectorValue(
      irs::bytes_view(read_data, value.size() - (read_data - value.data())),
      type->asArray().elementType());
    // validate read vector offsets/sizes
    for (size_t i = 0; i < vector_size; ++i) {
      if (nulls_data && !velox::bits::isBitSet(nulls_data->as<uint64_t>(), i)) {
        // nulls could contain garbage offsets/sizes
        continue;
      }
      const auto offset = offsets->as<velox::vector_size_t>()[i];
      const auto size = sizes->as<velox::vector_size_t>()[i];
      EXPECT_GE(offset, 0);
      EXPECT_LE(offset + size, elements->size());
    }
    return std::make_shared<velox::ArrayVector>(
      pool_.get(), type, nulls_data, vector_size, offsets, sizes, elements);
  }

  velox::VectorPtr ReadArrayValue(irs::bytes_view value,
                                  velox::TypePtr element_type) {
    velox::VectorPtr elements_vector = ReadVectorValue(value, element_type);
    auto offsets = velox::allocateOffsets(1, pool_.get());
    auto sizes = velox::allocateSizes(1, pool_.get());
    *offsets->asMutable<uint32_t>() = 0;
    *sizes->asMutable<uint32_t>() = elements_vector->size();
    return std::make_shared<velox::ArrayVector>(
      pool_.get(), ARRAY(element_type), nullptr, 1, offsets, sizes,
      elements_vector);
  }

  velox::VectorPtr ReadSingleValue(irs::bytes_view value_data,
                                   velox::TypePtr type) {
    if (type->isArray()) {
      return ReadArrayValue(value_data, type->asArray().elementType());
    } else if (type->isRow()) {
      return ReadRowValue<true>(value_data, type);
    } else if (type->isMap()) {
      return ReadMapValue(value_data, type);
    } else {
      return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH_ALL(ReadSingleScalarValue,
                                                    type->kind(), value_data);
    }
    EXPECT_TRUE(false);  // unexpected type
  }

  template<bool SingleValue>
  velox::VectorPtr ReadRowValue(irs::bytes_view value, velox::TypePtr type) {
    EXPECT_EQ(type->kind(), velox::TypeKind::ROW);
    const auto& row_type = type->asRow();
    auto* read_data = value.data();
    [[maybe_unused]] velox::BufferPtr nulls_data = nullptr;
    [[maybe_unused]] size_t nulls_mask_size = 0;
    [[maybe_unused]] uint32_t vector_size = 0;
    uint32_t length_size = 0;
    if constexpr (!SingleValue) {
      vector_size = irs::vread<uint32_t>(read_data);
      if (vector_size == 0) {
        EXPECT_EQ(read_data, value.data() + value.size());
        return velox::BaseVector::getOrCreateEmpty(nullptr, type, pool_.get());
      }
      const auto flags = std::bit_cast<ValueFlags>(*(read_data++));
      const bool is_constant =
        (flags & ValueFlags::Constant) == ValueFlags::Constant;
      const bool have_nulls =
        (flags & ValueFlags::HaveNulls) == ValueFlags::HaveNulls;
      if (!is_constant) {
        // constant never have length array
        length_size = irs::vread<uint32_t>(read_data);
      }
      if (have_nulls) {
        nulls_mask_size = velox::bits::nbytes(vector_size);
        nulls_data = velox::AlignedBuffer::allocate<uint64_t>(
          velox::bits::nwords(vector_size), pool_.get());
        memcpy(nulls_data->asMutable<uint8_t>(), read_data, nulls_mask_size);
        read_data += nulls_mask_size;
      }
      if (is_constant) {
        velox::VectorPtr result;
        if (read_data == value.data() + value.size()) {
          result = velox::BaseVector::create(type, 1, pool_.get());
          result->setNull(0, true);
        } else {
          result = ReadRowValue<true>(
            irs::bytes_view(read_data,
                            value.size() - (read_data - value.data())),
            type);
        }
        result = velox::BaseVector::wrapInConstant(vector_size, 0, result);
        if (have_nulls) {
          // apply nulls mask
          auto indices = makeIndices(vector_size, [](auto i) { return i; });
          result = velox::BaseVector::wrapInDictionary(nulls_data, indices,
                                                       vector_size, result);
        }
        return result;
      }
    } else {
      length_size = irs::vread<uint32_t>(read_data);
    }
    std::vector<std::string> names;
    std::vector<velox::VectorPtr> children;
    for (size_t i = 0; i < row_type.children().size(); ++i) {
      names.push_back(row_type.nameOf(i));
    }

    auto values_data = read_data + length_size;
    for (size_t i = 0; i < row_type.children().size(); ++i) {
      const auto current_size =
        i == row_type.children().size() - 1
          ? (value.size() - (values_data - value.data()))
          : irs::vread<uint32_t>(read_data);
      EXPECT_LE(current_size, value.size() - (values_data - value.data()));
      const auto value_data = irs::bytes_view(values_data, current_size);
      if constexpr (SingleValue) {
        children.push_back(ReadSingleValue(value_data, row_type.childAt(i)));
      } else {
        if (row_type.childAt(i)->isRow()) {
          children.push_back(
            ReadRowValue<false>(value_data, row_type.childAt(i)));
        } else if (row_type.childAt(i)->isArray()) {
          children.push_back(ReadArrayVector(value_data, row_type.childAt(i)));
        } else if (row_type.childAt(i)->isMap()) {
          children.push_back(ReadMapVector(value_data, row_type.childAt(i)));
        } else {
          auto child = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            ReadFlatScalarVector, row_type.childAt(i)->kind(), value_data);
          children.push_back(child);
        }
      }
      values_data += current_size;
    }
    if constexpr (!SingleValue) {
      return std::make_shared<velox::RowVector>(pool_.get(), type, nulls_data,
                                                vector_size, children);
    }
    return makeRowVector(names, children);
  }

  // Test column of arrays. Possibly dictionary encoded.
  template<velox::TypeKind Kind, typename DataType>
  void MakeTestDictionaryEncodedArrayColumn(
    const std::vector<std::vector<DataType>>& data,
    const std::vector<velox::vector_size_t>& indices,
    const std::vector<bool>& dict_nulls) {
    using T = typename velox::TypeTraits<Kind>::NativeType;
    static_assert(
      std::is_same_v<DataType, std::optional<T>> || std::is_same_v<DataType, T>,
      "Test data type mast match requested Kind's NativeType possibly optional "
      "wrapped");
    velox::ArrayVectorPtr array_vector;
    if constexpr (std::is_same_v<DataType, T>) {
      array_vector = makeArrayVector<T>(data);
    } else {
      array_vector = makeNullableArrayVector<T>(data);
    }
    velox::VectorPtr vector = array_vector;
    if (!indices.empty()) {
      vector = wrapInDictionary(makeIndices(indices), vector);
      if (!dict_nulls.empty()) {
        auto nulls = makeNulls(dict_nulls);
        vector->setNulls(nulls);
      }
    }
    MakeTestWriteImpl(velox::ARRAY(createScalarType(Kind)), vector);
  }

  template<velox::TypeKind Kind, typename DataType>
  void MakeTestWriteArray(const std::vector<std::vector<DataType>>& data) {
    MakeTestDictionaryEncodedArrayColumn<Kind>(data, {}, {});
  }

  // Test array made of Dictionary encoded vectors.
  template<velox::TypeKind Kind, typename DataType>
  void MakeTestEncodedArrayDict(
    const std::vector<DataType>& data,
    const std::vector<velox::vector_size_t>& indices,
    const std::vector<bool>& dict_nulls,
    const std::vector<velox::vector_size_t>& array_offsets,
    const std::vector<velox::vector_size_t> array_nulls) {
    using T = typename velox::TypeTraits<Kind>::NativeType;
    static_assert(
      std::is_same_v<DataType, std::optional<T>> || std::is_same_v<DataType, T>,
      "Test data type mast match requested Kind's NativeType possibly optional "
      "wrapped");
    velox::VectorPtr flat_vector;
    if constexpr (std::is_same_v<DataType, T>) {
      flat_vector = makeFlatVector<T>(data);
    } else {
      flat_vector = makeNullableFlatVector<T>(data);
    }
    auto indices_vector = makeIndices(indices);

    velox::BufferPtr nulls_vector =
      dict_nulls.empty() ? velox::BufferPtr{} : makeNulls(dict_nulls);
    auto array_vector = makeArrayVector(
      array_offsets,
      velox::BaseVector::wrapInDictionary(nulls_vector, indices_vector,
                                          indices.size(), flat_vector),
      array_nulls);
    MakeTestWriteImpl(velox::ARRAY(createScalarType(Kind)), array_vector);
  }

  void MakeTestWriteImpl(velox::TypePtr type, velox::VectorPtr data) {
    std::vector<std::string> names{"struct"};
    sdb::ObjectId object_key;
    sdb::connector::primary_key::Keys row_keys{*pool_.get()};
    MakeRocksDBWrite(names, {data}, object_key, row_keys);
    auto* loaded_data = data->loadedVector();
    ASSERT_NE(loaded_data, nullptr);
    std::string key = sdb::connector::key_utils::PrepareTableKey(object_key);
    const auto base_size = key.size();
    for (velox::vector_size_t i = 0; i < data->size(); ++i) {
      SCOPED_TRACE(testing::Message("Vector:") << i);
      rocksdb::ReadOptions read_options;
      std::string value;
      key.resize(base_size);
      sdb::rocksutils::Append(key, static_cast<sdb::catalog::Column::Id>(0),
                              std::string_view{row_keys[i]});
      ASSERT_TRUE(
        _db->Get(read_options, _cf_handles.front(), rocksdb::Slice(key), &value)
          .ok());

      if (value.empty()) {
        ASSERT_TRUE(data->isNullAt(i));
        continue;
      }
      // make address sanitizer happy
      irs::bytes_view value_bytes(
        reinterpret_cast<const irs::byte_type*>(value.data()), value.size());
      velox::VectorPtr actual;
      if (type->isRow()) {
        actual = ReadRowValue<true>(value_bytes, type);
      } else if (type->isArray()) {
        actual = ReadArrayValue(value_bytes, type->asArray().elementType());
      } else if (type->isMap()) {
        actual = ReadMapValue(value_bytes, type);
      } else {
        actual = VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(ReadSingleScalarValue,
                                                    type->kind(), value_bytes);
      }
      ASSERT_NE(actual, nullptr);
      ASSERT_TRUE(loaded_data->equalValueAt(actual.get(), i, 0))
        << "Expected: " << loaded_data->toString(i)
        << ", Actual: " << actual->toString(0);
    }
  }

  template<velox::TypeKind Kind, velox::VectorEncoding::Simple Encoding,
           typename DataType>
  void MakeTestWriteColumnDictionary(
    const std::vector<DataType>& data,
    const std::vector<velox::vector_size_t>& indices,
    const std::vector<bool>& dict_nulls) {
    using T = typename velox::TypeTraits<Kind>::NativeType;
    static_assert(
      std::is_same_v<DataType, std::optional<T>> || std::is_same_v<DataType, T>,
      "Test data type mast match requested Kind's NativeType possibly optional "
      "wrapped");
    velox::VectorPtr vector;
    if constexpr (Encoding == velox::VectorEncoding::Simple::LAZY) {
      if constexpr (std::is_same_v<DataType, T>) {
        vector = makeFlatVector<T>(data);
      } else {
        vector = makeNullableFlatVector<T>(data);
      }
      vector = wrapInLazyDictionary(vector);
    } else if constexpr (Encoding == velox::VectorEncoding::Simple::FLAT) {
      if constexpr (std::is_same_v<DataType, T>) {
        vector = makeFlatVector<T>(data);
      } else {
        vector = makeNullableFlatVector<T>(data);
      }
    } else if constexpr (Encoding == velox::VectorEncoding::Simple::BIASED) {
      vector = vectorMaker_.biasVector(data);
    } else if constexpr (Encoding == velox::VectorEncoding::Simple::CONSTANT) {
      ASSERT_TRUE(
        absl::c_all_of(data, [&](const auto& val) { return val == data[0]; }));
      vector = makeConstant(data[0], data.size());
    } else {
      static_assert(false, "Not supported encoding");
    }
    if (!indices.empty()) {
      auto indices_vector = makeIndices(indices);
      velox::BufferPtr nulls_vector =
        dict_nulls.empty() ? velox::BufferPtr{} : makeNulls(dict_nulls);
      vector = velox::BaseVector::wrapInDictionary(nulls_vector, indices_vector,
                                                   indices.size(), vector);
    }
    MakeTestWriteImpl(createScalarType(Kind), vector);
  }

  template<velox::TypeKind Kind, typename DataType>
  void MakeTestWriteDictionary(const std::vector<DataType>& data,
                               const std::vector<velox::vector_size_t>& indices,
                               const std::vector<bool>& dict_nulls) {
    MakeTestWriteColumnDictionary<Kind, velox::VectorEncoding::Simple::FLAT>(
      data, indices, dict_nulls);
  }

  template<velox::TypeKind Kind, typename DataType>
  void MakeTestWriteFlat(const std::vector<DataType>& data) {
    MakeTestWriteColumnDictionary<Kind, velox::VectorEncoding::Simple::FLAT>(
      data, {}, {});
  }

  template<velox::TypeKind Kind>
  void MakeTestWriteBias(
    const std::vector<
      std::optional<typename velox::TypeTraits<Kind>::NativeType>>& data,
    const std::vector<velox::vector_size_t>& indices,
    const std::vector<bool>& dict_nulls) {
    MakeTestWriteColumnDictionary<Kind, velox::VectorEncoding::Simple::BIASED>(
      data, indices, dict_nulls);
  }

  template<velox::TypeKind Kind, typename DataType>
  void MakeTestWriteConstantScalar(
    const DataType& data, size_t size,
    const std::vector<velox::vector_size_t>& indices,
    const std::vector<bool>& dict_nulls) {
    std::vector<DataType> row_data;
    row_data.resize(size, data);
    MakeTestWriteColumnDictionary<Kind,
                                  velox::VectorEncoding::Simple::CONSTANT>(
      row_data, {}, {});
  }

 protected:
  std::string _path;
  std::vector<rocksdb::ColumnFamilyDescriptor> _cf_families;
  rocksdb::TransactionDB* _db{nullptr};
  std::vector<rocksdb::ColumnFamilyHandle*> _cf_handles;
};

TEST_F(DataSinkTest, test_tableWriteMulticolumnScalar) {
  constexpr size_t kDocs = 100;
  std::vector<std::string> names{"int", "flag", "name"};
  sdb::ObjectId object_id;
  sdb::connector::primary_key::Keys row_keys{*pool_.get()};
  MakeRocksDBWrite(
    names,
    {makeFlatVector<int>(kDocs, [&](auto row) { return row; }),
     makeFlatVector<bool>(kDocs, [&](auto row) { return row % 2; }),
     makeFlatVector<velox::StringView>(kDocs,
                                       [&](auto row) {
                                         return velox::StringView::makeInline(
                                           fmt::format("A{}", row % 100));
                                       })},
    object_id, row_keys);

  // TODO(Dronplane) verify written vectors
}

TEST_F(DataSinkTest, test_tableWriteFlatInt) {
  std::vector<int32_t> data = {1, 2, 3, 3, 2, 1, 5, 6, 7, 7, 6, 5, 9, 9, 9};
  MakeTestWriteFlat<velox::TypeKind::INTEGER>(data);
}

TEST_F(DataSinkTest, test_tableWriteFlatIntNulls) {
  std::vector<std::optional<int32_t>> data = {1, 2, 3, 3, 2, 1, 5, std::nullopt,
                                              7, 7, 6, 5, 9, 9, 9};
  MakeTestWriteFlat<velox::TypeKind::INTEGER>(data);
}

TEST_F(DataSinkTest, test_tableWriteFlatVarchar) {
  std::vector<velox::StringView> data = {
    "ff",     "long enough to not be inlined string yeaaaahhhh",
    "",       "a",
    "\0",     "\0\0",
    "\0 123", "basr"};
  MakeTestWriteFlat<velox::TypeKind::VARCHAR>(data);
}

TEST_F(DataSinkTest, test_tableWriteFlatVarcharNulls) {
  std::vector<std::optional<velox::StringView>> data = {
    "ff",     "long enough to not be inlined string yeaaaahhhh",
    "",       "a",
    "\0",     "\0\0",
    "\0 123", std::nullopt,
    "basr"};
  MakeTestWriteFlat<velox::TypeKind::VARCHAR>(data);
}

TEST_F(DataSinkTest, test_tableWriteFlatBool) {
  std::vector<bool> data = {true, false, true, false};
  MakeTestWriteFlat<velox::TypeKind::BOOLEAN>(data);
}

TEST_F(DataSinkTest, test_tableWriteFlatBoolNulls) {
  std::vector<std::optional<bool>> data = {
    true, false, true, false, true, false, true, std::nullopt, false};
  MakeTestWriteFlat<velox::TypeKind::BOOLEAN>(data);
}

TEST_F(DataSinkTest, test_tableWriteFlatTimestamp) {
  std::vector<velox::Timestamp> data = {velox::Timestamp::fromMillis(123),
                                        velox::Timestamp::fromMillis(0)};
  MakeTestWriteFlat<velox::TypeKind::TIMESTAMP>(data);
}

TEST_F(DataSinkTest, test_tableWriteFlatTimestampNulls) {
  std::vector<std::optional<velox::Timestamp>> data = {
    velox::Timestamp::fromMillis(123), std::nullopt,
    velox::Timestamp::fromMillis(0)};
  MakeTestWriteFlat<velox::TypeKind::TIMESTAMP>(data);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryInt) {
  std::vector<int32_t> data = {1, 2, 3, 5};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{};
  MakeTestWriteDictionary<velox::TypeKind::INTEGER>(data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryNullsInt) {
  std::vector<int32_t> data = {1, 2, 3, 5};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{false, false, false, true,
                               false, true,  false, true};
  MakeTestWriteDictionary<velox::TypeKind::INTEGER>(data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryAllNullsInt) {
  std::vector<int32_t> data = {1, 2, 3, 5};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{true, true, true, true, true, true, true, true};
  MakeTestWriteDictionary<velox::TypeKind::INTEGER>(data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryIntNulls) {
  std::vector<std::optional<int32_t>> data = {1, 2, 3, std::nullopt};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{};
  MakeTestWriteDictionary<velox::TypeKind::INTEGER>(data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryNullsIntNulls) {
  std::vector<std::optional<int32_t>> data = {1, 2, 3, std::nullopt};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{false, false, false, true,
                               false, true,  false, true};
  MakeTestWriteDictionary<velox::TypeKind::INTEGER>(data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryBool) {
  std::vector<bool> data = {true, false, false, false};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{};
  MakeTestWriteDictionary<velox::TypeKind::BOOLEAN>(data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryNullsBoolNulls) {
  std::vector<std::optional<bool>> data = {true, false, false, std::nullopt};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{false, false, false, true,
                               false, true,  false, true};
  MakeTestWriteDictionary<velox::TypeKind::BOOLEAN>(data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryVarchar) {
  std::vector<velox::StringView> data = {
    "", "foo", "some long string that can not be inlined", " stringnullopt"};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{};
  MakeTestWriteDictionary<velox::TypeKind::VARCHAR>(data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryNullsVarcharNulls) {
  std::vector<std::optional<velox::StringView>> data = {
    "", "foo", "some long string that can not be inlined", std::nullopt};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{false, false, false, true,
                               false, true,  false, true};
  MakeTestWriteDictionary<velox::TypeKind::VARCHAR>(data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryNullsVarcharAllNulls) {
  std::vector<std::optional<velox::StringView>> data = {
    "", "foo", "some long string that can not be inlined", std::nullopt};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{true, true, true, true, true, true, true, true};
  MakeTestWriteDictionary<velox::TypeKind::VARCHAR>(data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteIntArray) {
  std::vector<std::vector<int32_t>> data = {
    {1, 2, 3}, {3, 2, 1}, {}, {7, 6, 5}, {9, 9, 9}};
  MakeTestWriteArray<velox::TypeKind::INTEGER>(data);
}

TEST_F(DataSinkTest, test_tableWriteIntEmptyArray) {
  std::vector<std::vector<int32_t>> data = {{}};
  MakeTestWriteArray<velox::TypeKind::INTEGER>(data);
}

TEST_F(DataSinkTest, test_tableWriteIntArrayNulls) {
  std::vector<std::vector<std::optional<int32_t>>> data = {
    {1, 2, std::nullopt, 3},
    {},
    {std::nullopt},
    {std::nullopt, 5, 6, 7},
    {std::nullopt, std::nullopt, std::nullopt},
    {5, 9, std::nullopt}};
  MakeTestWriteArray<velox::TypeKind::INTEGER>(data);
}

TEST_F(DataSinkTest, test_tableWriteBoolArray) {
  std::vector<std::vector<bool>> data = {{true, false, true},
                                         {false, true, true},
                                         {false, false, false},
                                         {true, false}};
  MakeTestWriteArray<velox::TypeKind::BOOLEAN>(data);
}

TEST_F(DataSinkTest, test_tableWriteBoolArrayNulls) {
  std::vector<std::vector<std::optional<bool>>> data = {
    {true, false, std::nullopt, true},
    {false, true, true},
    {std::nullopt, false, false, false},
    {std::nullopt, std::nullopt, std::nullopt},
    {true, false, std::nullopt}};
  MakeTestWriteArray<velox::TypeKind::BOOLEAN>(data);
}

TEST_F(DataSinkTest, test_tableWriteWholeNullBoolArray) {
  std::vector<std::vector<std::optional<bool>>> data = {
    {true, false, std::nullopt, true}};
  MakeTestWriteArray<velox::TypeKind::BOOLEAN>(data);
}

TEST_F(DataSinkTest, test_tableWriteVarcharArray) {
  std::vector<std::vector<velox::StringView>> data = {
    {"foo", "bar",
     "very long string that do not fit into inline velox::StringView"},
    {"vbar", "vbas", "raz"},
    {"aaaaa", "bbbbbbbbbbbbbbbbb", "not inline not inline no no no please"},
    {"sssss", "zzzzz"}};
  MakeTestWriteArray<velox::TypeKind::VARCHAR>(data);
}

TEST_F(DataSinkTest, test_tableWriteVarcharArrayNulls) {
  std::vector<std::vector<std::optional<velox::StringView>>> data = {
    {"foo", "bar", std::nullopt,
     "very long string that do not fit into inline velox::StringView"},
    {"vbar", "vbas", "raz"},
    {std::nullopt, "aaaaa", "bbbbbbbbbbbbbbbbb",
     "not inline not inline no no no please"},
    {std::nullopt, std::nullopt, std::nullopt},
    {"sssss", "zzzzz", std::nullopt}};
  MakeTestWriteArray<velox::TypeKind::VARCHAR>(data);
}

// TODO(Dronplane) find a way to write this test. Currently
// makeNullableArrayVector does not have velox::TypeKind argument and
// velox::StringView gives us VARCHAR not VARBINARY TEST_F(RocksDBConnectorTest,
// test_tableWriteVarbinaryArrayNulls) {

TEST_F(DataSinkTest, test_tableWriteIntArrayNullsDictionary) {
  std::vector<std::vector<std::optional<int32_t>>> data = {
    {1, 2, std::nullopt, 3},
    {3, 2, 1},
    {std::nullopt, 5, 6, 7},
    {std::nullopt, std::nullopt, std::nullopt},
    {5, 9, std::nullopt}};
  std::vector<velox::vector_size_t> indices{3, 2, 1};
  std::vector<bool> dict_nulls = {};
  MakeTestDictionaryEncodedArrayColumn<velox::TypeKind::INTEGER>(data, indices,
                                                                 dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteIntArrayNullsDictionaryNulls) {
  std::vector<std::vector<std::optional<int32_t>>> data = {
    {1, 2, std::nullopt, 3},
    {3, 2, 1},
    {std::nullopt, 5, 6, 7},
    {std::nullopt, std::nullopt, std::nullopt},
    {5, 9, std::nullopt}};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 4, 4, 0};
  std::vector<bool> dict_nulls = {true, false, false, true, false, false};
  MakeTestDictionaryEncodedArrayColumn<velox::TypeKind::INTEGER>(data, indices,
                                                                 dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteIntArrayDictionaryNulls) {
  std::vector<std::vector<int32_t>> data = {
    {1, 2, 3}, {3, 2, 1}, {5, 6, 7}, {5, 9}};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0};
  std::vector<bool> dict_nulls = {true, false, false, true};
  MakeTestDictionaryEncodedArrayColumn<velox::TypeKind::INTEGER>(data, indices,
                                                                 dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteBoolArrayNullsDictionary) {
  std::vector<std::vector<std::optional<bool>>> data = {
    {true, false, std::nullopt, true},
    {false, true, false},
    {std::nullopt, true, false},
    {std::nullopt, std::nullopt, std::nullopt},
    {false, true, std::nullopt}};
  std::vector<velox::vector_size_t> indices{3, 2, 1};
  std::vector<bool> dict_nulls = {};
  MakeTestDictionaryEncodedArrayColumn<velox::TypeKind::BOOLEAN>(data, indices,
                                                                 dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteBoolArrayNullsDictionaryNulls) {
  std::vector<std::vector<std::optional<bool>>> data = {
    {true, false, std::nullopt, true},
    {false, true, false},
    {std::nullopt, true, false},
    {std::nullopt, std::nullopt, std::nullopt},
    {false, true, std::nullopt}};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 4, 4, 0};
  std::vector<bool> dict_nulls = {true, false, false, true, false, false};
  MakeTestDictionaryEncodedArrayColumn<velox::TypeKind::BOOLEAN>(data, indices,
                                                                 dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteBoolArrayDictionaryNulls) {
  std::vector<std::vector<bool>> data = {{true, true, true},
                                         {true, true, true},
                                         {false, false, false},
                                         {false, true}};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0};
  std::vector<bool> dict_nulls = {true, false, false, true};
  MakeTestDictionaryEncodedArrayColumn<velox::TypeKind::BOOLEAN>(data, indices,
                                                                 dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteVarcharArrayNullsDictionary) {
  std::vector<std::vector<std::optional<velox::StringView>>> data = {
    {"true", "false long non inlined string", std::nullopt, "rue"},
    {"", "a", "aaaad"},
    {"", "", ""},
    {std::nullopt, std::nullopt, std::nullopt},
    {"", "", std::nullopt}};
  std::vector<velox::vector_size_t> indices{3, 2, 1};
  std::vector<bool> dict_nulls = {};
  MakeTestDictionaryEncodedArrayColumn<velox::TypeKind::VARCHAR>(data, indices,
                                                                 dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteVarcharArrayNullsDictionaryNulls) {
  std::vector<std::vector<std::optional<velox::StringView>>> data = {
    {"true", "false long non inlined string", std::nullopt, "rue"},
    {"", "a", "aaaad"},
    {"a", "vvvv", ""},
    {std::nullopt, std::nullopt, std::nullopt},
    {"", "", std::nullopt}};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 4, 4, 0};
  std::vector<bool> dict_nulls = {true, false, false, true, false, false};
  MakeTestDictionaryEncodedArrayColumn<velox::TypeKind::VARCHAR>(data, indices,
                                                                 dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteVarcharArrayDictionaryNulls) {
  std::vector<std::vector<velox::StringView>> data = {
    {"long string non online and truly true", "true", "true"},
    {"true", "true", ""},
    {"false", "false", "false"},
    {"false", "true"}};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0};
  std::vector<bool> dict_nulls = {true, false, false, true};
  MakeTestDictionaryEncodedArrayColumn<velox::TypeKind::VARCHAR>(data, indices,
                                                                 dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteScalarStruct) {
  std::vector<std::string> fields{"foo", "bar", "bas", "baf"};
  auto data = makeRowVector(
    fields,
    {makeFlatVector<int32_t>({1, 2, 3, 4}),
     makeFlatVector<bool>({true, false, true, false}),
     makeFlatVector<velox::StringView>(
       {"", "a", "\0", "long lomng glgkfklgfklgf sdflkjsdlfk"}),
     makeFlatVector<velox::Timestamp>(
       {velox::Timestamp::fromMillis(123), velox::Timestamp::fromMillis(55555),
        velox::Timestamp::fromMillis(22222),
        velox::Timestamp::fromMillis(99999999999)})});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteScalarStructNulls) {
  std::vector<std::string> fields{"foo", "bar", "bas", "baf"};
  auto data = makeRowVector(
    fields,
    {makeNullableFlatVector<int32_t>({std::nullopt, 1, 2, 3, 4, 5}),
     makeNullableFlatVector<bool>(
       {true, std::nullopt, false, true, false, true}),
     makeNullableFlatVector<velox::StringView>(
       {"", "a", std::nullopt, "\0", "long lomng glgkfklgfklgf sdflkjsdlfk",
        ""}),
     makeNullableFlatVector<velox::Timestamp>(
       {velox::Timestamp::fromMillis(123), velox::Timestamp::fromMillis(55555),
        velox::Timestamp::fromMillis(22222), std::nullopt,
        velox::Timestamp::fromMillis(99999999999),
        velox::Timestamp::fromMillis(999)})},
    [](velox::vector_size_t i) -> bool { return i == 5; });
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteScalarStructNullsDictionary) {
  std::vector<std::string> fields{"foo", "bar", "bas", "baf"};
  auto nulls_int = makeNulls(
    {false, false, true, true, true, false, false, false, true, true, true});
  std::vector<velox::vector_size_t> indices_raw = {5, 4, 3, 2, 1, 0,
                                                   1, 2, 3, 4, 5};
  auto indices = makeIndices(indices_raw);
  auto nulls_bool = makeNulls(
    {false, false, false, true, true, false, false, false, true, false, true});
  auto nulls_string = makeNulls(
    {false, false, true, false, true, false, false, false, false, false, true});
  auto nulls_ts = makeNulls(
    {false, false, true, false, false, false, false, true, true, false, false});
  auto data = makeRowVector(
    fields,
    {velox::BaseVector::wrapInDictionary(
       nulls_int, indices, indices_raw.size(),
       makeNullableFlatVector<int32_t>({std::nullopt, 1, 2, 3, 4, 5})),
     velox::BaseVector::wrapInDictionary(
       nulls_bool, indices, indices_raw.size(),
       makeNullableFlatVector<bool>(
         {true, std::nullopt, false, true, false, true})),
     velox::BaseVector::wrapInDictionary(
       nulls_string, indices, indices_raw.size(),
       makeNullableFlatVector<velox::StringView>(
         {"", "a", std::nullopt, "\0", "long lomng glgkfklgfklgf sdflkjsdlfk",
          ""})),
     velox::BaseVector::wrapInDictionary(
       nulls_ts, indices, indices_raw.size(),
       makeNullableFlatVector<velox::Timestamp>(
         {velox::Timestamp::fromMillis(123),
          velox::Timestamp::fromMillis(55555),
          velox::Timestamp::fromMillis(22222), std::nullopt,
          velox::Timestamp::fromMillis(99999999999),
          velox::Timestamp::fromMillis(999)}))},
    [](velox::vector_size_t i) -> bool { return i == 5; });
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteArrayStructNulls) {
  std::vector<std::string> fields{"foo", "bar", "bas"};
  std::vector<std::vector<std::optional<int32_t>>> int_array{
    {1, 2, 3}, {3, std::nullopt, 1}, {555, 4545445, 0}, {1}};
  std::vector<std::vector<std::optional<bool>>> bool_array{
    {true, false}, {std::nullopt}, {false, std::nullopt}, {true}};
  std::vector<std::vector<std::optional<velox::StringView>>> str_array{
    {"1", "2", std::nullopt, "3"},
    {"dfdfsfdfsdsfdfs", "", "dfdf"},
    {"", "", ""},
    {"null"}};
  auto data =
    makeRowVector(fields,
                  {makeNullableArrayVector<int32_t>(int_array),
                   makeNullableArrayVector<bool>(bool_array),
                   makeNullableArrayVector<velox::StringView>(str_array)},
                  [](velox::vector_size_t idx) { return idx == 3; });
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteMapScalar) {
  using MapValueType = std::vector<std::pair<int32_t, std::optional<int32_t>>>;
  std::vector<std::optional<MapValueType>> map_data = {
    MapValueType{{1, 2}, {2, 4}, {3, std::nullopt}, {4, 8}},
    std::nullopt,
    MapValueType{{1, std::nullopt}, {23, 334}, {3, std::nullopt}, {14, 18}},
    MapValueType{{1, 2}, {2, 3}, {-23, std::nullopt}, {24, -8}},
    MapValueType{{1, 2}, {2, 4}},
    MapValueType{}};
  MakeTestWriteImpl(velox::MAP(createScalarType(velox::TypeKind::INTEGER),
                               createScalarType(velox::TypeKind::INTEGER)),
                    makeNullableMapVector(map_data));
}

TEST_F(DataSinkTest, test_tableWriteMapOfArray) {
  auto data = createMapOfArraysVector<int64_t, int64_t>(
    {{{1, std::nullopt}, {2, {}}, {3, {{1, 2, std::nullopt}}}},
     {},
     {{2, {{4, 5, std::nullopt}}}},
     {{std::nullopt, {{7, 8, 9}}}}});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryEncodedArrayInt) {
  std::vector<int32_t> data = {1, 2, 3, 4};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {};
  MakeTestEncodedArrayDict<velox::TypeKind::INTEGER>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryEncodedArrayIntNull) {
  std::vector<std::optional<int32_t>> data = {1, 2, 3, std::nullopt};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{false, false, false, true,
                               false, true,  false, true};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {1};
  MakeTestEncodedArrayDict<velox::TypeKind::INTEGER>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryEncodedArrayIntOnlyDictNull) {
  std::vector<int32_t> data = {1, 2, 3, 4};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{false, false, false, true,
                               false, true,  false, true};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {};
  MakeTestEncodedArrayDict<velox::TypeKind::INTEGER>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryEncodedArrayBool) {
  std::vector<bool> data = {true, true, false, false};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {};
  MakeTestEncodedArrayDict<velox::TypeKind::BOOLEAN>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryEncodedArrayBoolNull) {
  std::vector<std::optional<bool>> data = {true, false, true, std::nullopt};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{false, false, false, true,
                               false, true,  false, true};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {1};
  MakeTestEncodedArrayDict<velox::TypeKind::BOOLEAN>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryEncodedArrayBoolOnlyDictNull) {
  std::vector<bool> data = {true, false, true, false};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{false, false, false, true,
                               false, true,  false, true};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {};
  MakeTestEncodedArrayDict<velox::TypeKind::BOOLEAN>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryNullsMany) {
  auto structs =
    makeRowVector({makeArrayVector<int32_t>(
                    1000, [](velox::vector_size_t row) { return row % 5; },
                    [](velox::vector_size_t row, velox::vector_size_t idx) {
                      return row * 1000 + idx;
                    },
                    [](velox::vector_size_t row) { return row % 13 == 3; })},
                  [](velox::vector_size_t i) { return i == 0 || i == 3; });
  auto dictionary = velox::BaseVector::wrapInDictionary(
    makeNulls(1000, [](velox::vector_size_t row) { return row % 3 == 0; }),
    makeIndicesInReverse(1000), 1000, structs);
  auto data = makeArrayVector({0, 100, 200}, dictionary);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryEncodedArrayVarchar) {
  std::vector<velox::StringView> data = {
    "foo", "dflkgjfdlk;gf;lgjfsd;lgjfds;lgfdl;kg", "baaaaaaar", "b"};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {};
  MakeTestEncodedArrayDict<velox::TypeKind::VARCHAR>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryEncodedArrayVarcharNull) {
  std::vector<std::optional<velox::StringView>> data = {
    "foo", "dflkgjfdlk;gf;lgjfsd;lgjfds;lgfdl;kg", std::nullopt, "b"};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{false, false, false, true,
                               false, true,  false, true};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {1};
  MakeTestEncodedArrayDict<velox::TypeKind::VARCHAR>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryEncodedArrayVarcharOnlyDictNull) {
  std::vector<velox::StringView> data = {
    "foo", "dflkgjfdlk;gf;lgjfsd;lgjfds;lgfdl;kg", "baaaaaaar", "b"};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{false, false, false, true,
                               false, true,  false, true};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {};
  MakeTestEncodedArrayDict<velox::TypeKind::VARCHAR>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest,
       test_tableWriteDictionaryEncodedArrayVarcharOnlyDictAllNull) {
  std::vector<velox::StringView> data = {
    "foo", "dflkgjfdlk;gf;lgjfsd;lgjfds;lgfdl;kg", "baaaaaaar", "b"};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{true, true, true, true, true, true, true, true};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {};
  MakeTestEncodedArrayDict<velox::TypeKind::VARCHAR>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryEncodedArrayAllNullsVarchar) {
  std::vector<std::optional<velox::StringView>> data = {
    std::nullopt, std::nullopt, std::nullopt, std::nullopt};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{};
  std::vector<velox::vector_size_t> offsets = {0, 3, 5};
  std::vector<velox::vector_size_t> array_nulls = {};
  MakeTestEncodedArrayDict<velox::TypeKind::VARCHAR>(data, indices, dict_nulls,
                                                     offsets, array_nulls);
}

TEST_F(DataSinkTest, test_writeBiasTest) {
  {
    std::vector<std::optional<int64_t>> data{12553123123LL, 12553123124LL,
                                             12553123125LL, 12553123126LL};
    MakeTestWriteBias<velox::TypeKind::BIGINT>(data, {}, {});
  }
  {
    std::vector<std::optional<int32_t>> data{53123123, 53123124, 53123125,
                                             53123126};
    MakeTestWriteBias<velox::TypeKind::INTEGER>(data, {}, {});
  }
}

TEST_F(DataSinkTest, test_writeBiasDictTest) {
  {
    std::vector<std::optional<int64_t>> data{12553123123LL, 12553123124LL,
                                             12553123125LL, 12553123126LL};
    MakeTestWriteBias<velox::TypeKind::BIGINT>(data, {3, 2, 1, 0, 1, 1, 2, 3},
                                               {});
  }
  {
    std::vector<std::optional<int32_t>> data{53123123, 53123124, 53123125,
                                             53123126};
    MakeTestWriteBias<velox::TypeKind::INTEGER>(
      data, {3, 3, 3, 2, 1, 1, 0, 0},
      {true, false, true, false, false, false, true, true});
  }
}

TEST_F(DataSinkTest, test_writeScalarConstant) {
  MakeTestWriteConstantScalar<velox::TypeKind::INTEGER>(10, 10, {}, {});
  MakeTestWriteConstantScalar<velox::TypeKind::INTEGER>(
    10, 10, {3, 3, 3, 2, 1, 1, 0, 0},
    {true, false, true, false, false, false, true, true});
  MakeTestWriteConstantScalar<velox::TypeKind::BOOLEAN>(true, 10, {}, {});
  MakeTestWriteConstantScalar<velox::TypeKind::BOOLEAN>(
    false, 10, {3, 3, 3, 2, 1, 1, 0, 0},
    {true, false, true, false, false, false, true, true});
  MakeTestWriteConstantScalar<velox::TypeKind::VARCHAR>(
    velox::StringView{"333"}, 10, {}, {});
  MakeTestWriteConstantScalar<velox::TypeKind::VARCHAR>(
    velox::StringView{"222"}, 10, {3, 3, 3, 2, 1, 1, 0, 0},
    {true, false, true, false, false, false, true, true});
}

TEST_F(DataSinkTest, test_tableWriteScalarConstantStruct) {
  std::vector<std::string> fields{"foo", "bar", "bas", "baf"};
  auto type =
    velox::ROW(fields, {createScalarType(velox::TypeKind::INTEGER),
                        createScalarType(velox::TypeKind::BOOLEAN),
                        createScalarType(velox::TypeKind::VARCHAR),
                        createScalarType(velox::TypeKind::TIMESTAMP)});
  auto data =
    makeConstantRow(type,
                    velox::Variant::row(
                      {1, true, "test", velox::Timestamp::fromMillis(123123)}),
                    12);
  MakeTestWriteImpl(type, data);
}

TEST_F(DataSinkTest, test_tableWriteLazyInt) {
  std::vector<int32_t> data = {1, 2, 3, 5};
  MakeTestWriteColumnDictionary<velox::TypeKind::INTEGER,
                                velox::VectorEncoding::Simple::LAZY>(data, {},
                                                                     {});
}

TEST_F(DataSinkTest, test_tableWriteLazyIntNulls) {
  std::vector<std::optional<int32_t>> data = {1, 2, std::nullopt, 3, 5};
  MakeTestWriteColumnDictionary<velox::TypeKind::INTEGER,
                                velox::VectorEncoding::Simple::LAZY>(data, {},
                                                                     {});
}

TEST_F(DataSinkTest, test_tableWriteLazyDictionaryInt) {
  std::vector<int32_t> data = {1, 2, 3, 5};
  std::vector<velox::vector_size_t> indices{3, 2, 1, 0, 0, 1, 2, 3};
  std::vector<bool> dict_nulls{};
  MakeTestWriteColumnDictionary<velox::TypeKind::INTEGER,
                                velox::VectorEncoding::Simple::LAZY>(
    data, indices, dict_nulls);
}

TEST_F(DataSinkTest, test_tableWriteConstantStructInStruct) {
  auto sub_row2 = makeRowVector(
    {makeFlatVector<int32_t>({5556}), makeFlatVector<bool>({true})});
  auto sub_row = makeRowVector({sub_row2, makeFlatVector<int32_t>({12334})});
  auto row = makeRowVector({sub_row, makeFlatVector<int32_t>({123})});

  auto data = velox::BaseVector::wrapInConstant(100, 0, row);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteStructInStruct) {
  auto sub_row2 = makeRowVector(
    {makeFlatVector<velox::StringView>(
       {"", "Long stig", "very long not inlined string to check allocation",
        "sdsadd", "bbbbbb"}),
     makeFlatVector<bool>({true, false, true, false, true})});
  auto sub_row = makeRowVector(
    {sub_row2, makeFlatVector<int32_t>({12334, 123, 666, 0, -23232})});
  auto data =
    makeRowVector({sub_row, makeFlatVector<int32_t>({123, 0, 0, 0, 0})});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteStructInStructNull) {
  auto sub_row2 = makeRowVector(
    {makeNullableFlatVector<velox::StringView>(
       {"", "Long stig", "very long not inlined string to check allocation",
        std::nullopt, "bbbbbb"}),
     makeNullableFlatVector<bool>({true, false, std::nullopt, false, true})});
  auto sub_row = makeRowVector(
    {sub_row2,
     makeNullableFlatVector<int32_t>({12334, 123, std::nullopt, 0, -23232})});
  auto data =
    makeRowVector({sub_row, makeFlatVector<int32_t>({123, 0, 0, 0, 0})});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteArrayOfStructs) {
  auto structs = makeRowVector(
    {makeFlatVector<velox::StringView>(
       {"qqqqqqqq", "", "long long long long l kfjhkhskfljslfkjsadlfka", "a",
        "b", "c", "ddddddd", "dfddfdffd"}),
     makeFlatVector<bool>(
       {true, false, true, true, true, true, false, false, true})});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteArrayOfStructsNulls) {
  auto structs = makeRowVector(
    {makeNullableFlatVector<velox::StringView>(
       {"qqqqqqqq", "", "long long long long l kfjhkhskfljslfkjsadlfka", "a",
        "b", "c", std::nullopt, "ddddddd", "dfddfdffd"}),
     makeFlatVector<bool>(
       {true, false, true, true, true, true, true, false, false, true})},
    [](velox::vector_size_t i) { return i == 0 || i == 3; });
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteWholeArrayOfStructsNulls) {
  std::vector<std::string> fields{"foo"};
  std::vector<std::string> sub_fields{"sub_foo", "sub_bar"};
  auto data_array = makeArrayVector(
    {0},
    makeArrayVector(
      {0}, makeNullableFlatVector<velox::StringView>(
             {"qqqqqqqq", "", "long long long long l kfjhkhskfljslfkjsadlfka",
              "a", "b", "c", std::nullopt, "ddddddd", "dfddfdffd"})));
  auto data = makeRowVector({data_array, makeFlatVector<bool>({true})});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteArrayOfStructsDictionaryNulls) {
  std::vector<std::string> fields{"foo"};
  std::vector<std::string> sub_fields{"sub_foo", "sub_bar", "null_bool",
                                      "arr_bar", "null_arr_bar"};
  auto struct_type = velox::ROW(
    sub_fields, std::vector<velox::TypePtr>{
                  createScalarType(velox::TypeKind::VARCHAR),
                  createScalarType(velox::TypeKind::BOOLEAN),
                  createScalarType(velox::TypeKind::BOOLEAN),
                  velox::ARRAY(createScalarType(velox::TypeKind::INTEGER)),
                  velox::ARRAY(createScalarType(velox::TypeKind::INTEGER))});
  auto type = ARRAY(struct_type);
  auto structs = makeRowVector(
    {makeNullableFlatVector<velox::StringView>(
       {"qqqqqqqq", "", "long long long long l kfjhkhskfljslf", ".kjsadlfka",
        "a", "b", "c", std::nullopt, "ddddddd", "dfddfdffd"}),
     makeFlatVector<bool>(
       {true, false, true, true, true, true, true, false, false, true}),
     makeNullableFlatVector<bool>(
       {true, false, true, true, true, true, true, false, std::nullopt, true}),
     makeArrayVector<int32_t>(
       {{1}, {4}, {7}, {1}, {}, {7}, {1}, {4}, {7}, {1}}),
     makeNullableArrayVector<int32_t>(
       {{1}, {4}, {7}, {1}, {4}, {std::nullopt}, {1}, {}, {7}, {1}})},
    [](velox::vector_size_t i) { return i == 0 || i == 3; });
  auto dictionary = velox::BaseVector::wrapInDictionary(
    makeNulls({false, false, true, false, false, true, false, false, false,
               false, false, true, false, false, false, false}),
    makeIndices({7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6, 7, 8}), 16, structs);
  auto data = makeArrayVector({0, 3, 3, 6}, dictionary);
  MakeTestWriteImpl(type, data);
}

TEST_F(DataSinkTest, test_tableWriteMapVectorIntToInt) {
  auto keys = makeFlatVector<int32_t>({1, 2, 3, 4});
  auto values = makeFlatVector<int32_t>({10, 20, 0, 40});
  auto map_vector = makeMapVector<int32_t, int32_t>(
    {{{1, 10}, {2, 20}}, {{3, 5}}, {{4, 40}}, {}});
  MakeTestWriteImpl(map_vector->type(), map_vector);
}

TEST_F(DataSinkTest, test_tableWriteMapVectorIntToIntNullable) {
  auto keys = makeNullableFlatVector<int32_t>({1, std::nullopt, 3, 4});
  auto values = makeNullableFlatVector<int32_t>({10, 20, std::nullopt, 40});
  auto map_vector = makeMapVector<int32_t, int32_t>(
    {{{1, 10}, {2, 20}}, {{3, std::nullopt}}, {{4, 40}}, {}});
  MakeTestWriteImpl(map_vector->type(), map_vector);
}

TEST_F(DataSinkTest, test_tableWriteMapVectorIntToBoolNullable) {
  auto map_vector = makeMapVector<int32_t, bool>(
    {{{1, true}, {2, false}}, {{3, std::nullopt}}, {{4, false}}, {}});
  MakeTestWriteImpl(map_vector->type(), map_vector);
}

TEST_F(DataSinkTest, test_tableWriteMapVectorIntToStringNullable) {
  using S = velox::StringView;
  auto map_vector =
    makeMapVector<int32_t, S>({{{1, S{"red"}}, {2, std::nullopt}},
                               {{3, S{"green"}}},
                               {{4, S{"yellow"}}},
                               {}});
  MakeTestWriteImpl(map_vector->type(), map_vector);
}

TEST_F(DataSinkTest, test_tableWriteMapVectorIntToArrayNullable) {
  auto keys = makeNullableArrayVector<int32_t>(
    {{1, 2, std::nullopt}, {3}, {}, {123, 234}});
  auto values = makeNullableArrayVector<int32_t>(
    {{4, std::nullopt, 5}, {}, {6}, {345, 456}});
  auto map_vector = makeMapVector({0, 2, 3, 3}, keys, values, {4});
  MakeTestWriteImpl(map_vector->type(), map_vector);
}

TEST_F(DataSinkTest, test_tableWriteMapDictionaryNullable) {
  auto keys_raw = makeNullableArrayVector<int32_t>(
    {{1, 2, std::nullopt}, {3}, {}, {123, 234}});
  auto keys = velox::BaseVector::wrapInDictionary(
    makeNulls(
      {false, false, true, false, false, true, false, false, false, false}),
    makeIndices({0, 1, 2, 3, 3, 2, 1, 0, 1, 2}), 10, keys_raw);
  auto values_raw = makeNullableArrayVector<int32_t>(
    {{4, std::nullopt, 5}, {}, {6}, {345, 456}});
  auto values = velox::BaseVector::wrapInDictionary(
    makeNulls(
      {true, false, true, false, false, true, false, true, false, false}),
    makeIndices({0, 0, 0, 1, 1, 1, 2, 2, 2, 3}), 10, values_raw);
  auto map_vector_raw = makeMapVector({0, 2, 3, 3, 6}, keys, values, {4});
  auto map_vector = velox::BaseVector::wrapInDictionary(
    makeNulls({false, false, true, false, false, true}),
    makeIndices({0, 1, 2, 3, 4, 0}), 6, map_vector_raw);
  MakeTestWriteImpl(map_vector->type(), map_vector);
}

TEST_F(DataSinkTest, test_tableWriteMapDictionaryDictNulls) {
  auto keys_raw = makeArrayVector<int32_t>({{1, 2, 4}, {3}, {}, {123, 234}});
  auto keys = velox::BaseVector::wrapInDictionary(
    nullptr, makeIndices({0, 1, 2, 3, 3, 2, 1, 0, 1, 2}), 10, keys_raw);
  auto values_raw = makeArrayVector<int32_t>({{4, 5, 5}, {}, {6}, {345, 456}});
  auto values = velox::BaseVector::wrapInDictionary(
    nullptr, makeIndices({0, 0, 0, 1, 1, 1, 2, 2, 2, 3}), 10, values_raw);
  auto map_vector_raw = makeMapVector({0, 2, 3, 3, 6}, keys, values);
  auto map_vector = velox::BaseVector::wrapInDictionary(
    makeNulls({false, false, true, false, false, true}),
    makeIndices({0, 1, 2, 3, 4, 0}), 6, map_vector_raw);
  MakeTestWriteImpl(map_vector->type(), map_vector);
}

TEST_F(DataSinkTest, test_tableWriteMapStructDictionaryDictNulls) {
  auto keys_raw = makeArrayVector<int32_t>({{1, 2, 4}, {3}, {}, {123, 234}});
  auto keys = velox::BaseVector::wrapInDictionary(
    nullptr, makeIndices({0, 1, 2, 3, 3, 2, 1, 0, 1, 2}), 10, keys_raw);
  auto values_raw = makeRowVector(
    {"a", "b"}, {makeFlatVector<int32_t>({10, 20, 45, 40, 70, 80, 345}),
                 makeNullableFlatVector<bool>(
                   {true, false, true, true, true, true, false})});
  auto values = velox::BaseVector::wrapInDictionary(
    makeNulls(
      {false, false, true, false, false, true, false, false, true, false}),
    makeIndices({0, 0, 0, 1, 1, 1, 2, 2, 2, 3}), 10, values_raw);
  auto map_vector_raw = makeMapVector({0, 2, 3, 3, 6}, keys, values);
  auto map_vector = velox::BaseVector::wrapInDictionary(
    nullptr, makeIndices({0, 1, 2, 3, 4, 0}), 6, map_vector_raw);
  MakeTestWriteImpl(map_vector->type(), map_vector);
}

TEST_F(DataSinkTest, test_tableWriteMapArray) {
  auto keys = makeNullableArrayVector<int32_t>(
    {{1, 2, std::nullopt}, {3}, {}, {123, 234}});
  auto values = makeNullableArrayVector<int32_t>(
    {{4, std::nullopt, 5}, {}, {6}, {345, 456}});
  auto map_vector = makeMapVector({0, 2, 3, 3}, keys, values, {3});
  auto data = makeArrayVector({0}, map_vector);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteMapDictionaryArray) {
  auto keys = makeNullableArrayVector<int32_t>(
    {{1, 2, std::nullopt}, {3}, {}, {123, 234}});
  auto values = makeNullableArrayVector<int32_t>(
    {{4, std::nullopt, 5}, {}, {6}, {345, 456}});
  auto dictionary_map_vector = velox::BaseVector::wrapInDictionary(
    makeNulls({false, true, false, false, false, true, false}),
    makeIndices({3, 2, 1, 0, 1, 2, 3}), 7,
    makeMapVector({0, 2, 3, 3}, keys, values));
  auto data = makeArrayVector({0}, dictionary_map_vector);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteMapDictionaryArrayNulls) {
  auto keys = makeNullableArrayVector<int32_t>(
    {{1, 2, std::nullopt}, {3}, {}, {123, 234}});
  auto values = makeNullableArrayVector<int32_t>(
    {{4, std::nullopt, 5}, {}, {6}, {345, 456}});
  auto dictionary_map_vector = velox::BaseVector::wrapInDictionary(
    makeNulls({false, true, false, false, false, true, false}),
    makeIndices({3, 2, 1, 0, 1, 2, 3}), 7,
    makeRowVector({makeMapVector({0, 2, 3, 3}, keys, values, {2})}));
  auto data = makeArrayVector({0, 3, 3}, dictionary_map_vector);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteScalarConstantVector) {
  auto structs = makeRowVector(
    {makeConstant<velox::StringView>(std::nullopt, 8), makeConstant(true, 8)});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteScalarNullConstant) {
  auto data = makeConstant<int32_t>(std::nullopt, 8);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryScalarConstantVector) {
  auto structs = makeRowVector(
    {makeConstant<velox::StringView>(std::nullopt, 8),
     velox::BaseVector::wrapInDictionary(
       makeNulls({false, true, false, false, false, true, false, false}),
       makeIndices({3, 2, 1, 0, 1, 2, 3, 0}), 8, makeConstant(true, 4))});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConstantRowValueVector) {
  auto numbers = velox::BaseVector::wrapInDictionary(
    makeNulls({false, false, false}), makeIndices({0, 1, 2, 1}), 4,
    makeRowVector({makeFlatVector<int32_t>({10, 20, 4545})}));
  auto structs =
    makeRowVector({velox::BaseVector::wrapInConstant(8, 3, numbers)});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConstantRowValueVectorNulls) {
  auto numbers = velox::BaseVector::wrapInDictionary(
    makeNulls({false, true, false, true}), makeIndices({0, 1, 2, 1}), 4,
    makeRowVector({makeFlatVector<int32_t>({10, 20, 4545})}));
  auto structs =
    makeRowVector({velox::BaseVector::wrapInConstant(8, 3, numbers)});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConstantMapValueVector) {
  auto keys = makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9});
  auto values = makeFlatVector<int32_t>({11, 21, 4540, 0, 2, 2, 3, 5});
  auto maps = velox::BaseVector::wrapInDictionary(
    makeNulls({false, false, false, false}), makeIndices({0, 1, 2, 1}), 4,
    makeMapVector({0, 3, 3, 5}, keys, values));
  auto structs = makeRowVector({velox::BaseVector::wrapInConstant(8, 1, maps)});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConstantEmptyMapValueVector) {
  auto keys = makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9});
  auto values = makeFlatVector<int32_t>({11, 21, 4540, 0, 2, 2, 3, 5});
  auto maps = velox::BaseVector::wrapInDictionary(
    makeNulls({false, false, false, false}), makeIndices({0, 1, 2, 1}), 4,
    makeMapVector({0, 3, 3, 5}, keys, values));
  auto structs = makeRowVector({velox::BaseVector::wrapInConstant(8, 3, maps)});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConstantEmptyMapValueVectorNulls) {
  auto keys = makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9});
  auto values = makeFlatVector<int32_t>({11, 21, 4540, 0, 2, 2, 3, 5});
  auto maps = velox::BaseVector::wrapInDictionary(
    makeNulls({false, true, false, true}), makeIndices({0, 1, 2, 1}), 4,
    makeMapVector({0, 3, 3, 5}, keys, values));
  auto structs = makeRowVector({velox::BaseVector::wrapInConstant(8, 1, maps)});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConstantMapValueVectorNulls) {
  auto keys = makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9});
  auto values = makeFlatVector<int32_t>({11, 21, 4540, 0, 2, 2, 3, 5});
  auto maps = velox::BaseVector::wrapInDictionary(
    makeNulls({false, true, false, true}),
    makeIndices({0, 1, 2, 1, 1, 2, 2, 2}), 8,
    velox::BaseVector::wrapInConstant(
      8, 2, makeMapVector({0, 3, 3, 5}, keys, values)));
  auto structs = makeRowVector({maps});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteDictionaryConstantVector) {
  auto numbers = makeFlatVector<int32_t>({10, 20, 45, 40, 70, 80, 345});
  auto structs = makeRowVector(
    {makeConstant<velox::StringView>(std::nullopt, 8),
     velox::BaseVector::wrapInDictionary(
       makeNulls({false, true, false, false, false, true, false, false}),
       makeIndices({3, 2, 1, 0, 1, 2, 3, 0}), 8,
       velox::BaseVector::wrapInConstant(8, 3, numbers))});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConstantArrayValueVector) {
  auto elements = makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9});
  auto arrays = velox::BaseVector::wrapInDictionary(
    makeNulls({false, false, false, false}), makeIndices({0, 1, 2, 1}), 4,
    makeArrayVector({0, 3, 3, 5}, elements));
  auto structs =
    makeRowVector({velox::BaseVector::wrapInConstant(8, 1, arrays)});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConstantEmptyArrayValueVector) {
  auto elements = makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9});
  auto arrays = velox::BaseVector::wrapInDictionary(
    makeNulls({false, false, false, false}), makeIndices({0, 1, 2, 1}), 4,
    makeArrayVector({0, 3, 3, 5}, elements));
  auto structs =
    makeRowVector({velox::BaseVector::wrapInConstant(8, 3, arrays)});
  auto data = makeArrayVector({0, 3, 3, 6}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteLazyMapValueVector) {
  auto keys = makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9});
  auto values = makeFlatVector<int32_t>({11, 21, 4540, 0, 2, 2, 3, 5});
  auto maps =
    wrapInLazyDictionary(makeMapVector({0, 3, 3, 5, 7}, keys, values));
  auto structs = makeRowVector({maps});
  auto data = makeArrayVector({0, 3, 3, 4}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteLazyDictionaryMapValueVector) {
  auto keys = makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9});
  auto indices = makeIndices({7, 6, 5, 4, 3, 2, 1, 0});
  auto values = wrapInDictionary(
    indices, wrapInLazyDictionary(
               makeFlatVector<int32_t>({11, 21, 4540, 0, 2, 2, 3, 5})));
  auto maps =
    wrapInLazyDictionary(makeMapVector({0, 3, 3, 5, 7}, keys, values));
  auto structs = makeRowVector({maps});
  auto data = makeArrayVector({0, 3, 3, 4}, structs);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteLazyValue) {
  auto data = makeRowVector({wrapInLazyDictionary(
    makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9}))});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConstValue) {
  auto data = makeRowVector(
    {makeConstant<int32_t>(10, 7), makeConstant<int32_t>(std::nullopt, 7)});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConstRowValue) {
  auto data = makeRowVector({makeConstantRow(
    velox::ROW("foo", velox::createScalarType(velox::TypeKind::INTEGER)),
    velox::Variant::row({10}), 7)});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteBiasValue) {
  auto data =
    makeRowVector({vectorMaker_.biasVector<int32_t>({1, 5, 3, 8, 9})});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatUnknownColumn) {
  constexpr velox::vector_size_t kSize = 10;
  auto flat_vector =
    velox::BaseVector::create(velox::UNKNOWN(), kSize, pool_.get());
  for (auto i = 0; i < kSize; i++) {
    flat_vector->setNull(i, true);
  }
  MakeTestWriteImpl(flat_vector->type(), flat_vector);
}

TEST_F(DataSinkTest, test_tableWriteUnknownConstantColumn) {
  auto data =
    velox::BaseVector::createNullConstant(velox::UNKNOWN(), 10, pool_.get());
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteUnknownFlatArrayColumn) {
  constexpr velox::vector_size_t kSize = 10;
  auto flat_vector =
    velox::BaseVector::create(velox::UNKNOWN(), kSize, pool_.get());
  for (auto i = 0; i < kSize; i++) {
    flat_vector->setNull(i, true);
  }
  auto data = makeArrayVector({3, 6, 6, 8}, flat_vector);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteUnknownFlatArrayColumnNoExplicitNulls) {
  constexpr velox::vector_size_t kSize = 10;
  auto flat_vector =
    velox::BaseVector::create(velox::UNKNOWN(), kSize, pool_.get());
  auto data = makeArrayVector({3, 6, 6, 8}, flat_vector);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteUnknownArrayColumn) {
  auto data = makeArrayVector(
    {3, 6, 6, 8},
    velox::BaseVector::createNullConstant(velox::UNKNOWN(), 10, pool_.get()));
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteUnknownMapColumn) {
  auto data = makeMapVector(
    {3, 6, 6, 8},
    velox::BaseVector::createNullConstant(velox::UNKNOWN(), 10, pool_.get()),
    velox::BaseVector::createNullConstant(velox::UNKNOWN(), 10, pool_.get()));
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteUnknownRowColumn) {
  auto data = makeRowVector(
    {velox::BaseVector::createNullConstant(velox::UNKNOWN(), 10, pool_.get()),
     velox::BaseVector::createNullConstant(velox::UNKNOWN(), 10, pool_.get())});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteUnknownFlatRowColumn) {
  constexpr velox::vector_size_t kSize = 10;
  auto flat_vector =
    velox::BaseVector::create(velox::UNKNOWN(), kSize, pool_.get());
  for (auto i = 0; i < kSize; i++) {
    flat_vector->setNull(i, true);
  }
  auto data = makeRowVector({flat_vector, flat_vector});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteSequenceIntColumn) {
  auto data = vectorMaker_.sequenceVector<int32_t>(
    {10, 10, 10, 20, 20, 30, 40, std::nullopt, 10, 10});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteSequenceVarcharColumn) {
  auto data = vectorMaker_.sequenceVector<velox::StringView>(
    {"foo", "long not inlined value to check proper copying",
     "long not inlined value to check proper copying", "baz", "baz",
     std::nullopt, "qux"});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteSequenceBoolColumn) {
  auto data = vectorMaker_.sequenceVector<bool>(
    {true, true, true, std::nullopt, std::nullopt, false, false});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteSequenceArrayColumn) {
  auto elements = makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9});
  auto arrays = velox::BaseVector::wrapInDictionary(
    makeNulls({false, false, true, false}), makeIndices({0, 1, 2, 1}), 4,
    makeArrayVector({0, 3, 3, 5}, elements));
  auto length = velox::allocateSizes(4, pool_.get());
  auto* writer = length->asMutable<velox::vector_size_t>();
  writer[0] = 2;
  writer[1] = 3;
  writer[2] = 1;
  writer[3] = 4;
  auto data = std::make_shared<velox::SequenceVector<velox::ComplexType>>(
    pool_.get(), 4, arrays, length);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteSequenceMapColumn) {
  auto keys = makeFlatVector<int32_t>({10, 20, 4545, 5, 6, 7, 8, 9});
  auto values =
    makeFlatVector<int32_t>({100, 200, 300, 400, 500, 600, 700, 800});
  auto maps = makeMapVector({0, 3, 3, 5}, keys, values);
  auto length = velox::allocateSizes(4, pool_.get());
  auto* writer = length->asMutable<velox::vector_size_t>();
  writer[0] = 2;
  writer[1] = 3;
  writer[2] = 1;
  writer[3] = 4;
  auto data = std::make_shared<velox::SequenceVector<velox::ComplexType>>(
    pool_.get(), 4, maps, length);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteRowSequenceColumn) {
  std::vector<std::string> fields{"foo", "bar", "bas", "baf"};
  auto rows = makeRowVector(
    fields,
    {makeFlatVector<int32_t>({1, 2, 3, 4}),
     makeFlatVector<bool>({true, false, true, false}),
     makeFlatVector<velox::StringView>(
       {"", "a", "\0", "long lomng glgkfklgfklgf sdflkjsdlfk"}),
     makeFlatVector<velox::Timestamp>(
       {velox::Timestamp::fromMillis(123), velox::Timestamp::fromMillis(55555),
        velox::Timestamp::fromMillis(22222),
        velox::Timestamp::fromMillis(99999999999)})});
  auto length = velox::allocateSizes(4, pool_.get());
  auto* writer = length->asMutable<velox::vector_size_t>();
  writer[0] = 2;
  writer[1] = 3;
  writer[2] = 1;
  writer[3] = 4;
  auto data = std::make_shared<velox::SequenceVector<velox::ComplexType>>(
    pool_.get(), 9, rows, length);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteSequenceRowColumn) {
  std::vector<std::string> fields{"foo"};
  auto data = makeRowVector(
    fields, {vectorMaker_.sequenceVector<int32_t>(
              {10, 10, 10, 20, 20, 30, 40, std::nullopt, 10, 10})});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteScalarArraySequenceColumn) {
  auto data = makeArrayVector(
    {0, 3, 3, 4, 7},
    vectorMaker_.sequenceVector<int32_t>(
      {10, 10, 10, 10, 10, 20, 20, 30, 40, std::nullopt, 10, 10}));
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteRowArraySequenceColumn) {
  std::vector<std::string> fields{"foo", "bar", "bas", "baf"};
  auto rows = makeRowVector(
    fields,
    {makeFlatVector<int32_t>({1, 2, 3, 4, 5}),
     makeFlatVector<bool>({true, false, true, false, true}),
     makeFlatVector<velox::StringView>(
       {"", "a", "\0", "long lomng glgkfklgfklgf sdflkjsdlfk", "111"}),
     makeFlatVector<velox::Timestamp>(
       {velox::Timestamp::fromMillis(123), velox::Timestamp::fromMillis(113),
        velox::Timestamp::fromMillis(55555),
        velox::Timestamp::fromMillis(22222),
        velox::Timestamp::fromMillis(99999999999)})});
  auto length = velox::allocateSizes(5, pool_.get());
  auto* writer = length->asMutable<velox::vector_size_t>();
  writer[0] = 2;
  writer[1] = 3;
  writer[2] = 1;
  writer[3] = 4;
  writer[4] = 4;
  auto sequence_rows =
    std::make_shared<velox::SequenceVector<velox::ComplexType>>(pool_.get(), 14,
                                                                rows, length);
  auto data = makeArrayVector({0, 1, 1, 3}, sequence_rows);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteBiasedVector) {
  auto biased_data = vectorMaker_.biasVector<int32_t>(
    {5, 10, 15, 20, 25, 30, 35, 40, 45, 47, 48, 56, 66, 76, 86});
  auto data = makeArrayVector({0, 3, 3, 4, 7}, biased_data);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteBiasedVectorNull) {
  auto biased_data = vectorMaker_.biasVector<int32_t>(
    {std::nullopt, 10, 15, 20, 25, 30, 35, std::nullopt, 47, 48, 56, 66, 76,
     std::nullopt});
  auto data = makeArrayVector({0, 3, 3, 4, 7}, biased_data);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteWholeBiasedVectorNull) {
  auto biased_data = vectorMaker_.biasVector<int32_t>(
    {std::nullopt, 10, 15, 20, 25, 30, 35, std::nullopt, 47, 48, 56, 66, 76,
     std::nullopt});
  auto data = makeArrayVector({0}, biased_data);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteWholeBiasedVectorInDictionaryNull) {
  auto biased_data = velox::BaseVector::wrapInDictionary(
    makeNulls({false, false, true, false}), makeIndices({0, 1, 2, 1}), 4,
    (vectorMaker_.biasVector<int32_t>({10, 15, 20})));
  auto data = makeArrayVector({0}, biased_data);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatMapColumn) {
  auto data = vectorMaker_.flatMapVector<std::string, std::string>({
    {{"a", "1"}, {"b", "2"}},
    {{"a", std::nullopt}, {"b", std::nullopt}},
    {{"a", "This is a test"},
     {"b", "This is another test"},
     {"c", std::nullopt}},
    {{"b", "5"}, {"d", "last"}},
  });
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatMapIntColumn) {
  auto data = vectorMaker_.flatMapVector<int64_t, std::string>({
    {{5, "1"}, {7, "2"}},
    {{6, std::nullopt}, {7, std::nullopt}},
    {{7, "This is a test"}, {5, "This is another test"}, {6, std::nullopt}},
    {{5, "5"}, {7, "last"}},
  });
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatMapIntColumnNullable) {
  std::vector<
    std::optional<std::vector<std::pair<int64_t, std::optional<std::string>>>>>
    elements = {
      std::vector<std::pair<int64_t, std::optional<std::string>>>{{5, "1"},
                                                                  {7, "2"}},
      std::nullopt,
      std::vector<std::pair<int64_t, std::optional<std::string>>>{
        {7, "This is a test"}, {5, "This is another test"}, {6, std::nullopt}},
      std::vector<std::pair<int64_t, std::optional<std::string>>>{{5, "5"},
                                                                  {7, "last"}}};
  auto data =
    vectorMaker_.flatMapVectorNullable<int64_t, std::string>(elements);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatMapEmpty) {
  auto data = vectorMaker_.flatMapVector<std::string, std::string>({});
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatMapRow) {
  const std::vector<std::string> names = {"a", "b", "c"};
  auto value_structs_1 = makeRowVector(
    names,
    {makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt, 6, 7, 8}),
     makeNullableFlatVector<bool>(
       {std::nullopt, true, false, true, std::nullopt, false, false, false}),
     makeNullableFlatVector<velox::StringView>(
       {"foo", "true", "false", std::nullopt, std::nullopt, "bar", "bas",
        "boo bar bas long bar bas long long long"})});
  auto value_structs_2 = makeRowVector(
    names, {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8}),
            makeFlatVector<bool>(
              {false, false, false, true, true, false, false, false}),
            makeFlatVector<velox::StringView>(
              {"foo", "222", "11111", "3333", "3333", "bar", "bas",
               "boo bar bas long bar bas long long long"})});
  auto keys = makeArrayVector<velox::StringView>({{"key1", "key2"}, {}});
  std::vector<velox::BufferPtr> in_maps = {
    MakeInMap({true, false, true, true, false, true, false, true}), nullptr};

  auto type = velox::MAP(
    velox::ARRAY(velox::createScalarType(velox::TypeKind::VARCHAR)),
    velox::ROW(names, {velox::createScalarType(velox::TypeKind::INTEGER),
                       velox::createScalarType(velox::TypeKind::BOOLEAN),
                       velox::createScalarType(velox::TypeKind::VARCHAR)}));

  std::vector<velox::VectorPtr> map_values = {value_structs_1, value_structs_2};
  auto data = std::make_shared<velox::FlatMapVector>(
    pool_.get(), type, nullptr, 8, keys, map_values, std::move(in_maps));
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatMapRowArray) {
  const std::vector<std::string> names = {"a", "b", "c"};
  auto value_structs_1 = makeRowVector(
    names,
    {makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt, 6, 7, 8}),
     makeNullableFlatVector<bool>(
       {std::nullopt, true, false, true, std::nullopt, false, false, false}),
     makeNullableFlatVector<velox::StringView>(
       {"foo", "true", "false", std::nullopt, std::nullopt, "bar", "bas",
        "boo bar bas long bar bas long long long"})});
  auto value_structs_2 = makeRowVector(
    names, {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8}),
            makeFlatVector<bool>(
              {false, false, false, true, true, false, false, false}),
            makeFlatVector<velox::StringView>(
              {"foo", "222", "11111", "3333", "3333", "bar", "bas",
               "boo bar bas long bar bas long long long"})});
  auto keys = makeArrayVector<velox::StringView>({{"key1", "key2"}, {}});
  std::vector<velox::BufferPtr> in_maps = {
    MakeInMap({true, false, true, true, false, true, false, true}), nullptr};

  auto type = velox::MAP(
    velox::ARRAY(velox::createScalarType(velox::TypeKind::VARCHAR)),
    velox::ROW(names, {velox::createScalarType(velox::TypeKind::INTEGER),
                       velox::createScalarType(velox::TypeKind::BOOLEAN),
                       velox::createScalarType(velox::TypeKind::VARCHAR)}));

  std::vector<velox::VectorPtr> map_values = {value_structs_1, value_structs_2};
  auto elements = std::make_shared<velox::FlatMapVector>(
    pool_.get(), type, nullptr, 8, keys, map_values, std::move(in_maps));
  auto data = makeArrayVector({0, 4, 4, 6}, elements);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatMapRowWholeArray) {
  const std::vector<std::string> names = {"a", "b", "c"};
  auto value_structs_1 = makeRowVector(
    names,
    {makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt, 6, 7, 8}),
     makeNullableFlatVector<bool>(
       {std::nullopt, true, false, true, std::nullopt, false, false, false}),
     makeNullableFlatVector<velox::StringView>(
       {"foo", "true", "false", std::nullopt, std::nullopt, "bar", "bas",
        "boo bar bas long bar bas long long long"})});
  auto value_structs_2 = makeRowVector(
    names, {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8}),
            makeFlatVector<bool>(
              {false, false, false, true, true, false, false, false}),
            makeFlatVector<velox::StringView>(
              {"foo", "222", "11111", "3333", "3333", "bar", "bas",
               "boo bar bas long bar bas long long long"})});
  auto keys = makeArrayVector<velox::StringView>({{"key1", "key2"}, {}});
  std::vector<velox::BufferPtr> in_maps = {
    MakeInMap({true, false, true, true, false, true, false, true}), nullptr};

  auto type = velox::MAP(
    velox::ARRAY(velox::createScalarType(velox::TypeKind::VARCHAR)),
    velox::ROW(names, {velox::createScalarType(velox::TypeKind::INTEGER),
                       velox::createScalarType(velox::TypeKind::BOOLEAN),
                       velox::createScalarType(velox::TypeKind::VARCHAR)}));

  std::vector<velox::VectorPtr> map_values = {value_structs_1, value_structs_2};
  auto elements = std::make_shared<velox::FlatMapVector>(
    pool_.get(), type, nullptr, 8, keys, map_values, std::move(in_maps));
  auto data = makeArrayVector({0}, elements);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatMapRowArrayNotAllValuesUsed) {
  const std::vector<std::string> names = {"a"};
  auto value_structs_1 = makeRowVector(
    names,
    {makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt, 6, 7, 8})});
  auto value_structs_2 =
    makeRowVector(names, {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8})});
  std::vector<velox::BufferPtr> in_maps = {
    MakeInMap({false, false, false, false, false, true, false, true}), nullptr};

  auto type = velox::MAP(
    velox::ARRAY(velox::createScalarType(velox::TypeKind::VARCHAR)),
    velox::ROW(names, {velox::createScalarType(velox::TypeKind::INTEGER)}));
  auto keys = makeArrayVector<velox::StringView>({{"key1", "key2"}, {}});
  std::vector<velox::VectorPtr> map_values = {value_structs_1, value_structs_2};
  auto elements = std::make_shared<velox::FlatMapVector>(
    pool_.get(), type, nullptr, 8, keys, map_values, std::move(in_maps));
  auto data = makeArrayVector({0, 4, 4, 6}, elements);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatMapRowArrayNulls) {
  const std::vector<std::string> names = {"a"};
  auto value_structs_1 = makeRowVector(
    names,
    {makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt, 6, 7, 8})});
  auto value_structs_2 =
    makeRowVector(names, {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8})});
  std::vector<velox::BufferPtr> in_maps = {
    MakeInMap({false, false, false, false, false, true, false, true}), nullptr};

  auto type = velox::MAP(
    velox::ARRAY(velox::createScalarType(velox::TypeKind::VARCHAR)),
    velox::ROW(names, {velox::createScalarType(velox::TypeKind::INTEGER)}));
  auto keys = makeArrayVector<velox::StringView>({{"key1", "key2"}, {}});
  std::vector<velox::VectorPtr> map_values = {value_structs_1, value_structs_2};
  auto elements = std::make_shared<velox::FlatMapVector>(
    pool_.get(), type,
    makeNulls({false, false, false, false, false, true, false, true}), 8, keys,
    map_values, std::move(in_maps));
  auto data = makeArrayVector({0, 4, 4, 6}, elements);
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteFlatMapRowArrayDictionaryNulls) {
  const std::vector<std::string> names = {"a"};
  auto value_structs_1 = makeRowVector(
    names,
    {makeNullableFlatVector<int32_t>({1, 2, 3, 4, std::nullopt, 6, 7, 8})});
  auto value_structs_2 =
    makeRowVector(names, {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8})});
  std::vector<velox::BufferPtr> in_maps = {
    MakeInMap({false, false, false, false, false, true, false, true}), nullptr};

  auto type = velox::MAP(
    velox::ARRAY(velox::createScalarType(velox::TypeKind::VARCHAR)),
    velox::ROW(names, {velox::createScalarType(velox::TypeKind::INTEGER)}));
  auto keys = makeArrayVector<velox::StringView>({{"key1", "key2"}, {}});
  std::vector<velox::VectorPtr> map_values = {value_structs_1, value_structs_2};
  auto elements = std::make_shared<velox::FlatMapVector>(
    pool_.get(), type,
    makeNulls({false, false, false, false, false, true, false, true}), 8, keys,
    map_values, std::move(in_maps));
  auto data = makeArrayVector(
    {0, 4, 4, 6}, velox::BaseVector::wrapInDictionary(
                    makeNulls({true, true, true, true, true, false, false,
                               false, false, false}),
                    makeIndices({0, 1, 2, 3, 1, 2, 3, 1, 2, 3}), 10, elements));
  MakeTestWriteImpl(data->type(), data);
}

TEST_F(DataSinkTest, test_tableWriteConcurrent) {
  std::vector<std::string> names = {"id", "name"};
  auto data = makeRowVector(
    names, {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            makeFlatVector<velox::StringView>({"one", "two", "three", "four",
                                               "five", "six", "seven", "eight",
                                               "nine", "ten"})});
  auto data_non_conflict = makeRowVector(
    names,
    {makeFlatVector<int32_t>({11, 12, 13, 14}),
     makeFlatVector<velox::StringView>({"one", "two", "three", "four"})});
  auto data_conflict = makeRowVector(
    names, {makeFlatVector<int32_t>({15, 6, 7}),
            makeFlatVector<velox::StringView>({"one", "two", "three"})});
  const std::vector<velox::column_index_t> pk = {0};
  std::unique_ptr<rocksdb::Transaction> transaction;
  std::unique_ptr<rocksdb::Transaction> transaction_non_conflict;
  std::unique_ptr<rocksdb::Transaction> transaction_conflict;
  sdb::connector::primary_key::Keys written_row_keys{*pool_.get()};
  sdb::connector::primary_key::Keys written_row_keys2{*pool_.get()};
  sdb::connector::primary_key::Keys written_row_keys3{*pool_.get()};
  PrepareRocksDBWrite(data, kObjectKey, pk, transaction, written_row_keys);
  PrepareRocksDBWrite(data_non_conflict, kObjectKey, pk,
                      transaction_non_conflict, written_row_keys2);
  ASSERT_TRUE(transaction_non_conflict->Commit().ok());
  ASSERT_ANY_THROW(PrepareRocksDBWrite(
    data_conflict, kObjectKey, pk, transaction_conflict, written_row_keys3));
  // should be empty
  transaction_conflict->Commit();
  written_row_keys3.clear();
  ASSERT_TRUE(transaction->Commit().ok());
  rocksdb::ReadOptions read_options;
  size_t i = 0;
  auto column_key = sdb::connector::key_utils::PrepareColumnKey(kObjectKey, 1);
  const auto base_size = column_key.size();
  for (const std::string_view key : written_row_keys) {
    std::string value;
    column_key.resize(base_size);
    sdb::rocksutils::Append(column_key, key);
    ASSERT_TRUE(_db
                  ->Get(read_options, _cf_handles.front(),
                        rocksdb::Slice(column_key), &value)
                  .ok());
    ASSERT_EQ(
      value, data->childAt(1)->asFlatVector<velox::StringView>()->valueAt(i++));
  }
  i = 0;
  for (const std::string_view key : written_row_keys2) {
    std::string value;
    column_key.resize(base_size);
    sdb::rocksutils::Append(column_key, key);
    ASSERT_TRUE(_db
                  ->Get(read_options, _cf_handles.front(),
                        rocksdb::Slice(column_key), &value)
                  .ok());
    ASSERT_EQ(
      value,
      data_non_conflict->childAt(1)->asFlatVector<velox::StringView>()->valueAt(
        i++));
  }
  PrepareRocksDBWrite(data_conflict, kObjectKey, pk, transaction_conflict,
                      written_row_keys3);
  ASSERT_TRUE(transaction_conflict->Commit().ok());
  i = 0;
  for (const std::string_view key : written_row_keys3) {
    std::string value;
    column_key.resize(base_size);
    sdb::rocksutils::Append(column_key, key);
    ASSERT_TRUE(_db
                  ->Get(read_options, _cf_handles.front(),
                        rocksdb::Slice(column_key), &value)
                  .ok());
    ASSERT_EQ(
      value,
      data_conflict->childAt(1)->asFlatVector<velox::StringView>()->valueAt(
        i++));
  }
}

TEST_F(DataSinkTest, test_deleteDataSink) {
  std::vector<std::string> names = {"hero", "role", "skill_level"};
  std::vector<velox::TypePtr> types = {velox::VARCHAR(), velox::VARCHAR(),
                                       velox::INTEGER()};

  std::vector<velox::VectorPtr> data = {
    makeFlatVector<velox::StringView>({"pudge", "babsky", "vedernikoff"}),
    makeFlatVector<velox::StringView>({"mid", "support", "intern"}),
    makeFlatVector<int32_t>({9001, 42, 1})};

  sdb::ObjectId object_key;
  sdb::connector::primary_key::Keys written_row_keys{*pool_.get()};

  MakeRocksDBWrite(names, data, object_key, written_row_keys);

  rocksdb::ReadOptions read_options;
  std::unique_ptr<rocksdb::Iterator> it{
    _db->NewIterator(read_options, _cf_handles.front())};
  int count = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    count++;
  }
  ASSERT_GT(count, 0) << "Data should have been written";

  names.push_back("id");
  types.push_back(velox::INTEGER());

  std::vector<velox::VectorPtr> delete_data = {
    makeFlatVector<int32_t>({0, 1, 2})};
  auto row_data = makeRowVector({"id"}, delete_data);
  auto row_type = velox::ROW(names, types);

  rocksdb::TransactionOptions trx_opts;
  trx_opts.skip_concurrency_control = true;
  rocksdb::WriteOptions wo;
  std::unique_ptr<rocksdb::Transaction> transaction{
    _db->BeginTransaction(wo, trx_opts, nullptr)};
  ASSERT_NE(transaction, nullptr);

  sdb::connector::RocksDBDeleteDataSink delete_sink(
    *transaction, *_cf_handles.front(), row_type, object_key, {0, 1, 2, 3});

  delete_sink.appendData(row_data);
  ASSERT_TRUE(delete_sink.finish());

  auto close_result = delete_sink.close();
  ASSERT_TRUE(transaction->Commit().ok());

  std::unique_ptr<rocksdb::Iterator> it_after{
    _db->NewIterator(read_options, _cf_handles.front())};
  int count_after = 0;
  for (it_after->SeekToFirst(); it_after->Valid(); it_after->Next()) {
    count_after++;
  }
  ASSERT_EQ(count_after, 0) << "All data should have been deleted";
}

TEST_F(DataSinkTest, test_deleteDataSinkPartial) {
  const std::vector<sdb::catalog::Column::Id> column_ids = {0, 1, 2, 3};
  std::vector<std::string> names = {"hero", "role", "skill_level"};
  std::vector<velox::TypePtr> types = {velox::VARCHAR(), velox::VARCHAR(),
                                       velox::INTEGER()};

  std::vector<velox::VectorPtr> data = {
    makeFlatVector<velox::StringView>({"pudge", "anchin-chan", "vedernikoff"}),
    makeFlatVector<velox::StringView>({"tank", "support", "intern"}),
    makeFlatVector<int32_t>({9001, 42, 1})};

  sdb::ObjectId object_key;
  sdb::connector::primary_key::Keys written_row_keys{*pool_.get()};

  MakeRocksDBWrite(names, data, object_key, written_row_keys);

  rocksdb::ReadOptions read_options;
  std::unique_ptr<rocksdb::Iterator> it{
    _db->NewIterator(read_options, _cf_handles.front())};
  int count_initial = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    count_initial++;
  }
  ASSERT_EQ(count_initial, 12) << "Should have 3 rows x 4 columns = 12 keys";

  names.push_back("id");
  types.push_back(velox::INTEGER());

  std::vector<velox::VectorPtr> delete_data = {makeFlatVector<int32_t>({1, 2})};

  auto row_data = makeRowVector({"id"}, delete_data);
  auto row_type = velox::ROW(names, types);

  rocksdb::TransactionOptions trx_opts;
  trx_opts.skip_concurrency_control = true;
  trx_opts.lock_timeout = 100;
  rocksdb::WriteOptions wo;
  std::unique_ptr<rocksdb::Transaction> transaction{
    _db->BeginTransaction(wo, trx_opts, nullptr)};
  ASSERT_NE(transaction, nullptr);
  std::unique_ptr<rocksdb::Transaction> transaction2{
    _db->BeginTransaction(wo, trx_opts, nullptr)};
  ASSERT_NE(transaction2, nullptr);

  sdb::connector::RocksDBDeleteDataSink delete_sink(
    *transaction, *_cf_handles.front(), row_type, object_key, column_ids);

  delete_sink.appendData(row_data);
  ASSERT_TRUE(delete_sink.finish());

  // check for conflict
  {
    sdb::connector::RocksDBDeleteDataSink delete_sink2(
      *transaction2, *_cf_handles.front(), row_type, object_key, column_ids);
    ASSERT_ANY_THROW(delete_sink2.appendData(row_data));
    // should be empty
    ASSERT_TRUE(transaction2->Commit().ok());
    std::unique_ptr<rocksdb::Iterator> it_after{
      _db->NewIterator(read_options, _cf_handles.front())};
    int count_after = 0;
    for (it_after->SeekToFirst(); it_after->Valid(); it_after->Next()) {
      count_after++;
    }
    ASSERT_EQ(count_after, 12) << "Should still have all data";
  }

  ASSERT_TRUE(transaction->Commit().ok());

  std::unique_ptr<rocksdb::Iterator> it_after{
    _db->NewIterator(read_options, _cf_handles.front())};
  int count_after = 0;
  for (it_after->SeekToFirst(); it_after->Valid(); it_after->Next()) {
    count_after++;
  }
  ASSERT_EQ(count_after, 4)
    << "Should have 4 keys remaining (1 row x 4 columns)";
}

TEST_F(DataSinkTest, test_insertDeleteConflict) {
  const std::vector<sdb::catalog::Column::Id> column_ids = {0, 1};
  std::vector<std::string> names = {"id", "name"};
  auto row_type =
    velox::ROW(names, {velox::createScalarType(velox::TypeKind::INTEGER),
                       velox::createScalarType(velox::TypeKind::VARCHAR)});
  auto data = makeRowVector(
    names, {makeFlatVector<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9, 10}),
            makeFlatVector<velox::StringView>({"one", "two", "three", "four",
                                               "five", "six", "seven", "eight",
                                               "nine", "ten"})});

  const std::vector<velox::column_index_t> pk = {0};
  std::unique_ptr<rocksdb::Transaction> transaction;
  sdb::connector::primary_key::Keys written_row_keys{*pool_.get()};
  PrepareRocksDBWrite(data, kObjectKey, pk, transaction, written_row_keys);

  rocksdb::TransactionOptions trx_opts;
  trx_opts.skip_concurrency_control = true;
  trx_opts.lock_timeout = 100;
  rocksdb::WriteOptions wo;
  std::unique_ptr<rocksdb::Transaction> transaction_delete{
    _db->BeginTransaction(wo, trx_opts, nullptr)};
  sdb::connector::RocksDBDeleteDataSink delete_sink(
    *transaction_delete, *_cf_handles.front(), row_type, kObjectKey,
    column_ids);
  auto delete_data = makeRowVector({makeFlatVector<int32_t>({15, 6, 7, 10})});
  ASSERT_ANY_THROW(delete_sink.appendData(delete_data));
  // should be empty
  ASSERT_TRUE(transaction_delete->Commit().ok());
  ASSERT_TRUE(transaction->Commit().ok());
  // insert should be fully written
  rocksdb::ReadOptions read_options;
  size_t i = 0;
  auto column_key =
    sdb::connector::key_utils::PrepareColumnKey(kObjectKey, column_ids[1]);
  const auto base_size = column_key.size();
  for (const std::string_view key : written_row_keys) {
    column_key.resize(base_size);
    std::string value;
    sdb::rocksutils::Append(column_key, key);
    ASSERT_TRUE(_db
                  ->Get(read_options, _cf_handles.front(),
                        rocksdb::Slice(column_key), &value)
                  .ok());
    ASSERT_EQ(
      value, data->childAt(1)->asFlatVector<velox::StringView>()->valueAt(i++));
  }
}

}  // namespace
