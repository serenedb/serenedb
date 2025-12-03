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
#include <velox/buffer/Buffer.h>
#include <velox/common/memory/MemoryPool.h>
#include <velox/type/Type.h>
#include <velox/vector/BaseVector.h>
#include <velox/vector/FlatVector.h>

#include <type_traits>

#include "basics/down_cast.h"
#include "catalog/object.h"
#include "catalog/virtual_table.h"
#include "pg/information_schema/fwd.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {

template<typename T>
velox::TypePtr GetFieldType();

template<typename Field>
auto GetField(const Field& field) {
  if constexpr (std::is_enum_v<Field>) {
    return GetField(std::to_underlying(field));
  } else if constexpr (std::is_same_v<Field, std::string_view>) {
    return velox::StringView{field};
  } else if constexpr (std::is_same_v<Field, irs::bytes_view>) {
    return velox::StringView{irs::ViewCast<char>(field)};
  } else if constexpr (IsArray<Field>::value) {
    using OutputType =
      decltype(GetField(std::declval<typename Field::value_type>()));
    if constexpr (std::is_same_v<OutputType, velox::UnknownValue>) {
      return velox::UnknownValue{};
    } else {
      return field;
    }
  } else if constexpr (std::is_same_v<Field, char>) {
    return velox::StringView{&field, 1};
  } else if constexpr (std::is_same_v<Field, bool>) {
    return field;
  } else if constexpr (std::is_same_v<Field, int8_t>) {
    return static_cast<int8_t>(field);
  } else if constexpr (std::is_same_v<Field, int16_t>) {
    return static_cast<int16_t>(field);
  } else if constexpr (std::is_same_v<Field, int32_t>) {
    return static_cast<int32_t>(field);
  } else if constexpr (std::is_same_v<Field, int64_t> ||
                       std::is_same_v<Field, uint64_t>) {
    return static_cast<int64_t>(field);
  } else {
    return velox::UnknownValue{};
  }
}

template<typename Field>
velox::TypePtr GetFieldType() {
  using OutputType = decltype(GetField(std::declval<Field>()));
  if constexpr (std::is_same_v<Field, Bytea>) {
    // Since Bytea is mapped to velox::StringView, we need to explicitly
    // return VARBINARY type here to avoid confusion with VARCHAR.
    return velox::VARBINARY();
  } else if constexpr (IsArray<Field>::value) {
    return velox::ARRAY(GetFieldType<typename Field::value_type>());
  } else {
    return velox::CppToType<OutputType>::create();
  }
}

template<typename Field>
velox::VectorPtr CreateColumn(velox::vector_size_t size,
                              velox::memory::MemoryPool* pool) {
  auto type = GetFieldType<Field>();
  using OutputType = decltype(GetField(std::declval<Field>()));
  static_assert(!std::is_reference_v<OutputType>);
  static_assert(!std::is_const_v<OutputType>);
  velox::VectorPtr column;
  if constexpr (IsArray<OutputType>::value) {
    column = std::make_shared<velox::ArrayVector>(
      pool, type, nullptr, size,
      velox::AlignedBuffer::allocate<velox::vector_size_t>(size, pool),
      velox::AlignedBuffer::allocate<velox::vector_size_t>(size, pool),
      velox::BaseVector::create<
        velox::FlatVector<typename OutputType::value_type>>(type->childAt(0),
                                                            size, pool));
  } else {
    column = velox::BaseVector::create<velox::FlatVector<OutputType>>(
      type, size, pool);
  }
  return column;
}

template<typename T>
void WriteData(std::vector<velox::VectorPtr>& out, const T& value,
               uint64_t nulls, velox::vector_size_t row,
               velox::memory::MemoryPool* pool) {
  uint32_t column = 0;
  boost::pfr::for_each_field(value, [&]<typename Field>(const Field& field) {
    using OutputType = decltype(GetField(field));
    static_assert(!std::is_reference_v<OutputType>);
    static_assert(!std::is_const_v<OutputType>);
    auto vector = out[column];
    SDB_ASSERT(vector);
    if (nulls & (uint64_t{1} << column)) {
      vector->setNull(row, true);
    } else if constexpr (IsArray<OutputType>::value) {
      using ElementType =
        decltype(GetField(std::declval<typename OutputType::value_type>()));
      auto& array_vector = basics::downCast<velox::ArrayVector>(*vector);
      size_t offset = row == 0 ? 0
                               : array_vector.sizeAt(row - 1) +
                                   array_vector.offsetAt(row - 1);
      size_t size = field.size();
      array_vector.setOffsetAndSize(row, offset, size);
      auto elements =
        array_vector.elements()->as<velox::FlatVector<ElementType>>();
      for (size_t i = 0; i < size; ++i) {
        elements->set(offset + i, GetField(field[i]));
      }
    } else {
      auto& flat_vector =
        basics::downCast<velox::FlatVector<OutputType>>(*vector);
      flat_vector.set(row, GetField(field));
    }
    ++column;
  });
}

template<typename T>
class SystemTable;

template<typename T>
class SystemTableSnapshot final : public catalog::VritualTableSnapshot {
 public:
  explicit SystemTableSnapshot(const catalog::VirtualTable& table,
                               ObjectId database)
    : VritualTableSnapshot{{},
                           database,
                           table.Id(),
                           std::string{table.Name()},
                           catalog::ObjectType::Virtual} {
    _table = &table;
  }

  velox::RowTypePtr RowType() const noexcept final {
    return basics::downCast<SystemTable<T>>(*_table).RowType();
  }

  velox::RowVectorPtr GetData(std::vector<std::string> names,
                              velox::memory::MemoryPool& pool) final {
    if (auto data = _data.lock()) {
      SDB_ASSERT(data->pool() == &pool);
      SDB_ASSERT(names != data->rowType()->names());
      return MakeData(std::move(names), data->children(), pool);
    }
    auto children = GetTableData(pool);
    auto data = MakeData(std::move(names), std::move(children), pool);
    _data = data;
    return data;
  }

  std::vector<velox::VectorPtr> GetTableData(velox::memory::MemoryPool& pool) {
    return std::vector<velox::VectorPtr>(boost::pfr::tuple_size_v<T>);
  }

 private:
  velox::RowVectorPtr MakeData(std::vector<std::string> names,
                               std::vector<velox::VectorPtr> children,
                               velox::memory::MemoryPool& pool) {
    const auto rows =
      !children.empty() && children[0] ? children[0]->size() : 0;
    auto row_type = velox::ROW(std::move(names), RowType()->children());

    return std::make_shared<velox::RowVector>(
      &pool, row_type, velox::BufferPtr{}, rows, std::move(children));
  }

  std::weak_ptr<velox::RowVector> _data;

  void WriteInternal(vpack::Builder& build) const final {}

  void WriteProperties(vpack::Builder& build) const final {}
};

template<typename T>
class SystemTable : public catalog::VirtualTable {
 public:
  constexpr SystemTable() {
    _id = ObjectId{T::kId};
    _name = T::kName;
  }

  std::shared_ptr<catalog::VritualTableSnapshot> CreateSnapshot(
    ObjectId database) const final {
    return std::make_shared<SystemTableSnapshot<T>>(*this, database);
  }

  velox::RowTypePtr RowType() const noexcept final {
    static const velox::RowTypePtr kRowType = [] {
      std::vector<std::string> names;
      std::vector<velox::TypePtr> types;
      names.reserve(boost::pfr::tuple_size_v<T>);
      types.reserve(boost::pfr::tuple_size_v<T>);
      boost::pfr::for_each_field_with_name(
        T{}, [&]<typename Field>(std::string_view name, const Field& field) {
          names.emplace_back(name);
          types.emplace_back(GetFieldType<Field>());
        });
      return velox::ROW(std::move(names), std::move(types));
    }();
    return kRowType;
  }
};

}  // namespace sdb::pg
