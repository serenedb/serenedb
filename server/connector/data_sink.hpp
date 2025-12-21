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

#include <velox/common/memory/HashStringAllocator.h>
#include <velox/common/memory/MemoryPool.h>
#include <velox/connectors/Connector.h>
#include <velox/vector/ConstantVector.h>
#include <velox/vector/VectorStream.h>
#include <velox/vector/VectorTypeUtils.h>

#include <vector>

#include "catalog/identifiers/object_id.h"
#include "catalog/table_options.h"
#include "primary_key.hpp"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/write_batch.h"

namespace sdb::connector {

class RocksDBDataSink : public velox::connector::DataSink {
 public:
  RocksDBDataSink(rocksdb::Transaction& transaction,
                  rocksdb::ColumnFamilyHandle& cf,
                  const velox::RowTypePtr& row_type,
                  velox::memory::MemoryPool& memory_pool, ObjectId object_key,
                  std::span<const velox::column_index_t> key_childs,
                  std::vector<catalog::Column::Id> column_ids,
                  bool skip_primary_key_columns = false);

  void appendData(velox::RowVectorPtr input) final;
  bool finish() final;
  std::vector<std::string> close() final;
  void abort() final;
  Stats stats() const final;

 private:
  // VERTICAL encoding methods
  void WriteColumn(const velox::VectorPtr& input,
                   const folly::Range<const velox::IndexRange*>& ranges,
                   std::span<const velox::vector_size_t> idx);

  template<velox::TypeKind Kind>
  void WriteFlatColumn(const velox::BaseVector& input,
                       const folly::Range<const velox::IndexRange*>& ranges,
                       std::span<const velox::vector_size_t> idx);

  template<velox::TypeKind Kind>
  void WriteBiasedColumn(const velox::BaseVector& input,
                         const folly::Range<const velox::IndexRange*>& ranges,
                         std::span<const velox::vector_size_t> idx);

  // TODO(Dronplane)
  // Here and below some methods accept VectorPtr as
  // BaseVector utility methods requre const VectorPtr mostly for Lazy
  // vector operations. Can we eventually get rid of this and have consistent
  // vector argument type?
  void WriteDictionaryColumn(
    const velox::VectorPtr& input,
    const folly::Range<const velox::IndexRange*>& ranges,
    std::span<const velox::vector_size_t> idx);

  template<velox::TypeKind Kind>
  void WriteConstantColumn(const velox::BaseVector& input,
                           const folly::Range<const velox::IndexRange*>& ranges,
                           std::span<const velox::vector_size_t> idx);

  template<velox::VectorEncoding::Simple Encoding>
  void WriteComplexColumn(const velox::BaseVector& input,
                          const folly::Range<const velox::IndexRange*>& ranges,
                          std::span<const velox::vector_size_t> idx);

  // HORIZONTAL encoding methods
  void WriteVector(const velox::VectorPtr& input,
                   const folly::Range<const velox::IndexRange*>& ranges,
                   rocksdb::Slice wrapper_nulls, bool force_nulls);
  template<bool HaveNulls>
  void WriteRowVector(const velox::BaseVector& input,
                      const folly::Range<const velox::IndexRange*>& ranges,
                      rocksdb::Slice wrapper_nulls, bool force_nulls);
  template<bool HaveNulls>
  void WriteArrayVector(const velox::BaseVector& input,
                        const folly::Range<const velox::IndexRange*>& ranges,
                        rocksdb::Slice wrapper_nulls, bool force_nulls);
  template<bool HaveNulls>
  void WriteMapVector(const velox::BaseVector& input,
                      const folly::Range<const velox::IndexRange*>& ranges,
                      rocksdb::Slice wrapper_nulls, bool force_nulls);
  template<bool HaveNulls>
  void WriteFlatMapVector(const velox::BaseVector& input,
                          const folly::Range<const velox::IndexRange*>& ranges,
                          rocksdb::Slice wrapper_nulls, bool force_nulls);
  template<bool HaveNulls>
  void WriteDictionaryVector(
    const velox::VectorPtr& input,
    const folly::Range<const velox::IndexRange*>& ranges,
    rocksdb::Slice wrapper_nulls);
  template<bool HaveNulls, velox::TypeKind Kind>
  void WriteFlatVector(const velox::BaseVector& input,
                       const folly::Range<const velox::IndexRange*>& ranges,
                       rocksdb::Slice wrapper_nulls, bool force_nulls);
  template<bool ForceNulls, velox::TypeKind Kind>
  void WriteConstantVector(const velox::BaseVector& input,
                           const folly::Range<const velox::IndexRange*>& ranges,
                           rocksdb::Slice wrapper_nulls);
  template<bool HaveNulls, velox::TypeKind Kind>
  void WriteBiasedVector(const velox::BaseVector& input,
                         const folly::Range<const velox::IndexRange*>& ranges,
                         rocksdb::Slice wrapper_nulls, bool force_nulls);

  void WriteValue(const velox::VectorPtr& input, velox::vector_size_t idx);

  template<velox::TypeKind Kind>
  void WriteBiasedValue(const velox::BaseVector& input,
                        velox::vector_size_t idx);

  template<velox::TypeKind Kind>
  void WriteConstantValue(const velox::BaseVector& input);

  template<velox::TypeKind Kind>
  void WriteFlatValueWrapper(const velox::BaseVector& input,
                             velox::vector_size_t idx);

  template<typename T>
  void WriteFlatValue(const velox::FlatVector<T>& input,
                      velox::vector_size_t idx);

  void WriteRowValue(const velox::BaseVector& input, velox::vector_size_t idx);

  void WriteMapValue(const velox::BaseVector& input, velox::vector_size_t idx);
  void WriteFlatMapValue(const velox::BaseVector& input,
                         velox::vector_size_t idx);

  void WriteArrayValue(const velox::BaseVector& input,
                       velox::vector_size_t idx);

  template<typename T>
  void WritePrimitive(const T& value);

  void WriteRowSlices(std::string_view key);

  const std::string& SetupRowKey(
    velox::vector_size_t idx,
    std::span<const velox::vector_size_t> original_idx);

  void ResetForNewRow() noexcept;

  void GatherNulls(const velox::BaseVector& input,
                   const folly::Range<const velox::IndexRange*>& ranges,
                   velox::vector_size_t total_rows_number, bool whole_vector,
                   rocksdb::Slice wrapper_nulls, bool force_nulls);

  // TODO(Dronplane) make this shared somewhere
  template<typename T>
  using ManagedVector = std::vector<T, velox::memory::StlAllocator<T>>;

  using SliceVector = ManagedVector<rocksdb::Slice>;
  using IndiciesVector = ManagedVector<velox::vector_size_t>;

  IndiciesVector GatherIndicies(
    const folly::Range<const velox::IndexRange*>& ranges,
    velox::vector_size_t total_rows_number);

  velox::RowTypePtr _row_type;
  rocksdb::Transaction& _transaction;
  rocksdb::ColumnFamilyHandle& _cf;
  ObjectId _object_key;
  std::vector<velox::column_index_t> _key_childs;
  std::vector<catalog::Column::Id> _column_ids;
  velox::memory::MemoryPool& _memory_pool;
  SliceVector _row_slices;
  primary_key::Keys _keys_buffers;
  velox::HashStringAllocator _bytes_allocator;
  catalog::Column::Id _column_id;
  bool _skip_primary_key_columns;
};

class RocksDBDeleteDataSink : public velox::connector::DataSink {
 public:
  RocksDBDeleteDataSink(rocksdb::Transaction& transaction,
                        rocksdb::ColumnFamilyHandle& cf,
                        velox::RowTypePtr row_type, ObjectId object_key,
                        std::vector<catalog::Column::Id> column_ids);

  void appendData(velox::RowVectorPtr input) final;
  bool finish() final;
  std::vector<std::string> close() final;
  void abort() final;
  Stats stats() const final;

 private:
  // we should store original type as data passed to appendData
  // contains only primary key columns but we need remove all.
  velox::RowTypePtr _row_type;
  rocksdb::Transaction& _transaction;
  rocksdb::ColumnFamilyHandle& _cf;
  ObjectId _object_key;
  std::vector<catalog::Column::Id> _column_ids;
  std::vector<velox::column_index_t> _key_childs;
};

}  // namespace sdb::connector
