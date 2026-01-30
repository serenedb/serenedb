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
#include "common.h"
#include "primary_key.hpp"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/write_batch.h"
#include "rocksdb_sink_writer.hpp"
#include "sink_writer_base.hpp"
#include "sst_sink_writer.hpp"

namespace sdb::connector {

template<typename DataWriterType, typename SubWriterType>
class RocksDBDataSinkBase : public velox::connector::DataSink {
 protected:
  RocksDBDataSinkBase(
    DataWriterType data_writer, velox::memory::MemoryPool& memory_pool,
    ObjectId object_key, std::span<const velox::column_index_t> key_childs,
    std::vector<catalog::Column::Id> column_ids,
    std::vector<std::unique_ptr<SubWriterType>>&& index_writers);

 public:
  bool finish() override;
  std::vector<std::string> close() final;
  void abort() override;
  Stats stats() const final;

 protected:
  void WriteInputColumn(catalog::Column::Id column_id, velox::vector_size_t idx,
                        velox::RowVector& vector,
                        const folly::Range<const velox::IndexRange*>& range);

  template<bool SkipPrimaryKeyColumns>
  void WriteColumns(const velox::RowVectorPtr& input,
                    folly::Range<const velox::IndexRange*> ranges,
                    std::span<const velox::vector_size_t> original_idx);

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
  void WriteNull(std::string_view key);

  const std::string& SetupRowKey(
    velox::vector_size_t idx,
    std::span<const velox::vector_size_t> original_idx);

  void ResetForNewRow() noexcept;

  void GatherNulls(const velox::BaseVector& input,
                   const folly::Range<const velox::IndexRange*>& ranges,
                   velox::vector_size_t total_rows_number, bool whole_vector,
                   rocksdb::Slice wrapper_nulls, bool force_nulls);

  using SliceVector = ManagedVector<rocksdb::Slice>;
  using IndiciesVector = ManagedVector<velox::vector_size_t>;

  IndiciesVector GatherIndicies(
    const folly::Range<const velox::IndexRange*>& ranges,
    velox::vector_size_t total_rows_number);

  DataWriterType _data_writer;
  std::vector<std::unique_ptr<SubWriterType>> _index_writers;
  ObjectId _object_key;
  std::vector<velox::column_index_t> _key_childs;
  std::vector<catalog::Column::Id> _column_ids;
  velox::memory::MemoryPool& _memory_pool;
  SliceVector _row_slices;
  primary_key::Keys _store_keys_buffers;
  velox::HashStringAllocator _bytes_allocator;
  catalog::Column::Id _column_id;
};

class RocksDBInsertDataSink final
  : public RocksDBDataSinkBase<RocksDBSinkWriter, SinkInsertWriter> {
 public:
  RocksDBInsertDataSink(
    rocksdb::Transaction& transaction, rocksdb::ColumnFamilyHandle& cf,
    velox::memory::MemoryPool& memory_pool, ObjectId object_key,
    std::span<const velox::column_index_t> key_childs,
    std::vector<catalog::Column::Id> column_ids,
    std::vector<std::unique_ptr<SinkInsertWriter>>&& index_writers);

  void appendData(velox::RowVectorPtr input) final;
};

class RocksDBUpdateDataSink final
  : public RocksDBDataSinkBase<RocksDBSinkWriter, SinkUpdateWriter> {
 public:
  RocksDBUpdateDataSink(
    rocksdb::Transaction& transaction, rocksdb::ColumnFamilyHandle& cf,
    velox::memory::MemoryPool& memory_pool, ObjectId object_key,
    std::span<const velox::column_index_t> key_childs,
    std::vector<catalog::Column::Id> column_ids,
    std::vector<catalog::Column::Id> all_column_ids, bool update_pk,
    velox::RowTypePtr table_row_type,
    std::vector<std::unique_ptr<SinkUpdateWriter>>&& index_writers);

  void appendData(velox::RowVectorPtr input) final;

 private:
  template<bool RewriteData>
  void RewriteColumn(rocksdb::Iterator& it, catalog::Column::Id column_id,
                     const primary_key::Keys& old_keys,
                     primary_key::Keys& new_keys);

  bool IsUpdatedColumn(catalog::Column::Id column_id) const {
    SDB_ASSERT(_update_pk || !_index_writers.empty(),
               "Used only when updating PK or indexes");
    auto it = _column_id_to_input_idx.find(column_id);
    if (it == _column_id_to_input_idx.end()) {
      // Not in input
      return false;
    }

    // First '_key_childs' children are old PK
    return it->second >= _key_childs.size();
  }

  std::vector<catalog::Column::Id> _all_column_ids;
  std::vector<velox::column_index_t> _updated_key_childs;
  primary_key::Keys _old_keys_buffers;
  containers::FlatHashMap<catalog::Column::Id, size_t> _column_id_to_input_idx;
  containers::FlatHashMap<catalog::Column::Id, velox::TypeKind>
    _column_id_to_kind;
  bool _update_pk{};
};

class SSTInsertDataSink final
  : public RocksDBDataSinkBase<SSTSinkWriter, SinkInsertWriter> {
 public:
  SSTInsertDataSink(rocksdb::DB& db, rocksdb::ColumnFamilyHandle& cf,
                    velox::memory::MemoryPool& memory_pool, ObjectId object_key,
                    std::span<const velox::column_index_t> key_childs,
                    std::vector<catalog::Column::Id> column_oids,
                    std::string_view rocksdb_directory);

  void appendData(velox::RowVectorPtr input) final;

  bool finish() final {
    _data_writer.Finish();
    return true;
  }

  void abort() final {
    _data_writer.Abort();
    RocksDBDataSinkBase<SSTSinkWriter, SinkInsertWriter>::abort();
  }
};

class RocksDBDeleteDataSink : public velox::connector::DataSink {
 public:
  RocksDBDeleteDataSink(
    rocksdb::Transaction& transaction, rocksdb::ColumnFamilyHandle& cf,
    velox::RowTypePtr row_type, ObjectId object_key,
    std::vector<catalog::Column::Id> column_ids,
    std::vector<std::unique_ptr<SinkDeleteWriter>>&& index_writers);

  void appendData(velox::RowVectorPtr input) final;
  bool finish() final;
  std::vector<std::string> close() final;
  void abort() final;
  Stats stats() const final;

 private:
  // we should store original type as data passed to appendData
  // contains only primary key columns but we need remove all.
  velox::RowTypePtr _row_type;
  RocksDBSinkWriter _data_writer;
  std::vector<std::unique_ptr<SinkDeleteWriter>> _index_writers;
  ObjectId _object_key;
  std::vector<catalog::Column::Id> _column_ids;
  std::vector<velox::column_index_t> _key_childs;
};

}  // namespace sdb::connector
