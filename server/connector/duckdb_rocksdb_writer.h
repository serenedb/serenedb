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

#include <duckdb.hpp>
#include <duckdb/common/arena_containers/arena_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/storage/arena_allocator.hpp>
#include <string>

#include "connector/common.h"
#include "connector/duckdb_sink_writer_base.h"
#include "rocksdb/slice.h"

namespace rocksdb {

class Transaction;
class ColumnFamilyHandle;
class SstFileWriter;

}  // namespace rocksdb
namespace sdb::connector {

// Append a PK column value to a key buffer (big-endian sorted encoding).
void AppendPKValueFromDuckDB(std::string& key, const duckdb::Vector& vec,
                             duckdb::idx_t idx,
                             const duckdb::LogicalType& type);

// Port of RocksDBDataSinkBase -- full column serialization for DuckDB.
// Two layers:
//   Layer 1 (per-column): WriteColumn dispatches by vector type + logical type.
//     For each row: SetupRowKey -> serialize value -> WriteRowSlices (RocksDB
//     Put).
//   Layer 2 (sub-vector): WriteSubVector serializes elements of complex types
//     into _row_slices (zero-copy where possible).
class DuckDBColumnSerializer {
 public:
  // Data writer -- wraps RocksDB Put. Concrete types passed as template param
  // to WriteColumn (same as old DataWriterype template on RocksDBDataSinkBase).
  struct TxnWriter {
    rocksdb::Transaction* txn;
    rocksdb::ColumnFamilyHandle* cf;

    void Write(const std::vector<rocksdb::Slice>& slices,
               std::string_view key) const;
    void WriteNull(std::string_view key) const;
  };

  struct SstWriter {
    rocksdb::SstFileWriter* writer;

    void Write(const std::vector<rocksdb::Slice>& slices,
               std::string_view key) const;
    void WriteNull(std::string_view key) const;
  };

  explicit DuckDBColumnSerializer(duckdb::Allocator& allocator);

  // --- Layer 1: Per-column write (one RocksDB Put per row) ---
  // Templated on Writer (TxnWriter or SstWriter) -- inlined, no virtual
  // dispatch.

  // row_keys with empty string = skipped row (same as old SetupRowKey->nullptr)
  template<typename Writer>
  void WriteColumn(Writer& writer, const duckdb::Vector& vec,
                   const duckdb::LogicalType& type, duckdb::idx_t num_rows,
                   std::vector<std::string>& row_keys,
                   std::span<DuckDBSinkIndexWriter*> index_writers);

  // --- Layer 2: Sub-vector serialization (into _row_slices) ---

  void WriteSubVector(const duckdb::Vector& vec, duckdb::idx_t offset,
                      duckdb::idx_t count, const duckdb::LogicalType& type);

  template<typename T>
  void WriteFlatSubVector(const duckdb::Vector& vec, duckdb::idx_t offset,
                          duckdb::idx_t count);

  void WriteFlatSubVectorVarchar(const duckdb::Vector& vec,
                                 duckdb::idx_t offset, duckdb::idx_t count);
  void WriteFlatSubVectorBool(const duckdb::Vector& vec, duckdb::idx_t offset,
                              duckdb::idx_t count);

  void WriteListValue(const duckdb::Vector& vec, duckdb::idx_t idx,
                      const duckdb::LogicalType& type);
  void WriteListSubVector(const duckdb::Vector& vec, duckdb::idx_t offset,
                          duckdb::idx_t count, const duckdb::LogicalType& type);

  void WriteMapValue(const duckdb::Vector& vec, duckdb::idx_t idx,
                     const duckdb::LogicalType& type);
  void WriteStructValue(const duckdb::Vector& vec, duckdb::idx_t idx,
                        const duckdb::LogicalType& type);
  void WriteArrayValue(const duckdb::Vector& vec, duckdb::idx_t idx,
                       const duckdb::LogicalType& type);

  // Write a single value at idx without sub-vector header.
  // Port of WriteValue (data_sink.cpp:2000). Used by WriteStructValue.
  void WriteSingleValue(const duckdb::Vector& vec, duckdb::idx_t idx,
                        const duckdb::LogicalType& type);

  void WriteConstantSubVector(const duckdb::Vector& vec, duckdb::idx_t count,
                              const duckdb::LogicalType& type);
  void WriteDictionarySubVector(const duckdb::Vector& vec, duckdb::idx_t offset,
                                duckdb::idx_t count,
                                const duckdb::LogicalType& type);

  // IMPORTANT: value must be in stable memory (vector buffer, ConstantVector
  // data, or arena). NOT a stack temporary.
  template<typename T>
  void WritePrimitive(const T& value);

  // GEOMETRY write path: raw WKB bytes, no kStringPrefix disambiguation --
  // empty value means NULL (valid WKB is at least 5 bytes: byte-order + type).
  // value must be in stable memory (vector buffer, arena, or ConstantVector
  // data). NOT a stack temporary.
  void WriteGeometryRaw(const duckdb::string_t& value);

  // Per-element write for WriteSingleValue (struct fields, map keys/values).
  // For FLAT_VECTOR: zero-copy pointer into the vector's stable heap buffer.
  // For non-flat (dict, constant): copies to arena, since GetVectorValue<T>
  // returns a temporary that would otherwise dangle in _row_slices.
  // Not used for BOOLEAN (WritePrimitive<bool> is safe with temporaries)
  // or VARCHAR/BLOB (handled separately in WriteSingleValue).
  template<typename T>
  void WriteScalarField(const duckdb::Vector& vec, duckdb::idx_t idx);

  void ResetForNewRow() noexcept;
  rocksdb::Slice Finalize(std::string& output) const;

 private:
  char* Allocate(size_t size);
  bool WriteNullBitmap(const duckdb::ValidityMask& validity,
                       duckdb::idx_t offset, duckdb::idx_t count);
  bool WriteDictNullBitmap(const duckdb::UnifiedVectorFormat& vdata,
                           duckdb::idx_t offset, duckdb::idx_t count);

  template<typename T>
  void WriteDictionaryFixedSubVector(const duckdb::UnifiedVectorFormat& vdata,
                                     duckdb::idx_t offset, duckdb::idx_t count);
  void WriteDictionarySubVectorBool(const duckdb::UnifiedVectorFormat& vdata,
                                    duckdb::idx_t offset, duckdb::idx_t count);
  void WriteDictionarySubVectorVarchar(const duckdb::UnifiedVectorFormat& vdata,
                                       duckdb::idx_t offset,
                                       duckdb::idx_t count);
  void WriteDictionaryComplexSubVector(const duckdb::Vector& vec,
                                       duckdb::idx_t offset,
                                       duckdb::idx_t count,
                                       const duckdb::LogicalType& type);

  // Layer 1 helpers -- templated on Writer
  template<typename Writer, typename T>
  void WriteFlatColumn(Writer& writer, const duckdb::Vector& vec,
                       duckdb::idx_t num_rows,
                       std::vector<std::string>& row_keys,
                       std::span<DuckDBSinkIndexWriter*> index_writers);

  template<typename Writer>
  void WriteConstantColumn(Writer& writer, const duckdb::Vector& vec,
                           const duckdb::LogicalType& type,
                           duckdb::idx_t num_rows,
                           std::vector<std::string>& row_keys,
                           std::span<DuckDBSinkIndexWriter*> index_writers);

  template<typename Writer>
  void WriteUnifiedColumn(Writer& writer,
                          const duckdb::UnifiedVectorFormat& vdata,
                          const duckdb::Vector& vec,
                          const duckdb::LogicalType& type,
                          duckdb::idx_t num_rows,
                          std::vector<std::string>& row_keys,
                          std::span<DuckDBSinkIndexWriter*> index_writers);

  template<typename Writer>
  void WriteArrayColumn(Writer& writer, const duckdb::Vector& vec,
                        const duckdb::LogicalType& type, duckdb::idx_t num_rows,
                        std::vector<std::string>& row_keys,
                        std::span<DuckDBSinkIndexWriter*> index_writers);

  template<typename Writer>
  void WriteComplexColumn(Writer& writer, const duckdb::Vector& vec,
                          const duckdb::LogicalType& type,
                          duckdb::idx_t num_rows,
                          std::vector<std::string>& row_keys,
                          std::span<DuckDBSinkIndexWriter*> index_writers);

  template<typename Writer>
  void WriteRowSlices(Writer& writer, std::string_view key,
                      std::span<DuckDBSinkIndexWriter*> index_writers);
  duckdb::ArenaAllocator _arena;
  std::vector<rocksdb::Slice> _row_slices;
  // Temporary vectors whose buffers are referenced by slices in _row_slices.
  // WriteDictionarySubVector builds a local flat vector, and
  // WriteFlatSubVector<T> stores zero-copy slices into its StandardVectorBuffer
  // (heap-allocated, stable address). Moving the vector here keeps the buffer
  // alive until ResetForNewRow drops the last shared_ptr reference after
  // WriteRowSlices.
  std::vector<duckdb::Vector> _temp_vectors;
};

}  // namespace sdb::connector
