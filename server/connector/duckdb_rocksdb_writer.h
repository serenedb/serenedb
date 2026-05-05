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

// Big-endian sorted PK encoding.
void AppendPKValue(std::string& key, const duckdb::UnifiedVectorFormat& fmt,
                   duckdb::idx_t row_idx, const duckdb::LogicalType& type);

class DuckDBColumnSerializer {
 public:
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

  // Empty row_keys[i] = skip row i.
  template<typename Writer>
  void WriteColumn(Writer& writer, const duckdb::Vector& vec,
                   const duckdb::LogicalType& type, duckdb::idx_t num_rows,
                   std::vector<std::string>& row_keys,
                   std::span<DuckDBSinkIndexWriter*> index_writers);

  void WriteSubVector(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                      duckdb::idx_t offset, duckdb::idx_t count,
                      const duckdb::LogicalType& type);

  template<typename T>
  void WriteSubVectorPrimitive(const duckdb::UnifiedVectorFormat& fmt,
                               duckdb::idx_t offset, duckdb::idx_t count);
  void WriteSubVectorBool(const duckdb::UnifiedVectorFormat& fmt,
                          duckdb::idx_t offset, duckdb::idx_t count);
  void WriteSubVectorVarchar(const duckdb::UnifiedVectorFormat& fmt,
                             duckdb::idx_t offset, duckdb::idx_t count);

  void WriteListValue(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                      duckdb::idx_t idx, const duckdb::LogicalType& type);
  void WriteListSubVector(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                          duckdb::idx_t offset, duckdb::idx_t count,
                          const duckdb::LogicalType& type);

  void WriteMapValue(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                     duckdb::idx_t idx, const duckdb::LogicalType& type);

  void WriteStructValue(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                        duckdb::idx_t idx, const duckdb::LogicalType& type);

  void WriteArrayValue(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                       duckdb::idx_t idx, const duckdb::LogicalType& type);

  // Asserts on nested types -- callers must use WriteComplexValue.
  void WriteScalarValue(const duckdb::UnifiedVectorFormat& fmt,
                        duckdb::idx_t row_idx, const duckdb::LogicalType& type);

  void WriteComplexValue(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                         duckdb::idx_t row_idx,
                         const duckdb::LogicalType& type);

  // value must live in stable memory -- not a stack temporary.
  template<typename T>
  void WritePrimitive(const T& value);

  template<typename T>
  void WriteScalarField(const duckdb::UnifiedVectorFormat& fmt,
                        duckdb::idx_t row_idx);

  void ResetForNewRow() noexcept;
  rocksdb::Slice Finalize(std::string& output) const;

 private:
  char* Allocate(size_t size);

  // Returns false (no slice emitted) when the validity mask is all-valid.
  bool WriteNullBitmap(const duckdb::UnifiedVectorFormat& fmt,
                       duckdb::idx_t offset, duckdb::idx_t count);

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
                          const duckdb::RecursiveUnifiedVectorFormat& rdata,
                          const duckdb::LogicalType& type,
                          duckdb::idx_t num_rows,
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
};

}  // namespace sdb::connector
