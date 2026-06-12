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
#include <span>
#include <string>

#include "connector/common.h"
#include "connector/duckdb_sink_writer_base.h"
#include "rocksdb/slice.h"

namespace rocksdb {

class Transaction;
class ColumnFamilyHandle;
class SstFileWriter;

}  // namespace rocksdb
namespace sdb::query {

class Transaction;

}  // namespace sdb::query
namespace sdb::connector {

// Big-endian sorted PK encoding.
void AppendPKValue(std::string& key, const duckdb::UnifiedVectorFormat& fmt,
                   duckdb::idx_t row_idx, const duckdb::LogicalType& type);

class DuckDBColumnSerializer {
 public:
  // Writes column cells into a rocksdb transaction.
  class TxnWriter {
   public:
    TxnWriter(query::Transaction& sdb_txn,
              rocksdb::ColumnFamilyHandle* cf) noexcept;

    void SwitchColumn(const ColumnDescriptor& col) noexcept { _cur_col = col; }

    void Write(const std::vector<rocksdb::Slice>& slices, std::string_view key);
    void WriteNull(std::string_view key);

   private:
    rocksdb::Transaction* _txn;
    rocksdb::ColumnFamilyHandle* _cf;
    ColumnDescriptor _cur_col{};
  };

  // No-op writer for paths that only want the per-row slice pipeline to
  // light up the inverted-index sink (e.g. indexed-expression eval). The
  // serializer's WriteColumn template duck-types this; SwitchColumn /
  // Write / WriteNull are all silent.
  class NoopWriter {
   public:
    void SwitchColumn(const ColumnDescriptor&) noexcept {}
    void Write(std::span<const rocksdb::Slice>, std::string_view) noexcept {}
    void WriteNull(std::string_view) noexcept {}
  };

  // Writes column cells into an SST file.
  class SstWriter {
   public:
    explicit SstWriter(rocksdb::SstFileWriter* writer) noexcept
      : _writer{writer} {}

    void SwitchColumn(const ColumnDescriptor& col) noexcept { _cur_col = col; }

    void Write(const std::vector<rocksdb::Slice>& slices, std::string_view key);
    void WriteNull(std::string_view key);

   private:
    rocksdb::SstFileWriter* _writer;
    ColumnDescriptor _cur_col{};
  };

  explicit DuckDBColumnSerializer(duckdb::Allocator& allocator);

  // Empty row_keys[i] = skip row i. `col` describes the catalog column being
  // serialized; stashed on the serializer so the per-row helpers
  // (WriteRowSlices, WriteConstantColumn, ...) can branch on column-level
  // attributes without threading the descriptor through every recursive call.
  // `col.type` is the column's logical type used for the dispatch.
  template<typename Writer>
  void WriteColumn(Writer& writer, const duckdb::Vector& vec,
                   duckdb::idx_t num_rows, std::vector<std::string>& row_keys,
                   std::span<DuckDBSinkColumnWriter*> index_writers,
                   ColumnDescriptor col);

  size_t WriteSubVector(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                        duckdb::idx_t offset, duckdb::idx_t count,
                        const duckdb::LogicalType& type);

  template<typename T>
  size_t WriteSubVectorPrimitive(const duckdb::UnifiedVectorFormat& fmt,
                                 duckdb::idx_t offset, duckdb::idx_t count);
  size_t WriteSubVectorBool(const duckdb::UnifiedVectorFormat& fmt,
                            duckdb::idx_t offset, duckdb::idx_t count);
  size_t WriteSubVectorVarchar(const duckdb::UnifiedVectorFormat& fmt,
                               duckdb::idx_t offset, duckdb::idx_t count);

  // Per-value/sub-vector writers return the total bytes appended to
  // `_row_slices` (header + payload). Nested callers (WriteStructValue,
  // WriteListValue) propagate this without a slice-summing scan.
  size_t WriteListValue(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                        duckdb::idx_t idx, const duckdb::LogicalType& type);
  size_t WriteListSubVector(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                            duckdb::idx_t offset, duckdb::idx_t count,
                            const duckdb::LogicalType& type);

  size_t WriteMapValue(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                       duckdb::idx_t idx, const duckdb::LogicalType& type);

  size_t WriteStructValue(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                          duckdb::idx_t idx, const duckdb::LogicalType& type);
  size_t WriteStructSubVector(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                              duckdb::idx_t offset, duckdb::idx_t count,
                              const duckdb::LogicalType& type);

  size_t WriteArrayValue(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                         duckdb::idx_t idx, const duckdb::LogicalType& type);

  // Asserts on nested types -- callers must use WriteComplexValue.
  size_t WriteScalarValue(const duckdb::UnifiedVectorFormat& fmt,
                          duckdb::idx_t row_idx,
                          const duckdb::LogicalType& type);

  size_t WriteComplexValue(const duckdb::RecursiveUnifiedVectorFormat& rdata,
                           duckdb::idx_t row_idx,
                           const duckdb::LogicalType& type);

  // value must live in stable memory -- not a stack temporary.
  // Returns the number of bytes appended to `_row_slices`.
  template<typename T>
  size_t WritePrimitive(const T& value);

  // GEOMETRY write path: raw WKB bytes, no kStringPrefix disambiguation --
  // empty value means NULL (valid WKB is at least 5 bytes: byte-order + type).
  // value must be in stable memory (vector buffer, arena, or ConstantVector
  // data). NOT a stack temporary.
  size_t WriteGeometryRaw(const duckdb::string_t& value);

  template<typename T>
  size_t WriteScalarField(const duckdb::UnifiedVectorFormat& fmt,
                          duckdb::idx_t row_idx);

  void ResetForNewRow() noexcept;
  rocksdb::Slice Finalize(std::string& output) const;

 private:
  char* Allocate(size_t size);

  // Returns the bitmap size in bytes, or 0 if the validity mask is all-valid
  // (in which case nothing is emitted).
  size_t WriteNullBitmap(const duckdb::UnifiedVectorFormat& fmt,
                         duckdb::idx_t offset, duckdb::idx_t count);

  template<typename Writer, typename T>
  void WriteFlatColumn(Writer& writer, const duckdb::Vector& vec,
                       duckdb::idx_t num_rows,
                       std::vector<std::string>& row_keys,
                       std::span<DuckDBSinkColumnWriter*> index_writers);

  template<typename Writer>
  void WriteConstantColumn(Writer& writer, const duckdb::Vector& vec,
                           const duckdb::LogicalType& type,
                           duckdb::idx_t num_rows,
                           std::vector<std::string>& row_keys,
                           std::span<DuckDBSinkColumnWriter*> index_writers);

  template<typename Writer>
  void WriteUnifiedColumn(Writer& writer,
                          const duckdb::RecursiveUnifiedVectorFormat& rdata,
                          const duckdb::LogicalType& type,
                          duckdb::idx_t num_rows,
                          std::vector<std::string>& row_keys,
                          std::span<DuckDBSinkColumnWriter*> index_writers);

  template<typename Writer>
  void WriteComplexColumn(Writer& writer, const duckdb::Vector& vec,
                          const duckdb::LogicalType& type,
                          duckdb::idx_t num_rows,
                          std::vector<std::string>& row_keys,
                          std::span<DuckDBSinkColumnWriter*> index_writers);

  template<typename Writer>
  void WriteRowSlices(Writer& writer, std::string_view key,
                      std::span<DuckDBSinkColumnWriter*> index_writers);
  duckdb::ArenaAllocator _arena;
  std::vector<rocksdb::Slice> _row_slices;
};

}  // namespace sdb::connector
