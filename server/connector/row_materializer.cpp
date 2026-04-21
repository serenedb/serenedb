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

#include "connector/row_materializer.h"

#include "connector/counter_row_materializer.h"
#include "connector/duckdb_external_scan.h"
#include "connector/duckdb_table_function.h"
#include "connector/parquet_row_materializer.h"
#include "connector/rocksdb_row_materializer.h"

namespace sdb::connector {

std::unique_ptr<RowMaterializer> MakeRowMaterializer(
  duckdb::ClientContext& context, const SereneDBScanBindData& bind_data,
  const rocksdb::Snapshot* snapshot, std::span<const std::string> all_pks,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids) {
  if (bind_data.table && bind_data.table->GetTableType() == TableType::File) {
    if (IsParquetExternalTable(*bind_data.table)) {
      return std::make_unique<ParquetRowMaterializer>(
        context, bind_data.table, all_pks, projected_columns, projected_types,
        bind_column_ids);
    }
    // CSV / JSON (and anything else without a native row_number virtual
    // column): fall back to counter + re-scan.
    // TODO: instead make a separate TextMaterializer and make PK of CSV =
    // offset into file
    return std::make_unique<CounterRowMaterializer>(
      context, bind_data.table, projected_columns, projected_types,
      bind_column_ids);
  }
  return std::make_unique<RocksDBRowMaterializer>(
    bind_data.table ? bind_data.table->GetId() : ObjectId{}, snapshot,
    projected_columns, projected_types, bind_column_ids);
}

std::string_view RowMaterializerName(const SereneDBScanBindData& bind_data) {
  if (bind_data.table && bind_data.table->GetTableType() == TableType::File) {
    return IsParquetExternalTable(*bind_data.table)
             ? "parquet (file_row_number)"
             : "counter (re-scan)";
  }
  return "rocksdb (point Get)";
}

}  // namespace sdb::connector
