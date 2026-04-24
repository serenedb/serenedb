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

#include <absl/strings/match.h>

#include "connector/duckdb_external_scan.h"
#include "connector/duckdb_table_function.h"
#include "connector/file_materializer.h"
#include "connector/rocksdb_row_materializer.h"

namespace sdb::connector {

std::unique_ptr<RowMaterializer> MakeRowMaterializer(
  duckdb::ClientContext& context, const SereneDBScanBindData& bind_data,
  const rocksdb::Snapshot* snapshot,
  std::span<const duckdb::idx_t> projected_columns,
  std::span<const duckdb::LogicalType> projected_types,
  std::span<const catalog::Column::Id> bind_column_ids,
  rocksdb::Transaction* txn) {
  if (bind_data.table && bind_data.table->GetTableType() == TableType::File) {
    // All supported external readers (parquet, csv, json) expose
    // `file_row_number` via their multi-file reader; the same materializer
    // works for all of them. The value is an int64 PK:
    //  - parquet: row index in the file
    //  - csv / json: byte offset of the row/record start in the file
    return std::make_unique<FileMaterializer>(context, bind_data.table,
                                              projected_columns,
                                              projected_types, bind_column_ids);
  }
  return std::make_unique<RocksDBRowMaterializer>(
    bind_data.table ? bind_data.table->GetId() : ObjectId{}, snapshot,
    projected_columns, projected_types, bind_column_ids, txn);
}

}  // namespace sdb::connector
