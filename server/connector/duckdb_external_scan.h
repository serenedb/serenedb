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

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/unique_ptr.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <memory>

#include "catalog/table.h"
#include "connector/duckdb_table_function.h"

namespace sdb::connector {

// Bind data for the SereneDB external-table scan wrapper. Inherits from
// SereneDBScanBindData so that the iresearch_plan / rocksdb_plan
// optimizers can dynamic_cast into it and mutate `scan_source` when a
// PHRASE / ANN / equality predicate is found on the index entry. Also
// owns the underlying reader's TableFunction + FunctionData; when the
// scan is NOT rewritten (e.g. bare SELECT from the index entry), the
// wrapper's delegating callbacks execute the underlying parquet scan
// end-to-end.
struct ExternalScanBindData final : public SereneDBScanBindData {
  duckdb::TableFunction underlying;
  duckdb::unique_ptr<duckdb::FunctionData> underlying_bind_data;
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<std::string> names;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const override;
  bool Equals(const duckdb::FunctionData& other) const override;
};

// Builds a TableFunction that reads the external (file-backed) data for
// `sdb_table` and produces rows in the table's declared column order. The
// returned function is a SereneDB-branded wrapper whose callbacks delegate
// to DuckDB's built-in reader (parquet / csv / json) but whose
// get_bind_info returns `table_entry`, so CREATE INDEX / DELETE / UPDATE
// binders can resolve the base table.
//
// To obtain the parquet file_row_number at scan time, add
// `MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER` to the
// LogicalGet's column_ids -- the underlying reader exposes it as a
// virtual column.
//
// Throws duckdb::CatalogException if the reader for the requested format is
// not available, or a parser/binder exception if the reader's bind fails.
duckdb::TableFunction MakeExternalScanFunction(
  duckdb::ClientContext& context, std::shared_ptr<catalog::Table> sdb_table,
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table_entry,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data);

// Builds a standalone lookup-mode TableFunction matching the file at `path`'s
// extension (parquet -> MakeParquetLookupTableFunction, csv ->
// MakeCSVLookupTableFunction, json -> MakeJSONLookupTableFunction). The
// returned TableFunction shares MultiFileBindData shape with read_parquet /
// read_csv / read_json, so the caller passes a pre-bound bind_data via
// TableFunctionInput::bind_data when invoking its callbacks (no separate bind
// step).
//
// Throws duckdb::CatalogException for unsupported file extensions.
duckdb::TableFunction MakeExternalLookupTableFunction(std::string_view path);

}  // namespace sdb::connector
