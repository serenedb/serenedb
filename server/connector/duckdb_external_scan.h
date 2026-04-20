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

#include <duckdb/common/unique_ptr.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <memory>

#include "catalog/table.h"

namespace sdb::connector {

// Builds a TableFunction that reads the external (file-backed) data for
// `sdb_table` and produces rows in the table's declared column order. The
// caller must publish the returned bind_data alongside the function.
//
// Internally resolves DuckDB's built-in reader for the table's format
// (parquet / csv / text / json) and binds it with the declared path and the
// user-supplied format options.
//
// Throws duckdb::CatalogException if the reader for the requested format is
// not available, or a parser/binder exception if the reader's bind fails.
duckdb::TableFunction MakeExternalScanFunction(
  duckdb::ClientContext& context, std::shared_ptr<catalog::Table> sdb_table,
  duckdb::unique_ptr<duckdb::FunctionData>& bind_data);

}  // namespace sdb::connector
