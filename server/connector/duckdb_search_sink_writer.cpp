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

#include "connector/duckdb_search_sink_writer.h"

#include <velox/type/Type.h>

#include "basics/assert.h"

namespace sdb::connector {

// DuckDB LogicalType → velox::TypeKind, then delegate to the existing
// SearchSinkInsertBaseImpl::SwitchColumnImpl which uses velox::Type.
// This avoids duplicating the complex SetupColumnWriter logic.
static velox::TypePtr DuckDBLogicalTypeToVeloxType(
  const duckdb::LogicalType& type) {
  switch (type.id()) {
    case duckdb::LogicalTypeId::BOOLEAN:
      return velox::BOOLEAN();
    case duckdb::LogicalTypeId::TINYINT:
      return velox::TINYINT();
    case duckdb::LogicalTypeId::SMALLINT:
      return velox::SMALLINT();
    case duckdb::LogicalTypeId::INTEGER:
      return velox::INTEGER();
    case duckdb::LogicalTypeId::BIGINT:
      return velox::BIGINT();
    case duckdb::LogicalTypeId::FLOAT:
      return velox::REAL();
    case duckdb::LogicalTypeId::DOUBLE:
      return velox::DOUBLE();
    case duckdb::LogicalTypeId::VARCHAR:
      return velox::VARCHAR();
    case duckdb::LogicalTypeId::BLOB:
      return velox::VARBINARY();
    case duckdb::LogicalTypeId::TIMESTAMP:
    case duckdb::LogicalTypeId::TIMESTAMP_TZ:
      return velox::TIMESTAMP();
    case duckdb::LogicalTypeId::DATE:
      return velox::DATE();
    case duckdb::LogicalTypeId::HUGEINT:
      return velox::HUGEINT();
    default:
      return velox::VARCHAR();
  }
}

bool DuckDBSearchSinkInsertWriter::SwitchColumn(
  const duckdb::LogicalType& type, bool have_nulls,
  catalog::Column::Id column_id) {
  auto velox_type = DuckDBLogicalTypeToVeloxType(type);
  return SwitchColumnImpl(*velox_type, have_nulls, column_id);
}

bool DuckDBSearchSinkUpdateWriter::SwitchColumn(
  const duckdb::LogicalType& type, bool have_nulls,
  catalog::Column::Id column_id) {
  auto velox_type = DuckDBLogicalTypeToVeloxType(type);
  return SwitchColumnImpl(*velox_type, have_nulls, column_id);
}

}  // namespace sdb::connector
