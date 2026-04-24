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

#include "basics/assert.h"

namespace sdb::connector {

bool DuckDBSearchSinkInsertWriter::SwitchColumn(const duckdb::LogicalType& type,
                                                bool have_nulls,
                                                catalog::Column::Id column_id) {
  return SwitchColumnImpl(type, have_nulls, column_id);
}

bool DuckDBSearchSinkUpdateWriter::SwitchColumn(const duckdb::LogicalType& type,
                                                bool have_nulls,
                                                catalog::Column::Id column_id) {
  return SwitchColumnImpl(type, have_nulls, column_id);
}

}  // namespace sdb::connector
