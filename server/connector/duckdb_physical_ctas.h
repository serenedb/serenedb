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

#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>

#include "connector/duckdb_physical_sst_insert.h"

namespace sdb::connector {

// Physical operator for CREATE TABLE AS SELECT on SereneDB tables.
// Inherits SST bulk insert logic from SereneDBPhysicalSSTInsert.
// Adds: table creation with tombstone in GetGlobalSinkState,
//       tombstone removal in Finalize, rollback in destructor.
class SereneDBPhysicalCTAS : public SereneDBPhysicalSSTInsert {
 public:
  SereneDBPhysicalCTAS(duckdb::PhysicalPlan& plan,
                       duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
                       duckdb::SchemaCatalogEntry& schema,
                       duckdb::idx_t estimated_cardinality);

  duckdb::unique_ptr<duckdb::GlobalSinkState> GetGlobalSinkState(
    duckdb::ClientContext& context) const override;
  duckdb::SinkFinalizeType Finalize(
    duckdb::Pipeline& pipeline, duckdb::Event& event,
    duckdb::ClientContext& context,
    duckdb::OperatorSinkFinalizeInput& input) const override;

 private:
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> _info;
  duckdb::SchemaCatalogEntry& _schema;
};

}  // namespace sdb::connector
