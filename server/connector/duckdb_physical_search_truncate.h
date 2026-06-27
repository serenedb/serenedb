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
#include <duckdb/execution/physical_operator.hpp>
#include <memory>

#include "catalog/table.h"

namespace sdb::connector {

class SereneDBSearchTruncate final : public duckdb::PhysicalOperator {
 public:
  SereneDBSearchTruncate(duckdb::PhysicalPlan& plan,
                         std::shared_ptr<catalog::Table> table,
                         duckdb::idx_t estimated_cardinality);

  bool IsSource() const final { return true; }
  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const final;

 private:
  std::shared_ptr<catalog::Table> _table;
};

}  // namespace sdb::connector
