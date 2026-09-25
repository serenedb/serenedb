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

#include <duckdb/planner/operator/logical_extension_operator.hpp>
#include <string>

namespace sdb::connector::ai {

class LogicalAIEvaluate final : public duckdb::LogicalExtensionOperator {
 public:
  LogicalAIEvaluate(
    duckdb::TableIndex table_index,
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> calls);

  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() final;

  duckdb::vector<duckdb::TableIndex> GetTableIndex() const final;

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext& context,
    duckdb::PhysicalPlanGenerator& planner) final;

  void Serialize(duckdb::Serializer& serializer) const final;

  bool SupportSerialization() const final { return false; }

  std::string GetName() const final;

 protected:
  void ResolveTypes() final;

 private:
  duckdb::TableIndex _table_index;
};

}  // namespace sdb::connector::ai
