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
#include <memory>

#include "connector/segment_rewrite/segment_rewrite_job.h"

namespace sdb::connector {

class LogicalSegmentRewrite final : public duckdb::LogicalExtensionOperator {
 public:
  LogicalSegmentRewrite(duckdb::idx_t bind_index,
                        std::shared_ptr<SegmentRewriteJob> job);

  duckdb::PhysicalOperator& CreatePlan(
    duckdb::ClientContext& context,
    duckdb::PhysicalPlanGenerator& planner) final;
  duckdb::vector<duckdb::ColumnBinding> GetColumnBindings() final;
  duckdb::vector<duckdb::TableIndex> GetTableIndex() const final;
  duckdb::string GetName() const final;
  duckdb::InsertionOrderPreservingMap<duckdb::string> ParamsToString()
    const final;
  bool RequireOptimizer() const final { return false; }
  bool SupportSerialization() const final { return false; }

 protected:
  void ResolveTypes() final;

 private:
  duckdb::idx_t _bind_index;
  std::shared_ptr<SegmentRewriteJob> _job;
};

}  // namespace sdb::connector
