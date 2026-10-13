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

#include <cstddef>
#include <iresearch/index/merge/merge_plan.hpp>

namespace duckdb {

class DatabaseInstance;
class TaskScheduler;

}  // namespace duckdb
namespace sdb::search {

class PipelineMergeExecutor final : public irs::MergeExecutor {
 public:
  PipelineMergeExecutor(duckdb::DatabaseInstance& db,
                        duckdb::TaskScheduler* scheduler,
                        size_t threads) noexcept
    : _db{db}, _scheduler{scheduler}, _threads{threads} {}

  bool Run(irs::MergePlan& plan) final;
  size_t Threads() const noexcept final { return _threads; }

 private:
  duckdb::DatabaseInstance& _db;
  duckdb::TaskScheduler* _scheduler;
  size_t _threads;
};

}  // namespace sdb::search
