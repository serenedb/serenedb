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

#include "index/merge/support/parallel_merge.hpp"

#include <absl/container/flat_hash_map.h>
#include <absl/synchronization/mutex.h>

#include <duckdb/common/fault_points.hpp>
#include <iresearch/utils/duckdb_engine.hpp>

namespace tests::merge {

ShuffledUnits::ShuffledUnits(bool shuffled)
  : _armed{shuffled && duckdb::FaultPoints::Arm("irs::ExecuteMerge::Shuffle")} {
}

ShuffledUnits::~ShuffledUnits() {
  if (_armed) {
    duckdb::FaultPoints::Disarm("irs::ExecuteMerge::Shuffle");
  }
}

duckdb::shared_ptr<duckdb::DedicatedPool> NewMergePool(size_t threads) {
  return duckdb::TaskScheduler::GetScheduler(
           irs::DuckDBEngine::Instance().instance())
    .CreateDedicatedPool(threads);
}

duckdb::shared_ptr<duckdb::DedicatedPool> MergePool(size_t threads) {
  static absl::Mutex mutex;
  static absl::flat_hash_map<size_t, duckdb::shared_ptr<duckdb::DedicatedPool>>
    pools;
  absl::MutexLock lock{&mutex};
  auto& pool = pools[threads];
  if (!pool) {
    pool = NewMergePool(threads);
  }
  return pool;
}

}  // namespace tests::merge
