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

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>

#include <deque>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/optional_idx.hpp>
#include <duckdb/formats/row_ranges.hpp>
#include <memory>
#include <string_view>
#include <vector>

#include "pg/copy_in_batches.h"

namespace sdb::pg {

class CopyInStream final : public duckdb::RowSource {
 public:
  CopyInStream(CopyInBatches& batches, size_t window)
    : _batches{batches}, _window{window} {}

  duckdb::idx_t Read(char* data, duckdb::idx_t count,
                     duckdb::idx_t offset) const final;
  void Release(duckdb::idx_t position) final;
  duckdb::optional_idx End() const final;

 private:
  struct UnitBytes {
    CopyBatchLease lease;
    duckdb::AllocatedData owned;
    std::vector<std::string_view> segments;
  };

  struct Unit {
    duckdb::idx_t offset;
    duckdb::idx_t size;
    std::shared_ptr<UnitBytes> bytes;
  };

  void Pull(duckdb::idx_t end) const;
  void Spill() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_lock);

  CopyInBatches& _batches;
  const size_t _window;
  mutable absl::Mutex _lock;
  mutable std::deque<Unit> _units ABSL_GUARDED_BY(_lock);
  mutable std::vector<duckdb::AllocatedData> _spare ABSL_GUARDED_BY(_lock);
  mutable size_t _leased ABSL_GUARDED_BY(_lock) = 0;
  mutable duckdb::idx_t _pulled ABSL_GUARDED_BY(_lock) = 0;
  mutable bool _pulling ABSL_GUARDED_BY(_lock) = false;
  mutable bool _finished ABSL_GUARDED_BY(_lock) = false;
};

}  // namespace sdb::pg
