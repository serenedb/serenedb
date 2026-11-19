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

#include "pg/copy_in_batches.h"

namespace sdb::pg {

class CopyInStream final : public duckdb::RowSource {
 public:
  explicit CopyInStream(CopyInBatches& batches) : _batches{batches} {}

  duckdb::idx_t Read(char* data, duckdb::idx_t count,
                     duckdb::idx_t offset) const final;
  void Release(duckdb::idx_t position) final;
  duckdb::optional_idx End() const final;

 private:
  struct Unit {
    duckdb::idx_t offset;
    duckdb::idx_t size;
    duckdb::AllocatedData bytes;
  };

  void Pull() const;

  CopyInBatches& _batches;
  mutable absl::Mutex _lock;
  mutable std::deque<Unit> _units ABSL_GUARDED_BY(_lock);
  mutable duckdb::idx_t _pulled ABSL_GUARDED_BY(_lock) = 0;
  mutable bool _pulling ABSL_GUARDED_BY(_lock) = false;
  mutable bool _finished ABSL_GUARDED_BY(_lock) = false;
};

}  // namespace sdb::pg
