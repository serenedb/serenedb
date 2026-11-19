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

#include "pg/copy_in_stream.h"

#include <absl/cleanup/cleanup.h>
#include <absl/container/inlined_vector.h>

#include <algorithm>
#include <cstring>
#include <iresearch/utils/assert.hpp>

namespace sdb::pg {

duckdb::idx_t CopyInStream::Read(char* data, duckdb::idx_t count,
                                 duckdb::idx_t offset) const {
  const auto end = offset + count;
  absl::InlinedVector<const Unit*, 4> covering;
  for (;;) {
    {
      absl::MutexLock lock{&_lock};
      const auto ready = [&]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_lock) {
        return !_pulling || _pulled >= end || _finished;
      };
      _lock.Await(absl::Condition(&ready));
      if (_pulled >= end || _finished) {
        SDB_ASSERT(offset >=
                   (_units.empty() ? _pulled : _units.front().offset));
        for (const auto& unit : _units) {
          if (unit.offset + unit.size <= offset) {
            continue;
          }
          if (unit.offset >= end) {
            break;
          }
          covering.push_back(&unit);
        }
        break;
      }
      _pulling = true;
    }
    Pull();
  }
  duckdb::idx_t copied = 0;
  for (const auto* unit : covering) {
    const auto from = std::max(offset, unit->offset);
    const auto to = std::min(end, unit->offset + unit->size);
    std::memcpy(data + (from - offset),
                unit->bytes.get() + (from - unit->offset), to - from);
    copied = to - offset;
  }
  return copied;
}

void CopyInStream::Pull() const {
  absl::Cleanup done = [this] {
    absl::MutexLock lock{&_lock};
    _pulling = false;
  };
  auto lease = _batches.Next();
  if (!lease) {
    absl::MutexLock lock{&_lock};
    _finished = true;
    return;
  }
  const auto size = static_cast<duckdb::idx_t>(lease->bytes);
  auto bytes = duckdb::Allocator::DefaultAllocator().Allocate(size);
  auto* out = bytes.get();
  for (const auto segment : lease->segments) {
    if (!segment.empty()) {
      std::memcpy(out, segment.data(), segment.size());
      out += segment.size();
    }
  }
  lease.Reset();
  absl::MutexLock lock{&_lock};
  _units.push_back({_pulled, size, std::move(bytes)});
  _pulled += size;
}

void CopyInStream::Release(duckdb::idx_t position) {
  absl::MutexLock lock{&_lock};
  while (!_units.empty() &&
         _units.front().offset + _units.front().size <= position) {
    _units.pop_front();
  }
}

duckdb::optional_idx CopyInStream::End() const {
  absl::MutexLock lock{&_lock};
  return _finished ? duckdb::optional_idx{_pulled} : duckdb::optional_idx{};
}

}  // namespace sdb::pg
