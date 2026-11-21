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
#include <duckdb/formats/row_stream.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/assert.hpp>
#include <span>

namespace sdb::pg {
namespace {

constexpr size_t kSpareUnits = 64;

}  // namespace

duckdb::idx_t CopyInStream::Read(char* data, duckdb::idx_t count,
                                 duckdb::idx_t offset) const {
  struct Covering {
    duckdb::idx_t offset;
    std::shared_ptr<UnitBytes> bytes;
  };
  const auto end = offset + count;
  absl::InlinedVector<Covering, 4> covering;
  const auto readable = [&]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_lock) {
    return _pulled >= end || _finished ||
           (_pulled > offset && !_batches.Pending());
  };
  const auto ready = [&]() ABSL_EXCLUSIVE_LOCKS_REQUIRED(_lock) {
    return !_pulling || readable();
  };
  for (;;) {
    bool wait = false;
    {
      absl::MutexLock lock{&_lock};
      if (readable()) {
        SDB_ASSERT(offset >=
                   (_units.empty() ? _pulled : _units.front().offset));
        for (const auto& unit : _units) {
          if (unit.offset + unit.size <= offset) {
            continue;
          }
          if (unit.offset >= end) {
            break;
          }
          covering.push_back({unit.offset, unit.bytes});
        }
        break;
      }
      wait = _pulling;
      _pulling = true;
    }
    if (wait) {
      duckdb::TaskScheduler::BlockingScope blocking;
      absl::MutexLock lock{&_lock};
      _lock.Await(absl::Condition(&ready));
      continue;
    }
    Pull(offset, end);
  }
  duckdb::idx_t copied = 0;
  for (const auto& [unit_offset, bytes] : covering) {
    auto position = unit_offset;
    const std::span<const std::string_view> segments =
      bytes->lease ? std::span<const std::string_view>{bytes->lease->segments}
                   : std::span<const std::string_view>{bytes->segments};
    for (const auto segment : segments) {
      const auto segment_end = position + segment.size();
      const auto from = std::max(position, offset);
      const auto to = std::min(segment_end, end);
      if (from < to) {
        std::memcpy(data + (from - offset), segment.data() + (from - position),
                    to - from);
        copied = to - offset;
      }
      position = segment_end;
    }
  }
  return copied;
}

void CopyInStream::Spill() const {
  auto& unit = _units[_units.size() - _leased];
  duckdb::AllocatedData owned;
  if (!_spare.empty() && _spare.back().GetSize() >= unit.size) {
    owned = std::move(_spare.back());
    _spare.pop_back();
  } else {
    owned = duckdb::Allocator::DefaultAllocator().Allocate(
      std::max<duckdb::idx_t>(unit.size, kCopyBatchBytes));
  }
  auto* out = owned.get();
  for (const auto segment : unit.bytes->lease->segments) {
    if (!segment.empty()) {
      std::memcpy(out, segment.data(), segment.size());
      out += segment.size();
    }
  }
  auto spilled = std::make_shared<UnitBytes>();
  spilled->segments.emplace_back(reinterpret_cast<const char*>(owned.get()),
                                 unit.size);
  spilled->owned = std::move(owned);
  unit.bytes = std::move(spilled);
  --_leased;
}

void CopyInStream::Pull(duckdb::idx_t offset, duckdb::idx_t end) const {
  absl::Cleanup done = [this] {
    absl::MutexLock lock{&_lock};
    _pulling = false;
  };
  for (;;) {
    {
      absl::MutexLock lock{&_lock};
      if (_pulled > offset && !_batches.Pending()) {
        return;
      }
      while (_leased > 0 && _leased + 1 >= _window) {
        Spill();
      }
    }
    auto lease = _batches.Next();
    if (!lease) {
      absl::MutexLock lock{&_lock};
      _finished = true;
      return;
    }
    const auto size = static_cast<duckdb::idx_t>(lease->bytes);
    auto bytes = std::make_shared<UnitBytes>();
    bytes->lease = std::move(lease);
    absl::MutexLock lock{&_lock};
    _units.push_back({_pulled, size, std::move(bytes)});
    ++_leased;
    _pulled += size;
    if (_pulled >= end) {
      return;
    }
  }
}

void CopyInStream::Release(duckdb::idx_t position) {
  absl::MutexLock lock{&_lock};
  while (!_units.empty() &&
         _units.front().offset + _units.front().size <= position) {
    auto& front = _units.front();
    if (_leased == _units.size()) {
      --_leased;
    } else if (front.bytes.use_count() == 1 && _spare.size() < kSpareUnits) {
      _spare.push_back(std::move(front.bytes->owned));
    }
    _units.pop_front();
  }
}

duckdb::optional_idx CopyInStream::End() const {
  absl::MutexLock lock{&_lock};
  return _finished ? duckdb::optional_idx{_pulled} : duckdb::optional_idx{};
}

bool CopyInStream::Available(duckdb::idx_t offset) const {
  absl::MutexLock lock{&_lock};
  return _pulled > offset || _finished || _batches.Pending();
}

CopyInStreamOffer::CopyInStreamOffer(duckdb::ClientContext& context,
                                     const std::string& path,
                                     CopyInBatches& batches, size_t window)
  : _context{context}, _key{duckdb::RowStreamKey(path)} {
  _context.registered_state->Insert(
    _key, duckdb::make_shared_ptr<duckdb::RowStreamState>(
            duckdb::make_shared_ptr<CopyInStream>(batches, window)));
}

CopyInStreamOffer::~CopyInStreamOffer() {
  _context.registered_state->Remove(_key);
}

}  // namespace sdb::pg
