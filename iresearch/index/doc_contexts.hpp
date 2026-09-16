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
/// Copyright holder is SereneDB GmbH
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <algorithm>
#include <cstdint>
#include <span>
#include <utility>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/resource_manager.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class DocContexts final {
 public:
  struct Run final {
    uint64_t tick;
    uint32_t begin;
    bool committed;
  };

  DocContexts() = default;
  explicit DocContexts(IResourceManager& rm) : _runs{Allocator{rm}} {}

  DocContexts(const DocContexts&) = default;
  DocContexts& operator=(const DocContexts&) = default;

  DocContexts(DocContexts&& other) noexcept
    : _runs{std::move(other._runs)}, _size{std::exchange(other._size, 0)} {}

  DocContexts& operator=(DocContexts&& other) noexcept(
    std::is_nothrow_move_assignable_v<ManagedVector<Run>>) {
    _runs = std::move(other._runs);
    _size = std::exchange(other._size, 0);
    return *this;
  }

  size_t size() const noexcept { return _size; }
  bool empty() const noexcept { return _size == 0; }

  std::span<const Run> Runs() const noexcept { return _runs; }

  size_t RunIndex(size_t doc) const noexcept {
    SDB_ASSERT(doc < _size);
    SDB_ASSERT(!_runs.empty());
    const auto it = std::upper_bound(
      _runs.begin(), _runs.end(), doc,
      [](size_t doc, const Run& run) noexcept { return doc < run.begin; });
    return static_cast<size_t>(it - _runs.begin()) - 1;
  }

  size_t RunEnd(size_t run) const noexcept {
    SDB_ASSERT(run < _runs.size());
    return run + 1 < _runs.size() ? _runs[run + 1].begin : _size;
  }

  void Append(size_t count, uint64_t tick) {
    if (count == 0) {
      return;
    }
    SDB_ASSERT(_size + count <= doc_limits::eof());
    if (_runs.capacity() < _runs.size() + 2) {
      _runs.reserve(std::max<size_t>(4, 2 * _runs.capacity()));
    }
    PushOrMerge(_size, tick, false);
    _size += count;
  }

  uint64_t TickAt(size_t doc) const noexcept {
    return _runs[RunIndex(doc)].tick;
  }

  size_t UpperBound(uint64_t tick) const noexcept {
    const auto it = std::upper_bound(
      _runs.begin(), _runs.end(), tick,
      [](uint64_t tick, const Run& run) noexcept { return tick < run.tick; });
    return it == _runs.end() ? _size : it->begin;
  }

  void Truncate(size_t size) noexcept {
    SDB_ASSERT(size <= _size);
    while (!_runs.empty() && size <= _runs.back().begin) {
      _runs.pop_back();
    }
    _size = size;
  }

  void RewriteTail(size_t from, uint64_t tick) noexcept {
    SDB_ASSERT(from <= _size);
    if (from == _size) {
      return;
    }
    const auto size = _size;
    Truncate(from);
    SDB_ASSERT(_runs.size() < _runs.capacity());
    PushOrMerge(from, tick, true);
    _size = size;
  }

  void Commit(size_t from, uint64_t delta) noexcept {
    if (_size <= from) {
      return;
    }
    auto run = RunIndex(from);
    SDB_ASSERT(_runs[run].begin == from);
    SDB_ASSERT(!_runs[run].committed);
    for (; run < _runs.size(); ++run) {
      _runs[run].tick += delta;
      _runs[run].committed = true;
    }
  }

  void Clear() noexcept {
    _runs.clear();
    _size = 0;
  }

  size_t MemoryActive() const noexcept { return _runs.size() * sizeof(Run); }
  size_t MemoryReserved() const noexcept {
    return _runs.capacity() * sizeof(Run);
  }

  IResourceManager& ResourceManager() const noexcept {
    return _runs.get_allocator().Manager();
  }

 private:
  using Allocator = ManagedTypedAllocator<Run>;

  void PushOrMerge(size_t begin, uint64_t tick, bool committed) noexcept {
    if (!_runs.empty() && _runs.back().tick == tick &&
        _runs.back().committed == committed) {
      return;
    }
    _runs.push_back(Run{tick, static_cast<uint32_t>(begin), committed});
  }

  ManagedVector<Run> _runs;
  size_t _size{0};
};

}  // namespace irs
