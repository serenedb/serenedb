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

#include "search/search_pending_delta.h"

#include <algorithm>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <utility>

namespace sdb::search {

void PendingDelta::PruneFloor::Release() noexcept {
  if (auto* delta = std::exchange(_delta, nullptr)) {
    delta->ReleaseFloor(_mark);
  }
}

void PendingDelta::Install(std::shared_ptr<PendingGeneration> generation) {
  if (!generation || generation->Empty()) {
    return;
  }
  absl::MutexLock lock{&_mutex};
  generation->sequence = ++_installed;
  _generations.push_back(std::move(generation));
}

PendingDelta::Reading PendingDelta::Read(
  absl::FunctionRef<void()> read_reader) const {
  absl::ReaderMutexLock lock{&_mutex};
  read_reader();
  return {.generations = _generations, .mark = _installed};
}

PendingDelta::Claimed PendingDelta::Claim(
  std::optional<uint64_t> certified, absl::FunctionRef<void()> read_reader) {
  absl::MutexLock lock{&_mutex};
  read_reader();

  Claimed claimed;
  claimed.from_scratch = !certified || _pruned_through > *certified;
  const uint64_t from = claimed.from_scratch ? 0 : *certified;
  for (const auto& generation : _generations) {
    if (generation->sequence > from) {
      claimed.generations.push_back(generation);
    }
  }
  claimed.floor = PruneFloor{*this, _installed};
  _floors.insert(_installed);
  return claimed;
}

std::vector<PendingGenerationPtr> PendingDelta::Extend(PruneFloor& floor) {
  SDB_ASSERT(floor.Armed());
  absl::MutexLock lock{&_mutex};
  std::vector<PendingGenerationPtr> tail;
  for (const auto& generation : _generations) {
    if (generation->sequence > floor._mark) {
      tail.push_back(generation);
    }
  }
  if (floor._mark != _installed) {
    _floors.erase(_floors.find(floor._mark));
    _floors.insert(_installed);
    floor._mark = _installed;
  }
  return tail;
}

void PendingDelta::Prune(uint64_t tick) {
  absl::MutexLock lock{&_mutex};
  const uint64_t floor =
    _floors.empty() ? std::numeric_limits<uint64_t>::max() : *_floors.begin();
  uint64_t pruned = _pruned_through;
  std::erase_if(_generations,
                [&](const PendingGenerationPtr& generation) noexcept {
                  if (generation->tick > tick || generation->sequence > floor) {
                    return false;
                  }
                  pruned = std::max(pruned, generation->sequence);
                  return true;
                });
  _pruned_through = pruned;
}

void PendingDelta::Clear() {
  absl::MutexLock lock{&_mutex};
  // No floor survives this: the clear kills the rows every generation
  // describes, so a certifier holding one has nothing left to come back for
  // and has to start again from the reader the clear installs.
  _pruned_through = _installed;
  _generations.clear();
}

bool PendingDelta::Empty() const {
  absl::ReaderMutexLock lock{&_mutex};
  return _generations.empty();
}

void PendingDelta::ReleaseFloor(uint64_t tick) noexcept {
  absl::MutexLock lock{&_mutex};
  const auto it = _floors.find(tick);
  SDB_ASSERT(it != _floors.end());
  _floors.erase(it);
}

}  // namespace sdb::search
