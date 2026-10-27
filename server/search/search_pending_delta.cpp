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

#include <utility>

namespace sdb::search {

void PendingDelta::Install(PendingGenerationPtr generation) {
  if (!generation || generation->Empty()) {
    return;
  }
  absl::MutexLock lock{&_mutex};
  _generations.push_back(std::move(generation));
}

std::vector<PendingGenerationPtr> PendingDelta::Snapshot(
  absl::FunctionRef<void()> read_reader) const {
  absl::ReaderMutexLock lock{&_mutex};
  read_reader();
  return _generations;
}

void PendingDelta::Prune(uint64_t tick) {
  absl::MutexLock lock{&_mutex};
  std::erase_if(_generations, [tick](const PendingGenerationPtr& generation) {
    return generation->tick <= tick;
  });
}

void PendingDelta::Clear() {
  absl::MutexLock lock{&_mutex};
  _generations.clear();
}

bool PendingDelta::Empty() const {
  absl::ReaderMutexLock lock{&_mutex};
  return _generations.empty();
}

}  // namespace sdb::search
