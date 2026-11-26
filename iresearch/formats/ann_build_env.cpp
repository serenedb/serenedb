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

#include "iresearch/formats/ann_build_env.hpp"

#include <algorithm>
#include <atomic>
#include <duckdb/common/exception.hpp>

#include "iresearch/formats/ann_crew.hpp"

namespace irs {

void FanOut(const AnnBuildEnv* env, size_t tasks,
            absl::FunctionRef<void(size_t)> task) {
  std::atomic<size_t> next{0};
  const auto work = [&](size_t) {
    for (auto t = next.fetch_add(1); t < tasks; t = next.fetch_add(1)) {
      task(t);
    }
  };
  AnnCrew crew{
    env, static_cast<uint32_t>(std::clamp<size_t>(tasks, 1, kHnswMaxWorkers))};
  crew.Spawn(work);
  for (auto t = next.fetch_add(1); t < tasks; t = next.fetch_add(1)) {
    task(t);
    crew.Grow(work);
  }
  crew.Join();
}

void FanOut(const AnnBuildEnv* env, MemoryBudget* parent, size_t tasks,
            size_t task_bytes,
            absl::FunctionRef<void(size_t, FixedBudget*)> task) {
  const auto want = std::clamp<size_t>(tasks, 1, kHnswMaxWorkers);
  TrackedVector<TrackedPtr<FixedBudget>> shares{
    TrackedAllocator<TrackedPtr<FixedBudget>>{parent}};
  shares.reserve(want);
  for (size_t w = 0; w < want; ++w) {
    try {
      shares.push_back(MakeTracked<FixedBudget>(parent, parent, task_bytes));
    } catch (const duckdb::OutOfMemoryException&) {
      if (shares.empty()) {
        throw;
      }
      break;
    }
  }
  std::atomic<size_t> next{0};
  const auto work = [&](size_t w) {
    auto* budget = shares[w].get();
    for (auto t = next.fetch_add(1); t < tasks; t = next.fetch_add(1)) {
      task(t, budget);
    }
  };
  AnnCrew crew{env, static_cast<uint32_t>(shares.size())};
  crew.Spawn(work);
  for (auto t = next.fetch_add(1); t < tasks; t = next.fetch_add(1)) {
    task(t, shares.front().get());
    crew.Grow(work);
  }
  crew.Join();
}

}  // namespace irs
