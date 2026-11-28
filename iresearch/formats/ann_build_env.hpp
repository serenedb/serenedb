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

#include <absl/functional/function_ref.h>

#include <cstddef>
#include <cstdint>
#include <yaclib/exe/executor.hpp>

#include "iresearch/utils/tracked.hpp"

namespace irs {

inline constexpr uint32_t kHnswMaxWorkers = 64;

struct AnnBuildEnv {
  yaclib::IExecutor& executor;
  absl::FunctionRef<uint32_t(uint32_t, uint32_t)> acquire;
  absl::FunctionRef<void(uint32_t)> release;
};

void FanOut(const AnnBuildEnv* env, size_t tasks,
            absl::FunctionRef<void(size_t)> task);

void FanOut(const AnnBuildEnv* env, MemoryBudget& parent, size_t tasks,
            absl::FunctionRef<void(size_t, TrackedAllocator<>)> task);

inline auto AnnParallelFor(const AnnBuildEnv* env) {
  return [env](size_t count, absl::FunctionRef<void(size_t)> body) {
    FanOut(env, count, body);
  };
}

}  // namespace irs
