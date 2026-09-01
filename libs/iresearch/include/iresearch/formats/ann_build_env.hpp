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

#include <cstdint>
#include <yaclib/exe/executor.hpp>

namespace irs {

// Server-injected budget for a parallel ANN build. iresearch owns no threads:
// `executor` is the pool that already admitted this merge, and acquire/release
// draw extra workers from the same gate, so the build degrades to serial when
// the pool is saturated. A null env means "build on the calling thread": the
// build then never suspends and its Future is ready on return.
//
// acquire/release are consulted around the parallel phase only, never for the
// whole merge, so helpers are not held across the column copy.
//
// The FunctionRef referents must outlive the merge -- bind named locals, never
// temporaries.
struct AnnBuildEnv {
  yaclib::IExecutor* executor = nullptr;
  absl::FunctionRef<uint32_t(uint32_t)> acquire;
  absl::FunctionRef<void(uint32_t)> release;
};

}  // namespace irs
