////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "basics/lifecycle.h"

#include <atomic>
#include <string>

namespace sdb::lifecycle {
namespace {

std::atomic_bool gIsStopping = false;
std::string gDataDirArg;

}  // namespace

bool IsStopping() noexcept {
  return gIsStopping.load(std::memory_order_acquire);
}

void BeginShutdown() noexcept {
  gIsStopping.store(true, std::memory_order_release);
}

void SetDataDirArg(std::string_view arg) { gDataDirArg = arg; }

std::string_view DataDirArg() noexcept { return gDataDirArg; }

}  // namespace sdb::lifecycle
