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

#include "scripted_file_system.hpp"

#include <absl/base/call_once.h>
#include <signal.h>

#include <array>
#include <atomic>

namespace tests {
namespace {

constexpr size_t kMostScripted = 1024;

constinit std::array<std::atomic<ScriptedMapping*>, kMostScripted> gScripted{};
struct sigaction gPrevious = {};
absl::once_flag gInstalled;

void OnFault(int /*signal*/, siginfo_t* info, void* /*context*/) {
  for (auto& slot : gScripted) {
    auto* const mapping = slot.load(std::memory_order_acquire);
    if (mapping && mapping->FaultIn(info->si_addr)) {
      return;
    }
  }
  ::sigaction(SIGSEGV, &gPrevious, nullptr);
}

void Install() {
  struct sigaction action = {};
  action.sa_sigaction = &OnFault;
  action.sa_flags = SA_SIGINFO;
  ::sigemptyset(&action.sa_mask);
  ::sigaction(SIGSEGV, &action, &gPrevious);
}

}  // namespace

void RegisterScripted(ScriptedMapping& mapping) {
  absl::call_once(gInstalled, &Install);
  for (auto& slot : gScripted) {
    ScriptedMapping* empty = nullptr;
    if (slot.compare_exchange_strong(empty, &mapping,
                                     std::memory_order_acq_rel)) {
      return;
    }
  }
  SDB_ENSURE(false, "ScriptedMapping: too many scripted mappings");
}

void UnregisterScripted(ScriptedMapping& mapping) noexcept {
  for (auto& slot : gScripted) {
    ScriptedMapping* held = &mapping;
    if (slot.compare_exchange_strong(held, nullptr,
                                     std::memory_order_acq_rel)) {
      return;
    }
  }
}

void DropScriptedMappings() {
  for (auto& slot : gScripted) {
    if (auto* const mapping = slot.load(std::memory_order_acquire)) {
      mapping->DropAll();
    }
  }
}

}  // namespace tests
