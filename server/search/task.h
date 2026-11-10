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

#include <memory>
#include <span>
#include <yaclib/algo/wait_group.hpp>
#include <yaclib/async/future.hpp>

#include "search/store.h"

namespace sdb::search {

void AddMaintenanceTarget(const std::shared_ptr<Store>& store);
yaclib::Future<> MaintenanceLoop(yaclib::WaitGroup<>& jobs);

void DoCleanup(Store& idx);
void QueueReleaseCleanup(const std::shared_ptr<Store>& store, Tick released);
void CleanupReleased(Tick oldest, std::span<const StoreSnapshots> stores);
void RequestCleanups() noexcept;
bool ReleasedPending() noexcept;

void PruneVersions(std::span<const Tick> running, Tick published,
                   std::span<const StoreSnapshots> stores);
bool FreeVersions();

}  // namespace sdb::search
