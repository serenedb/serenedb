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
#include <yaclib/async/future.hpp>

#include "search/store.h"

namespace sdb::search {

// One coroutine per maintenance target drives background refresh (+ cleanup);
// another coordinates compaction. Each holds a weak_ptr so a dropped target
// ends the loop, and the returned Future lets SearchEngine::stop() join it.
yaclib::Future<> RefreshLoop(std::weak_ptr<Store> weak);
yaclib::Future<> CompactionCoordinator(std::weak_ptr<Store> weak);

}  // namespace sdb::search
