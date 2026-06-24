////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <memory>
#include <yaclib/async/future.hpp>

namespace sdb::search {

class InvertedIndexStorage;

// One coroutine per index drives background refresh (+ cleanup tail); another
// coordinates parallel compaction. Each holds a weak_ptr so a dropped index
// ends the loop (no shared-ptr cycle keeps storage alive) and resets it while
// idle so a concurrent DROP isn't blocked. The returned Future is collected by
// SearchEngine so stop() can join every loop.
yaclib::Future<> RefreshLoop(std::weak_ptr<InvertedIndexStorage> weak);
yaclib::Future<> CompactionCoordinator(
  std::weak_ptr<InvertedIndexStorage> weak);

}  // namespace sdb::search
