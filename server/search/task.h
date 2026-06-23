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
class SearchTable;

// One coroutine per maintenance target drives background refresh (+ cleanup
// tail); another coordinates parallel compaction. Each holds a weak_ptr so a
// dropped target ends the loop (no shared-ptr cycle keeps it alive) and resets
// it while idle so a concurrent DROP isn't blocked. The returned Future is
// collected by SearchEngine so stop() can join every loop.
//
// Templated on the storage type so the same loops drive both an inverted index
// (InvertedIndexStorage) and a search table (SearchTable): both expose the same
// maintenance interface (GetId / GetTasksSettings / RefreshUnsafe /
// CleanupUnsafe / CompactUnsafe / CompactionGeneration / NudgeCompaction /
// StalePressure&co). The only per-storage difference -- where the merge's
// irs::IndexFieldOptions come from -- is isolated to a PinCompactionOptions
// overload in the .cpp. Instantiated only for the two types below.
template<class Storage>
yaclib::Future<> RefreshLoop(std::weak_ptr<Storage> weak);
template<class Storage>
yaclib::Future<> CompactionCoordinator(std::weak_ptr<Storage> weak);

extern template yaclib::Future<> RefreshLoop(
  std::weak_ptr<InvertedIndexStorage>);
extern template yaclib::Future<> CompactionCoordinator(
  std::weak_ptr<InvertedIndexStorage>);
extern template yaclib::Future<> RefreshLoop(std::weak_ptr<SearchTable>);
extern template yaclib::Future<> CompactionCoordinator(
  std::weak_ptr<SearchTable>);

}  // namespace sdb::search
