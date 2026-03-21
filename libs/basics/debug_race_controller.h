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

#include <absl/synchronization/mutex.h>
#ifdef SDB_DEV

#include <any>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

namespace sdb {
namespace app {

class AppServer;
}

namespace basics {

class DebugRaceController {
 public:
  static DebugRaceController& sharedInstance();

  DebugRaceController() = default;

  // Reset the stored state here, will free the stored data.
  void reset();

  // Caller is required to COPY the data to store here.
  // Otherwise, a concurrent thread might try to read it,
  // after the caller has freed the memory.
  auto waitForOthers(size_t number_of_threads_to_wait_for, std::any my_data,
                     const sdb::app::AppServer& server)
    -> std::optional<std::vector<std::any>>;

 private:
  bool didTrigger(size_t number_of_threads_to_wait_for) const
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  mutable absl::Mutex _mutex;
  absl::CondVar _cond_variable;
  std::vector<std::any> _data ABSL_GUARDED_BY(_mutex);
};

}  // namespace basics
}  // namespace sdb

// This are used to synchronize parallel locking of two shards
// in our tests. Do NOT include in production.

#endif
