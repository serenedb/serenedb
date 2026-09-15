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

#include <absl/base/thread_annotations.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/time.h>

#include <cstdint>

namespace sdb::search {

class WriterGenerations {
 public:
  [[nodiscard]] unsigned Register();
  void Deregister(unsigned slot) noexcept;
  [[nodiscard]] bool Drain(absl::FunctionRef<bool()> cancelled,
                           absl::Duration poll);

 private:
  bool WaitUntilEmpty(unsigned slot, absl::FunctionRef<bool()> cancelled,
                      absl::Duration poll)
    ABSL_EXCLUSIVE_LOCKS_REQUIRED(_mutex);

  absl::Mutex _mutex;
  absl::CondVar _cv;
  unsigned _generation ABSL_GUARDED_BY(_mutex) = 0;
  uint64_t _writers[2] ABSL_GUARDED_BY(_mutex) = {0, 0};
};

}  // namespace sdb::search
