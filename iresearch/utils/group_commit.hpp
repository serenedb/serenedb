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

#include <absl/synchronization/mutex.h>

#include <exception>
#include <span>
#include <utility>
#include <vector>

namespace irs {

template<typename Request>
class GroupCommit {
 public:
  template<typename Lead>
  void Submit(Request& request, Lead&& lead) {
    Entry entry{.owner = this, .request = &request};
    _mutex.Lock();
    _pending.push_back(&entry);
    _mutex.Await(absl::Condition{&entry, &Entry::Ready});
    if (!entry.done) {
      _leading = true;
      auto batch = std::exchange(_pending, {});
      _mutex.Unlock();
      std::vector<Request*> requests;
      requests.reserve(batch.size());
      for (auto* pending : batch) {
        requests.push_back(pending->request);
      }
      std::exception_ptr error;
      try {
        lead(std::span<Request* const>{requests});
      } catch (...) {
        error = std::current_exception();
      }
      _mutex.Lock();
      for (auto* pending : batch) {
        pending->done = true;
        pending->error = error;
      }
      _leading = false;
    }
    _mutex.Unlock();
    if (entry.error) {
      std::rethrow_exception(entry.error);
    }
  }

 private:
  struct Entry {
    const GroupCommit* owner;
    Request* request;
    bool done = false;
    std::exception_ptr error;

    bool Ready() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(owner->_mutex) {
      return done || !owner->_leading;
    }
  };

  absl::Mutex _mutex;
  std::vector<Entry*> _pending ABSL_GUARDED_BY(_mutex);
  bool _leading ABSL_GUARDED_BY(_mutex) = false;
};

}  // namespace irs
