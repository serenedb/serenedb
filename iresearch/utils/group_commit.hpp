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
#include <utility>

namespace irs {

template<typename Request>
class GroupCommit {
  struct Entry {
    bool Ready() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(owner->_mutex) {
      return done || !owner->_leading;
    }

    const GroupCommit* owner;
    Request* request;
    Entry* next = nullptr;
    bool done = false;
    std::exception_ptr error;
  };

 public:
  struct Iterator {
    Request* operator*() const noexcept { return entry->request; }
    Iterator& operator++() noexcept {
      entry = entry->next;
      return *this;
    }
    bool operator==(const Iterator&) const = default;

    const Entry* entry;
  };

  struct Batch {
    Iterator begin() const noexcept { return {head}; }
    Iterator end() const noexcept { return {nullptr}; }

    const Entry* head;
  };

  template<typename Lead>
  void Submit(Request& request, Lead&& lead) {
    Entry entry{.owner = this, .request = &request};
    _mutex.Lock();
    entry.next = std::exchange(_pending, &entry);
    _mutex.Await(absl::Condition{&entry, &Entry::Ready});
    if (!entry.done) {
      _leading = true;
      auto* head = std::exchange(_pending, nullptr);
      _mutex.Unlock();
      std::exception_ptr error;
      try {
        lead(Batch{head});
      } catch (...) {
        error = std::current_exception();
      }
      _mutex.Lock();
      for (auto* pending = head; pending;) {
        auto* next = pending->next;
        pending->done = true;
        pending->error = error;
        pending = next;
      }
      _leading = false;
    }
    _mutex.Unlock();
    if (entry.error) {
      std::rethrow_exception(entry.error);
    }
  }

 private:
  absl::Mutex _mutex;
  Entry* _pending ABSL_GUARDED_BY(_mutex) = nullptr;
  bool _leading ABSL_GUARDED_BY(_mutex) = false;
};

}  // namespace irs
