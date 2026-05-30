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

#include <absl/cleanup/cleanup.h>
#include <errno.h>
#include <signal.h>

#include <chrono>
#include <thread>

#include "basics/operating-system.h"

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/condition_variable.h"
#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "thread.h"

#ifdef SERENEDB_HAVE_PROCESS_H
#include <process.h>
#endif

#ifdef SERENEDB_HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

using namespace sdb;
using namespace sdb::app;
using namespace sdb::basics;

namespace {

// ever-increasing counter for thread numbers.
// not used on Windows
std::atomic<uint64_t> gNextThreadId(1);

// helper struct to assign and retrieve a thread id
struct ThreadNumber {
  ThreadNumber() noexcept
    : _value(gNextThreadId.fetch_add(1, std::memory_order_seq_cst)) {}

  uint64_t Get() const noexcept { return _value; }

 private:
  uint64_t _value;
};

}  // namespace

/// local thread number
static thread_local ::ThreadNumber gLocalThreadNumber{};
static thread_local const char* gLocalThreadName = nullptr;

// retrieve the current thread's name. the string view will
// remain valid as long as the ThreadNameFetcher remains valid.
ThreadNameFetcher::ThreadNameFetcher() noexcept {
  memset(&_buffer[0], 0, sizeof(_buffer));

  const char* name = gLocalThreadName;
  if (name != nullptr) {
    size_t len = strlen(name);
    if (len > sizeof(_buffer) - 1) {
      len = sizeof(_buffer) - 1;
    }
    memcpy(&_buffer[0], name, len);
  } else {
#ifdef SERENEDB_HAVE_SYS_PRCTL_H
    // will read at most 16 bytes and null-terminate the data
    prctl(PR_GET_NAME, &_buffer, 0, 0, 0);
    // be extra cautious about null-termination of the buffer
    _buffer[sizeof(_buffer) - 1] = '\0';
#endif
  }
  if (_buffer[0] == '\0') {
    // if there is no other name, simply return "main"
    memcpy(&_buffer[0], "main", 4);
  }
}

std::string_view ThreadNameFetcher::get() const noexcept {
  // guaranteed to be properly null-terminated
  return {&_buffer[0]};
}

/// static started with access to the private variables
void Thread::startThread(void* arg) {
  SDB_ASSERT(arg != nullptr);
  Thread* ptr = static_cast<Thread*>(arg);
  SDB_ASSERT(ptr != nullptr);

  ptr->_thread_number = gLocalThreadNumber.Get();

  gLocalThreadName = ptr->name().c_str();

  // make sure we drop our reference when we are finished!
  absl::Cleanup guard = [ptr]() noexcept {
    gLocalThreadName = nullptr;
    ptr->_state.store(ThreadState::Stopped);
    ptr->releaseRef();
  };

  ThreadState expected = ThreadState::Starting;
  bool res =
    ptr->_state.compare_exchange_strong(expected, ThreadState::Started);
  if (!res) {
    SDB_ASSERT(expected == ThreadState::Stopping);
    // we are already shutting down -> don't bother calling run!
    return;
  }

  try {
    ptr->runMe();
  } catch (const std::exception& ex) {
    SDB_WARN(GENERAL, "caught exception in thread '",
             ptr->_name, "': ", ex.what());
    throw;
  }
}

/// returns the process id
pid_t Thread::currentProcessId() { return getpid(); }

/// returns the thread process id
uint64_t Thread::currentThreadNumber() noexcept {
  return gLocalThreadNumber.Get();
}

std::string Thread::stringify(ThreadState state) {
  switch (state) {
    case ThreadState::Created:
      return "created";
    case ThreadState::Starting:
      return "starting";
    case ThreadState::Started:
      return "started";
    case ThreadState::Stopping:
      return "stopping";
    case ThreadState::Stopped:
      return "stopped";
  }
  return "unknown";
}

/// constructs a thread
Thread::Thread(app::AppServer& server, const std::string& name,
               bool delete_on_exit, uint32_t termination_timeout)
  : _server(server),
    _thread_struct_initialized(false),
    _refs(0),
    _name(name),
    _thread(),
    _thread_number(0),
    _termination_timeout(termination_timeout),
    _delete_on_exit(delete_on_exit),
    _finished_condition(nullptr),
    _state(ThreadState::Created) {
  SdbInitThread(&_thread);
}

/// deletes the thread
Thread::~Thread() {
  SDB_ASSERT(_refs.load() == 0);

  auto state = _state.load();
  SDB_TRACE(GENERAL, "delete(", _name,
            "), state: ", stringify(state));

  if (state != ThreadState::Stopped && state != ThreadState::Created) {
    SDB_FATAL(GENERAL, "thread '", _name,
              "' is not stopped but ", stringify(state),
              ". shutting down hard");
    FatalErrorAbort();
  }
}

/// flags the thread as stopping
void Thread::beginShutdown() {
  SDB_TRACE(GENERAL, "beginShutdown(", _name, ") in state ",
            stringify(_state.load()));

  ThreadState state = _state.load();

  while (state == ThreadState::Created) {
    _state.compare_exchange_weak(state, ThreadState::Stopped);
  }

  while (state != ThreadState::Stopping && state != ThreadState::Stopped) {
    _state.compare_exchange_weak(state, ThreadState::Stopping);
  }

  SDB_TRACE(GENERAL, "beginShutdown(", _name,
            ") reached state ", stringify(_state.load()));
}

/// MUST be called from the destructor of the MOST DERIVED class
void Thread::shutdown() {
  SDB_TRACE(GENERAL, "shutdown(", _name, ")");

  beginShutdown();
  if (_thread_struct_initialized.exchange(false, std::memory_order_acquire)) {
    if (SdbIsSelfThread(&_thread)) {
      // we must ignore any errors here, but SdbDetachThread will log them
      SdbDetachThread(&_thread);
    } else {
      auto ret = SdbJoinThreadWithTimeout(&_thread, _termination_timeout);

      if (ret != ERROR_OK) {
        SDB_FATAL(GENERAL, "cannot shutdown thread '",
                  _name, "', giving up");
        FatalErrorAbort();
      }
    }
  }
  SDB_ASSERT(_refs.load() == 0);
  SDB_ASSERT(_state.load() == ThreadState::Stopped);
}

/// checks if the current thread was asked to stop
bool Thread::isStopping() const noexcept {
  // need acquire to ensure we establish a happens before relation with the
  // update that updates _state, so threads that wait for isStopping to return
  // true are properly synchronized
  auto state = _state.load(std::memory_order_acquire);
  return state == ThreadState::Stopping || state == ThreadState::Stopped;
}

/// starts the thread
bool Thread::start(ConditionVariable* finished_condition) {
  if (!isSystem() && !_server.isPrepared()) {
    SDB_FATAL(GENERAL, "trying to start a thread '", _name,
      "' before prepare has finished, current state: ", (int)_server.state());
    FatalErrorAbort();
  }

  _finished_condition = finished_condition;
  ThreadState state = _state.load();

  if (state != ThreadState::Created) {
    SDB_FATAL(GENERAL,
              "called started on an already started thread '", _name,
              "', thread is in state ", stringify(state));
    FatalErrorAbort();
  }

  ThreadState expected = ThreadState::Created;
  if (!_state.compare_exchange_strong(expected, ThreadState::Starting)) {
    // This should never happen! If it does, it means we have multiple calls to
    // start().
    SDB_WARN(GENERAL, "failed to set thread '", _name,
             "' to state 'starting'; thread is in unexpected state ",
             stringify(expected));
    FatalErrorAbort();
  }

  // we count two references - one for the current thread and one for the thread
  // that we are trying to start.
  _refs.fetch_add(2);
  SDB_ASSERT(_refs.load() == 2);

  SDB_ASSERT(_thread_struct_initialized == false);
  SdbInitThread(&_thread);

  bool ok = SdbStartThread(&_thread, _name.c_str(), &startThread, this);
  if (!ok) {
    // could not start the thread -> decrement ref for the foreign thread
    _refs.fetch_sub(1);
    _state.store(ThreadState::Stopped);
    SDB_ERROR(GENERAL, "could not start thread '", _name,
              "': ", LastError());
  } else {
    _thread_struct_initialized.store(true, std::memory_order_release);
  }

  releaseRef();

  return ok;
}

void Thread::markAsStopped() noexcept {
  _state.store(ThreadState::Stopped);

  if (_finished_condition != nullptr) {
    absl::MutexLock locker{&_finished_condition->mutex};
    _finished_condition->cv.notify_all();
  }
}

void Thread::runMe() {
  // make sure the thread is marked as stopped under all circumstances
  absl::Cleanup sg = [&]() noexcept { markAsStopped(); };

  try {
    run();
  } catch (const std::exception& ex) {
    if (!isSilent()) {
      SDB_ERROR(GENERAL, "exception caught in thread '", _name,
                "': ", ex.what());
    }
    throw;
  } catch (...) {
    if (!isSilent()) {
      SDB_ERROR(GENERAL,
                "unknown exception caught in thread '", _name, "'");
    }
    throw;
  }
}

void Thread::releaseRef() noexcept {
  auto refs = _refs.fetch_sub(1) - 1;
  SDB_ASSERT(refs >= 0);
  if (refs == 0 && _delete_on_exit) {
    gLocalThreadName = nullptr;
    delete this;
  }
}
