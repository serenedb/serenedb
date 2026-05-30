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

#include <string.h>

#include "basics/application-exit.h"
#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/errors.h"
#include "basics/logger/logger.h"
#include "basics/signals.h"
#include "threads.h"

#ifdef SERENEDB_HAVE_POSIX_THREADS
#include <time.h>

#ifdef SERENEDB_HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#ifdef SERENEDB_HAVE_THREAD_POLICY
#include <mach/mach.h>
#endif

namespace sdb {

////////////////////////////////////////////////////////////////////////////////
/// data block for thread starter
////////////////////////////////////////////////////////////////////////////////

struct ThreadDataT {
  void (*starter)(void*);
  void* data;
  std::string name;

  ThreadDataT(void (*starter)(void*), void* data, const char* name)
    : starter(starter), data(data), name(name) {}
};

////////////////////////////////////////////////////////////////////////////////
/// starter function for thread
////////////////////////////////////////////////////////////////////////////////

static void* ThreadStarter(void* data) {
  sdb::signals::MaskAllSignals();

  // this will automatically free the thread struct when leaving this function
  std::unique_ptr<ThreadDataT> d(static_cast<ThreadDataT*>(data));

  SDB_ASSERT(d != nullptr);

#ifdef SERENEDB_HAVE_SYS_PRCTL_H
  prctl(PR_SET_NAME, d->name.c_str(), 0, 0, 0);
#endif

  try {
    d->starter(d->data);
  } catch (...) {
    // we must not throw from here
  }

  return nullptr;
}

////////////////////////////////////////////////////////////////////////////////
/// initializes a thread
////////////////////////////////////////////////////////////////////////////////

void SdbInitThread(pthread_t* thread) { memset(thread, 0, sizeof(pthread_t)); }

////////////////////////////////////////////////////////////////////////////////
/// starts a thread
////////////////////////////////////////////////////////////////////////////////

bool SdbStartThread(pthread_t* thread, const char* name, void (*starter)(void*),
                    void* data) {
  std::unique_ptr<ThreadDataT> d;

  try {
    d.reset(new ThreadDataT(starter, data, name));
  } catch (...) {
    SDB_ERROR(GENERAL,
              "could not start thread: out of memory");
    return false;
  }

  SDB_ASSERT(d != nullptr);

  pthread_attr_t stack_size_attribute;
  size_t stack_size = 0;

  auto err = pthread_attr_init(&stack_size_attribute);
  if (err) {
    SDB_ERROR(GENERAL,
              "could not initialize stack size attribute.");
    return false;
  }
  err = pthread_attr_getstacksize(&stack_size_attribute, &stack_size);
  if (err) {
    SDB_ERROR(GENERAL,
              "could not acquire stack size from pthread.");
    return false;
  }

  if (stack_size < 8388608) {  // 8MB
    err = pthread_attr_setstacksize(&stack_size_attribute, 8388608);
    if (err) {
      SDB_ERROR(GENERAL,
                "could not assign new stack size in pthread.");
      return false;
    }
  }

  int rc =
    pthread_create(thread, &stack_size_attribute, &ThreadStarter, d.get());

  if (rc != 0) {
    errno = rc;
    SetError(ERROR_SYS_ERROR);
    SDB_ERROR(GENERAL,
              "could not start thread: ", strerror(errno));

    return false;
  }

  // object must linger around until later
  d.release();

  return true;
}

////////////////////////////////////////////////////////////////////////////////
/// waits for a thread to finish
////////////////////////////////////////////////////////////////////////////////

ErrorCode SdbJoinThread(pthread_t* thread) {
  SDB_ASSERT(!SdbIsSelfThread(thread));
  int res = pthread_join(*thread, nullptr);

  if (res != 0) {
    SDB_WARN(GENERAL,
             "cannot join thread: ", strerror(res));
    return ERROR_FAILED;
  } else {
    return ERROR_OK;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// waits for a thread to finish within the specified timeout (in ms).
////////////////////////////////////////////////////////////////////////////////

ErrorCode SdbJoinThreadWithTimeout(pthread_t* thread, uint32_t timeout) {
  if (timeout == SERENEDB_INFINITE) {
    return SdbJoinThread(thread);
  }

  SDB_ASSERT(!SdbIsSelfThread(thread));

  timespec ts;
  if (!timespec_get(&ts, TIME_UTC)) {
    SDB_FATAL(GENERAL,
              "could not initialize timespec with current time");
    FatalErrorAbort();
  }
  ts.tv_sec += timeout / 1000;
  ts.tv_nsec = (timeout % 1000) * 1'000'000;

  int res = pthread_timedjoin_np(*thread, nullptr, &ts);
  if (res != 0) {
    SDB_WARN(GENERAL,
             "cannot join thread: ", strerror(res));
    return ERROR_FAILED;
  }
  return ERROR_OK;
}

////////////////////////////////////////////////////////////////////////////////
/// detaches a thread
////////////////////////////////////////////////////////////////////////////////

bool SdbDetachThread(pthread_t* thread) {
  int res = pthread_detach(*thread);

  if (res != 0) {
    SDB_WARN(GENERAL,
             "cannot detach thread: ", strerror(res));
    return false;
  } else {
    return true;
  }
}

////////////////////////////////////////////////////////////////////////////////
/// checks if we are the thread
////////////////////////////////////////////////////////////////////////////////

bool SdbIsSelfThread(pthread_t* thread) { return pthread_self() == *thread; }

}  // namespace sdb

#endif
