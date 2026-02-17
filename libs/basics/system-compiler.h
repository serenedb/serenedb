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

#include "basics/assert.h"
#include "basics/operating-system.h"

#ifdef SDB_DEV
#define SDB_UNREACHABLE()                          \
  do {                                             \
    SDB_ASSERT(false, "unreachable code reached"); \
    std::unreachable();                            \
  } while (false)
#else
#define SDB_UNREACHABLE() std::unreachable();
#endif

// likely/unlikely branch indicator
// macro definitions similar to the ones at
// https://kernelnewbies.org/FAQ/LikelyUnlikely
#if defined(__GNUC__) || defined(__GNUG__)
#define SDB_UNLIKELY(v) __builtin_expect(!!(v), 0)
#else
#define SDB_UNLIKELY(v) v
#endif

// pretty function name macro
#if defined(__clang__) || defined(__GNUC__)
#define SDB_PRETTY_FUNCTION __PRETTY_FUNCTION__
#else
#define SDB_PRETTY_FUNCTION __func__
#endif

// TODO(mkornaukhov) use `__builtin_trap()` for clang and gcc,
// but it makes recovery tests two times longer.
// immediate program termination
#define SDB_IMMEDIATE_ABORT() std::exit(1)
