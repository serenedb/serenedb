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

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <utility>

namespace sdb::coverage {

inline std::atomic<bool> gDebugVerifyEnabled = false;

[[noreturn]] void Fail(const char* what) {
  std::puts(what);
  std::abort();
}

}  // namespace sdb::coverage

#define DUCKDB_DEBUG_VERIFY_ENABLED() \
  ::sdb::coverage::gDebugVerifyEnabled.load(std::memory_order_relaxed)

#define DUCKDB_DEBUG_VERIFY_GUARD()                  \
  do {                                               \
    if (!DUCKDB_DEBUG_VERIFY_ENABLED()) [[likely]] { \
      return;                                        \
    }                                                \
  } while (false)

#define D_ASSERT(condition)              \
  do {                                   \
    if (!(condition)) [[unlikely]] {     \
      ::sdb::coverage::Fail(#condition); \
    }                                    \
  } while (false)

#define D_ASSERT_V(condition)                                         \
  do {                                                                \
    if (DUCKDB_DEBUG_VERIFY_ENABLED() && !(condition)) [[unlikely]] { \
      ::sdb::coverage::Fail(#condition);                              \
    }                                                                 \
  } while (false)

#define SDB_VERIFY(expr, ...)       \
  do {                              \
    if (!(expr)) [[unlikely]] {     \
      ::sdb::coverage::Fail(#expr); \
    }                               \
  } while (false)

#define SDB_ASSERT(expr, ...) SDB_VERIFY((expr), __VA_ARGS__)

#define SDB_UNREACHABLE() \
  do {                    \
    SDB_ASSERT(false);    \
    std::unreachable();   \
  } while (false)

namespace sdb::coverage {

int gSum = 0;

void Verify(int x) {
  DUCKDB_DEBUG_VERIFY_GUARD();
  gSum += x;
}

int Work(int x, int y) {
  if (DUCKDB_DEBUG_VERIFY_ENABLED()) {
    gSum += 2;
  }
  D_ASSERT(x > 0 || y > 0);
  D_ASSERT_V(x < 100 && y < 100);
  SDB_ASSERT(x != 7);
  if (x > 0 && y > 0) {
    gSum += 1;
  }
  switch (x) {
    case 1:
      return gSum;
    case 2:
      SDB_UNREACHABLE();
    default:
      std::unreachable();
  }
}

}  // namespace sdb::coverage

int main(int argc, char**) {
  sdb::coverage::Verify(argc);
  return sdb::coverage::Work(argc, argc) == 1 ? 0 : 1;
}
