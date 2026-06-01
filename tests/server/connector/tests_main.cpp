////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

// Custom test entrypoint -- replaces the default gtest_main() so we can
// stand up a DuckDBEngine before any SDB_* macro fires (logger.h contract:
// gLogger is a plain pointer, not an atomic, and the hot path no longer
// null-checks it). Shutdown() runs after RUN_ALL_TESTS returns so the
// duckdb::DuckDB tears down BEFORE static dtors (BlockAllocator dtor reads
// a thread_local cache libc destroys before static dtors).

#include <gtest/gtest.h>

#include "basics/duckdb_engine.h"

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Bring up DuckDBEngine BEFORE RUN_ALL_TESTS so SDB_* macros that fire
  // inside test code dispatch through duckdb::LogManager (logger.h
  // contract: gLogger is a plain pointer, no nullptr check on the hot
  // path).
  //
  // Tear DuckDBEngine DOWN before main returns -- duckdb's BlockAllocator
  // dtor reads a thread_local cache that libc destroys *before* static
  // dtors, so any duckdb::DuckDB instance whose lifetime extends to the
  // static-dtor phase trips heap-use-after-free.
  sdb::DuckDBEngine::Instance().Initialize();
  const int rc = RUN_ALL_TESTS();
  sdb::DuckDBEngine::Instance().Shutdown();
  return rc;
}
