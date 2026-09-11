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

// Residual helpers pinned alive by tests/libs/iresearch/search/search_bench_-
// test.cpp. New code should NOT add callers here -- use std::filesystem /
// std::getenv / openssl direct instead. Once that search-bench is cleaned
// up this header / .cpp pair goes away entirely.

#include <cstddef>
#include <string>

namespace sdb {

bool SlurpFile(const char* filename, std::string& result);

bool SlurpGzipFile(const char* filename, std::string& result);

struct Sha256Functor {
  Sha256Functor();
  ~Sha256Functor();

  bool operator()(const char* data, size_t size) noexcept;

  std::string Finalize();

 private:
  void* _context;
};

}  // namespace sdb
