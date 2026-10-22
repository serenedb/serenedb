////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "tests_param.hpp"

#include <absl/strings/str_cat.h>

#include <atomic>
#include <filesystem>
#include <iresearch/utils/duckdb_engine.hpp>

namespace tests {
namespace {

constinit std::atomic<uint64_t> gLocalDirectories{0};

}  // namespace

std::string ToString(dir_generator_f generator) {
  if (generator == &MemoryDirectory) {
    return "memory";
  }

  if (generator == &LocalDirectory) {
    return "local";
  }

  if (generator == &ObjectStoreDirectory) {
    return "sim";
  }

  return "unknown";
}

std::shared_ptr<irs::IndexFiles> MemoryDirectory(const TestBase* test) {
  return std::make_shared<irs::IndexFiles>(
    irs::DuckDBEngine::Instance().instance(), irs::MemoryIndexRoot(),
    test ? test->GetResourceManager().options
         : irs::ResourceManagementOptions::gDefault);
}

std::shared_ptr<irs::IndexFiles> LocalDirectory(const TestBase* test) {
  if (!test) {
    return nullptr;
  }
  auto path =
    test->test_dir() / absl::StrCat("index_", gLocalDirectories.fetch_add(
                                                1, std::memory_order_relaxed));
  std::filesystem::create_directories(path);
  auto* files =
    new irs::IndexFiles{irs::DuckDBEngine::Instance().instance(), path.string(),
                        test->GetResourceManager().options};
  return {files, [path = std::move(path)](irs::IndexFiles* p) {
            delete p;
            std::filesystem::remove_all(path);
          }};
}

std::shared_ptr<irs::IndexFiles> ObjectStoreDirectory(const TestBase* test) {
  if (!test) {
    return nullptr;
  }
  ObjectStore();
  auto root =
    absl::StrCat(irs::SimulatedObjectStore::kPrefix, "tests/",
                 gLocalDirectories.fetch_add(1, std::memory_order_relaxed));
  auto* files = new irs::IndexFiles{irs::DuckDBEngine::Instance().instance(),
                                    root, test->GetResourceManager().options};
  return {files, [root = std::move(root)](irs::IndexFiles* p) {
            delete p;
            ObjectStore().RemoveDirectory(root);
          }};
}

irs::SimulatedObjectStore& ObjectStore() {
  static auto& store = irs::SimulatedObjectStore::Register(
    irs::DuckDBEngine::Instance().instance());
  return store;
}

}  // namespace tests
