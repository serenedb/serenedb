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

#include <absl/strings/str_cat.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <thread>
#include <vector>

#include "search/inverted_index_storage.h"

namespace sdb::search {
namespace {

TEST(SearchStorageDir, CreationSurvivesConcurrentRemovalOfEmptiedParents) {
  const auto root = std::filesystem::temp_directory_path() /
                    absl::StrCat("search_storage_dir_", ::getpid());
  std::filesystem::remove_all(root);
  constexpr int kThreads = 8;
  constexpr int kRounds = 2000;
  std::atomic<int> failures = 0;
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t] {
      for (int round = 0; round < kRounds; ++round) {
        const auto dir =
          root / "schema" / "table" / absl::StrCat(t, "_", round);
        std::error_code ec;
        CreateStorageDir(dir, ec);
        if (ec || !std::filesystem::is_directory(dir)) {
          failures.fetch_add(1, std::memory_order_relaxed);
          continue;
        }
        RemoveStorageDir(dir, 2);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(failures.load(), 0);
  std::filesystem::remove_all(root);
}

}  // namespace
}  // namespace sdb::search
