////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// @author Valerii Mironov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/store/memory_directory.hpp>
#include <random>
#include <thread>
#include <vector>

static constexpr uint64_t kThreads = 8;
static constexpr uint64_t kFiles = 1000;

static void WriteFile(uint64_t size) {
  irs::MemoryFile file{irs::IResourceManager::gNoop};
  irs::MemoryIndexOutput output{file};
  for (uint64_t i = 0; i != size; ++i) {
    output.WriteByte(42);
  }
}

int main() {
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (uint64_t i = 0; i != kThreads; ++i) {
    threads.emplace_back([i] {
      std::mt19937_64 rng{43 * i};
      std::uniform_int_distribution<uint64_t> file_size_power{8, 24};
      for (uint64_t i = 0; i != kFiles; ++i) {
        const auto size = uint64_t{1} << file_size_power(rng);
        WriteFile(size);
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
}
