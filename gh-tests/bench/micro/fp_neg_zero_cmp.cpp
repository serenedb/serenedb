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

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

float ReplaceEq0Float(float x) {
  if (x == 0) {
    return 0;
  }
  return x;
}

double ReplaceEq0Double(double x) {
  if (x == 0) {
    return 0;
  }
  return x;
}

float ReplaceShiftFloat(float x) {
  if ((std::bit_cast<uint32_t>(x) << 1) == 0) {
    return 0;
  }
  return x;
}

double ReplaceShiftDouble(double x) {
  if ((std::bit_cast<uint64_t>(x) << 1) == 0) {
    return 0;
  }
  return x;
}

float ReplaceBitwiseCmpFloat(float x) {
  if (std::bit_cast<int32_t>(x) == std::numeric_limits<int32_t>::min()) {
    return 0;
  }
  return x;
}

double ReplaceBitwiseDouble(double x) {
  if (std::bit_cast<int64_t>(x) == std::numeric_limits<int64_t>::min()) {
    return 0;
  }
  return x;
}

int main() {
  constexpr std::size_t kCount = 100'000'000;
  constexpr std::size_t kSeed = 42;
  std::random_device rd;
  std::mt19937 gen(kSeed);
  std::uniform_real_distribution<double> dist(-1000.0, 1000.0);

  std::vector<double> doubles;
  doubles.reserve(kCount);

  for (std::size_t i = 0; i < kCount; ++i) {
    doubles.push_back(dist(gen));
  }

  std::vector<size_t> indices(doubles.size());
  std::iota(indices.begin(), indices.end(), 0);

  std::mt19937 g(kSeed);
  std::shuffle(indices.begin(), indices.end(), g);

  auto replace_func = ReplaceShiftDouble;

  double prev = 0;
  for (size_t idx : indices) {
    prev += replace_func(doubles[idx]);
  }

  auto start = std::chrono::high_resolution_clock::now();
  for (size_t idx : indices) {
    doubles[idx] = replace_func(doubles[idx]);
  }
  auto end = std::chrono::high_resolution_clock::now();
  auto duration =
    std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
  std::cout << "Elapsed: " << duration.count() << "ms\n";
  std::cout << "Sum: " << std::accumulate(doubles.begin(), doubles.end(), 0)
            << std::endl;
  std::cout << "Prev: " << prev << std::endl;
  return 0;
}
