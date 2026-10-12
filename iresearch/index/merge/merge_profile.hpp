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

#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace irs {

enum class MergeStep : uint8_t {
  Prepare = 0,
  Norms,
  Columns,
  ColumnsCommit,
  Ann,
  Postings,
  Commit,
};

inline constexpr size_t kMergeStepCount = 7;

constexpr std::string_view MergeStepName(MergeStep step) noexcept {
  constexpr std::array<std::string_view, kMergeStepCount> kNames{
    "prepare", "norms",    "columns", "columns_commit",
    "ann",     "postings", "commit",
  };
  return kNames[static_cast<size_t>(step)];
}

class MergeProfile {
 public:
  MergeProfile() = default;
  MergeProfile(const MergeProfile& other) noexcept { CopyFrom(other); }
  MergeProfile& operator=(const MergeProfile& other) noexcept {
    CopyFrom(other);
    return *this;
  }

  void Add(MergeStep step, uint64_t nanos) noexcept {
    _nanos[static_cast<size_t>(step)].fetch_add(nanos,
                                                std::memory_order_relaxed);
  }

  void SetWallNanos(uint64_t nanos) noexcept {
    _wall.store(nanos, std::memory_order_relaxed);
  }

  uint64_t Nanos(MergeStep step) const noexcept {
    return _nanos[static_cast<size_t>(step)].load(std::memory_order_relaxed);
  }

  uint64_t TotalNanos() const noexcept {
    if (const auto wall = _wall.load(std::memory_order_relaxed); wall != 0) {
      return wall;
    }
    uint64_t total = 0;
    for (const auto& nanos : _nanos) {
      total += nanos.load(std::memory_order_relaxed);
    }
    return total;
  }

 private:
  void CopyFrom(const MergeProfile& other) noexcept {
    for (size_t step = 0; step != kMergeStepCount; ++step) {
      _nanos[step].store(other._nanos[step].load(std::memory_order_relaxed),
                         std::memory_order_relaxed);
    }
    _wall.store(other._wall.load(std::memory_order_relaxed),
                std::memory_order_relaxed);
  }

  std::array<std::atomic<uint64_t>, kMergeStepCount> _nanos{};
  std::atomic<uint64_t> _wall{0};
};

class MergeStepClock {
 public:
  explicit MergeStepClock(MergeProfile& profile) noexcept
    : _profile{profile}, _last{std::chrono::steady_clock::now()} {}

  void Lap(MergeStep step) noexcept {
    const auto now = std::chrono::steady_clock::now();
    _profile.Add(
      step, static_cast<uint64_t>(
              std::chrono::duration_cast<std::chrono::nanoseconds>(now - _last)
                .count()));
    _last = now;
  }

 private:
  MergeProfile& _profile;
  std::chrono::steady_clock::time_point _last;
};

struct MergeStats {
  std::vector<std::string> inputs{};
  uint64_t input_docs = 0;
  uint64_t input_live_docs = 0;
  uint64_t input_bytes = 0;
  std::string output{};
  uint64_t output_docs = 0;
  uint64_t output_bytes = 0;
  MergeProfile profile{};
};

}  // namespace irs
