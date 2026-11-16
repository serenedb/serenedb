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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <sdsl/sd_vector.hpp>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace irs {

class FmIndex {
 public:
  explicit FmIndex(std::span<const std::string> terms,
                   std::span<const size_t> group_starts);
  ~FmIndex();
  FmIndex(const FmIndex&) = delete;
  FmIndex& operator=(const FmIndex&) = delete;

  std::optional<std::vector<uint64_t>> Find(std::string_view pattern,
                                            size_t max_occurrences) const;
  size_t SizeInBytes() const;
  static uint64_t LiveBytes() noexcept {
    return _live_bytes.load(std::memory_order_relaxed);
  }

 private:
  struct Backend;
  std::unique_ptr<Backend> _backend;
  sdsl::sd_vector<> _starts;
  sdsl::sd_vector<>::rank_1_type _rank;
  size_t _bytes{0};
  inline static std::atomic<uint64_t> _live_bytes{0};
};

}  // namespace irs
