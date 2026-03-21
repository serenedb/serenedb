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

#include <cstdint>
#include <string>
#include <string_view>

#include "metrics/metric.h"

namespace sdb::metrics {

/**
 * Counter functionality
 */
class Counter final : public Metric {
 public:
  Counter(uint64_t n, std::string_view name, std::string_view help,
          std::string_view labels);
  ~Counter() final;

  [[nodiscard]] std::string_view type() const noexcept final;
  void toPrometheus(std::string& result, std::string_view globals,
                    bool ensure_whitespace) const final;

  [[nodiscard]] uint64_t load() const noexcept;
  void store(uint64_t n) noexcept;
  void count(uint64_t n = 1) noexcept;

  Counter& operator=(uint64_t n) noexcept;
  Counter& operator+=(uint64_t n) noexcept;
  Counter& operator++() noexcept;

  std::ostream& print(std::ostream& output) const;

 private:
  // TODO(mbkkt) implementation is pretty naive, better is using sharded
  // counter, It can be implemented per process via rcu, e.g. check userver
  mutable std::atomic_uint64_t _c;
  [[maybe_unused]] char _[ABSL_CACHELINE_SIZE - sizeof(std::atomic_uint64_t)];
  mutable std::atomic_uint64_t _b;
};

}  // namespace sdb::metrics
