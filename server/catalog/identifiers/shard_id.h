////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2023-2023 ArangoDB GmbH, Cologne, Germany
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
#include <string_view>

#include "basics/result_or.h"

namespace sdb {

struct ShardID {
  static ResultOr<ShardID> shardIdFromString(std::string_view s);

  constexpr ShardID() noexcept = default;
  constexpr explicit ShardID(uint64_t id) noexcept : _id{id} {}
  explicit ShardID(std::string_view id);

  std::string toStr() const;

  friend auto operator<=>(const ShardID&, const ShardID&) noexcept = default;
  friend bool operator==(const ShardID&, const ShardID&) noexcept = default;

  bool operator==(std::string_view other) const;

  bool isValid() const noexcept {
    // We can never have ShardID 0. So we use it as invalid value.
    return _id != 0;
  }

  uint64_t id() const noexcept { return _id; }

  template<typename H>
  friend H AbslHashValue(H h, ShardID id) {
    return H::combine(std::move(h), id.id());
  }

 private:
  uint64_t _id = 0;
};

}  // namespace sdb
