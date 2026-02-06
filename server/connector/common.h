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

#pragma once
#include <velox/common/memory/HashStringAllocator.h>

#include <cstdint>
#include <string_view>

#include "basics/bit_utils.hpp"
#include "basics/fwd.h"
#include "catalog/table_options.h"

namespace sdb::connector {

struct ColumnInfo {
  catalog::Column::Id id;
  std::string_view name;
};

enum class ValueFlags : uint8_t {
  None = 0,
  HaveNulls = 1 << 0,
  HaveLength = 1 << 1,
  Constant = 1 << 2,
  FlatMap = 1 << 3,
};

ENABLE_BITMASK_ENUM(ValueFlags);

// TODO(Dronplane) unify with key?
inline constexpr std::string_view kStringPrefix{"\0", 1};
inline constexpr std::string_view kTrueValue{"\1", 1};
inline constexpr std::string_view kFalseValue{"\0", 1};

static_assert(
  sizeof(ValueFlags) == 1,
  "ValueFlags should be one byte. If need more adjust writing/reading");

template<typename T>
using ManagedVector = std::vector<T, velox::memory::StlAllocator<T>>;

}  // namespace sdb::connector
