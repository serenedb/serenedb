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

#include <velox/type/Type.h>

#include <cstdint>
#include <string_view>

#include "basics/fwd.h"

namespace sdb::pg {

velox::int128_t IntervalIn(std::string_view input, int32_t range,
                           int32_t precision);

std::string IntervalOut(const velox::int128_t& interval);

struct UnpackedInterval {
  int64_t time;  // microseconds
  int32_t day;
  int32_t month;

  UnpackedInterval& Negate() {
    time = -time;
    day = -day;
    month = -month;
    return *this;
  }
};

velox::int128_t PackInterval(const UnpackedInterval& interval);

UnpackedInterval UnpackInterval(velox::int128_t packed);

}  // namespace sdb::pg
