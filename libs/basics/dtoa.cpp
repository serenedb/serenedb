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

#include "basics/dtoa.h"

#include <absl/strings/numbers.h>

#include <charconv>
#include <cstdbool>
#include <cstring>

#include "basics/assert.h"
#include "basics/number_utils.h"

namespace sdb::basics {

template<typename Float>
char* dtoa_fast(Float d, char* buf) {  // NOLINT
  char* end = buf + kNumberStrMaxLen;
  if (std::signbit(d)) {
    d = -d;
    *buf++ = '-';
  }

  if (d < number_utils::Max<uint64_t>()) [[likely]] {
    const auto try_u = static_cast<uint64_t>(d);
    if (d == static_cast<Float>(try_u)) [[likely]] {
      return absl::numbers_internal::FastIntToBuffer(try_u, buf);
    }
  }

  auto res = std::to_chars(buf, end, d);
  SDB_ASSERT(res);
  return res.ptr;
}

template char* dtoa_fast(float, char* buf);
template char* dtoa_fast(double, char* buf);

}  // namespace sdb::basics
