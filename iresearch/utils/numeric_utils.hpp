////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <duckdb/common/radix.hpp>

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::numeric_utils {

template<typename T>
inline constexpr size_t kNumericTermSize = sizeof(T);

inline constexpr size_t kNumericTermMaxSize =
  kNumericTermSize<duckdb::uhugeint_t>;

template<typename T>
bytes_view EncodeNumericTerm(byte_type* buf, T value) {
  duckdb::Radix::EncodeData(buf, value);
  return {buf, kNumericTermSize<T>};
}

template<typename T>
T DecodeNumericTerm(bytes_view term) {
  SDB_ASSERT(term.size() == kNumericTermSize<T>);
  return duckdb::Radix::DecodeData<T>(term.data());
}

}  // namespace irs::numeric_utils
