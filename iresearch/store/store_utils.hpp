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

#include "data_input.hpp"
#include "data_output.hpp"
#include "iresearch/utils/attributes.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/number_utils.hpp"
#include "iresearch/utils/numeric_utils.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/std.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

IRS_FORCE_INLINE inline void WriteZV32(DataOutput& out, int32_t v) {
  out.WriteV32(irs::ZigZagEncode32(v));
}

int32_t ReadZV32(auto& in) { return irs::ZigZagDecode32(in.ReadV32()); }

template<typename StringType>
IRS_FORCE_INLINE inline void WriteStr(DataOutput& out, const StringType& str) {
  SDB_ASSERT(str.size() < std::numeric_limits<uint32_t>::max());
  out.WriteV32(static_cast<uint32_t>(str.size()));
  out.WriteData(reinterpret_cast<const byte_type*>(str.data()), str.size());
}

}  // namespace irs
