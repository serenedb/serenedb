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

#include <absl/functional/function_ref.h>

#include <duckdb/common/constants.hpp>

#include "iresearch/formats/flush_state.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs::format_utils {

inline constexpr uint64_t kTrailerLen = 2 * sizeof(uint32_t);

struct Footer {
  uint64_t data_len = 0;
  uint32_t data_expected_crc32c = 0;
};

void WriteFooter(IndexOutput& out,
                 absl::FunctionRef<void(duckdb::BinarySerializer&)> write);

Footer ReadFooter(
  IndexInput& in, std::string_view name,
  absl::FunctionRef<void(duckdb::BinaryDeserializer&, uint64_t)> read);

void PrepareOutput(std::string& str, IndexOutput::ptr& out,
                   const FlushState& state, std::string_view ext);

}  // namespace irs::format_utils
