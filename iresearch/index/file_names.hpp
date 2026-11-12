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
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <span>
#include <string>
#include <vector>

#include "iresearch/utils/string.hpp"

namespace irs {

inline constexpr uint8_t kColFile = 1 << 0;
inline constexpr uint8_t kIdxFile = 1 << 1;
inline constexpr uint8_t kDocFile = 1 << 2;
inline constexpr uint8_t kPosFile = 1 << 3;
inline constexpr uint8_t kPayFile = 1 << 4;

uint8_t SegmentFileKinds(std::span<const std::string> files) noexcept;

std::vector<std::string> SegmentFiles(std::string_view segment, uint8_t kinds);

std::string FileName(uint64_t id);

// Returns string in the following format : {name}.{ext}
void FileName(std::string& out, std::string_view name, std::string_view ext);

inline std::string FileName(std::string_view name, std::string_view ext) {
  std::string result;
  FileName(result, name, ext);
  return result;
}

bool ParseSegmentId(std::string_view name, uint64_t& id) noexcept;

}  // namespace irs
