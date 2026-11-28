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

#include <array>
#include <bit>
#include <cstdint>
#include <string>
#include <vector>

#include "iresearch/utils/string.hpp"

namespace irs {

inline constexpr uint8_t kColFile = 1 << 0;
inline constexpr uint8_t kIdxFile = 1 << 1;
inline constexpr uint8_t kDocFile = 1 << 2;
inline constexpr uint8_t kPosFile = 1 << 3;
inline constexpr uint8_t kPayFile = 1 << 4;
inline constexpr size_t kFileKinds = 5;

constexpr size_t KindIndex(uint8_t kind) noexcept {
  return static_cast<size_t>(std::countr_zero(kind));
}

struct FileSizes {
  bool operator==(const FileSizes&) const = default;

  uint64_t file = 0;
  uint32_t footer = 0;
};

using SegmentFileSizes = std::array<FileSizes, kFileKinds>;

uint64_t SegmentBytes(const SegmentFileSizes& files) noexcept;

std::vector<std::string> SegmentFiles(std::string_view segment, uint8_t kinds);

std::string FileName(uint64_t id);

std::string FileName(std::string_view name, std::string_view ext);

bool ParseSegmentId(std::string_view name, uint64_t& id) noexcept;

}  // namespace irs
