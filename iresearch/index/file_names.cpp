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

#include "file_names.hpp"

#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <charconv>
#include <system_error>

#include "iresearch/utils/shared.hpp"

namespace irs {

std::string FileName(std::string_view prefix, uint64_t gen) {
  return absl::StrCat(prefix, gen);
}

std::optional<uint64_t> SegmentNumber(std::string_view name) noexcept {
  if (name.size() < 2 || name.front() != '_') {
    return std::nullopt;
  }
  uint64_t number = 0;
  const auto* end = name.data() + name.size();
  const auto [ptr, ec] = std::from_chars(name.data() + 1, end, number);
  if (ec != std::errc{} || ptr != end) {
    return std::nullopt;
  }
  return number;
}

void FileName(std::string& result, std::string_view name,
              std::string_view ext) {
  result.clear();
  absl::StrAppend(&result, name, ".", ext);
}

std::string FileName(std::string_view name, uint64_t gen,
                     std::string_view ext) {
  return absl::StrCat(name, ".", gen, ".", ext);
}

bool ParseFileName(std::string_view file, std::string_view ext,
                   std::string_view& name, uint64_t& gen) noexcept {
  if (!absl::ConsumeSuffix(&file, ext) || !absl::ConsumeSuffix(&file, ".")) {
    return false;
  }

  const auto dot = file.rfind('.');

  if (dot == std::string_view::npos || dot == 0 ||
      !absl::SimpleAtoi(file.substr(dot + 1), &gen)) {
    return false;
  }

  name = file.substr(0, dot);
  return true;
}

}  // namespace irs
