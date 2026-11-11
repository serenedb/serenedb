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

#include <absl/strings/str_cat.h>
#include <fast_float/fast_float.h>

#include <array>
#include <system_error>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {
namespace {

struct SegmentFileKind {
  uint8_t kind;
  std::string_view ext;
};

constexpr std::array kSegmentFileKinds{
  SegmentFileKind{kColFile, kFormatExt},
  SegmentFileKind{kIdxFile, kIdxFormatExt},
  SegmentFileKind{kDocFile, PostingsWriter::kDocExt},
  SegmentFileKind{kPosFile, PostingsWriter::kPosExt},
  SegmentFileKind{kPayFile, PostingsWriter::kPayExt},
};

}  // namespace

uint8_t SegmentFileKinds(std::span<const std::string> files) noexcept {
  uint8_t kinds = 0;
  for (std::string_view file : files) {
    const auto dot = file.rfind('.');
    if (dot == std::string_view::npos) {
      continue;
    }
    const auto ext = file.substr(dot + 1);
    for (const auto& [kind, known] : kSegmentFileKinds) {
      if (ext == known) {
        kinds |= kind;
      }
    }
  }
  return kinds;
}

std::vector<std::string> SegmentFiles(std::string_view segment, uint8_t kinds) {
  std::vector<std::string> files;
  for (const auto& [kind, ext] : kSegmentFileKinds) {
    if ((kinds & kind) != 0) {
      files.push_back(FileName(segment, ext));
    }
  }
  return files;
}

std::string FileName(std::string_view prefix, uint64_t gen) {
  return absl::StrCat(prefix, gen);
}

void FileName(std::string& result, std::string_view name,
              std::string_view ext) {
  result.clear();
  absl::StrAppend(&result, name, ".", ext);
}

bool ParseSegmentId(std::string_view name, uint64_t& id) noexcept {
  if (name.size() < 2 || name.front() != '_') {
    return false;
  }
  const auto* begin = name.data() + 1;
  const auto* end = name.data() + name.size();
  const auto [ptr, ec] = fast_float::from_chars(begin, end, id);
  return ec == std::errc{} && (ptr == end || *ptr == '.');
}

}  // namespace irs
