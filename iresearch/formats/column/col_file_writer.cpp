////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "iresearch/formats/column/col_file_writer.hpp"

#include <absl/base/internal/endian.h>

#include <limits>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

constexpr byte_type kZeros[8]{};

}  // namespace

uint64_t ColFileWriter::Append(std::string_view bytes) {
  return Write(1, {&bytes, 1});
}

uint64_t ColFileWriter::AppendAligned(std::span<const std::string_view> parts) {
  return Write(8, parts);
}

uint64_t ColFileWriter::AppendString(std::string_view string) {
  SDB_ENSURE(string.size() <= std::numeric_limits<uint32_t>::max(),
             "string too long for overflow format");
  char length[sizeof(uint32_t)];
  absl::little_endian::Store32(length, static_cast<uint32_t>(string.size()));
  const std::string_view parts[] = {{length, sizeof(length)}, string};
  return Write(1, parts);
}

void ColFileWriter::Finish(format_utils::FooterWriter footer) {
  format_utils::WriteFooter(*_out, footer);
  _out.reset();
}

uint64_t ColFileWriter::Write(uint64_t alignment,
                              std::span<const std::string_view> parts) {
  if (const auto misalign = _out->Position() % alignment; misalign != 0) {
    _out->WriteData(kZeros, alignment - misalign);
  }
  const auto offset = _out->Position();
  for (const auto part : parts) {
    _out->WriteData(reinterpret_cast<const byte_type*>(part.data()),
                    part.size());
  }
  return offset;
}

}  // namespace irs
