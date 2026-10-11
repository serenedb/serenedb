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

#include "iresearch/formats/column/col_block_sink.hpp"

#include <limits>

#include "iresearch/store/data_output.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

void WriteBytes(IndexOutput& out, std::string_view bytes) {
  out.WriteData(reinterpret_cast<const byte_type*>(bytes.data()), bytes.size());
}

}  // namespace

uint64_t ColBlockSink::Position() {
  std::lock_guard lock{_mutex};
  return _out->Position();
}

uint64_t ColBlockSink::Append(std::string_view bytes) {
  std::lock_guard lock{_mutex};
  const uint64_t offset = _out->Position();
  WriteBytes(*_out, bytes);
  return offset;
}

uint64_t ColBlockSink::AppendAligned(std::span<const std::string_view> parts) {
  std::lock_guard lock{_mutex};
  if (const uint64_t misalign = _out->Position() % 8; misalign != 0) {
    static constexpr byte_type kPad[8]{};
    _out->WriteData(kPad, 8 - misalign);
  }
  const uint64_t offset = _out->Position();
  for (const auto part : parts) {
    WriteBytes(*_out, part);
  }
  return offset;
}

uint64_t ColBlockSink::AppendString(std::string_view string) {
  SDB_ENSURE(string.size() <= std::numeric_limits<uint32_t>::max(),
             "string too long for overflow format");
  std::lock_guard lock{_mutex};
  const uint64_t offset = _out->Position();
  _out->WriteU32(static_cast<uint32_t>(string.size()));
  WriteBytes(*_out, string);
  return offset;
}

}  // namespace irs
