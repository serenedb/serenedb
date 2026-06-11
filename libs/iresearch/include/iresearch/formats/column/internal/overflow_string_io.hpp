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

#pragma once

#include <absl/base/internal/endian.h>

#include <cstdint>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>

#include "basics/assert.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/types.hpp"

namespace irs {

class IndexOutputOverflowWriter final : public duckdb::OverflowStringWriter {
 public:
  explicit IndexOutputOverflowWriter(IndexOutput& out) noexcept : _out{&out} {}

  void WriteString(duckdb::UncompressedStringSegmentState& /*state*/,
                   duckdb::string_t string, duckdb::block_id_t& result_block,
                   int32_t& result_offset) final {
    result_block = static_cast<duckdb::block_id_t>(_out->Position());
    result_offset = 0;
    const auto len = string.GetSize();
    SDB_ENSURE(len <= std::numeric_limits<uint32_t>::max(), sdb::ERROR_INTERNAL,
               "string too long for overflow format");
    _out->WriteU32(len);
    _out->WriteBytes(reinterpret_cast<const byte_type*>(string.GetData()), len);
  }

  void Flush() final {}

 private:
  IndexOutput* _out;
};

class IndexInputOverflowReader final : public duckdb::OverflowStringReader {
 public:
  explicit IndexInputOverflowReader(IndexInput& in) noexcept : _in{&in} {}

  duckdb::string_t ReadString(duckdb::Vector& result, duckdb::block_id_t block,
                              int32_t offset) final {
    SDB_ASSERT(offset == 0);
    const auto pos = static_cast<uint64_t>(block);
    uint32_t len;
    _in->ReadBytes(pos, reinterpret_cast<byte_type*>(&len), sizeof(len));
    len = absl::little_endian::Load32(reinterpret_cast<byte_type*>(&len));
    if (const auto* body = _in->ReadStable(pos + sizeof(len), len)) {
      return duckdb::string_t{reinterpret_cast<const char*>(body), len};
    }
    auto out = duckdb::StringVector::EmptyString(result, len);
    _in->ReadBytes(pos + sizeof(len),
                   reinterpret_cast<byte_type*>(out.GetDataWriteable()), len);
    out.Finalize();
    return out;
  }

 private:
  IndexInput* _in;
};

}  // namespace irs
