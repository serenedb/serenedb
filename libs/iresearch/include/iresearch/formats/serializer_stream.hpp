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

#include <cstring>
#include <duckdb/common/serializer/read_stream.hpp>
#include <duckdb/common/serializer/write_stream.hpp>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class IndexOutputWriteStream final : public duckdb::WriteStream {
 public:
  explicit IndexOutputWriteStream(IndexOutput& out) noexcept : _out{&out} {}

  void WriteData(duckdb::const_data_ptr_t buffer, duckdb::idx_t size) final {
    _out->WriteBytes(reinterpret_cast<const byte_type*>(buffer), size);
  }

 private:
  IndexOutput* _out;
};

class MemoryReadStream final : public duckdb::ReadStream {
 public:
  MemoryReadStream(const byte_type* data, uint64_t size) noexcept
    : _cur{data}, _end{data + size} {}

  void ReadData(duckdb::data_ptr_t buffer, duckdb::idx_t size) final {
    SDB_ENSURE(_cur + size <= _end, sdb::ERROR_INTERNAL,
               "short read in footer (need ", size, " bytes, ", (_end - _cur),
               " remaining)");
    std::memcpy(buffer, _cur, size);
    _cur += size;
  }

  void ReadData(duckdb::QueryContext, duckdb::data_ptr_t buffer,
                duckdb::idx_t size) final {
    ReadData(buffer, size);
  }

 private:
  const byte_type* _cur;
  const byte_type* _end;
};

}  // namespace irs
