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

#include <duckdb/common/serializer/write_stream.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <string_view>

#include "iresearch/formats/column/col_file_writer.hpp"

namespace irs {

inline uint64_t AppendAligned(ColFileWriter& out, std::string_view bytes) {
  return out.AppendAligned({&bytes, 1});
}

class WriteContext final {
 public:
  WriteContext(duckdb::BufferManager& buffers, ColFileWriter& blocks) noexcept
    : _buffers{&buffers}, _blocks{&blocks} {}

  WriteContext(const WriteContext&) = delete;
  WriteContext& operator=(const WriteContext&) = delete;

  duckdb::BufferManager& BufferManager() noexcept { return *_buffers; }
  ColFileWriter& Blocks() noexcept { return *_blocks; }

 private:
  duckdb::BufferManager* _buffers;
  ColFileWriter* _blocks;
};

}  // namespace irs
