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

#include <duckdb/storage/buffer_manager.hpp>
#include <string_view>

#include "iresearch/store/data_output.hpp"

namespace irs {

inline uint64_t AppendAligned(IndexOutput& out, std::string_view bytes) {
  constexpr byte_type kZeros[8]{};
  if (const auto misalign = out.Position() % sizeof(kZeros); misalign != 0) {
    out.WriteData(kZeros, sizeof(kZeros) - misalign);
  }
  const auto offset = out.Position();
  out.WriteData(reinterpret_cast<const byte_type*>(bytes.data()), bytes.size());
  return offset;
}

class WriteContext final {
 public:
  WriteContext(duckdb::BufferManager& buffers, IndexOutput& out) noexcept
    : _buffers{&buffers}, _out{&out} {}

  WriteContext(const WriteContext&) = delete;
  WriteContext& operator=(const WriteContext&) = delete;

  duckdb::BufferManager& BufferManager() noexcept { return *_buffers; }
  IndexOutput& Out() noexcept { return *_out; }

 private:
  duckdb::BufferManager* _buffers;
  IndexOutput* _out;
};

}  // namespace irs
