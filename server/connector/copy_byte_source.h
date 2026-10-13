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

#include <cstdint>
#include <duckdb/common/file_system.hpp>

#include "server/utils/message_buffer.h"

namespace sdb::connector {

// Drain everything committed to `buffer` into `handle` as raw bytes, leaving
// the buffer empty for the next chunk. Shared by the binary (PGCOPY) and text
// COPY TO writers.
inline void DrainToHandle(message::Buffer& buffer, duckdb::FileHandle& handle) {
  auto chain = buffer.ReleaseChain();
  for (auto* chunk = chain.head; chunk; chunk = chunk->Next()) {
    const auto data = chunk->Data(chunk->GetEnd());
    if (!data.empty()) {
      handle.Write(const_cast<uint8_t*>(data.data()),
                   static_cast<duckdb::idx_t>(data.size()));
    }
  }
}

}  // namespace sdb::connector
