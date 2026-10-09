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
#include <duckdb/storage/buffer_manager.hpp>
#include <string_view>
#include <vector>

namespace sdb::replication {

class SpillBuffer {
 public:
  explicit SpillBuffer(duckdb::BufferManager& buffers) : _buffers{buffers} {}

  size_t Size() const noexcept { return _size; }

  void Append(std::string_view message);
  void Truncate(size_t offset);

  class Reader {
   public:
    explicit Reader(SpillBuffer& buffer) : _buffer{buffer} {}

    bool Next(std::string_view& message);

   private:
    SpillBuffer& _buffer;
    duckdb::BufferHandle _pinned;
    size_t _block = 0;
    size_t _pos = 0;
  };

  Reader Read() {
    _tail = {};
    return Reader{*this};
  }

 private:
  struct Block {
    duckdb::shared_ptr<duckdb::BlockHandle> handle;
    size_t capacity = 0;
    size_t used = 0;
    size_t start = 0;
  };

  duckdb::BufferManager& _buffers;
  std::vector<Block> _blocks;
  duckdb::BufferHandle _tail;
  size_t _size = 0;
};

}  // namespace sdb::replication
