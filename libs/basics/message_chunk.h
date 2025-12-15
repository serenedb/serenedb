////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <span>
#include <string_view>

#include "basics/assert.h"

namespace sdb::message {

class Chunk {
 public:
  static Chunk* Create(size_t capacity) {
    SDB_ASSERT(capacity != 0);
    return new Chunk{capacity};
  }

  Chunk* AppendChunk(Chunk* chunk) {
    SDB_ASSERT(!_next);
    return _next = chunk;
  }

  void SetBegin(size_t begin) {
    SDB_ASSERT(_begin <= _capacity);
    _begin = begin;
    // _begin <= _end, but we cannot touch _end without data race
  }

  size_t GetBegin() const { return _begin; }
  size_t GetEnd() const { return _end; }
  size_t GetCapacity() const { return _capacity; }

  void AdjustEnd(size_t size) {
    _end += size;
    SDB_ASSERT(_end <= _capacity);
  }

  size_t FreeSpace() const { return _capacity - _end; }

  size_t Size() const { return _end - _begin; }

  size_t TryWrite(std::string_view data);

  Chunk* Next() const { return _next; }

  void FreeData() {
    SDB_ASSERT(_data);
    ::operator delete(_data, _capacity);
    _data = nullptr;
  }

  std::span<const uint8_t> Data(size_t end) const {
    return {_data + _begin, end - _begin};
  }
  uint8_t* WritableData() { return _data; }

  ~Chunk() {
    if (_data) {
      ::operator delete(_data, _capacity);
    }
  }

 private:
  Chunk(size_t capacity);

  uint8_t* _data;
  size_t _begin{0};
  size_t _end{0};
  size_t _capacity;
  Chunk* _next{nullptr};
};

struct BufferOffset {
  const Chunk* chunk = nullptr;
  size_t in_chunk = 0;

  bool operator==(const BufferOffset& other) const = default;

  explicit operator bool() const { return chunk != nullptr; }
};

}  // namespace sdb::message
