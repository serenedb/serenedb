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

#include <atomic>
#include <cstdint>
#include <functional>
#include <span>
#include <string_view>

#include "basics/message_chunk.h"
#include "basics/message_sequence_view.h"

namespace sdb::message {

class Buffer {
 public:
  Buffer(size_t min_growth, size_t max_growth,
         size_t flush_size = std::numeric_limits<size_t>::max(),
         std::function<void(SequenceView)> send_callback = {});

  void WriteUncommitted(std::string_view data);

  void Write(std::string_view data, bool need_flush) {
    WriteUncommitted(data);
    Commit(need_flush);
  }

  size_t GetUncommittedSize() const { return _uncommitted_size; }

  void FlushDone();

  void FlushStart();

  /// Returns a pointer to a contiguous buffer with exactly `capacity` bytes.
  /// The caller MUST write exactly `capacity` bytes to the returned buffer.
  [[nodiscard]] uint8_t* GetContiguousData(size_t capacity);

  /// Allocates a contiguous buffer with `capacity` bytes and invokes `op` to
  /// write to it.
  /// `op` receives a pointer to the buffer and MUST return the actual number of
  /// bytes written. The buffer is resized to match the number of bytes actually
  /// written by `op`.
  template<typename Op>
  void WriteContiguousData(size_t capacity, Op op) {
    auto* writable_data = AllocateContiguousData(capacity);
    const auto used = std::move(op)(writable_data);
    _tail->AdjustEnd(used);
    _uncommitted_size += used;
  }

  void Commit(bool need_flush);

  std::span<uint8_t> Reserve(size_t min_capacity);
  void CommitWrite(size_t size);
  bool Readable() const noexcept {
    return _head != _tail || _head->Size() != 0;
  }
  std::string_view Front() const noexcept;
  void Consume(size_t size);

  size_t ReadableSize() const noexcept;
  SequenceView ReadableView(size_t length) const noexcept;

  SequenceView Written() const noexcept {
    return SequenceView{BufferOffset{_head, _head->GetBegin()},
                        BufferOffset{_tail, _tail->GetEnd()}};
  }
  void Clear() noexcept;

  ~Buffer() {
    SDB_ASSERT(!_tail->Next());
    FreeTill(nullptr);
  }

 private:
  Chunk* CreateChunk(size_t size) const;

  void SendData() const;

  void FreeTill(const Chunk* end);

  [[nodiscard]] uint8_t* AllocateContiguousData(size_t capacity);

  std::function<void(SequenceView)> _send_callback;

  const size_t _flush_size;
  size_t _volatile_size{0};
  size_t _uncommitted_size{0};

  const size_t _max_growth;
  mutable size_t _growth;
  // _socket_end - the end of socket buffer
  BufferOffset _socket_end;
  // _send_end - the end of must-send buffer
  std::atomic<BufferOffset> _send_end;

  Chunk* _head{nullptr};
  Chunk* _tail{nullptr};
};

}  // namespace sdb::message
