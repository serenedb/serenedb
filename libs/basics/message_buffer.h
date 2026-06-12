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

  // Monotonic count of bytes ever Commit()ed. Producer-side: read it only from
  // the producer; pair with an external written-bytes counter to track
  // committed-but-unsent bytes for drain/backpressure.
  size_t TotalCommitted() const { return _total_committed; }

  // Detached chain of committed chunks, ready to be spliced into another
  // buffer. `bytes` counts readable payload (chunks may be partially filled).
  struct Chain {
    Chunk* head = nullptr;
    Chunk* tail = nullptr;
    size_t bytes = 0;

    ~Chain() {
      auto* chunk = head;
      while (chunk != nullptr) {
        delete std::exchange(chunk, chunk->Next());
      }
    }
    Chain() = default;
    Chain(Chunk* head_p, Chunk* tail_p, size_t bytes_p)
      : head{head_p}, tail{tail_p}, bytes{bytes_p} {}
    Chain(Chain&& other) noexcept
      : head{std::exchange(other.head, nullptr)},
        tail{std::exchange(other.tail, nullptr)},
        bytes{std::exchange(other.bytes, 0)} {}
    Chain& operator=(Chain&& other) noexcept {
      std::swap(head, other.head);
      std::swap(tail, other.tail);
      std::swap(bytes, other.bytes);
      return *this;
    }

    void Append(Chain&& other) {
      if (other.head == nullptr) {
        return;
      }
      if (head == nullptr) {
        *this = std::move(other);
        return;
      }
      tail->AppendChunk(other.head);
      tail = std::exchange(other.tail, nullptr);
      other.head = nullptr;
      bytes += std::exchange(other.bytes, 0);
    }
  };

  // Single-owner producer op: detaches everything committed so far as a Chain
  // and resets this buffer to a fresh empty chunk. No uncommitted bytes may be
  // outstanding.
  Chain ReleaseChain() {
    SDB_ASSERT(_uncommitted_size == 0);
    const size_t bytes = std::exchange(_volatile_size, 0);
    if (bytes == 0) {
      return {};
    }
    auto* head = std::exchange(_head, nullptr);
    auto* tail = std::exchange(_tail, nullptr);
    _head = _tail = CreateChunk(0);
    return Chain{head, tail, bytes};
  }

  // Single-owner producer op (the splice counterpart of ReleaseChain): appends
  // a donor chain's chunks to the writable end as already-committed bytes --
  // O(1), no copy. Triggers the flush machinery like Commit().
  void SpliceCommitted(Chain&& chain, bool need_flush) {
    SDB_ASSERT(_uncommitted_size == 0);
    if (chain.head == nullptr) {
      return;
    }
    _tail->AppendChunk(chain.head);
    _tail = chain.tail;
    chain.head = nullptr;
    chain.tail = nullptr;
    _total_committed += chain.bytes;
    _volatile_size += std::exchange(chain.bytes, 0);
    if (need_flush || _volatile_size >= _flush_size) {
      FlushStart();
    }
  }

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

  // Receive-channel half: a producer (e.g. the io reader) appends raw bytes
  // via Reserve/CommitWrite; CommitWrite publishes an atomic watermark. The
  // consumer-side operations below (Readable/Front/ReadableSize/ReadableView/
  // Consume) bound themselves by a snapshot of that watermark, never by the
  // producer's _tail/_end, so a DIFFERENT thread may consume concurrently --
  // the mirror of the send-channel _send_end discipline. Single-owner users
  // are unaffected (the watermark is simply always current).
  std::span<uint8_t> Reserve(size_t min_capacity);
  void CommitWrite(size_t size);
  bool Readable() noexcept {
    RefreshReadable();
    return _readable.chunk != nullptr &&
           !(_head == _readable.chunk &&
             _head->GetBegin() == _readable.in_chunk);
  }
  std::string_view Front() noexcept;
  void Consume(size_t size);

  size_t ReadableSize() noexcept;
  SequenceView ReadableView(size_t length) noexcept;

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

  void RefreshReadable() noexcept {
    _readable = _read_end.load(std::memory_order_acquire);
  }

  const size_t _flush_size;
  size_t _volatile_size{0};
  size_t _uncommitted_size{0};
  size_t _total_committed{0};

  const size_t _max_growth;
  mutable size_t _growth;
  // _socket_end - the end of socket buffer
  BufferOffset _socket_end;
  // _send_end - the end of must-send buffer
  std::atomic<BufferOffset> _send_end;
  // Receive-channel watermark: published by CommitWrite (producer), snapshot
  // into _readable (consumer-owned) by the consumer-side operations.
  std::atomic<BufferOffset> _read_end;
  BufferOffset _readable;

  Chunk* _head{nullptr};
  Chunk* _tail{nullptr};
};

}  // namespace sdb::message
