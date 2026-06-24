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
#include <cstring>
#include <functional>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

#include "basics/message_chunk.h"
#include "basics/message_sequence_view.h"
#include "basics/shared.hpp"

namespace sdb::message {

// A detached, self-owning run of committed chunks -- released from one Buffer
// (ReleaseChain) and either spliced into another (SpliceCommitted) or handed to
// a consumer that decodes it and drops it. Sole owner of its chunks; moving the
// Chain moves the bytes, no copy and no shared refcount. `bytes` counts
// readable payload (chunks may be partially filled).
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

// The byte buffer's cold/consumer half. The producer side -- the per-message
// write fast path -- lives entirely inline in Writer (below); Buffer only owns
// the grow/flush/consume machinery (all out-of-line) and trivial getters, so
// the hot path never calls a Buffer method until a chunk fills or the flush
// threshold trips.
class Buffer {
 public:
  Buffer(size_t min_growth, size_t max_growth,
         size_t flush_size = std::numeric_limits<size_t>::max(),
         std::function<void(SequenceView)> flush_callback = {});

  // Monotonic count of bytes ever Commit()ed. Producer-side: read it only from
  // the producer; pair with an external written-bytes counter to track
  // committed-but-unsent bytes for drain/backpressure.
  size_t TotalCommitted() const { return _total_committed; }

  // Publish committed-but-unflushed bytes to the consumer (the producer's
  // KickSend after it finishes a batch of messages).
  void Flush();

  // Single-owner producer op: detaches everything committed so far as a Chain
  // and resets this buffer to a fresh empty chunk. No uncommitted bytes may be
  // outstanding.
  Chain ReleaseChain();

  // Single-owner producer op (the splice counterpart of ReleaseChain): appends
  // a donor chain's chunks to the writable end as already-committed bytes --
  // O(1), no copy. Triggers the flush machinery like a Writer Commit().
  void SpliceCommitted(Chain&& chain, bool need_flush);

  void FlushDone();

  void FlushStart();

  // Receive-channel ops. The consumer ops below snapshot the atomic watermark
  // (published by the producer's CommitWrite) instead of reading its live
  // _tail/_end, so the producer and consumer may run on different threads.
  std::span<uint8_t> Reserve(size_t min_capacity);
  void CommitWrite(size_t size);
  bool Readable() noexcept {
    RefreshReadable();
    return _consumed.chunk != nullptr &&
           !(_head == _consumed.chunk &&
             _head->GetBegin() == _consumed.in_chunk);
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
  friend class Writer;

  // Writer cold paths (out-of-line so the inlined producer stays tiny). GrowFor
  // adds a chunk holding `n` contiguous bytes and returns the new tail.
  // AppendSpanning writes `data` that overflowed the tail chunk (fill tail,
  // grow, write rest). Rewind drops chunks grown since a checkpoint (the dtor's
  // uncommitted path).
  Chunk* GrowFor(size_t n);
  void AppendSpanning(std::string_view data);
  void Rewind(Chunk* mark_chunk, size_t mark_end);
  void Grow(size_t capacity);

  Chunk* CreateChunk(size_t size) const;

  void SendData() const;

  void FreeTill(const Chunk* end);

  void RefreshReadable() noexcept {
    _consumed = _committed.load(std::memory_order_acquire);
  }

  std::function<void(SequenceView)> _flush_callback;
  const size_t _flush_size;
  size_t _volatile_size{0};
  size_t _total_committed{0};

  const size_t _max_growth;
  mutable size_t _growth;
  // The producer's published watermark -- the committed end the consumer may
  // advance to. A buffer is only ever one direction, so the two channels share
  // it: on a send buffer it is the flush-ownership token
  // (FlushStart/FlushDone); on a recv buffer it is the read watermark
  // (CommitWrite). Published by the producer, snapshot by the consumer.
  std::atomic<BufferOffset> _committed;
  // The consumer's position: the in-flight socket-write boundary on a send
  // buffer, the watermark snapshot on a recv buffer.
  BufferOffset _consumed;

  Chunk* _head{nullptr};
  Chunk* _tail{nullptr};
};

// RAII handle for writing one message into a Buffer, and the ONLY producer-side
// write surface. The per-message fast paths are inline here and operate
// directly on the buffer's tail chunk -- they never enter a Buffer method on
// the common (room-in-chunk) path; only growing a chunk or crossing the flush
// threshold tail-calls into Buffer's out-of-line slow paths.
//
// It also holds a checkpoint of the buffer position at construction; if it is
// destroyed without Commit (a serializer threw partway), the dtor rewinds the
// buffer so no half-written message is left behind to desync the client. After
// Commit the handle is spent (its Buffer* is nulled, which is also the
// not-yet-committed flag).
class Writer {
 public:
  IRS_FORCE_INLINE explicit Writer(Buffer& buffer)
    : _buffer{&buffer},
      _mark_chunk{buffer._tail},
      _mark_end{buffer._tail->GetEnd()} {}

  Writer(const Writer&) = delete;
  Writer& operator=(const Writer&) = delete;
  Writer(Writer&& other) noexcept
    : _buffer{std::exchange(other._buffer, nullptr)},
      _mark_chunk{other._mark_chunk},
      _mark_end{other._mark_end},
      _written{other._written} {}
  Writer& operator=(Writer&&) = delete;

  ~Writer() {
    if (_buffer) [[unlikely]] {
      _buffer->Rewind(_mark_chunk, _mark_end);
    }
  }

  // String literal: the length is the compile-time array size minus the NUL, so
  // the shared store below folds to a fixed-size store (no strlen, no memmove
  // dispatch). Prefer it over a char literal for structural bytes (Write("(")).
  template<size_t N>
  IRS_FORCE_INLINE void Write(const char (&lit)[N]) {
    static_assert(N > 1, "Write() of an empty string literal is a no-op");
    WriteBytes(lit, N - 1);
  }

  // Runtime bytes.
  IRS_FORCE_INLINE void Write(std::string_view data) {
    WriteBytes(data.data(), data.size());
  }

  // Reserve exactly `n` contiguous bytes; the caller writes all of them.
  [[nodiscard]] IRS_FORCE_INLINE uint8_t* Alloc(size_t n) {
    auto* tail = GrabTail(n);
    auto* p = tail->WritableData() + tail->GetEnd();
    tail->AdjustEnd(n);
    _written += n;
    return p;
  }

  // Reserve `capacity`, let `op` fill it and return the bytes actually written.
  template<typename Op>
  IRS_FORCE_INLINE void Write(size_t capacity, Op op) {
    static_assert(std::is_invocable_r_v<size_t, Op, uint8_t*>,
                  "op must take the write cursor and return the bytes written");
    auto* tail = GrabTail(capacity);
    const size_t n = std::move(op)(tail->WritableData() + tail->GetEnd());
    tail->AdjustEnd(n);
    _written += n;
  }

  // Bytes written into this message so far (since the checkpoint).
  size_t Written() const { return _written; }

  // Publish this message; returns the byte count committed. The handle is spent
  // afterwards (the nulled Buffer* is the committed flag -- no rewind).
  IRS_FORCE_INLINE size_t Commit(bool need_flush) {
    auto* buf = std::exchange(_buffer, nullptr);
    buf->_total_committed += _written;
    buf->_volatile_size += _written;
    if (need_flush || buf->_volatile_size >= buf->_flush_size) [[unlikely]] {
      buf->FlushStart();
    }
    return _written;
  }

 private:
  // Shared body for fixed- and runtime-length byte writes. Holds the tail chunk
  // in a register local across the store: the store goes through a uint8_t*
  // (the universal aliasing type), so re-reading _buffer->_tail from memory
  // afterward would force the compiler to reload both _buffer and _tail.
  // Keeping the pointer in a local sidesteps that. Overflow tail-calls the
  // out-of-line spanning path (which may split the bytes across two chunks --
  // callers that need them contiguous use Alloc/Write(capacity, op) instead).
  IRS_FORCE_INLINE void WriteBytes(const char* src, size_t n) {
    auto* tail = _buffer->_tail;
    _written += n;
    if (n <= tail->FreeSpace()) [[likely]] {
      std::memcpy(tail->WritableData() + tail->GetEnd(), src, n);
      tail->AdjustEnd(n);
    } else {
      _buffer->AppendSpanning({src, n});
    }
  }

  // Ensure the tail chunk has `n` contiguous writable bytes (grow a fresh chunk
  // only when it is full) and return it. Returning the chunk -- not just the
  // cursor -- lets the caller advance the end through the same register local,
  // so an aliasing store between the reserve and the advance cannot force a
  // reload of _buffer->_tail.
  [[nodiscard]] IRS_FORCE_INLINE Chunk* GrabTail(size_t n) {
    auto* tail = _buffer->_tail;
    if (n <= tail->FreeSpace()) [[likely]] {
      return tail;
    }
    return _buffer->GrowFor(n);
  }

  Buffer* _buffer;
  Chunk* _mark_chunk;
  size_t _mark_end;
  size_t _written = 0;
};

}  // namespace sdb::message
