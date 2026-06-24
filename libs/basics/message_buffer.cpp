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

#include "basics/message_buffer.h"

#include "basics/assert.h"

namespace sdb::message {

Buffer::Buffer(size_t min_growth, size_t max_growth, size_t flush_size,
               std::function<void(SequenceView)> flush_callback)
  : _flush_callback{std::move(flush_callback)},
    _flush_size{flush_size},
    _max_growth{max_growth},
    _growth{min_growth} {
  _head = _tail = CreateChunk(0);
}

void Buffer::FlushStart() {
  SDB_ASSERT(_tail);
  const BufferOffset tail_offset{_tail, _tail->GetEnd()};
  _volatile_size = 0;
  if (_committed.exchange(tail_offset, std::memory_order_acq_rel)) {
    return;
  }

  SDB_ASSERT(!_consumed);
  _consumed = tail_offset;
  SendData();
}

void Buffer::Grow(size_t capacity) {
  _tail = _tail->AppendChunk(CreateChunk(capacity));
  SDB_ASSERT(_tail->GetBegin() == _tail->GetEnd());
}

void Buffer::Flush() {
  if (_volatile_size != 0) {
    FlushStart();
  }
}

Chain Buffer::ReleaseChain() {
  const size_t bytes = std::exchange(_volatile_size, 0);
  if (bytes == 0) {
    return {};
  }
  auto* head = std::exchange(_head, nullptr);
  auto* tail = std::exchange(_tail, nullptr);
  _head = _tail = CreateChunk(0);
  return Chain{head, tail, bytes};
}

void Buffer::SpliceCommitted(Chain&& chain, bool need_flush) {
  if (!chain.head) {
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

std::span<uint8_t> Buffer::Reserve(size_t min_capacity) {
  if (_tail->FreeSpace() < min_capacity) {
    Grow(min_capacity);
  }
  return {_tail->WritableData() + _tail->GetEnd(), _tail->FreeSpace()};
}

void Buffer::CommitWrite(size_t size) {
  _tail->AdjustEnd(size);
  _committed.store(BufferOffset{_tail, _tail->GetEnd()},
                   std::memory_order_release);
}

// Bytes of `chunk` visible to the consumer: chunks before the watermark chunk
// are final (the producer moved past them -- GetEnd is stable); the watermark
// chunk is bounded by the snapshot offset, never its live _end.
namespace {

size_t ReadableEndOf(const Chunk* chunk, const BufferOffset& readable) {
  return chunk == readable.chunk ? readable.in_chunk : chunk->GetEnd();
}

}  // namespace

std::string_view Buffer::Front() noexcept {
  RefreshReadable();
  // A head chunk consumed exactly to its boundary stays in place (Consume
  // cannot free the then-watermark chunk); once the watermark moved on, skip
  // and free it here -- otherwise Front() reports an empty view while later
  // chunks hold readable bytes.
  while (_consumed && _head != _consumed.chunk &&
         _head->GetBegin() == ReadableEndOf(_head, _consumed)) {
    delete std::exchange(_head, _head->Next());
  }
  const auto data = _head->Data(ReadableEndOf(_head, _consumed));
  return {reinterpret_cast<const char*>(data.data()), data.size()};
}

size_t Buffer::ReadableSize() noexcept {
  RefreshReadable();
  if (!_consumed) {
    return 0;
  }
  size_t size = 0;
  const Chunk* chunk = _head;
  for (;;) {
    size += ReadableEndOf(chunk, _consumed) - chunk->GetBegin();
    if (chunk == _consumed.chunk) {
      return size;
    }
    chunk = chunk->Next();
    SDB_ASSERT(chunk != nullptr);
  }
}

SequenceView Buffer::ReadableView(size_t length) noexcept {
  RefreshReadable();
  const Chunk* chunk = _head;
  size_t pos = chunk->GetBegin();
  const BufferOffset begin{chunk, pos};
  while (length != 0) {
    const size_t available = ReadableEndOf(chunk, _consumed) - pos;
    if (length <= available) {
      return SequenceView{begin, BufferOffset{chunk, pos + length}};
    }
    length -= available;
    SDB_ASSERT(chunk != _consumed.chunk);
    chunk = chunk->Next();
    SDB_ASSERT(chunk != nullptr);
    pos = chunk->GetBegin();
  }
  return SequenceView{begin, begin};
}

void Buffer::Consume(size_t size) {
  RefreshReadable();
  while (size != 0) {
    const size_t available =
      ReadableEndOf(_head, _consumed) - _head->GetBegin();
    if (size < available) {
      _head->SetBegin(_head->GetBegin() + size);
      return;
    }
    size -= available;
    if (_head == _consumed.chunk) {
      // Fully consumed up to the watermark. The producer may still be
      // appending into this chunk (or already past it -- unknowable from the
      // snapshot), so it can be neither deleted nor Reset; a later refresh
      // moves _consumed past it and frees it then.
      SDB_ASSERT(size == 0);
      _head->SetBegin(_consumed.in_chunk);
      return;
    }
    delete std::exchange(_head, _head->Next());
  }
}

// Single-owner operation: not safe while a concurrent producer or consumer
// is active on either channel half.
void Buffer::Clear() noexcept {
  FreeTill(_tail);
  _head->Reset();
  _volatile_size = 0;
  _committed.store(BufferOffset{}, std::memory_order_relaxed);
  _consumed = BufferOffset{};
}

// Writer cold path for GrabTail: add a chunk holding `n` contiguous bytes and
// return the new tail.
Chunk* Buffer::GrowFor(size_t n) {
  Grow(n);
  return _tail;
}

// Writer cold path for Write: `data` overflowed the tail chunk. Fill the tail,
// grow one chunk for the remainder (CreateChunk sizes it to hold it), write it.
void Buffer::AppendSpanning(std::string_view data) {
  const size_t room = _tail->FreeSpace();
  std::memcpy(_tail->WritableData() + _tail->GetEnd(), data.data(), room);
  _tail->AdjustEnd(room);
  data.remove_prefix(room);
  Grow(data.size());
  std::memcpy(_tail->WritableData(), data.data(), data.size());
  _tail->AdjustEnd(data.size());
}

// Writer dtor's uncommitted path: drop every chunk grown since the checkpoint
// (they hold only uncommitted bytes; the checkpoint sits at/after the flush
// watermark, so the consumer never frees its chunk) and reset the write end.
void Buffer::Rewind(Chunk* mark_chunk, size_t mark_end) {
  auto* chunk = mark_chunk->DetachNext();
  while (chunk) {
    delete std::exchange(chunk, chunk->Next());
  }
  mark_chunk->SetEnd(mark_end);
  _tail = mark_chunk;
}

void Buffer::FlushDone() {
  SDB_ASSERT(_consumed);
  SDB_ASSERT(_committed.load(std::memory_order_relaxed));
  FreeTill(_consumed.chunk);
  SDB_ASSERT(_head == _consumed.chunk);

  _head->SetBegin(_consumed.in_chunk);
  auto consumed = std::exchange(_consumed, {});
  auto committed = _committed.load(std::memory_order_acquire);
  if (committed == consumed &&
      _committed.compare_exchange_strong(committed, BufferOffset{},
                                         std::memory_order_acq_rel)) {
    // We released "lock"/"ownership" here, so only local variables can be
    // touched here
    return;
  }
  _consumed = committed;
  SendData();
}

void Buffer::SendData() const {
  SDB_ASSERT(_consumed);
  SDB_ASSERT(_head != _consumed.chunk ||
             _head->GetBegin() <= _consumed.in_chunk);
  if (!_flush_callback) {
    return;
  }
  SequenceView data{BufferOffset{_head, _head->GetBegin()}, _consumed};
  _flush_callback(data);
}

Chunk* Buffer::CreateChunk(size_t size) const {
  // TODO(codeworse) allocate with allocate_at_least
  while (_growth < size) [[unlikely]] {
    _growth *= 2;
  }
  size = std::max(size, _growth);
  _growth = std::min(_max_growth, _growth * 2);
  return Chunk::Create(size);
}

void Buffer::FreeTill(const Chunk* end) {
  while (_head != end) {
    SDB_ASSERT(_head);
    auto* prev_head = std::exchange(_head, _head->Next());
    delete prev_head;
  }
}

}  // namespace sdb::message
