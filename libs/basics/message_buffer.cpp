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
               std::function<void(SequenceView)> send_callback)
  : _send_callback{std::move(send_callback)},
    _flush_size{flush_size},
    _max_growth{max_growth},
    _growth{min_growth} {
  _head = _tail = CreateChunk(0);
}

void Buffer::FlushStart() {
  SDB_ASSERT(_tail);
  const BufferOffset tail_offset{_tail, _tail->GetEnd()};
  _volatile_size = 0;
  if (_send_end.exchange(tail_offset, std::memory_order_acq_rel)) {
    return;
  }

  SDB_ASSERT(!_socket_end);
  _socket_end = tail_offset;
  SendData();
}

uint8_t* Buffer::AllocateContiguousData(size_t capacity) {
  if (_tail->FreeSpace() < capacity) {
    _tail = _tail->AppendChunk(CreateChunk(capacity));
    SDB_ASSERT(_tail->GetBegin() == _tail->GetEnd());
  }
  return _tail->WritableData() + _tail->GetEnd();
}

uint8_t* Buffer::GetContiguousData(size_t capacity) {
  auto* writable_data = AllocateContiguousData(capacity);
  _tail->AdjustEnd(capacity);
  _uncommitted_size += capacity;
  return writable_data;
}

void Buffer::Commit(bool need_flush) {
  _volatile_size += std::exchange(_uncommitted_size, 0);
  if (need_flush || _volatile_size >= _flush_size) {
    FlushStart();
  }
}

void Buffer::WriteUncommitted(std::string_view data) {
  if (data.empty()) {
    return;
  }
  const auto data_size = data.size();
  auto written = _tail->TryWrite(data);
  if (written < data.size()) {
    data.remove_prefix(written);
    _tail = _tail->AppendChunk(CreateChunk(data.size()));
    written += _tail->TryWrite(data);
  }
  SDB_ASSERT(data_size == written);
  _uncommitted_size += written;
}

void Buffer::FlushDone() {
  SDB_ASSERT(_socket_end);
  SDB_ASSERT(_send_end.load(std::memory_order_relaxed));
  FreeTill(_socket_end.chunk);
  SDB_ASSERT(_head == _socket_end.chunk);

  _head->SetBegin(_socket_end.in_chunk);
  auto socket_end = std::exchange(_socket_end, {});
  auto send_end = _send_end.load(std::memory_order_acquire);
  if (send_end == socket_end &&
      _send_end.compare_exchange_strong(send_end, BufferOffset{},
                                        std::memory_order_acq_rel)) {
    // We released "lock"/"ownership" here, so only local variables can be
    // touched here
    return;
  }
  _socket_end = send_end;
  SendData();
}

void Buffer::SendData() const {
  SDB_ASSERT(_socket_end);
  SDB_ASSERT(_head != _socket_end.chunk ||
             _head->GetBegin() <= _socket_end.in_chunk);
  SequenceView data{BufferOffset{_head, _head->GetBegin()}, _socket_end};
  if (!_send_callback || data.Empty()) {
    return;
  }
  _send_callback(data);
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
