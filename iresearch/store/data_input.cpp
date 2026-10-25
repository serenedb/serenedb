////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "data_input.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/crc.hpp"
#include "iresearch/utils/math_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {

std::span<ByteExtent> CoalesceExtents(std::span<ByteExtent> extents,
                                      uint64_t gap) noexcept {
  absl::c_sort(extents, [](const ByteExtent& l, const ByteExtent& r) {
    return l.offset < r.offset;
  });
  size_t n = 0;
  for (const auto& extent : extents) {
    if (n != 0 &&
        extent.offset <= extents[n - 1].offset + extents[n - 1].size + gap) {
      auto& last = extents[n - 1];
      last.size =
        std::max(last.size, extent.offset + extent.size - last.offset);
    } else {
      extents[n++] = extent;
    }
  }
  return extents.first(n);
}

InputFile::InputFile(std::unique_ptr<duckdb::MemoryMappedFile> mapping)
  : _length{mapping->Size()},
    _mapping{std::move(mapping)},
    _tail_start{_length} {
  if (_length != 0) {
    _bytes = {_mapping->GetData(0, _length), _length};
    _residency.Reset(math::DivCeil64(_length, file_utils::kPage));
  }
}

InputFile::InputFile(std::string path, std::unique_ptr<byte_type[]> bytes,
                     uint64_t size) noexcept
  : _bytes{bytes.get(), size},
    _length{size},
    _owned{std::move(bytes)},
    _path{std::move(path)},
    _tail_start{size} {}

InputFile::InputFile(duckdb::BlockCache& cache,
                     duckdb::shared_ptr<duckdb::BlockCacheFile> blocks,
                     std::shared_ptr<const bstring> tail) noexcept
  : _length{blocks->GetSize()},
    _cache{&cache},
    _blocks{std::move(blocks)},
    _tail{std::move(tail)},
    _tail_start{_tail ? _length - _tail->size() : _length} {
  SDB_ASSERT(!_tail || _tail->size() <= _length);
}

const std::string& InputFile::Path() const noexcept {
  if (_mapping) {
    return _mapping->GetPath();
  }
  if (_blocks) {
    return _blocks->GetPath();
  }
  return _path;
}

bytes_view InputFile::Window(uint64_t offset, uint64_t count) const noexcept {
  if (offset >= _bytes.size()) {
    return {};
  }
  return {_bytes.data() + offset,
          std::min<uint64_t>(count, _bytes.size() - offset)};
}

uint64_t InputFile::LastPage(uint64_t offset, uint64_t count) const noexcept {
  return (std::min<uint64_t>(offset + count, _length) - 1) / file_utils::kPage;
}

void InputFile::Prefetch(uint64_t offset, uint64_t count) const noexcept {
  if (_blocks) {
    if (offset >= _tail_start) {
      return;
    }
    try {
      _cache->Prefetch(_blocks, offset, std::min(count, _tail_start - offset));
    } catch (...) {
    }
    return;
  }
  if (_mapping) {
    _mapping->Prefetch(offset, count);
    return;
  }
  const auto window = Window(offset, count);
  file_utils::Prefetch(window.data(), window.size());
}

void InputFile::PrefetchExtents(std::span<ByteExtent> extents) const noexcept {
  if (_blocks) {
    extents = CoalesceExtents(extents, duckdb::BlockCache::MERGE_GAP);
  }
  for (const auto& extent : extents) {
    Prefetch(extent.offset, extent.size);
  }
}

bool InputFile::Warm(uint64_t offset, uint64_t count) const noexcept {
  if (_blocks) {
    return offset >= _tail_start ||
           _cache->IsCached(*_blocks, offset,
                            std::min(count, _tail_start - offset));
  }
  if (!_mapping || count == 0 || offset >= _length) {
    return false;
  }
  return _residency.Valid(file_utils::ResidencyEpoch()) &&
         _residency.Test(offset / file_utils::kPage, LastPage(offset, count));
}

void InputFile::MarkWarm(uint64_t offset, uint64_t count) const noexcept {
  if (!_mapping || count == 0 || offset >= _length) {
    return;
  }
  const auto epoch = file_utils::ResidencyEpoch();
  if (_residency.Valid(epoch) || _residency.Adopt(epoch)) {
    _residency.Set(offset / file_utils::kPage, LastPage(offset, count));
  }
}

bool InputFile::Resident(uint64_t offset, uint64_t count) const noexcept {
  if (_blocks) {
    return Warm(offset, count);
  }
  const auto window = Window(offset, count);
  return !window.empty() &&
         file_utils::IsResident(window.data(), window.size());
}

void InputFile::Advise(duckdb::MappedFileAccess access) const noexcept {
  if (_mapping) {
    _mapping->Advise(access);
  }
}

IndexInput::BlockState::~BlockState() = default;

IndexInput::IndexInput(std::unique_ptr<InputFile> file) noexcept
  : _length{file->Length()}, _file{file.get()}, _owned{std::move(file)} {
  if (!_file->InBlocks()) {
    const auto bytes = _file->Bytes();
    _begin = _pos = bytes.data();
    _end = bytes.data() + bytes.size();
  }
}

IndexInput::BlockState& IndexInput::State() {
  if (!_state) {
    _state = std::make_unique<BlockState>();
  }
  return *_state;
}

void IndexInput::Reset(bytes_view bytes) noexcept {
  _state.reset();
  _owned.reset();
  _file = nullptr;
  _begin = _pos = bytes.data();
  _end = bytes.data() + bytes.size();
  _start = 0;
  _length = bytes.size();
}

void IndexInput::SeekOutside(uint64_t pos) {
  if (Whole()) {
    _pos = _end;
    return;
  }
  ReleasePin();
  _begin = _pos = _end = nullptr;
  _start = pos;
}

void IndexInput::ReleasePin() {
  if (!_state) {
    return;
  }
  auto& state = *_state;
  if (state.pin_stable) {
    state.held.try_emplace(_start / duckdb::BlockCache::BLOCK_SIZE,
                           std::move(state.pin));
    state.pin_stable = false;
  }
  state.pin.Reset();
}

void IndexInput::Window(uint64_t block, const byte_type* data, uint64_t size,
                        uint64_t position) noexcept {
  _start = block * duckdb::BlockCache::BLOCK_SIZE;
  _begin = data;
  _end = _begin + std::min<uint64_t>(size, _length - _start);
  _pos = _begin + (position - _start);
}

bool IndexInput::Refill(bool ahead) {
  const auto position = Position();
  if (Whole() || position >= _length) {
    return false;
  }
  const auto& file = *_file;
  if (position >= file.TailStart()) {
    ReleasePin();
    _start = file.TailStart();
    _begin = file.Tail();
    _end = _begin + (_length - _start);
    _pos = _begin + (position - _start);
    return true;
  }
  const auto block = position / duckdb::BlockCache::BLOCK_SIZE;
  const auto& blocks = file.Blocks();
  auto& state = State();
  if (blocks->IsUnbacked()) {
    if (const auto it = state.held.find(block); it != state.held.end()) {
      const auto* data = it->second.Ptr();
      const auto size = it->second.Size();
      ReleasePin();
      Window(block, data, size, position);
      return _pos != _end;
    }
  }
  auto pin = file.Cache().PinInOrder(blocks, block, state.read_ahead,
                                     file.TailStart(), ahead);
  ReleasePin();
  state.pin = std::move(pin);
  Window(block, state.pin.Ptr(), state.pin.Size(), position);
  return _pos != _end;
}

byte_type IndexInput::ReadByteSlow() {
  if (!Refill(true)) {
    ThrowPastEnd();
  }
  return *_pos++;
}

const byte_type* IndexInput::ReadVolatile(uint64_t count) {
  if (Remain() >= count || (Remain() == 0 && Refill(true) && Remain() >= count))
    [[likely]] {
    const auto* begin = _pos;
    _pos += count;
    return begin;
  }
  if (Whole()) {
    ThrowPastEnd();
  }
  auto& scratch = State().scratch;
  scratch.resize(count);
  ReadData(scratch.data(), count);
  return scratch.data();
}

const byte_type* IndexInput::ReadStable(uint64_t count) {
  if (Whole()) {
    if (Remain() < count) {
      return nullptr;
    }
  } else if (!_file->Blocks()->IsUnbacked() ||
             (Remain() == 0 && !Refill(true)) || Remain() < count) {
    return nullptr;
  } else if (_state && _state->pin.IsValid()) {
    _state->pin_stable = true;
  }
  const auto* begin = _pos;
  _pos += count;
  return begin;
}

void IndexInput::ReadDataSlow(byte_type* b, uint64_t count) {
  if (Whole()) {
    ThrowPastEnd();
  }
  if (!_file->Blocks()->IsUnbacked()) {
    Prefetch(Position() + Remain(), count - Remain());
  }
  while (count != 0) {
    if (Remain() == 0 && !Refill(false)) {
      ThrowPastEnd();
    }
    const auto chunk = std::min<uint64_t>(count, Remain());
    std::memcpy(b, _pos, chunk);
    _pos += chunk;
    b += chunk;
    count -= chunk;
  }
}

uint32_t IndexInput::Checksum(uint64_t offset) const {
  Crc32c crc;
  if (Whole()) {
    crc.process_bytes(_pos, std::min<uint64_t>(offset, Remain()));
    return crc.checksum();
  }
  IndexInput copy{*this};
  const auto end = std::min<uint64_t>(Position() + offset, _length);
  while (copy.Position() < end) {
    if (copy.Remain() == 0 && !copy.Refill(true)) {
      break;
    }
    const auto chunk = std::min<uint64_t>(end - copy.Position(), copy.Remain());
    crc.process_bytes(copy._pos, chunk);
    copy._pos += chunk;
  }
  return crc.checksum();
}

bytes_view IndexInput::SpanWindow(uint64_t offset,
                                  uint64_t count) const noexcept {
  if (offset >= _length) {
    return {};
  }
  return {_begin + offset, std::min<uint64_t>(count, _length - offset)};
}

void IndexInput::Prefetch(uint64_t offset, uint64_t count) const noexcept {
  if (_file != nullptr) {
    _file->Prefetch(offset, count);
    return;
  }
  const auto window = SpanWindow(offset, count);
  file_utils::Prefetch(window.data(), window.size());
}

void IndexInput::PrefetchExtents(std::span<ByteExtent> extents) const noexcept {
  if (_file != nullptr) {
    _file->PrefetchExtents(extents);
    return;
  }
  for (const auto& extent : extents) {
    Prefetch(extent.offset, extent.size);
  }
}

bool IndexInput::Resident(uint64_t offset, uint64_t count) const noexcept {
  if (_file != nullptr) {
    return _file->Resident(offset, count);
  }
  const auto window = SpanWindow(offset, count);
  return !window.empty() &&
         file_utils::IsResident(window.data(), window.size());
}

void IndexInput::ThrowPastEnd() const {
  if (_file != nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("read past the end of '", _file->Path(),
                            "' at offset ", Position(), " of ", _length));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("read past the end of an in-memory buffer at offset ",
                          Position(), " of ", _length));
}

}  // namespace irs
