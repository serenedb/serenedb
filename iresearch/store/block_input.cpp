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

#include "iresearch/store/block_input.hpp"

#include <algorithm>
#include <cstring>
#include <utility>

#include "iresearch/utils/crc.hpp"
#include "iresearch/utils/pg/file_error.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

[[noreturn]] void ThrowPastEnd(const duckdb::BlockCacheFile& file,
                               uint64_t position) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("read past the end of '", file.GetPath(),
                          "' at offset ", position, " of ", file.GetSize()));
}

}  // namespace

BlockInput::BlockInput(duckdb::BlockCache& cache,
                       duckdb::shared_ptr<duckdb::BlockCacheFile> file,
                       std::shared_ptr<const bstring> tail) noexcept
  : _cache{cache},
    _file{std::move(file)},
    _length{_file->GetSize()},
    _tail{std::move(tail)},
    _tail_start{_tail ? _length - _tail->size() : _length} {
  SDB_ASSERT(!_tail || _tail->size() <= _length);
}

BlockInput::BlockInput(const BlockInput& other) noexcept
  : IndexInput{other},
    _cache{other._cache},
    _file{other._file},
    _length{other._length},
    _tail{other._tail},
    _tail_start{other._tail_start},
    _block_start{other.Position()} {}

void BlockInput::Seek(uint64_t pos) {
  if (_begin && pos >= _block_start &&
      pos - _block_start <= static_cast<uint64_t>(_end - _begin)) {
    _pos = _begin + (pos - _block_start);
    return;
  }
  ReleasePin();
  _begin = _pos = _end = nullptr;
  _block_start = pos;
}

void BlockInput::ReleasePin() {
  if (_pin_stable) {
    _held.try_emplace(_block_start / duckdb::BlockCache::BLOCK_SIZE,
                      std::move(_pin));
    _pin_stable = false;
  }
  _pin.Reset();
}

bool BlockInput::PinCurrent(bool readahead) try {
  const auto position = Position();
  if (position >= _length) {
    return false;
  }
  if (position >= _tail_start) {
    ReleasePin();
    _block_start = _tail_start;
    _begin = _tail->data();
    _end = _begin + _tail->size();
    _pos = _begin + (position - _tail_start);
    return true;
  }
  const auto block = position / duckdb::BlockCache::BLOCK_SIZE;
  if (!_file->IsUnbacked()) {
    ReadAhead(block, readahead);
  } else if (const auto it = _held.find(block); it != _held.end()) {
    const auto* data = it->second.Ptr();
    const auto size = it->second.Size();
    ReleasePin();
    return Window(block, data, size, position);
  }
  auto pin = _cache.Pin(_file, block);
  ReleasePin();
  _pin = std::move(pin);
  return Window(block, _pin.Ptr(), _pin.Size(), position);
} catch (const duckdb::IOException& e) {
  ThrowFileError(e);
}

bool BlockInput::Window(uint64_t block, const byte_type* data, uint64_t size,
                        uint64_t position) noexcept {
  _block_start = block * duckdb::BlockCache::BLOCK_SIZE;
  _begin = data;
  _end = _begin + std::min<uint64_t>(size, _length - _block_start);
  _pos = _begin + (position - _block_start);
  return _pos != _end;
}

void BlockInput::ReadAhead(uint64_t block, bool readahead) noexcept {
  constexpr uint64_t kBlock = duckdb::BlockCache::BLOCK_SIZE;
  if (block != std::exchange(_next_block, block + 1)) {
    _readahead = 0;
    _readahead_end = 0;
    return;
  }
  if (!readahead || _readahead_end > block + 1 + _readahead / 2) {
    return;
  }
  _readahead = std::clamp<uint64_t>(_readahead * 2, 2, kMaxReadaheadBlocks);
  _readahead_end = std::max(block + 1, _readahead_end) + _readahead;
  Prefetch(block * kBlock, (_readahead_end - block) * kBlock);
}

const byte_type* BlockInput::ReadStable(uint64_t count) {
  if (!_file->IsUnbacked() || (Remain() == 0 && !PinCurrent()) ||
      Remain() < count) {
    return nullptr;
  }
  _pin_stable = _pin_stable || _pin.IsValid();
  const auto* begin = _pos;
  _pos += count;
  return begin;
}

byte_type BlockInput::ReadByteSlow() {
  if (!PinCurrent()) {
    ThrowPastEnd(*_file, Position());
  }
  return *_pos++;
}

const byte_type* BlockInput::ReadVolatile(uint64_t count) {
  if (Remain() >= count || (Remain() == 0 && PinCurrent() && Remain() >= count))
    [[likely]] {
    const auto* begin = _pos;
    _pos += count;
    return begin;
  }
  _scratch.resize(count);
  ReadData(_scratch.data(), count);
  return _scratch.data();
}

void BlockInput::ReadData(byte_type* b, uint64_t count) {
  if (count > Remain() && !_file->IsUnbacked()) {
    Prefetch(Position() + Remain(), count - Remain());
  }
  while (count != 0) {
    if (Remain() == 0 && !PinCurrent(false)) {
      ThrowPastEnd(*_file, Position());
    }
    const auto chunk = std::min<uint64_t>(count, Remain());
    std::memcpy(b, _pos, chunk);
    _pos += chunk;
    b += chunk;
    count -= chunk;
  }
}

uint32_t BlockInput::Checksum(uint64_t offset) const {
  BlockInput copy{*this};
  const auto end = std::min<uint64_t>(Position() + offset, _length);
  Crc32c crc;
  while (copy.Position() < end) {
    if (copy.Remain() == 0 && !copy.PinCurrent()) {
      break;
    }
    const auto chunk = std::min<uint64_t>(end - copy.Position(), copy.Remain());
    crc.process_bytes(copy._pos, chunk);
    copy._pos += chunk;
  }
  return crc.checksum();
}

void BlockInput::Prefetch(uint64_t offset, uint64_t count) const noexcept {
  if (offset >= _tail_start) {
    return;
  }
  try {
    _cache.Prefetch(_file, offset, std::min(count, _tail_start - offset));
  } catch (...) {
  }
}

bool BlockInput::Warm(uint64_t offset, uint64_t count) const noexcept {
  return offset >= _tail_start ||
         _cache.IsCached(*_file, offset, std::min(count, _tail_start - offset));
}

}  // namespace irs
