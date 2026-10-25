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

#pragma once

#include <cstring>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <iterator>
#include <memory>
#include <span>
#include <string>

#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct ByteExtent {
  uint64_t offset = 0;
  uint64_t size = 0;
};

std::span<ByteExtent> CoalesceExtents(std::span<ByteExtent> extents,
                                      uint64_t gap) noexcept;

class InputFile {
 public:
  explicit InputFile(std::unique_ptr<duckdb::MemoryMappedFile> mapping);
  InputFile(std::string path, std::unique_ptr<byte_type[]> bytes,
            uint64_t size) noexcept;
  InputFile(duckdb::BlockCache& cache,
            duckdb::shared_ptr<duckdb::BlockCacheFile> blocks,
            std::shared_ptr<const bstring> tail) noexcept;
  InputFile(const InputFile&) = delete;
  InputFile& operator=(const InputFile&) = delete;

  uint64_t Length() const noexcept { return _length; }
  bool InBlocks() const noexcept { return _blocks != nullptr; }
  bytes_view Bytes() const noexcept { return _bytes; }
  const std::string& Path() const noexcept;

  duckdb::BlockCache& Cache() const noexcept { return *_cache; }
  const duckdb::shared_ptr<duckdb::BlockCacheFile>& Blocks() const noexcept {
    return _blocks;
  }
  uint64_t TailStart() const noexcept { return _tail_start; }
  const byte_type* Tail() const noexcept { return _tail->data(); }

  void Prefetch(uint64_t offset, uint64_t count) const noexcept;
  void PrefetchExtents(std::span<ByteExtent> extents) const noexcept;
  bool Warm(uint64_t offset, uint64_t count) const noexcept;
  void MarkWarm(uint64_t offset, uint64_t count) const noexcept;
  bool Resident(uint64_t offset, uint64_t count) const noexcept;
  void Advise(duckdb::MappedFileAccess access) const noexcept;

 private:
  bytes_view Window(uint64_t offset, uint64_t count) const noexcept;
  uint64_t LastPage(uint64_t offset, uint64_t count) const noexcept;

  bytes_view _bytes;
  uint64_t _length;
  std::unique_ptr<duckdb::MemoryMappedFile> _mapping;
  std::unique_ptr<byte_type[]> _owned;
  std::string _path;
  file_utils::ResidencyMap _residency;
  duckdb::BlockCache* _cache = nullptr;
  duckdb::shared_ptr<duckdb::BlockCacheFile> _blocks;
  std::shared_ptr<const bstring> _tail;
  uint64_t _tail_start;
};

class IndexInput {
 public:
  static constexpr bool kVolatileAlways = false;

  using iterator_category = std::forward_iterator_tag;
  using value_type = byte_type;
  using pointer = void;
  using reference = void;
  using difference_type = ptrdiff_t;

  IndexInput() noexcept = default;
  explicit IndexInput(bytes_view bytes) noexcept
    : _begin{bytes.data()},
      _pos{bytes.data()},
      _end{bytes.data() + bytes.size()},
      _length{bytes.size()} {}
  explicit IndexInput(std::unique_ptr<InputFile> file) noexcept;
  IndexInput(const IndexInput& other) noexcept
    : _begin{other._begin},
      _pos{other._pos},
      _end{other._end},
      _start{other._start},
      _length{other._length},
      _file{other._file} {
    if (!Whole()) {
      _start = other.Position();
      _begin = _pos = _end = nullptr;
    }
  }
  IndexInput(IndexInput&& other) noexcept = default;
  IndexInput& operator=(const IndexInput& other) noexcept {
    if (this != &other) {
      *this = IndexInput{other};
    }
    return *this;
  }
  IndexInput& operator=(IndexInput&& other) noexcept = default;

  explicit operator bool() const noexcept {
    return _file != nullptr || _begin != nullptr;
  }
  bool Whole() const noexcept { return _file == nullptr || !_file->InBlocks(); }

  IRS_FORCE_INLINE byte_type ReadByte() {
    if (_pos != _end) [[likely]] {
      return *_pos++;
    }
    return ReadByteSlow();
  }

  IRS_FORCE_INLINE uint64_t Position() const noexcept {
    return _start + static_cast<uint64_t>(_pos - _begin);
  }
  uint64_t Length() const noexcept { return _length; }
  bool IsEOF() const noexcept { return Position() >= _length; }

  int16_t ReadI16() {
    return Remain() < sizeof(uint16_t) ? irs::read<uint16_t>(*this)
                                       : irs::read<uint16_t>(_pos);
  }
  int32_t ReadI32() {
    return Remain() < sizeof(uint32_t) ? irs::read<uint32_t>(*this)
                                       : irs::read<uint32_t>(_pos);
  }
  int64_t ReadI64() {
    return Remain() < sizeof(uint64_t) ? irs::read<uint64_t>(*this)
                                       : irs::read<uint64_t>(_pos);
  }
  uint32_t ReadV32() {
    return Remain() < bytes_io<uint32_t>::kMaxVSize
             ? irs::vread<uint32_t>(*this)
             : irs::vread<uint32_t>(_pos);
  }
  uint64_t ReadV64() {
    return Remain() < bytes_io<uint64_t>::kMaxVSize
             ? irs::vread<uint64_t>(*this)
             : irs::vread<uint64_t>(_pos);
  }
  void SkipV32() {
    Remain() < bytes_io<uint32_t>::kMaxVSize ? irs::vskip<uint32_t>(*this)
                                             : irs::vskip<uint32_t>(_pos);
  }
  void SkipV64() {
    Remain() < bytes_io<uint64_t>::kMaxVSize ? irs::vskip<uint64_t>(*this)
                                             : irs::vskip<uint64_t>(_pos);
  }

  IRS_FORCE_INLINE void Seek(uint64_t pos) {
    if (pos >= _start && pos - _start <= static_cast<uint64_t>(_end - _begin))
      [[likely]] {
      _pos = _begin + (pos - _start);
      return;
    }
    SeekOutside(pos);
  }
  void Skip(uint64_t count) { Seek(Position() + count); }

  const byte_type* Current() const noexcept { return _pos; }
  const byte_type* At(uint64_t offset) const noexcept {
    return _begin + (offset - _start);
  }
  const byte_type* Peek(uint64_t count) const noexcept {
    return Remain() >= count ? _pos : nullptr;
  }

  const byte_type* ReadVolatile(uint64_t count);
  const byte_type* ReadVolatile(uint64_t offset, uint64_t count) {
    Seek(offset);
    return ReadVolatile(count);
  }
  const byte_type* ReadStable(uint64_t count);
  const byte_type* ReadStable(uint64_t offset, uint64_t count) {
    Seek(offset);
    return ReadStable(count);
  }

  IRS_FORCE_INLINE void ReadData(byte_type* b, uint64_t count) {
    if (count <= Remain()) [[likely]] {
      if (count != 0) [[likely]] {
        std::memcpy(b, _pos, count);
        _pos += count;
      }
      return;
    }
    ReadDataSlow(b, count);
  }
  void ReadData(uint64_t offset, byte_type* b, uint64_t count) {
    Seek(offset);
    ReadData(b, count);
  }

  uint32_t Checksum(uint64_t offset) const;

  void Prefetch(uint64_t offset, uint64_t count) const noexcept;
  void PrefetchExtents(std::span<ByteExtent> extents) const noexcept;
  bool Resident(uint64_t offset, uint64_t count) const noexcept;
  bool Warm(uint64_t offset, uint64_t count) const noexcept {
    return _file != nullptr && _file->Warm(offset, count);
  }
  void MarkWarm(uint64_t offset, uint64_t count) const noexcept {
    if (_file != nullptr) {
      _file->MarkWarm(offset, count);
    }
  }
  void Advise(duckdb::MappedFileAccess access) const noexcept {
    if (_file != nullptr) {
      _file->Advise(access);
    }
  }

  IRS_FORCE_INLINE byte_type operator*(this auto& self) {
    return self.ReadByte();
  }
  IRS_FORCE_INLINE auto& operator++(this auto& self) noexcept { return self; }
  IRS_FORCE_INLINE auto& operator++(this auto& self, int) noexcept {
    return self;
  }

 protected:
  IRS_FORCE_INLINE uint64_t Remain() const noexcept {
    return static_cast<uint64_t>(_end - _pos);
  }
  void Reset(bytes_view bytes) noexcept;

  const byte_type* _begin = nullptr;
  const byte_type* _pos = nullptr;
  const byte_type* _end = nullptr;
  uint64_t _start = 0;
  uint64_t _length = 0;

 private:
  struct BlockState {
    ~BlockState();

    duckdb::BlockCachePin pin;
    duckdb::BlockCacheReadAhead read_ahead;
    containers::FlatHashMap<uint64_t, duckdb::BlockCachePin> held;
    bstring scratch;
    bool pin_stable = false;
  };

  byte_type ReadByteSlow();
  void ReadDataSlow(byte_type* b, uint64_t count);
  void SeekOutside(uint64_t pos);
  bool Refill(bool ahead);
  BlockState& State();
  bytes_view SpanWindow(uint64_t offset, uint64_t count) const noexcept;
  void Window(uint64_t block, const byte_type* data, uint64_t size,
              uint64_t position) noexcept;
  void ReleasePin();
  [[noreturn]] void ThrowPastEnd() const;

  const InputFile* _file = nullptr;
  std::unique_ptr<InputFile> _owned;
  std::unique_ptr<BlockState> _state;
};

class BytesViewInput final : public IndexInput {
 public:
  static constexpr bool kVolatileAlways = true;

  BytesViewInput() noexcept = default;
  explicit BytesViewInput(bytes_view bytes) noexcept : IndexInput{bytes} {}
  explicit BytesViewInput(const IndexInput& whole) noexcept
    : IndexInput{whole} {
    SDB_ASSERT(Whole());
  }

  IRS_FORCE_INLINE byte_type ReadByte() noexcept {
    SDB_ASSERT(_pos < _end);
    return *_pos++;
  }
  IRS_FORCE_INLINE uint64_t Position() const noexcept {
    return static_cast<uint64_t>(_pos - _begin);
  }

  int16_t ReadI16() noexcept { return irs::read<uint16_t>(_pos); }
  int32_t ReadI32() noexcept { return irs::read<uint32_t>(_pos); }
  int64_t ReadI64() noexcept { return irs::read<uint64_t>(_pos); }
  uint32_t ReadV32() noexcept { return irs::vread<uint32_t>(_pos); }
  uint64_t ReadV64() noexcept { return irs::vread<uint64_t>(_pos); }
  void SkipV32() noexcept { irs::vskip<uint32_t>(_pos); }
  void SkipV64() noexcept { irs::vskip<uint64_t>(_pos); }

  IRS_FORCE_INLINE void Seek(uint64_t pos) noexcept {
    SDB_ASSERT(pos <= _length);
    _pos = _begin + pos;
  }
  IRS_FORCE_INLINE void Skip(uint64_t count) noexcept {
    SDB_ASSERT(_pos + count <= _end);
    _pos += count;
  }

  IRS_FORCE_INLINE const byte_type* ReadStable(uint64_t count) noexcept {
    const auto* begin = _pos;
    _pos = begin + count;
    SDB_ASSERT(_pos <= _end);
    return begin;
  }
  IRS_FORCE_INLINE const byte_type* ReadStable(uint64_t offset,
                                               uint64_t count) noexcept {
    const auto* begin = _begin + offset;
    _pos = begin + count;
    SDB_ASSERT(_pos <= _end);
    return begin;
  }
  IRS_FORCE_INLINE const byte_type* ReadVolatile(uint64_t count) noexcept {
    return ReadStable(count);
  }
  IRS_FORCE_INLINE const byte_type* ReadVolatile(uint64_t offset,
                                                 uint64_t count) noexcept {
    return ReadStable(offset, count);
  }

  void reset(const byte_type* data, size_t size) noexcept {
    Reset(bytes_view{data, size});
  }
  void reset(bytes_view ref) noexcept { reset(ref.data(), ref.size()); }
};

}  // namespace irs
