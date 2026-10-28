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

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <variant>

#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct ByteExtent {
  uint64_t offset = 0;
  uint64_t size = 0;
};

std::span<ByteExtent> CoalesceExtents(std::span<ByteExtent> extents,
                                      uint64_t gap) noexcept;

struct PinnedBytes {
  bytes_view bytes;
  duckdb::BlockCachePin pin;
};

struct SpanBytes {
  bytes_view bytes;
};

struct CursorPin {
  duckdb::BlockCachePin pin;
  duckdb::BlockCacheReadAhead ahead;
};

struct CursorBlock {
  uint64_t start = 0;
  std::unique_ptr<CursorPin> pinned;
};

class InputFile {
 public:
  explicit InputFile(std::unique_ptr<duckdb::MemoryMappedFile> mapping);
  InputFile(duckdb::BlockCache& cache,
            duckdb::shared_ptr<duckdb::BlockCacheFile> blocks) noexcept;
  InputFile(const InputFile&) = delete;
  InputFile& operator=(const InputFile&) = delete;

  uint64_t Length() const noexcept { return _length; }
  const std::string& Path() const noexcept;
  bytes_view Span() const noexcept;
  bool Paged() const noexcept;
  bytes_view Refill(uint64_t position, CursorBlock& block, bool ahead) const;
  PinnedBytes Pin(uint64_t offset, uint64_t size) const;

  bool Hint(uint64_t offset, uint64_t size) const noexcept;
  void Prefetch(uint64_t offset, uint64_t size) const noexcept;
  void PrefetchExtents(std::span<ByteExtent> extents) const noexcept;
  bool Resident(uint64_t offset, uint64_t size) const noexcept;
  void Advise(duckdb::MappedFileAccess access) const noexcept;

 private:
  struct MappedSpan {
    std::unique_ptr<duckdb::MemoryMappedFile> mapping;
    bytes_view bytes;
    file_utils::ResidencyMap residency;
    bool paged = false;
  };

  struct CacheBlocks {
    duckdb::BlockCache* cache;
    duckdb::shared_ptr<duckdb::BlockCacheFile> blocks;
  };

  bool Warm(uint64_t offset, uint64_t size) const noexcept;
  void MarkWarm(uint64_t offset, uint64_t size) const noexcept;
  bytes_view SpanWindow(uint64_t offset, uint64_t size) const noexcept;

  uint64_t _length;
  std::variant<MappedSpan, CacheBlocks> _kind;
};

[[noreturn]] void ThrowPastEnd(const InputFile* file, uint64_t position,
                               uint64_t length);

template<bool Blocks>
class BasicInput {
 public:
  static constexpr bool kSpan = !Blocks;
  using Pinned = std::conditional_t<Blocks, PinnedBytes, SpanBytes>;

  BasicInput() noexcept = default;
  explicit BasicInput(bytes_view bytes) noexcept
    : _begin{bytes.data()},
      _pos{bytes.data()},
      _end{bytes.data() + bytes.size()} {}
  explicit BasicInput(const InputFile& file) noexcept : _file{&file} {
    const auto span = file.Span();
    SDB_ASSERT(Blocks || span.size() == file.Length());
    _begin = _pos = span.data();
    _end = span.data() + span.size();
  }
  template<bool Other>
  explicit BasicInput(const BasicInput<Other>& whole) noexcept
    : _pos{whole.Current()}, _file{whole.File()} {
    static_assert(Other && !Blocks);
    SDB_ASSERT(whole.Whole());
    const auto window = whole.Window();
    _begin = window.data();
    _end = window.data() + window.size();
  }

  BasicInput(const BasicInput&) noexcept
    requires(!Blocks)
  = default;
  BasicInput(const BasicInput& other) noexcept
    requires(Blocks)
    : _begin{other._begin},
      _pos{other._pos},
      _end{other._end},
      _file{other._file} {
    _block.start = other._block.start;
    if (other._block.pinned) {
      _block.start = other.Position();
      _begin = _pos = _end = nullptr;
    }
  }
  BasicInput(BasicInput&&) noexcept
    requires(!Blocks)
  = default;
  BasicInput(BasicInput&& other) noexcept
    requires(Blocks)
    : _begin{std::exchange(other._begin, nullptr)},
      _pos{std::exchange(other._pos, nullptr)},
      _end{std::exchange(other._end, nullptr)},
      _file{other._file},
      _block{std::move(other._block)} {}
  BasicInput& operator=(const BasicInput&) noexcept
    requires(!Blocks)
  = default;
  BasicInput& operator=(const BasicInput& other) noexcept
    requires(Blocks)
  {
    if (this != &other) {
      *this = BasicInput{other};
    }
    return *this;
  }
  BasicInput& operator=(BasicInput&&) noexcept
    requires(!Blocks)
  = default;
  BasicInput& operator=(BasicInput&& other) noexcept
    requires(Blocks)
  {
    if (this != &other) {
      _begin = std::exchange(other._begin, nullptr);
      _pos = std::exchange(other._pos, nullptr);
      _end = std::exchange(other._end, nullptr);
      _file = other._file;
      _block = std::move(other._block);
    }
    return *this;
  }

  explicit operator bool() const noexcept {
    return _file != nullptr || _begin != nullptr;
  }

  IRS_FORCE_INLINE byte_type ReadByte() {
    if constexpr (Blocks) {
      if (_pos == _end) [[unlikely]] {
        return ReadByteSlow();
      }
    }
    SDB_ASSERT(_pos < _end);
    return *_pos++;
  }
  IRS_FORCE_INLINE int16_t ReadI16() { return ReadFixed<uint16_t>(); }
  IRS_FORCE_INLINE int32_t ReadI32() { return ReadFixed<uint32_t>(); }
  IRS_FORCE_INLINE int64_t ReadI64() { return ReadFixed<uint64_t>(); }
  IRS_FORCE_INLINE uint32_t ReadV32() { return ReadVarint<uint32_t>(); }
  IRS_FORCE_INLINE uint64_t ReadV64() { return ReadVarint<uint64_t>(); }

  IRS_FORCE_INLINE void ReadData(byte_type* b, uint64_t count) {
    if (count > Remain()) [[unlikely]] {
      return ReadDataSlow(b, count);
    }
    if (count != 0) [[likely]] {
      std::memcpy(b, _pos, count);
      _pos += count;
    }
  }
  void ReadData(uint64_t offset, byte_type* b, uint64_t count) {
    Seek(offset);
    ReadData(b, count);
  }

  IRS_FORCE_INLINE uint64_t Position() const noexcept {
    if constexpr (Blocks) {
      return _block.start + static_cast<uint64_t>(_pos - _begin);
    } else {
      return static_cast<uint64_t>(_pos - _begin);
    }
  }
  uint64_t Length() const noexcept {
    if constexpr (Blocks) {
      if (_file != nullptr) {
        return _file->Length();
      }
    }
    return static_cast<uint64_t>(_end - _begin);
  }
  bool IsEOF() const noexcept { return Position() >= Length(); }

  IRS_FORCE_INLINE void Seek(uint64_t position) {
    if constexpr (Blocks) {
      if (const auto offset = position - _block.start;
          offset <= static_cast<uint64_t>(_end - _begin)) [[likely]] {
        _pos = _begin + offset;
        return;
      }
      SeekOutside(position);
    } else {
      SDB_ASSERT(position <= Length());
      _pos = _begin + position;
    }
  }
  IRS_FORCE_INLINE void Skip(uint64_t count) {
    if constexpr (Blocks) {
      if (count <= Remain()) [[likely]] {
        _pos += count;
        return;
      }
      SeekOutside(Position() + count);
    } else {
      Consume(count);
    }
  }
  IRS_FORCE_INLINE void Consume(uint64_t count) noexcept {
    SDB_ASSERT(count <= Remain());
    _pos += count;
  }

  const byte_type* Current() const noexcept { return _pos; }
  const byte_type* Peek(uint64_t count) {
    if constexpr (Blocks) {
      if (_pos == _end) [[unlikely]] {
        Refill(true);
      }
    }
    return Remain() >= count ? _pos : nullptr;
  }

  IRS_FORCE_INLINE Pinned Pin(uint64_t count) {
    if constexpr (Blocks) {
      if (_block.pinned || count > Remain()) [[unlikely]] {
        return PinSlow(count);
      }
    } else if (count > Remain()) [[unlikely]] {
      ThrowPastEnd(_file, Position(), Length());
    }
    const bytes_view bytes{_pos, count};
    _pos += count;
    return {.bytes = bytes};
  }
  IRS_FORCE_INLINE Pinned Pin(uint64_t offset, uint64_t count) const {
    const auto length = static_cast<uint64_t>(_end - _begin);
    if constexpr (Blocks) {
      if (_block.pinned || offset > length || count > length - offset)
        [[unlikely]] {
        return PinSlow(offset, count);
      }
    } else if (offset > length || count > length - offset) [[unlikely]] {
      ThrowPastEnd(_file, offset, length);
    }
    return {.bytes = {_begin + offset, count}};
  }

  bytes_view Span() const noexcept {
    if (Whole()) {
      return Window();
    }
    return {};
  }
  bytes_view Window() const noexcept {
    return {_begin, static_cast<size_t>(_end - _begin)};
  }
  bool Whole() const noexcept {
    if constexpr (Blocks) {
      return !_block.pinned && (_begin != _end || Length() == 0);
    } else {
      return true;
    }
  }
  const InputFile* File() const noexcept { return _file; }

 private:
  struct Bytes {
    using iterator_category = std::input_iterator_tag;
    using value_type = byte_type;
    using difference_type = std::ptrdiff_t;
    using pointer = const byte_type*;
    using reference = byte_type;

    byte_type operator*() const { return in.ReadByte(); }
    Bytes& operator++() noexcept { return *this; }

    BasicInput& in;
  };

  IRS_FORCE_INLINE uint64_t Remain() const noexcept {
    return static_cast<uint64_t>(_end - _pos);
  }

  template<typename T>
  IRS_FORCE_INLINE T ReadFixed() {
    if constexpr (Blocks) {
      if (Remain() < sizeof(T)) [[unlikely]] {
        return ReadFixedSlow<T>();
      }
    }
    return irs::read<T>(_pos);
  }

  template<typename T>
  IRS_FORCE_INLINE T ReadVarint() {
    if constexpr (Blocks) {
      if (Remain() < bytes_io<T>::kMaxVSize) [[unlikely]] {
        return ReadVarintSlow<T>();
      }
    }
    return irs::vread<T>(_pos);
  }

  template<typename T>
  IRS_NO_INLINE T ReadFixedSlow() {
    Bytes bytes{*this};
    return irs::read<T>(bytes);
  }

  template<typename T>
  IRS_NO_INLINE T ReadVarintSlow() {
    Bytes bytes{*this};
    return irs::vread<T>(bytes);
  }

  IRS_NO_INLINE byte_type ReadByteSlow() {
    if (!Refill(true)) {
      ThrowPastEnd(_file, Position(), Length());
    }
    return *_pos++;
  }

  IRS_NO_INLINE Pinned PinSlow(uint64_t count) {
    if (Whole()) {
      ThrowPastEnd(_file, Position(), Length());
    }
    auto pinned = _file->Pin(Position(), count);
    Skip(count);
    return pinned;
  }

  IRS_NO_INLINE Pinned PinSlow(uint64_t offset, uint64_t count) const {
    if (Whole()) {
      ThrowPastEnd(_file, offset, Length());
    }
    return _file->Pin(offset, count);
  }

  IRS_NO_INLINE void ReadDataSlow(byte_type* b, uint64_t count) {
    if constexpr (Blocks) {
      if (!Whole()) {
        const auto at = Position();
        const auto from = at + Remain();
        const auto to = at + count;
        if (from / duckdb::BlockCache::BLOCK_SIZE !=
            (to - 1) / duckdb::BlockCache::BLOCK_SIZE) {
          _file->Prefetch(from, to - from);
        }
        do {
          if (_pos == _end && !Refill(false)) {
            break;
          }
          const auto chunk = std::min(count, Remain());
          std::memcpy(b, _pos, chunk);
          _pos += chunk;
          b += chunk;
          count -= chunk;
        } while (count != 0);
        if (count == 0) {
          return;
        }
      }
    }
    ThrowPastEnd(_file, Position(), Length());
  }

  void SeekOutside(uint64_t position) {
    if (Whole()) {
      _pos = _end;
      return;
    }
    if (_block.pinned) {
      _block.pinned->pin.Reset();
    }
    _begin = _pos = _end = nullptr;
    _block.start = position;
  }

  bool Refill(bool ahead) {
    if (Whole()) {
      return false;
    }
    const auto position = Position();
    if (position >= _file->Length()) {
      return false;
    }
    const auto window = _file->Refill(position, _block, ahead);
    _begin = window.data();
    _end = window.data() + window.size();
    _pos = _begin + (position - _block.start);
    return _pos != _end;
  }

  const byte_type* _begin = nullptr;
  const byte_type* _pos = nullptr;
  const byte_type* _end = nullptr;
  const InputFile* _file = nullptr;
  [[no_unique_address]] utils::Need<Blocks, CursorBlock> _block;
};

using BytesViewInput = BasicInput<false>;
using IndexInput = BasicInput<true>;

static_assert(sizeof(BytesViewInput) == 4 * sizeof(void*));

template<typename F>
auto ResolveInput(const IndexInput& in, F&& f) {
  if (in.Whole()) {
    return f.template operator()<BytesViewInput>();
  }
  return f.template operator()<IndexInput>();
}

}  // namespace irs
