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

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <memory>
#include <span>
#include <string>
#include <utility>

#include "iresearch/utils/empty.hpp"
#include "iresearch/utils/file_utils_ext.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

struct PinnedBytes {
  bytes_view bytes;
  duckdb::ExternalFileCachePin pin;
};

struct SpanBytes {
  bytes_view bytes;
};

struct CursorPin {
  duckdb::ExternalFileCachePin pin;
  duckdb::ExternalFileCacheReadAhead ahead;
};

struct CursorBlock {
  uint64_t start = 0;
  std::unique_ptr<CursorPin> pinned;
};

class InputFile {
 public:
  explicit InputFile(std::unique_ptr<duckdb::MemoryMappedFile> mapping);
  explicit InputFile(
    duckdb::shared_ptr<duckdb::ExternalFileCacheFile> blocks) noexcept;
  InputFile(const InputFile&) = delete;
  InputFile& operator=(const InputFile&) = delete;

  uint64_t Length() const noexcept { return _length; }
  const std::string& Path() const noexcept {
    return _blocks ? _blocks->GetPath() : _mapping->GetPath();
  }
  bool Contiguous() const noexcept { return !_blocks; }
  bytes_view Span() const noexcept { return _span; }
  duckdb::MemoryMappedFile* Mapping() const noexcept {
    return _span.empty() ? nullptr : _mapping.get();
  }
  bool Paged() const noexcept { return _paged; }
  bytes_view Refill(uint64_t position, CursorBlock& block, bool ahead) const;
  PinnedBytes Pin(uint64_t offset, uint64_t size) const;

  bool Hint(uint64_t offset, uint64_t size) const noexcept;
  void Prefetch(uint64_t offset, uint64_t size) const noexcept;
  void PrefetchExtents(
    std::span<duckdb::ExternalFileCacheRange> extents) const noexcept;
  void Evict(uint64_t offset, uint64_t size) const noexcept;
  bool Resident(uint64_t offset, uint64_t size) const noexcept;
  uint64_t Warm(uint64_t offset, uint64_t size) const noexcept;
  void MarkWarm(uint64_t offset, uint64_t size) const noexcept;
  bool Evicted(uint64_t offset, uint64_t size, uint32_t& epoch) const noexcept;

 private:
  uint64_t _length;
  bytes_view _span;
  std::unique_ptr<duckdb::MemoryMappedFile> _mapping;
  duckdb::shared_ptr<duckdb::ExternalFileCacheFile> _blocks;
  file_utils::ResidencyMap _residency;
  bool _paged = false;
};

[[noreturn]] void ThrowPastEnd(const InputFile* file, uint64_t position,
                               uint64_t length);

template<bool Contiguous>
class BasicInput {
 public:
  static constexpr bool kContiguous = Contiguous;
  using Pinned = std::conditional_t<Contiguous, SpanBytes, PinnedBytes>;

  BasicInput() noexcept = default;
  explicit BasicInput(bytes_view bytes) noexcept
    : _begin{bytes.data()},
      _pos{bytes.data()},
      _end{bytes.data() + bytes.size()} {}
  explicit BasicInput(const InputFile& file) noexcept : _file{&file} {
    SDB_ASSERT(!Contiguous || file.Contiguous());
    const auto span = file.Span();
    _begin = _pos = span.data();
    _end = span.data() + span.size();
  }

  BasicInput(const BasicInput&) noexcept
    requires(Contiguous)
  = default;
  BasicInput(BasicInput&&) noexcept
    requires(Contiguous)
  = default;
  BasicInput(BasicInput&& other) noexcept
    requires(!Contiguous)
    : _begin{std::exchange(other._begin, nullptr)},
      _pos{std::exchange(other._pos, nullptr)},
      _end{std::exchange(other._end, nullptr)},
      _file{other._file},
      _block{std::move(other._block)} {}
  BasicInput& operator=(const BasicInput&) noexcept
    requires(Contiguous)
  = default;
  BasicInput& operator=(BasicInput&&) noexcept
    requires(Contiguous)
  = default;
  BasicInput& operator=(BasicInput&& other) noexcept
    requires(!Contiguous)
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
    if constexpr (!Contiguous) {
      if (_pos == _end) [[unlikely]] {
        return ReadByteSlow();
      }
    }
    SDB_ASSERT(_pos < _end);
    return *_pos++;
  }
  IRS_FORCE_INLINE int32_t ReadI32() { return ReadFixed<uint32_t>(); }
  IRS_FORCE_INLINE int64_t ReadI64() { return ReadFixed<uint64_t>(); }

  IRS_FORCE_INLINE void ReadData(byte_type* b, uint64_t count) {
    if (count > Remain()) [[unlikely]] {
      return ReadDataSlow(b, count);
    }
    if (count != 0) [[likely]] {
      std::memcpy(b, _pos, count);
      _pos += count;
    }
  }
  void ReadData(uint64_t offset, byte_type* b, uint64_t count)
    requires(!Contiguous)
  {
    Seek(offset);
    ReadData(b, count);
  }

  IRS_FORCE_INLINE uint64_t Position() const noexcept {
    const auto offset = static_cast<uint64_t>(_pos - _begin);
    if constexpr (Contiguous) {
      return offset;
    } else {
      return _block.start + offset;
    }
  }
  uint64_t Length() const noexcept {
    if constexpr (!Contiguous) {
      if (_file != nullptr) {
        return _file->Length();
      }
    }
    return static_cast<uint64_t>(_end - _begin);
  }

  IRS_FORCE_INLINE void Seek(uint64_t position) {
    if constexpr (Contiguous) {
      SDB_ASSERT(position <= Length());
      _pos = _begin + position;
    } else if (const auto offset = position - _block.start;
               offset <= static_cast<uint64_t>(_end - _begin)) [[likely]] {
      _pos = _begin + offset;
    } else {
      SeekOutside(position);
    }
  }
  IRS_FORCE_INLINE void Skip(uint64_t count) {
    if constexpr (Contiguous) {
      Consume(count);
    } else if (count <= Remain()) [[likely]] {
      _pos += count;
    } else {
      SeekOutside(Position() + count);
    }
  }
  IRS_FORCE_INLINE void Consume(uint64_t count) noexcept {
    SDB_ASSERT(count <= Remain());
    _pos += count;
  }

  const byte_type* Current() const noexcept
    requires(Contiguous)
  {
    return _pos;
  }
  IRS_FORCE_INLINE bytes_view Peek(uint64_t count)
    requires(!Contiguous)
  {
    if (count <= Remain()) [[likely]] {
      return {_pos, count};
    }
    return PeekSlow(count);
  }

  IRS_FORCE_INLINE Pinned Pin(uint64_t count) {
    if constexpr (Contiguous) {
      SDB_ASSERT(count <= Remain());
    } else if (_block.pinned || count > Remain()) [[unlikely]] {
      return PinSlow(count);
    }
    const bytes_view bytes{_pos, count};
    _pos += count;
    return {.bytes = bytes};
  }
  IRS_FORCE_INLINE Pinned Pin(uint64_t offset, uint64_t count) const {
    const auto length = static_cast<uint64_t>(_end - _begin);
    if constexpr (Contiguous) {
      SDB_ASSERT(offset <= length && count <= length - offset);
    } else if (_block.pinned || offset > length || count > length - offset)
      [[unlikely]] {
      return PinSlow(offset, count);
    }
    return {.bytes = {_begin + offset, count}};
  }

  const InputFile* File() const noexcept { return _file; }

 private:
  IRS_FORCE_INLINE uint64_t Remain() const noexcept {
    return static_cast<uint64_t>(_end - _pos);
  }

  template<typename T>
  IRS_FORCE_INLINE T ReadFixed() {
    if constexpr (!Contiguous) {
      if (Remain() < sizeof(T)) [[unlikely]] {
        return ReadFixedSlow<T>();
      }
    }
    const auto value = absl::little_endian::Load<T>(_pos);
    _pos += sizeof(T);
    return value;
  }

  template<typename T>
  IRS_NO_INLINE T ReadFixedSlow() {
    byte_type bytes[sizeof(T)];
    for (auto& b : bytes) {
      b = ReadByte();
    }
    return absl::little_endian::Load<T>(bytes);
  }

  IRS_NO_INLINE bytes_view PeekSlow(uint64_t count) {
    if (_pos == _end && Refill(true) && count <= Remain()) {
      return {_pos, count};
    }
    return {};
  }

  IRS_NO_INLINE byte_type ReadByteSlow() {
    if (!Refill(true)) {
      ThrowPastEnd(_file, Position(), Length());
    }
    return *_pos++;
  }

  IRS_NO_INLINE Pinned PinSlow(uint64_t count) {
    auto pinned = PinSlow(Position(), count);
    Skip(count);
    return pinned;
  }

  IRS_NO_INLINE Pinned PinSlow(uint64_t offset, uint64_t count) const {
    if (_file == nullptr) {
      ThrowPastEnd(_file, offset, Length());
    }
    if (_block.pinned && offset >= _block.start) {
      const auto at = offset - _block.start;
      const auto length = static_cast<uint64_t>(_end - _begin);
      if (at <= length && count <= length - at) {
        if (auto pin = _block.pinned->pin.Share(at, count); pin.IsValid()) {
          return {.bytes = {_begin + at, count}, .pin = std::move(pin)};
        }
      }
    }
    return _file->Pin(offset, count);
  }

  IRS_NO_INLINE void ReadDataSlow(byte_type* b, uint64_t count) {
    if constexpr (!Contiguous) {
      if (_file != nullptr) {
        const auto at = Position();
        const auto from = at + Remain();
        const auto to = at + count;
        if (from / duckdb::ExternalFileCache::BLOCK_SIZE !=
            (to - 1) / duckdb::ExternalFileCache::BLOCK_SIZE) {
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

  IRS_NO_INLINE void SeekOutside(uint64_t position) {
    if (_file == nullptr) {
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
    const auto position = Position();
    if (_file == nullptr || position >= _file->Length()) {
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
  [[no_unique_address]] utils::Need<!Contiguous, CursorBlock> _block;
};

using BytesViewInput = BasicInput<true>;
using IndexInput = BasicInput<false>;

static_assert(sizeof(BytesViewInput) == 4 * sizeof(void*));

void ReadOnce(IndexInput& in, byte_type* b, uint64_t count);

template<typename F>
auto ResolveInput(const InputFile& file, F&& f) {
  if (file.Contiguous()) {
    return f.template operator()<BytesViewInput>();
  }
  return f.template operator()<IndexInput>();
}

template<typename F>
auto ResolveInput(const InputFile& doc, const InputFile& pos,
                  const InputFile* pay, F&& f) {
  if (doc.Contiguous() && pos.Contiguous() && (!pay || pay->Contiguous())) {
    return f.template operator()<BytesViewInput>();
  }
  return f.template operator()<IndexInput>();
}

}  // namespace irs
