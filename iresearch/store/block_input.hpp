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

#pragma once

#include <duckdb/storage/block_cache/block_cache.hpp>
#include <limits>
#include <memory>
#include <vector>

#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class BlockInput final : public IndexInput {
 public:
  BlockInput(duckdb::BlockCache& cache,
             duckdb::shared_ptr<duckdb::BlockCacheFile> file,
             std::shared_ptr<const bstring> tail = nullptr) noexcept;
  BlockInput(const BlockInput& other) noexcept;

  IRS_FORCE_INLINE byte_type ReadByte() final {
    if (_pos != _end) [[likely]] {
      return *_pos++;
    }
    return ReadByteSlow();
  }

  uint64_t Position() const noexcept final {
    return _block_start + (_pos - _begin);
  }
  uint64_t Length() const noexcept final { return _length; }
  bool IsEOF() const noexcept final { return Position() >= _length; }

  int16_t ReadI16() final {
    return Remain() < sizeof(uint16_t) ? irs::read<uint16_t>(*this)
                                       : irs::read<uint16_t>(_pos);
  }
  int32_t ReadI32() final {
    return Remain() < sizeof(uint32_t) ? irs::read<uint32_t>(*this)
                                       : irs::read<uint32_t>(_pos);
  }
  int64_t ReadI64() final {
    return Remain() < sizeof(uint64_t) ? irs::read<uint64_t>(*this)
                                       : irs::read<uint64_t>(_pos);
  }
  uint32_t ReadV32() final {
    return Remain() < bytes_io<uint32_t>::kMaxVSize
             ? irs::vread<uint32_t>(*this)
             : irs::vread<uint32_t>(_pos);
  }
  uint64_t ReadV64() final {
    return Remain() < bytes_io<uint64_t>::kMaxVSize
             ? irs::vread<uint64_t>(*this)
             : irs::vread<uint64_t>(_pos);
  }
  void SkipV32() final {
    Remain() < bytes_io<uint32_t>::kMaxVSize ? irs::vskip<uint32_t>(*this)
                                             : irs::vskip<uint32_t>(_pos);
  }
  void SkipV64() final {
    Remain() < bytes_io<uint64_t>::kMaxVSize ? irs::vskip<uint64_t>(*this)
                                             : irs::vskip<uint64_t>(_pos);
  }

  void Skip(uint64_t count) final { Seek(Position() + count); }
  void Seek(uint64_t pos) final;

  const byte_type* ReadVolatile(uint64_t count) final;
  const byte_type* ReadVolatile(uint64_t offset, uint64_t count) final {
    Seek(offset);
    return ReadVolatile(count);
  }
  const byte_type* ReadStable(uint64_t count) final;
  const byte_type* ReadStable(uint64_t offset, uint64_t count) final {
    Seek(offset);
    return ReadStable(count);
  }
  const byte_type* Peek(uint64_t count) noexcept final {
    return Remain() >= count ? _pos : nullptr;
  }

  using DataInput::ReadData;
  void ReadData(byte_type* b, uint64_t count) final;
  void ReadData(duckdb::QueryContext, byte_type* b, uint64_t count) final {
    ReadData(b, count);
  }
  void ReadData(uint64_t offset, byte_type* b, size_t count) final {
    Seek(offset);
    ReadData(b, count);
  }

  ptr Dup() const final { return std::make_unique<BlockInput>(*this); }
  uint32_t Checksum(uint64_t offset) const final;

  void Prefetch(uint64_t offset, uint64_t count) const noexcept final;
  void PrefetchExtents(std::span<ByteExtent> extents) const noexcept final;
  bool Resident(uint64_t offset, uint64_t count) const noexcept final {
    return Warm(offset, count);
  }
  bool Warm(uint64_t offset, uint64_t count) const noexcept final;

 private:
  static constexpr uint64_t kMaxReadaheadBlocks = 64;

  IRS_FORCE_INLINE uint64_t Remain() const noexcept { return _end - _pos; }

  byte_type ReadByteSlow();
  bool PinCurrent(bool readahead = true);
  bool Window(uint64_t block, const byte_type* data, uint64_t size,
              uint64_t position) noexcept;
  void ReleasePin();
  void ReadAhead(uint64_t block, bool readahead) noexcept;

  duckdb::BlockCache& _cache;
  duckdb::shared_ptr<duckdb::BlockCacheFile> _file;
  uint64_t _length;
  std::shared_ptr<const bstring> _tail;
  uint64_t _tail_start;
  duckdb::BlockCachePin _pin;
  bool _pin_stable = false;
  const byte_type* _begin = nullptr;
  const byte_type* _pos = nullptr;
  const byte_type* _end = nullptr;
  uint64_t _block_start = 0;
  uint64_t _next_block = std::numeric_limits<uint64_t>::max();
  uint64_t _readahead_end = 0;
  uint64_t _readahead = 0;
  std::vector<byte_type> _scratch;
  containers::FlatHashMap<uint64_t, duckdb::BlockCachePin> _held;
};

}  // namespace irs
