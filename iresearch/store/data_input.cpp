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

#include <algorithm>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/math_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

template<typename... Args>
void TryPrefetch(const duckdb::shared_ptr<duckdb::BlockCacheFile>& file,
                 Args&&... args) noexcept {
  try {
    file->GetCache().Prefetch(file, std::forward<Args>(args)...);
  } catch (...) {
  }
}

}  // namespace

void ReadOnce(IndexInput& in, byte_type* b, uint64_t count) {
  const auto* file = in.File();
  if (!file) {
    in.ReadData(b, count);
    return;
  }
  constexpr uint64_t kChunk = uint64_t{64} << 20;
  constexpr uint64_t kMappedAhead = 2 * kChunk;
  const auto start = in.Position();
  for (uint64_t done = 0; done < count;) {
    const auto rest = count - done;
    const auto n = std::min(rest, kChunk);
    file->Prefetch(start + done,
                   file->Contiguous() ? std::min(rest, kMappedAhead) : rest);
    in.ReadData(b + done, n);
    file->Evict(start + done, n);
    done += n;
  }
}

InputFile::InputFile(std::unique_ptr<duckdb::MemoryMappedFile> mapping)
  : _length{mapping->Size()},
    _mapping{std::move(mapping)},
    _paged{_mapping->OnDiskFile()} {
  if (_length != 0) {
    _span = {_mapping->GetData(0, _length), _length};
    if (_paged) {
      _residency.Reset(math::DivCeil64(_length, file_utils::kPage));
    }
  }
}

InputFile::InputFile(duckdb::shared_ptr<duckdb::BlockCacheFile> blocks) noexcept
  : _length{blocks->GetSize()}, _blocks{std::move(blocks)} {}

bytes_view InputFile::Refill(uint64_t position, CursorBlock& block,
                             bool ahead) const {
  SDB_ASSERT(position < _length);
  if (!_blocks) {
    block.start = 0;
    return _span;
  }
  if (!block.pinned) {
    block.pinned = std::make_unique<CursorPin>();
  }
  auto& pinned = *block.pinned;
  const auto number = position / duckdb::BlockCache::BLOCK_SIZE;
  pinned.pin =
    _blocks->GetCache().PinInOrder(_blocks, number, pinned.ahead, ahead);
  block.start = number * duckdb::BlockCache::BLOCK_SIZE;
  return {pinned.pin.Ptr(),
          std::min<uint64_t>(pinned.pin.Size(), _length - block.start)};
}

PinnedBytes InputFile::Pin(uint64_t offset, uint64_t size) const {
  if (offset > _length || size > _length - offset) [[unlikely]] {
    ThrowPastEnd(this, offset, _length);
  }
  if (!_blocks) {
    return {.bytes = {_span.data() + offset, size}};
  }
  if (size == 0) {
    return {};
  }
  auto pin = _blocks->GetCache().PinBytes(*_blocks, offset, size);
  return {.bytes = {pin.Ptr(), size}, .pin = std::move(pin)};
}

void InputFile::Prefetch(uint64_t offset, uint64_t size) const noexcept {
  if (_blocks) {
    TryPrefetch(_blocks, offset, size, duckdb::BlockCachePrefetch::HINT);
  } else if (_paged) {
    _mapping->Prefetch(offset, size);
  }
}

void InputFile::Evict(uint64_t offset, uint64_t size) const noexcept {
  if (_blocks) {
    _blocks->GetCache().MarkUnused(*_blocks, offset, size);
  } else if (_paged) {
    _mapping->Evict(offset, size);
  }
}

void InputFile::PrefetchExtents(
  std::span<duckdb::BlockCacheRange> extents) const noexcept {
  if (_blocks) {
    TryPrefetch(_blocks, extents, duckdb::BlockCachePrefetch::NEED);
    return;
  }
  for (const auto& extent : extents) {
    Prefetch(extent.offset, extent.size);
  }
}

void InputFile::MarkWarm(uint64_t offset, uint64_t size) const noexcept {
  if (!_paged || size == 0 || offset >= _length) {
    return;
  }
  const auto last =
    (std::min<uint64_t>(offset + size, _length) - 1) / file_utils::kPage;
  const auto epoch = file_utils::ResidencyEpoch();
  if (_residency.Valid(epoch) || _residency.Adopt(epoch)) {
    _residency.Set(offset / file_utils::kPage, last);
  }
}

uint64_t InputFile::Warm(uint64_t offset, uint64_t size) const noexcept {
  if (_blocks) {
    return _blocks->GetCache().IsCached(*_blocks, offset, size) ? size : 0;
  }
  if (!_paged) {
    return size;
  }
  if (size == 0 || offset >= _length ||
      !_residency.Valid(file_utils::ResidencyEpoch())) {
    return 0;
  }
  const uint64_t first = offset / file_utils::kPage;
  const uint64_t last =
    (std::min<uint64_t>(offset + size, _length) - 1) / file_utils::kPage;
  const uint64_t pages = _residency.Test(first, last);
  if (pages > last - first) {
    return size;
  }
  const uint64_t warm = (first + pages) * file_utils::kPage;
  return warm - std::min(warm, offset);
}

bool InputFile::Evicted(uint64_t offset, uint64_t size,
                        uint32_t& epoch) const noexcept {
  if (Warm(offset, size) == size && !file_utils::PollResidency(size)) {
    return false;
  }
  const auto current = file_utils::ResidencyEpoch();
  if (current == epoch) {
    return false;
  }
  epoch = current;
  return true;
}

bool InputFile::Resident(uint64_t offset, uint64_t size) const noexcept {
  if (_blocks) {
    return _blocks->GetCache().IsCached(*_blocks, offset, size);
  }
  if (offset >= _span.size()) {
    return false;
  }
  size = std::min<uint64_t>(size, _span.size() - offset);
  return size != 0 &&
         (!_paged || file_utils::IsResident(_span.data() + offset, size));
}

bool InputFile::Hint(uint64_t offset, uint64_t size) const noexcept {
  if (_blocks) {
    if (_blocks->GetCache().IsCached(*_blocks, offset, size)) {
      return false;
    }
    TryPrefetch(_blocks, offset, size, duckdb::BlockCachePrefetch::HINT);
    return true;
  }
  if (!_paged) {
    return false;
  }
  uint64_t warm = Warm(offset, size);
  SDB_ASSERT(warm <= size);
  if (warm != 0 && file_utils::PollResidency(warm)) {
    warm = 0;
  }
  if (warm == size) {
    return false;
  }
  offset += warm;
  size -= warm;
  const bool fetch =
    size <= file_utils::kPrefetchChunk || !Resident(offset, size);
  if (fetch) {
    _mapping->Prefetch(offset, size);
  }
  MarkWarm(offset, size);
  return fetch;
}

void ThrowPastEnd(const InputFile* file, uint64_t position, uint64_t length) {
  if (file != nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                    ERR_MSG("read past the end of '", file->Path(),
                            "' at offset ", position, " of ", length));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("read past the end of an in-memory buffer at offset ",
                          position, " of ", length));
}

}  // namespace irs
