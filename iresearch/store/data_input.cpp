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
void TryPrefetch(duckdb::BlockCache& cache, Args&&... args) noexcept {
  try {
    cache.Prefetch(std::forward<Args>(args)...);
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
    _kind{std::in_place_type<MappedSpan>, std::move(mapping)} {
  auto& span = *std::get_if<MappedSpan>(&_kind);
  span.paged = span.mapping->OnDiskFile();
  if (_length != 0) {
    _span = {span.mapping->GetData(0, _length), _length};
    if (span.paged) {
      span.residency.Reset(math::DivCeil64(_length, file_utils::kPage));
    }
  }
}

InputFile::InputFile(duckdb::BlockCache& cache,
                     duckdb::shared_ptr<duckdb::BlockCacheFile> blocks) noexcept
  : _length{blocks->GetSize()},
    _kind{std::in_place_type<CacheBlocks>, &cache, std::move(blocks)} {}

const std::string& InputFile::Path() const noexcept {
  if (const auto* span = std::get_if<MappedSpan>(&_kind)) {
    return span->mapping->GetPath();
  }
  return std::get_if<CacheBlocks>(&_kind)->blocks->GetPath();
}

duckdb::MemoryMappedFile* InputFile::Mapping() const noexcept {
  const auto* span = std::get_if<MappedSpan>(&_kind);
  return span && !_span.empty() ? span->mapping.get() : nullptr;
}

bool InputFile::Paged() const noexcept {
  const auto* span = std::get_if<MappedSpan>(&_kind);
  return span && span->paged;
}

bytes_view InputFile::Refill(uint64_t position, CursorBlock& block,
                             bool ahead) const {
  SDB_ASSERT(position < _length);
  if (Contiguous()) {
    block.start = 0;
    return _span;
  }
  const auto* blocks = std::get_if<CacheBlocks>(&_kind);
  if (!block.pinned) {
    block.pinned = std::make_unique<CursorPin>();
  }
  auto& pinned = *block.pinned;
  const auto number = position / duckdb::BlockCache::BLOCK_SIZE;
  pinned.pin =
    blocks->cache->PinInOrder(blocks->blocks, number, pinned.ahead, ahead);
  block.start = number * duckdb::BlockCache::BLOCK_SIZE;
  return {pinned.pin.Ptr(),
          std::min<uint64_t>(pinned.pin.Size(), _length - block.start)};
}

PinnedBytes InputFile::Pin(uint64_t offset, uint64_t size) const {
  if (offset > _length || size > _length - offset) [[unlikely]] {
    ThrowPastEnd(this, offset, _length);
  }
  if (Contiguous()) {
    return {.bytes = {_span.data() + offset, size}};
  }
  if (size == 0) {
    return {};
  }
  const auto* blocks = std::get_if<CacheBlocks>(&_kind);
  auto pin = blocks->cache->PinBytes(*blocks->blocks, offset, size);
  return {.bytes = {pin.Ptr(), size}, .pin = std::move(pin)};
}

bytes_view InputFile::SpanWindow(uint64_t offset,
                                 uint64_t size) const noexcept {
  const auto bytes = Span();
  if (offset >= bytes.size()) {
    return {};
  }
  return {bytes.data() + offset,
          std::min<uint64_t>(size, bytes.size() - offset)};
}

void InputFile::Prefetch(uint64_t offset, uint64_t size) const noexcept {
  if (const auto* span = std::get_if<MappedSpan>(&_kind)) {
    if (span->paged) {
      span->mapping->Prefetch(offset, size);
    }
    return;
  }
  const auto* blocks = std::get_if<CacheBlocks>(&_kind);
  TryPrefetch(*blocks->cache, blocks->blocks, offset, size,
              duckdb::BlockCachePrefetch::HINT);
}

void InputFile::Evict(uint64_t offset, uint64_t size) const noexcept {
  if (const auto* blocks = std::get_if<CacheBlocks>(&_kind)) {
    blocks->cache->MarkUnused(*blocks->blocks, offset, size);
    return;
  }
  if (const auto* span = std::get_if<MappedSpan>(&_kind); span->paged) {
    span->mapping->Evict(offset, size);
  }
}

void InputFile::PrefetchExtents(std::span<ByteExtent> extents) const noexcept {
  const auto* blocks = std::get_if<CacheBlocks>(&_kind);
  if (!blocks) {
    for (const auto& extent : extents) {
      Prefetch(extent.offset, extent.size);
    }
    return;
  }
  TryPrefetch(*blocks->cache, blocks->blocks, extents,
              duckdb::BlockCachePrefetch::NEED);
}

void InputFile::MarkWarm(uint64_t offset, uint64_t size) const noexcept {
  if (const auto* span = std::get_if<MappedSpan>(&_kind); span && span->paged) {
    MarkWarm(*span, offset, size);
  }
}

uint64_t InputFile::Warm(uint64_t offset, uint64_t size) const noexcept {
  if (const auto* blocks = std::get_if<CacheBlocks>(&_kind)) {
    return blocks->cache->IsCached(*blocks->blocks, offset, size) ? size : 0;
  }
  const auto& span = *std::get_if<MappedSpan>(&_kind);
  return span.paged ? Warm(span, offset, size) : size;
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

uint64_t InputFile::Warm(const MappedSpan& span, uint64_t offset,
                         uint64_t size) const noexcept {
  if (size == 0 || offset >= _length ||
      !span.residency.Valid(file_utils::ResidencyEpoch())) {
    return 0;
  }
  const uint64_t first = offset / file_utils::kPage;
  const uint64_t last =
    (std::min<uint64_t>(offset + size, _length) - 1) / file_utils::kPage;
  const uint64_t pages = span.residency.Test(first, last);
  if (pages > last - first) {
    return size;
  }
  const uint64_t warm = (first + pages) * file_utils::kPage;
  return warm - std::min(warm, offset);
}

void InputFile::MarkWarm(const MappedSpan& span, uint64_t offset,
                         uint64_t size) const noexcept {
  if (size == 0 || offset >= _length) {
    return;
  }
  const auto last =
    (std::min<uint64_t>(offset + size, _length) - 1) / file_utils::kPage;
  const auto epoch = file_utils::ResidencyEpoch();
  if (span.residency.Valid(epoch) || span.residency.Adopt(epoch)) {
    span.residency.Set(offset / file_utils::kPage, last);
  }
}

bool InputFile::Resident(uint64_t offset, uint64_t size) const noexcept {
  if (const auto* blocks = std::get_if<CacheBlocks>(&_kind)) {
    return blocks->cache->IsCached(*blocks->blocks, offset, size);
  }
  const auto window = SpanWindow(offset, size);
  return !window.empty() &&
         (!Paged() || file_utils::IsResident(window.data(), window.size()));
}

bool InputFile::Hint(uint64_t offset, uint64_t size) const noexcept {
  if (const auto* blocks = std::get_if<CacheBlocks>(&_kind)) {
    if (blocks->cache->IsCached(*blocks->blocks, offset, size)) {
      return false;
    }
    Prefetch(offset, size);
    return true;
  }
  const auto& span = *std::get_if<MappedSpan>(&_kind);
  if (!span.paged) {
    return false;
  }
  uint64_t warm = Warm(span, offset, size);
  SDB_ASSERT(warm <= size);
  if (warm != 0 && file_utils::PollResidency(warm)) {
    warm = 0;
  }
  if (warm == size) {
    return false;
  }
  offset += warm;
  size -= warm;
  const auto window = SpanWindow(offset, size);
  const bool fetch = size <= file_utils::kPrefetchChunk || window.empty() ||
                     !file_utils::IsResident(window.data(), window.size());
  if (fetch) {
    span.mapping->Prefetch(offset, size);
  }
  MarkWarm(span, offset, size);
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
