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

void ReadOnce(IndexInput& in, byte_type* b, uint64_t count) {
  const auto* file = in.File();
  if (!file) {
    in.ReadData(b, count);
    return;
  }
  constexpr uint64_t kChunk = uint64_t{64} << 20;
  constexpr uint64_t kAhead = 2 * kChunk;
  const auto start = in.Position();
  file->Prefetch(start, std::min(count, kAhead));
  for (uint64_t done = 0; done < count;) {
    const auto n = std::min(count - done, kChunk);
    if (done + kAhead < count) {
      file->Prefetch(start + done + kAhead,
                     std::min(count - done - kAhead, kChunk));
    }
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
  auto pin = blocks->cache->PinBytes(blocks->blocks, offset, size);
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
  try {
    blocks->cache->Prefetch(blocks->blocks, offset, size);
  } catch (...) {
  }
}

void InputFile::Evict(uint64_t offset, uint64_t size) const noexcept {
  if (const auto* span = std::get_if<MappedSpan>(&_kind); span && span->paged) {
    span->mapping->Evict(offset, size);
  }
}

void InputFile::PrefetchExtents(std::span<ByteExtent> extents) const noexcept {
  if (std::holds_alternative<CacheBlocks>(_kind)) {
    extents = CoalesceExtents(extents, duckdb::BlockCache::MERGE_GAP);
  }
  for (const auto& extent : extents) {
    Prefetch(extent.offset, extent.size);
  }
}

bool InputFile::Warm(const MappedSpan& span, uint64_t offset,
                     uint64_t size) const noexcept {
  if (size == 0 || offset >= _length) {
    return false;
  }
  const auto last =
    (std::min<uint64_t>(offset + size, _length) - 1) / file_utils::kPage;
  return span.residency.Valid(file_utils::ResidencyEpoch()) &&
         span.residency.Test(offset / file_utils::kPage, last);
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
  if (Warm(span, offset, size)) {
    file_utils::PollResidency();
    return false;
  }
  const bool fetch =
    size <= file_utils::kPrefetchChunk || !Resident(offset, size);
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
