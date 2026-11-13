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

#include <absl/algorithm/container.h>
#include <fsst.h>

#include <algorithm>
#include <cstdint>
#include <deque>
#include <iterator>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_inline.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_reader.hpp"
#include "iresearch/formats/index/term_dict_root.hpp"
#include "iresearch/formats/index/term_dict_single.hpp"
#include "iresearch/formats/posting/reader.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/containers/node_hash_map.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs::term_dict {

class FieldReader final {
 public:
  explicit FieldReader(IResourceManager& rm) noexcept : _rm{&rm} {}
  ~FieldReader();

  FieldReader(const FieldReader&) = delete;
  FieldReader& operator=(const FieldReader&) = delete;

  uint64_t CountMappedMemory() const;
  void prepare(const ReaderState& state);

  std::span<const TermReader* const> fields(field_id id) const;
  std::span<const field_id> field_ids() const noexcept { return _ids; }
  size_t size() const noexcept { return _dicts.size(); }

  const PostingsReader& Postings() const noexcept { return _pr; }
  const IndexInput& Input() const noexcept { return *_in; }
  const byte_type* Base() const noexcept { return _base; }
  void Account(uint64_t bytes) {
    if (bytes != 0) {
      _rm->Increase(bytes);
      _bytes += bytes;
    }
  }
  uint32_t BlockCount() const noexcept { return _block_count; }
  uint64_t BlockOffset(size_t block) const noexcept {
    uint64_t page = _first_page + block;
    if (!_spans.empty()) [[unlikely]] {
      const auto it = absl::c_lower_bound(
        _spans, block, [](const SpanningBlock& span, size_t b) noexcept {
          return span.block < b;
        });
      if (it != _spans.begin()) {
        page += std::prev(it)->extra;
      }
    }
    return std::min(page * kPage, _inline_offset);
  }
  uint64_t InlineBegin() const noexcept { return _inline_offset; }
  uint64_t InlineEnd() const noexcept { return _inline_offset + _inline_size; }
  const byte_type* InlineRegion() const noexcept { return _inline_region; }
  InlinePages::Span InlineSpan(uint32_t block) const noexcept {
    return _inline_pages.Of(block);
  }
  uint64_t RangesBegin() const noexcept { return InlineEnd(); }
  uint64_t RangesSize() const noexcept { return _ranges_size; }
  bytes_view Load(IndexInput& in, uint64_t offset, uint64_t size);
  bytes_view Copy(uint64_t offset, uint64_t size);
  const byte_type* CopyBlock(uint32_t block, uint64_t length);
  const byte_type* KeepInline(bytes_view bytes);

 private:
  struct PendingCopy {
    uint64_t offset;
    uint64_t size;
    byte_type* data;
  };

  struct FieldDicts {
    uint32_t first = 0;
    uint32_t count = 0;
  };

  Separators Bind(const TermsMeta& terms);
  void FillCopies();

  std::deque<TermReaderImpl> _keyed;
  std::deque<SingleTermReader> _singles;
  std::vector<const TermReader*> _dicts;
  containers::FlatHashMap<field_id, FieldDicts> _by_id;
  std::vector<field_id> _ids;
  PostingsReader _pr;
  IndexInput::ptr _in;
  const byte_type* _base = nullptr;
  IResourceManager* _rm;
  std::vector<SpanningBlock> _spans;
  InlinePages _inline_pages;
  containers::NodeHashMap<uint64_t, bstring> _copies;
  std::vector<byte_type> _single_inline;
  const byte_type* _inline_region = nullptr;
  uint64_t _first_page = 0;
  uint64_t _inline_offset = 0;
  uint64_t _inline_size = 0;
  uint64_t _ranges_size = 0;
  uint32_t _block_count = 0;
  std::vector<PendingCopy> _pending;
  uint64_t _bytes = 0;
};

inline TermPostings::ptr TermReaderImpl::PostingsOf(
  IndexFeatures features, const PostingMeta& meta,
  TermPostings::ptr reuse) const {
  return _owner->Postings().Postings(_meta.index_features, features, meta,
                                     _has_score_bounds, std::move(reuse));
}

inline uint64_t TermReaderImpl::BlockOffset(size_t block) const noexcept {
  return _owner->BlockOffset(_first_block + block);
}

inline InlinePages::Span TermReaderImpl::InlineSpan(
  uint32_t block) const noexcept {
  return _owner->InlineSpan(_first_block + block);
}

inline uint64_t TermReaderImpl::InlineBegin() const noexcept {
  return _owner->InlineBegin();
}

inline uint64_t TermReaderImpl::InlineEnd() const noexcept {
  return _owner->InlineEnd();
}

inline const byte_type* TermReaderImpl::ResolveInline(uint64_t end,
                                                      uint8_t size) const {
  const byte_type* base = _owner->InlineRegion();
  uint64_t begin = 0;
  uint64_t limit = InlineEnd() - InlineBegin();
  if (Small()) {
    base = _small_inline.data();
    begin = _inline_base;
    limit = _inline_base + _small_inline.size();
  }
  if (end < begin + size || end > limit) [[unlikely]] {
    Corrupted("inline postings", _meta.id);
  }
  return base + (end - size - begin);
}

}  // namespace irs::term_dict
