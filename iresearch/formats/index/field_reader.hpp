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

#include <array>
#include <bit>
#include <cstdint>
#include <deque>
#include <iterator>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_root.hpp"
#include "iresearch/formats/posting/reader.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/containers/node_hash_map.hpp"
#include "iresearch/utils/resource_manager.hpp"

namespace irs::term_dict {

class PrefixIndex {
 public:
  explicit PrefixIndex(std::vector<uint32_t> prefixes);

  size_t MemoryBytes() const noexcept {
    return sizeof(*this) +
           (_prefixes.capacity() + _pair_starts.capacity()) * sizeof(uint32_t);
  }

  uint32_t NextLabel(bytes_view head, uint32_t from,
                     const uint32_t*& at) const noexcept {
    if (head.empty()) {
      from = NextBit(_firsts.data(), from);
      if (from != kNoLabel) {
        at = _prefixes.data() + _buckets[from];
      }
      return from;
    }
    const auto first = head[0];
    if (head.size() == 1) {
      from = NextBit(_pairs.data() + first * kRowWords, from);
      if (from != kNoLabel) {
        at = _prefixes.data() + _pair_starts[Rank(first, from)];
      }
      return from;
    }
    const auto second = head[1];
    if (!HasBit(_pairs.data() + first * kRowWords, second)) {
      return kNoLabel;
    }
    const auto pair = Rank(first, second);
    const auto* begin = _prefixes.data() + _pair_starts[pair];
    const auto* end = _prefixes.data() + _pair_starts[pair + 1];
    const uint32_t shift = 8 * (kTermPrefix - 1 - head.size());
    const uint32_t base = TermPrefix(head);
    const uint32_t key = base | from << shift;
    if (!at || at < begin || at > end || (at != begin && at[-1] >= key)) {
      at = begin;
    }
    at = Gallop(at, end, key);
    if (at == end || ((*at ^ base) >> shift >> 8) != 0) {
      return kNoLabel;
    }
    return *at >> shift & 0xFF;
  }

 private:
  static constexpr size_t kRowWords = kNoLabel / 64;

  static bool HasBit(const uint64_t* row, uint32_t bit) noexcept {
    return (row[bit / 64] >> (bit % 64) & 1) != 0;
  }

  static uint32_t NextBit(const uint64_t* row, uint32_t from) noexcept {
    for (uint32_t word = from / 64; word < kRowWords; ++word) {
      uint64_t bits = row[word];
      if (word == from / 64) {
        bits &= ~uint64_t{0} << (from % 64);
      }
      if (bits != 0) {
        return word * 64 + static_cast<uint32_t>(std::countr_zero(bits));
      }
    }
    return kNoLabel;
  }

  uint32_t Rank(uint32_t first, uint32_t second) const noexcept {
    const auto* row = _pairs.data() + first * kRowWords;
    uint32_t rank = _pair_rows[first];
    for (uint32_t word = 0; word != second / 64; ++word) {
      rank += static_cast<uint32_t>(std::popcount(row[word]));
    }
    const uint64_t below = (uint64_t{1} << (second % 64)) - 1;
    return rank +
           static_cast<uint32_t>(std::popcount(row[second / 64] & below));
  }

  static const uint32_t* Gallop(const uint32_t* at, const uint32_t* end,
                                uint32_t key) noexcept {
    if (at == end || *at >= key) {
      return at;
    }
    size_t step = 1;
    while (step < static_cast<size_t>(end - at) && at[step] < key) {
      at += step;
      step <<= 1;
    }
    const std::span rest{
      at + 1, step < static_cast<size_t>(end - at) ? at + step + 1 : end};
    return rest.data() + (absl::c_lower_bound(rest, key) - rest.begin());
  }

  std::vector<uint32_t> _prefixes;
  std::vector<uint32_t> _pair_starts;
  std::array<uint64_t, kRowWords * kNoLabel> _pairs{};
  std::array<uint64_t, kRowWords> _firsts{};
  std::array<uint32_t, kNoLabel + 1> _buckets{};
  std::array<uint32_t, kNoLabel + 1> _pair_rows{};
};

class InlinePages {
 public:
  struct Span {
    uint64_t from = 0;
    uint64_t to = 0;
  };

  InlinePages() = default;
  InlinePages(bytes_view starts, uint32_t blocks, uint64_t offset,
              uint64_t size);

  Span Of(uint32_t block) const noexcept;
  size_t MemoryBytes() const noexcept {
    return (_ones.capacity() + _empty.capacity() + _samples.capacity()) *
           sizeof(uint64_t);
  }

 private:
  uint64_t Select(uint64_t rank) const noexcept;
  uint64_t Next(uint64_t at) const noexcept;

  std::vector<uint64_t> _ones;
  std::vector<uint64_t> _empty;
  std::vector<uint64_t> _samples;
  uint64_t _begin = 0;
  uint64_t _end = 0;
  uint32_t _blocks = 0;
};

class FieldReader;

struct BlockBuffer {
  IndexInput::ptr in;
  bstring copy;
};

struct RangeLevel {
  const byte_type* starts;
  const byte_type* firsts;
  const byte_type* offsets;
  const byte_type* records;
  uint32_t count;
};

class TermReaderImpl final : public TermReader {
 public:
  TermReaderImpl(FieldReader& owner, const FieldHead& head, IndexInput& in,
                 const Separators& separators);

  SeekTermIterator::ptr iterator() const final;
  SeekTermIterator::ptr IteratorUntil(bytes_view upper) const final;
  SeekTermIterator::ptr iterator(const RegexpAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const LevenshteinAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const RegexpConjunction& a) const final;
  SeekTermIterator::ptr iterator(const FuzzyConjunction& a) const final;

  PostingMeta Lookup(bytes_view term) const final;
  void LookupBatch(std::span<const bytes_view> terms,
                   std::span<PostingMeta> out) const final;
  void ReadDocs(bytes_view term, Acceptor acceptor) const final;
  void VisitRange(bytes_view from, bytes_view to,
                  RangeVisitor visit) const final;

  std::unique_ptr<IndexInput> ReopenPayload() const final;
  PostingsHandles Handles() const noexcept final;

  const FieldMeta& meta() const noexcept final { return _meta; }
  size_t size() const noexcept final { return _terms; }
  uint64_t docs_count() const noexcept final { return _docs; }
  bytes_view min() const noexcept final {
    return bytes_view{_min_key}.substr(_prefix_size);
  }
  bytes_view max() const noexcept final {
    return bytes_view{_max_key}.substr(_prefix_size);
  }
  void SampleTerms(
    uint64_t bytes,
    absl::FunctionRef<void(std::span<const bytes_view>)> visit) const final;
  bool HasScoreBounds() const noexcept final { return _has_score_bounds; }
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;

  const FieldReader& Owner() const noexcept { return *_owner; }
  TermPostings::ptr PostingsOf(IndexFeatures features, const PostingMeta& meta,
                               TermPostings::ptr reuse) const;
  uint32_t Blocks() const noexcept { return _blocks; }
  bool Single() const noexcept { return _blocks == 0; }
  const PostingMeta& SingleMeta() const noexcept { return _single; }
  bool Small() const noexcept { return _blocks == 1; }
  const duckdb_fsst_decoder_t* Fsst() const noexcept;
  const Root& GetRoot() const noexcept { return _root; }
  uint32_t BlockOf(bytes_view key) const noexcept {
    return Small() ? 0 : static_cast<uint32_t>(_root.Find(key));
  }
  uint32_t BlockFrom(uint32_t block, bytes_view key) const noexcept {
    return block + 1 < _blocks && _root.NotAfter(block + 1, key)
             ? static_cast<uint32_t>(_root.FindFrom(block + 1, key))
             : block;
  }
  uint32_t BlockEnd(bytes_view upper) const;
  bytes_view KeyPrefix() const noexcept {
    return bytes_view{_min_key}.substr(0, _prefix_size);
  }
  bytes_view MinKey() const noexcept { return _min_key; }
  bytes_view MaxKey() const noexcept { return _max_key; }
  uint32_t FirstEntry() const noexcept { return _first_entry; }
  uint32_t FirstRecord() const noexcept { return _first_record; }
  uint32_t LastEntry() const noexcept { return _last_entry; }
  bytes_view BlockPrefix(size_t block) const noexcept {
    const bytes_view lower = block == 0 ? MinKey() : _root.Separator(block);
    const bytes_view upper =
      block + 1 == _blocks ? MaxKey() : _root.Separator(block + 1);
    return lower.substr(0, CommonPrefixLength(lower, upper));
  }
  const byte_type* Block(size_t block, BlockBuffer& buffer) const;
  uint64_t BlockOffset(size_t block) const noexcept;
  uint64_t BlocksEnd() const noexcept { return BlockOffset(_blocks); }
  InlinePages::Span InlineSpan(uint32_t block) const noexcept;
  void PrefetchLookup(uint32_t block) const;
  bool Gates(bytes_view head) const noexcept {
    return _prefixes && head.size() < kTermPrefix;
  }
  uint32_t NextLabel(bytes_view head, uint32_t from,
                     const uint32_t*& at) const noexcept {
    if (!Gates(head) || from >= kNoLabel) {
      return from;
    }
    return _prefixes->NextLabel(head, from, at);
  }
  std::span<const RangeLevel> RangeLevels() const noexcept { return _levels; }
  uint32_t RangePrefix() const noexcept { return _range_prefix; }
  uint64_t InlineBegin() const noexcept;
  uint64_t InlineEnd() const noexcept;
  void ReadInline(PostingMeta& meta, BlockBuffer& buffer) const;

 private:
  bool Excludes(bytes_view term) const noexcept;
  uint64_t ReadRanges(FieldReader& owner, IndexInput& in,
                      const FieldHead& head);

  const FieldReader* _owner;
  FieldMeta _meta;
  uint64_t _terms;
  uint64_t _docs;
  bstring _min_key;
  bstring _max_key;
  PostingMeta _single;
  bytes_view _small_inline;
  uint64_t _inline_base = 0;
  uint32_t _prefix_size = 0;
  const byte_type* _small = nullptr;
  uint32_t _blocks = 0;
  uint32_t _first_block = 0;
  uint32_t _first_entry = 0;
  uint32_t _first_record = 0;
  uint32_t _last_entry = 0;
  bool _has_score_bounds;
  FreqAttr _freq;
  Root _root;
  std::vector<RangeLevel> _levels;
  std::unique_ptr<PrefixIndex> _prefixes;
  uint32_t _range_prefix = 0;
};

class FieldReader final {
 public:
  explicit FieldReader(IResourceManager& rm) noexcept : _rm{&rm} {}
  ~FieldReader();

  FieldReader(const FieldReader&) = delete;
  FieldReader& operator=(const FieldReader&) = delete;

  uint64_t CountMappedMemory() const;
  void prepare(const ReaderState& state);

  const TermReader* field(FieldKey key) const;
  std::span<const FieldKey> field_keys() const noexcept { return _keys; }
  std::span<const field_id> field_ids() const noexcept { return _ids; }
  size_t size() const noexcept { return _fields.size(); }

  const PostingsReader& Postings() const noexcept { return _pr; }
  const IndexInput& Input() const noexcept { return *_in; }
  const byte_type* Base() const noexcept { return _base; }
  void Account(uint64_t bytes) {
    if (bytes != 0) {
      _rm->Increase(bytes);
      _bytes += bytes;
    }
  }
  const duckdb_fsst_decoder_t* Fsst() const noexcept { return _fsst.get(); }
  uint32_t BlockCount() const noexcept { return _block_count; }
  uint64_t PageOffset(size_t block) const noexcept {
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
    return page * kPage;
  }
  uint64_t InlineBegin() const noexcept { return _inline_offset; }
  uint64_t InlineEnd() const noexcept { return _inline_offset + _inline_size; }
  InlinePages::Span InlineSpan(uint32_t block) const noexcept {
    return _inline_pages.Of(block);
  }
  uint64_t RangesBegin() const noexcept { return InlineEnd(); }
  uint64_t RangesSize() const noexcept { return _ranges_size; }
  bytes_view Load(IndexInput& in, uint64_t offset, uint64_t size);
  bytes_view Copy(uint64_t offset, uint64_t size);
  const byte_type* CopyBlock(uint32_t block, uint64_t length);

 private:
  struct PendingCopy {
    uint64_t offset;
    uint64_t size;
    byte_type* data;
  };

  struct Dicts {
    std::array<const TermReaderImpl*, kTermDicts> dicts{};
  };

  Separators Bind(TermsMeta& terms);
  void FillCopies();

  std::deque<TermReaderImpl> _fields;
  containers::FlatHashMap<field_id, Dicts> _by_id;
  std::vector<FieldKey> _keys;
  std::vector<field_id> _ids;
  PostingsReader _pr;
  IndexInput::ptr _in;
  const byte_type* _base = nullptr;
  IResourceManager* _rm;
  std::vector<SpanningBlock> _spans;
  InlinePages _inline_pages;
  containers::NodeHashMap<uint64_t, bstring> _copies;
  std::unique_ptr<duckdb_fsst_decoder_t> _fsst;
  uint64_t _first_page = 0;
  uint64_t _inline_offset = 0;
  uint64_t _inline_size = 0;
  uint64_t _ranges_size = 0;
  uint32_t _block_count = 0;
  std::vector<PendingCopy> _pending;
  uint64_t _bytes = 0;
};

inline const duckdb_fsst_decoder_t* TermReaderImpl::Fsst() const noexcept {
  return _owner->Fsst();
}

inline TermPostings::ptr TermReaderImpl::PostingsOf(
  IndexFeatures features, const PostingMeta& meta,
  TermPostings::ptr reuse) const {
  return _owner->Postings().Postings(_meta.index_features, features, meta,
                                     _has_score_bounds, std::move(reuse));
}

inline uint64_t TermReaderImpl::BlockOffset(size_t block) const noexcept {
  return _owner->PageOffset(_first_block + block);
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

}  // namespace irs::term_dict
