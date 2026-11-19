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

#include <fsst.h>

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_inline.hpp"
#include "iresearch/formats/index/term_dict_key.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_prefixes.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/byte_set.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::term_dict {

class FieldReader;

struct BlockBuffer {
  IndexInput::ptr in;
  bstring copy;
};

class TermReaderImpl final : public TermReader {
 public:
  TermReaderImpl(FieldReader& owner, const FieldHead& head, bytes_view prefix,
                 IndexInput& in,
                 std::vector<const TermReaderImpl*> levels = {});

  SeekTermIterator::ptr iterator() const final;
  SeekTermIterator::ptr IteratorUntil(bytes_view upper) const final;
  SeekTermIterator::ptr iterator(const RegexpAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const LevenshteinAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const RegexpConjunction& a) const final;
  SeekTermIterator::ptr iterator(const FuzzyConjunction& a) const final;

  PostingMeta Lookup(bytes_view term) const final;
  void LookupBatch(size_t count, TermAt term, FoundTerm found) const final;
  void ReadDocs(bytes_view term, Acceptor acceptor) const final {
    ReadPostingDocs(*this, Lookup(term), acceptor);
  }
  void ReadDocs(const PostingMeta& meta, Acceptor acceptor) const final {
    ReadPostingDocs(*this, meta, acceptor);
  }
  bool CoverRange(bytes_view from, bytes_view to,
                  TermVisitor visit) const final;

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
  bool Small() const noexcept { return _blocks == 1; }
  const duckdb_fsst_decoder_t* Fsst() const noexcept { return _fsst.get(); }
  Located Locate(Probe key) const noexcept {
    SDB_ASSERT(!Small() && key.size >= _prefix_size);
    return _trie.Locate({key.data + _prefix_size, key.size - _prefix_size});
  }
  uint32_t BlockOf(Probe key) const noexcept {
    return Small() ? 0 : Locate(key).block;
  }
  bool Gated() const noexcept {
    return key_width == 0 && _meta.dict.member == DictMember::Strings;
  }
  TrieCursor TrieRoot() const noexcept {
    return Gated() ? _trie.Root() : kTrieUngated;
  }
  uint32_t BlockEnd(bytes_view upper) const;
  bool Masked() const noexcept { return !_masks.empty(); }
  bool Holds(uint32_t block, const ByteSet& need) const noexcept {
    const auto& mask = _masks[block];
    return (mask[0] & need.words[0]) == need.words[0] &&
           (mask[1] & need.words[1]) == need.words[1];
  }
  uint32_t Holding(uint32_t block, uint32_t end,
                   const ByteSet& need) const noexcept {
    SDB_ASSERT(end <= _masks.size());
    for (; block < end && !Holds(block, need); ++block) {
    }
    return block;
  }
  bytes_view KeyPrefix() const noexcept {
    return bytes_view{_min_key}.substr(0, _prefix_size);
  }
  bytes_view MinKey() const noexcept { return _min_key; }
  bytes_view MaxKey() const noexcept { return _max_key; }
  uint32_t FirstEntry() const noexcept { return _first_entry; }
  uint32_t FirstRecord() const noexcept { return _first_record; }
  uint32_t LastEntry() const noexcept { return _last_entry; }
  const byte_type* Block(size_t block, BlockBuffer& buffer) const;
  uint64_t BlockOffset(size_t block) const noexcept;
  uint64_t BlocksEnd() const noexcept { return BlockOffset(_blocks); }
  InlinePages::Span InlineSpan(uint32_t block) const noexcept;
  void PrefetchLookup(uint32_t block) const;
  const PrefixTrie& Trie() const noexcept { return _trie; }
  std::span<const TermReaderImpl* const> RangeLevels() const noexcept {
    return _range_levels;
  }
  uint64_t InlineBegin() const noexcept;
  uint64_t InlineEnd() const noexcept;
  const byte_type* ResolveInline(uint64_t end, uint8_t size) const;

 private:
  bool Excludes(bytes_view term) const noexcept;

  const FieldReader* _owner;
  FieldMeta _meta;
  uint64_t _terms;
  uint64_t _docs;
  bstring _min_key;
  bstring _max_key;
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
  std::vector<const TermReaderImpl*> _range_levels;
  PrefixTrie _trie;
  std::vector<BlockMask> _masks;
  std::unique_ptr<duckdb_fsst_decoder_t> _fsst;
};

}  // namespace irs::term_dict
