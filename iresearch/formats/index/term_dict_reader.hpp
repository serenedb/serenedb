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
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_prefixes.hpp"
#include "iresearch/formats/index/term_dict_root.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::term_dict {

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

template<typename Field>
void ReadPostingDocs(const Field& field, const PostingMeta& meta,
                     TermReader::Acceptor acceptor) {
  if (meta.docs_count == 0) {
    return;
  }
  if (meta.docs_count == 1) {
    acceptor(doc_limits::min() + meta.doc_delta);
    return;
  }
  auto docs_it = field.PostingsOf(IndexFeatures::None, meta, {});
  doc_id_t docs[doc_limits::kBlockSize];
  while (const auto n = docs_it->NextDocs(docs, nullptr)) {
    for (uint32_t i = 0; i != n; ++i) {
      SDB_ASSERT(doc_limits::valid(docs[i]));
      if (!acceptor(docs[i])) {
        return;
      }
    }
  }
}

class TermReaderImpl final : public TermReader {
 public:
  TermReaderImpl(FieldReader& owner, const FieldHead& head, bytes_view prefix,
                 IndexInput& in, const Separators& separators);

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
  void VisitRange(bytes_view from, bytes_view to,
                  RangeVisitor visit) const final;

  std::unique_ptr<IndexInput> ReopenPayload() const final;
  PostingsHandles Handles() const noexcept final;

  const FieldMeta& meta() const noexcept final { return _meta; }
  size_t size() const noexcept final { return _terms; }
  uint64_t docs_count() const noexcept final { return _docs; }
  uint64_t total_term_freq() const noexcept final { return _total_term_freq; }
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

  const FieldReader& Owner() const noexcept { return *_owner; }
  TermPostings::ptr PostingsOf(IndexFeatures features, const PostingMeta& meta,
                               TermPostings::ptr reuse) const;
  uint32_t Blocks() const noexcept { return _blocks; }
  bool Small() const noexcept { return _blocks == 1; }
  const duckdb_fsst_decoder_t* Fsst() const noexcept;
  const Root& GetRoot() const noexcept { return _root; }
  uint32_t BlockOf(Probe key) const noexcept {
    return Small() ? 0 : static_cast<uint32_t>(_root.Find(key));
  }
  uint32_t BlockFrom(uint32_t block, Probe key) const noexcept {
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
  const byte_type* ResolveInline(uint64_t end, uint8_t size) const;

 private:
  bool Excludes(bytes_view term) const noexcept;
  uint64_t ReadRanges(FieldReader& owner, IndexInput& in,
                      const RangesHead& ranges);

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
  uint64_t _total_term_freq;
  bool _has_score_bounds;
  Root _root;
  std::vector<RangeLevel> _levels;
  std::unique_ptr<PrefixIndex> _prefixes;
  uint32_t _range_prefix = 0;
};

}  // namespace irs::term_dict
