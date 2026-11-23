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
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/term_dict_format.hpp"
#include "iresearch/formats/index/term_dict_index.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_store.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::term_dict {

class FieldReader;

struct PinnedRange {
  duckdb::FilePin pinned;
  uint64_t start = std::numeric_limits<uint64_t>::max();
};

class TermReaderImpl final : public TermReader {
 public:
  TermReaderImpl(FieldReader& owner, const FieldHead& head, bytes_view prefix,
                 const Separators& separators,
                 std::vector<const TermReaderImpl*> levels = {});

  SeekTermIterator::ptr iterator() const final;
  SeekTermIterator::ptr IteratorUntil(bytes_view upper) const final;
  SeekTermIterator::ptr iterator(const RegexpAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const LevenshteinAcceptor& a) const final;
  SeekTermIterator::ptr iterator(
    const LevenshteinPrefixAcceptor& a) const final;
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

  bool Prefetch(bytes_view term) const final;
  PostingsHandles Handles() const noexcept final;

  const FieldMeta& meta() const noexcept final { return _meta; }
  size_t size() const noexcept final { return _terms; }
  uint64_t docs_count() const noexcept final { return _docs; }
  bytes_view min() const noexcept final { return _index.MinTerm(); }
  bytes_view max() const noexcept final { return _index.MaxTerm(); }
  void SampleTerms(
    uint64_t bytes,
    absl::FunctionRef<void(std::span<const bytes_view>)> visit) const final;
  bool HasScoreBounds() const noexcept final { return _has_score_bounds; }
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;

  const BlockStore& Store() const noexcept;
  TermPostings::ptr PostingsOf(IndexFeatures features, const PostingMeta& meta,
                               TermPostings::ptr reuse) const;
  const DictIndex& Index() const noexcept { return _index; }
  uint32_t Blocks() const noexcept { return _index.Blocks(); }
  bool Small() const noexcept { return _index.Small(); }
  bytes_view KeyPrefix() const noexcept { return _index.KeyPrefix(); }
  const duckdb_fsst_decoder_t* Fsst() const noexcept { return _fsst.get(); }
  uint32_t FirstEntry() const noexcept { return _first_entry; }
  uint32_t FirstRecord() const noexcept { return _first_record; }
  uint32_t LastEntry() const noexcept { return _last_entry; }
  bytes_view Block(size_t block, PinnedRange& pinned) const;
  void HintBlock(uint32_t block) const noexcept;
  uint64_t BlockOffset(size_t block) const noexcept;
  std::span<const TermReaderImpl* const> RangeLevels() const noexcept {
    return _range_levels;
  }

 private:
  bool Excludes(bytes_view term) const noexcept;
  const byte_type* PinBlock(size_t block, PinnedRange& pinned) const;

  const FieldReader* _owner;
  FieldMeta _meta;
  uint64_t _terms;
  uint64_t _docs;
  DictIndex _index;
  const byte_type* _small = nullptr;
  uint32_t _first_block = 0;
  uint32_t _first_entry = 0;
  uint32_t _first_record = 0;
  uint32_t _last_entry = 0;
  bool _has_score_bounds;
  FreqAttr _freq;
  std::vector<const TermReaderImpl*> _range_levels;
  std::unique_ptr<duckdb_fsst_decoder_t> _fsst;
};

}  // namespace irs::term_dict
