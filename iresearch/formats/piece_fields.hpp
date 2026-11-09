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

#include <cstdint>
#include <deque>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/field_sink.hpp"
#include "iresearch/formats/posting/reader.hpp"
#include "iresearch/formats/posting/writer.hpp"
#include "iresearch/formats/reader_state.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class PieceFields;

class PieceTermReader final : public TermReader {
 public:
  PieceTermReader(const PieceFields& owner, FieldMeta meta,
                  uint32_t width) noexcept;

  void Add(bytes_view term, const PostingMeta& meta);
  void Seal(uint64_t docs, uint64_t total_freq);

  SeekTermIterator::ptr iterator() const final;
  SeekTermIterator::ptr iterator(const RegexpAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const LevenshteinAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const RegexpConjunction& a) const final;
  SeekTermIterator::ptr iterator(const FuzzyConjunction& a) const final;

  PostingMeta Lookup(bytes_view term) const final;
  void LookupBatch(size_t count, TermAt term, FoundTerm found) const final;
  void ReadDocs(bytes_view term, Acceptor acceptor) const final;
  void VisitRange(bytes_view from, bytes_view to,
                  RangeVisitor visit) const final;

  std::unique_ptr<IndexInput> ReopenPayload() const final;
  PostingsHandles Handles() const noexcept final;

  const FieldMeta& meta() const noexcept final { return _meta; }
  size_t size() const noexcept final { return _entries.size(); }
  uint64_t docs_count() const noexcept final { return _docs; }
  bytes_view min() const noexcept final { return TermOf(0); }
  bytes_view max() const noexcept final { return TermOf(_entries.size() - 1); }
  void SampleTerms(
    uint64_t bytes,
    absl::FunctionRef<void(std::span<const bytes_view>)> visit) const final;
  bool HasScoreBounds() const noexcept final { return false; }
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final;

  bytes_view TermOf(size_t i) const noexcept {
    const auto& entry = _entries[i];
    return bytes_view{_terms}.substr(entry.begin, entry.size);
  }
  const PostingMeta& MetaAt(size_t i) const noexcept {
    return _entries[i].meta;
  }
  size_t LowerBound(bytes_view term) const noexcept;
  size_t Find(bytes_view term) const noexcept;

  TermPostings::ptr PostingsOf(IndexFeatures features, const PostingMeta& meta,
                               TermPostings::ptr reuse) const;

  size_t Bytes() const noexcept {
    return _terms.capacity() + _inline.capacity() +
           _entries.capacity() * sizeof(Entry);
  }

 private:
  struct Entry {
    uint32_t begin = 0;
    uint32_t size = 0;
    uint32_t inline_at = 0;
    PostingMeta meta;
  };

  const PieceFields* _owner;
  FieldMeta _meta;
  bstring _terms;
  bstring _inline;
  std::vector<Entry> _entries;
  uint64_t _docs = 0;
  FreqAttr _freq;
};

class PieceFields final {
 public:
  PieceFields() = default;
  PieceFields(const PieceFields&) = delete;
  PieceFields& operator=(const PieceFields&) = delete;

  std::span<const TermReader* const> fields(field_id id) const;
  std::span<const field_id> field_ids() const noexcept { return _ids; }
  const PostingsReader& Postings() const noexcept { return _pr; }

  PieceTermReader& Add(FieldMeta meta, uint32_t width);
  void Seal(PieceTermReader& reader, uint64_t docs, uint64_t total_freq);
  void Open(const ReaderState& state, IndexFeatures features);
  size_t Bytes() const noexcept;

 private:
  struct Members {
    uint32_t first = 0;
    uint32_t count = 0;
  };

  PostingsReader _pr;
  std::deque<PieceTermReader> _readers;
  std::vector<const TermReader*> _dicts;
  containers::FlatHashMap<field_id, Members> _by_id;
  std::vector<field_id> _ids;
};

class PieceFieldWriter final : public FieldSink {
 public:
  explicit PieceFieldWriter(PieceFields& fields) noexcept : _fields{&fields} {}

  void prepare(const FlushState& state, uint64_t keyed_dicts) final;
  void write(DictRef dict, const BasicTermReader& reader) final;
  void WriteSlot(field_id id, DictRef dict,
                 std::span<const doc_id_t> docs) final;
  void end() final;

  IndexFeatures Features() const noexcept { return _features; }

 private:
  PostingsWriter _pw;
  PieceFields* _fields;
  IndexFeatures _features = IndexFeatures::None;
};

}  // namespace irs
