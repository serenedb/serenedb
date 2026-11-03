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

#include <memory>
#include <span>

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/index/term_dict_meta.hpp"
#include "iresearch/formats/index/term_dict_reader.hpp"
#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/field_meta.hpp"

namespace irs::term_dict {

class SingleTermReader final : public TermReader {
 public:
  SingleTermReader(FieldReader& owner, const FieldHead& head);

  SeekTermIterator::ptr iterator() const final;
  SeekTermIterator::ptr ReadOnceIterator() const final;
  SeekTermIterator::ptr iterator(const RegexpAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const LevenshteinAcceptor& a) const final;
  SeekTermIterator::ptr iterator(const RegexpConjunction& a) const final;
  SeekTermIterator::ptr iterator(const FuzzyConjunction& a) const final;

  PostingMeta Lookup(bytes_view term) const final {
    return term == min() ? _single : PostingMeta{};
  }
  void LookupBatch(size_t count, TermAt term, FoundTerm found) const final;
  void ReadDocs(bytes_view term, Acceptor acceptor) const final {
    ReadPostingDocs(*this, Lookup(term), acceptor);
  }
  void VisitRange(bytes_view from, bytes_view to,
                  RangeVisitor visit) const final;

  std::unique_ptr<IndexInput> ReopenPayload() const final;
  PostingsHandles Handles() const noexcept final;

  const FieldMeta& meta() const noexcept final { return _meta; }
  size_t size() const noexcept final { return 1; }
  uint64_t docs_count() const noexcept final { return _single.docs_count; }
  uint64_t total_term_freq() const noexcept final { return _total_term_freq; }
  bytes_view min() const noexcept final { return _term; }
  bytes_view max() const noexcept final { return _term; }
  void SampleTerms(
    uint64_t,
    absl::FunctionRef<void(std::span<const bytes_view>)> visit) const final {
    const auto term = min();
    visit({&term, 1});
  }
  bool HasScoreBounds() const noexcept final { return _has_score_bounds; }
  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

  TermPostings::ptr PostingsOf(IndexFeatures features, const PostingMeta& meta,
                               TermPostings::ptr reuse) const;
  const PostingMeta& SingleMeta() const noexcept { return _single; }

 private:
  const FieldReader* _owner;
  FieldMeta _meta;
  PostingMeta _single;
  bstring _term;
  uint64_t _total_term_freq;
  bool _has_score_bounds;
};

}  // namespace irs::term_dict
