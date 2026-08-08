////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "search_remove_filter.hpp"

#include <absl/algorithm/container.h>

#include <iresearch/index/index_reader.hpp>

#include "basics/memory.hpp"
#include "basics/primary_key.hpp"

namespace sdb::connector {
namespace {

bool Masked(const irs::DocumentMask* segment_mask,
            const irs::DocumentMask* pending_mask, irs::doc_id_t doc) noexcept {
  return (segment_mask && segment_mask->contains(doc)) ||
         (pending_mask && pending_mask->contains(doc));
}

template<typename Filter>
class SearchRemoveQuery : public irs::QueryBuilder {
 public:
  SearchRemoveQuery(const irs::SubReader& segment, const Filter& filter)
    : irs::QueryBuilder{segment}, _filter{filter} {}

  irs::DocIterator::ptr Execute(const irs::ExecutionContext& ctx,
                                const irs::StatsBuffer&) const final {
    return _filter.MakeIterator(_segment, ctx);
  }

  void Visit(irs::PreparedStateVisitor&, irs::score_t) const final {}

  irs::score_t Boost() const noexcept final { return irs::kNoBoost; }

 private:
  const Filter& _filter;
};

}  // namespace

irs::QueryBuilder::ptr SearchRemoveFilter::PrepareSegment(
  const irs::SubReader& segment, const irs::PrepareContext& ctx) const {
  if (_pks.empty()) {
    return irs::QueryBuilder::Empty();
  }
  return irs::memory::make_tracked<SearchRemoveQuery<SearchRemoveFilter>>(
    ctx.memory, segment, *this);
}

irs::DocIterator::ptr SearchRemoveFilter::MakeIterator(
  const irs::SubReader& segment, const irs::ExecutionContext& ctx) const {
  _segment_mask = segment.docs_mask();
  _pending_mask = ctx.pending_docs_mask;
  _pk_field = segment.field(_pk_field_id);
  SDB_ASSERT(_pk_field);
  _pos = 0;
  _doc = irs::doc_limits::invalid();
  return irs::memory::to_managed<irs::DocIterator>(
    const_cast<SearchRemoveFilter&>(*this));
}

irs::doc_id_t SearchRemoveFilter::advance() {
  while (true) {
    if (_pos == _pks.size()) [[unlikely]] {
      _doc = irs::doc_limits::eof();
      if (_pks.empty()) [[unlikely]] {
        _pks = {};
      }
      return irs::doc_limits::eof();
    }
    auto& pk = _pks[_pos];

    // Remove all occurrences of the PK in segment if any.
    // There is only one alive PK in the entire index. In general
    // that means we can remove pk from list once we found it.
    // But there are some edge cases:
    // 1. Same value PKs might exist in deleted documents and this number
    // of documents is arbitrary. So we need to check all of them.
    // In general we do not expect too many delete/insert of same PK
    // between compactions. So postings list should be short.

    // 2. Also we might have Delete/Insert sequence in a single batch,
    // So we must check pending docs mask as well in order to not fire on
    // already deleted documents by queries in the same batch. Also Removals
    // might be skipped during flushed segment processing due to ticks (e.g.
    // remove arrived before insert) but that is not a problem. As if we reached
    // "tick" limit we anyway should not find anymore valid targets.

    // 3. For segments with sorted field it should also work:
    // E.G. if we have INSERT PK1 FIELD_SORTED_2 | DELETE PK1 | INSERT PK1
    // FIELD_SORTED_1 Due to documents are sorted for storing after applying
    // queries it will still see documents in insertion order.

    auto doc = irs::doc_limits::eof();
    auto acceptor = [&](irs::doc_id_t found_doc) {
      if (Masked(_segment_mask, _pending_mask, found_doc)) {
        return true;  // skip deleted
      }
      // found alive document with this PK
      doc = found_doc;
      return false;
    };

    _pk_field->read_documents(pk, acceptor);

    if (irs::doc_limits::eof(doc)) {
      ++_pos;
      continue;
    }

    // if PK found alive it should be the only one in the entire index.
    pk = _pks.back();
    _pks.pop_back();
    _doc = doc;
    return doc;
  }
}

irs::QueryBuilder::ptr SearchRemovePrefixFilter::PrepareSegment(
  const irs::SubReader& segment, const irs::PrepareContext& ctx) const {
  if (_prefixes.empty()) {
    return irs::QueryBuilder::Empty();
  }
  return irs::memory::make_tracked<SearchRemoveQuery<SearchRemovePrefixFilter>>(
    ctx.memory, segment, *this);
}

irs::DocIterator::ptr SearchRemovePrefixFilter::MakeIterator(
  const irs::SubReader& segment, const irs::ExecutionContext& ctx) const {
  _segment_mask = segment.docs_mask();
  _pending_mask = ctx.pending_docs_mask;
  const auto* pk_field = segment.field(_pk_field_id);
  SDB_ASSERT(pk_field);
  _docs.reset();
  _pos = 0;
  _seek_pending = true;
  _doc = irs::doc_limits::invalid();
  _terms = pk_field->iterator(irs::SeekMode::NORMAL);
  return irs::memory::to_managed<irs::DocIterator>(
    const_cast<SearchRemovePrefixFilter&>(*this));
}

irs::doc_id_t SearchRemovePrefixFilter::advance() {
  while (true) {
    if (_docs) {
      while (!irs::doc_limits::eof(_docs->advance())) {
        const auto doc = _docs->value();
        if (Masked(_segment_mask, _pending_mask, doc)) {
          continue;  // skip deleted
        }
        return _doc = doc;
      }
      _docs.reset();
      if (!_terms->next()) {
        _pos = _prefixes.size();
      }
    }
    if (_pos == _prefixes.size()) {
      return _doc = irs::doc_limits::eof();
    }
    const irs::bytes_view prefix{_prefixes[_pos]};
    if (_seek_pending) {
      _seek_pending = false;
      if (irs::SeekResult::End == _terms->seek_ge(prefix)) {
        _pos = _prefixes.size();
        continue;
      }
    }
    const auto term = _terms->value();
    if (term.starts_with(prefix)) {
      _terms->read();
      _docs = _terms->postings(irs::IndexFeatures::None);
      SDB_ASSERT(_docs);
      continue;
    }
    // The dictionary is positioned past this prefix's range (seek_ge/next
    // only move forward): the range is exhausted.
    ++_pos;
    _seek_pending = true;
  }
}

irs::QueryBuilder::ptr SearchRemoveRangeMaskFilter::PrepareSegment(
  const irs::SubReader& segment, const irs::PrepareContext& ctx) const {
  if (_ranges.empty()) {
    return irs::QueryBuilder::Empty();
  }
  return irs::memory::make_tracked<
    SearchRemoveQuery<SearchRemoveRangeMaskFilter>>(ctx.memory, segment, *this);
}

irs::DocIterator::ptr SearchRemoveRangeMaskFilter::MakeIterator(
  const irs::SubReader& segment, const irs::ExecutionContext& ctx) const {
  _segment_mask = segment.docs_mask();
  _pending_mask = ctx.pending_docs_mask;
  const auto* pk_field = segment.field(_pk_field_id);
  SDB_ASSERT(pk_field);
  _terms = pk_field->iterator(irs::SeekMode::NORMAL);
  _docs.reset();
  _pos = 0;
  _lower = std::numeric_limits<int64_t>::min();
  _doc = irs::doc_limits::invalid();
  return irs::memory::to_managed<irs::DocIterator>(
    const_cast<SearchRemoveRangeMaskFilter&>(*this));
}

irs::doc_id_t SearchRemoveRangeMaskFilter::advance() {
  constexpr auto kLowerReset = std::numeric_limits<int64_t>::min();
  while (true) {
    if (_docs) {
      while (!irs::doc_limits::eof(_docs->advance())) {
        const auto doc = _docs->value();
        if (Masked(_segment_mask, _pending_mask, doc)) {
          continue;  // skip deleted
        }
        return _doc = doc;
      }
      _docs.reset();
    }
    if (_pos == _ranges.size()) {
      return _doc = irs::doc_limits::eof();
    }
    auto& range = _ranges[_pos];
    const auto row = range.dead(_lower);
    if (!row) {
      // This range's dead set is exhausted; the next prefix sorts higher,
      // so the shared forward iterator keeps working.
      ++_pos;
      _lower = kLowerReset;
      continue;
    }
    _target.assign(reinterpret_cast<const char*>(range.prefix.data()),
                   range.prefix.size());
    primary_key::AppendSigned(_target, *row);
    const auto res = _terms->seek_ge(irs::bytes_view{
      reinterpret_cast<const irs::byte_type*>(_target.data()), _target.size()});
    if (res == irs::SeekResult::End) {
      _pos = _ranges.size();
      continue;
    }
    if (res == irs::SeekResult::Found) {
      _lower = *row + 1;
      _terms->read();
      _docs = _terms->postings(irs::IndexFeatures::None);
      SDB_ASSERT(_docs);
      continue;
    }
    // Landed on the next greater term. Outside this prefix = the file has
    // no terms at or above the dead row: the range is exhausted (and so is
    // any dead-set tail past the file's last row).
    const auto term = _terms->value();
    const irs::bytes_view prefix{range.prefix};
    if (!term.starts_with(prefix)) {
      ++_pos;
      _lower = kLowerReset;
      continue;
    }
    // Same file, some rows absent from the dictionary (never indexed or
    // merged away): fast-forward the dead cursor to the landed row -- the
    // other half of the leapfrog.
    SDB_ASSERT(term.size() == prefix.size() + sizeof(int64_t));
    _lower = primary_key::ReadSigned<int64_t>(
      {reinterpret_cast<const char*>(term.data() + prefix.size()),
       sizeof(int64_t)});
  }
}

}  // namespace sdb::connector
