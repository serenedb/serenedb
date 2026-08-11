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

#include <iresearch/index/index_reader.hpp>
#include <limits>

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
      if ((_segment_mask && _segment_mask->contains(found_doc)) ||
          (_pending_mask && _pending_mask->contains(found_doc))) {
        return true;  // skip deleted
      }
      // found alive document with this PK
      doc = found_doc;
      return false;
    };

    _pk_field->ReadDocs(pk, acceptor);

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


SearchRemovePrefixFilter::SearchRemovePrefixFilter(irs::field_id pk_field_id)
  : _pk_field_id{pk_field_id} {}

SearchRemovePrefixFilter::~SearchRemovePrefixFilter() = default;

SearchRemovePrefixFilter::Entry& SearchRemovePrefixFilter::PushEntry(
  std::string_view prefix) {
  irs::bstring encoded{reinterpret_cast<const irs::byte_type*>(prefix.data()),
                       prefix.size()};
  SDB_ASSERT(_entries.empty() || _entries.back().prefix < encoded);
  return _entries.emplace_back(Entry{.prefix = std::move(encoded)});
}

void SearchRemovePrefixFilter::NextEntry() const noexcept {
  ++_pos;
  _terms.reset();
  _resume_row = std::numeric_limits<int64_t>::min();
}

irs::QueryBuilder::ptr SearchRemovePrefixFilter::PrepareSegment(
  const irs::SubReader& segment, const irs::PrepareContext& ctx) const {
  // Never empty: the remove builders return nullptr instead of an empty
  // filter, and entries are not consumed across segments.
  SDB_ASSERT(!_entries.empty());
  return irs::memory::make_tracked<SearchRemoveQuery<SearchRemovePrefixFilter>>(
    ctx.memory, segment, *this);
}

irs::DocIterator::ptr SearchRemovePrefixFilter::MakeIterator(
  const irs::SubReader& segment, const irs::ExecutionContext& ctx) const {
  _segment_mask = segment.docs_mask();
  _pending_mask = ctx.pending_docs_mask;
  _pk_field = segment.field(_pk_field_id);
  SDB_ASSERT(_pk_field);
  _terms.reset();
  _postings.reset();
  _pos = 0;
  _resume_row = std::numeric_limits<int64_t>::min();
  _doc = irs::doc_limits::invalid();
  return irs::memory::to_managed<irs::DocIterator>(
    const_cast<SearchRemovePrefixFilter&>(*this));
}

irs::doc_id_t SearchRemovePrefixFilter::advance() {
  while (true) {
    if (_postings) {
      while (true) {
        const auto doc = _postings->advance();
        if (irs::doc_limits::eof(doc)) {
          break;
        }
        if (Masked(_segment_mask, _pending_mask, doc)) {
          continue;
        }
        return _doc = doc;
      }
      _postings.reset();
    }
    if (_pos == _entries.size()) {
      return _doc = irs::doc_limits::eof();
    }
    auto& entry = _entries[_pos];
    const irs::bytes_view prefix{entry.prefix};
    if (entry.dead) {
      // Leapfrog: the cursor names the next dead row, seek_ge jumps to its
      // term, a landed alive term gallops the cursor forward. Seeks only --
      // never mixed with next() on one iterator.
      while (true) {
        const auto dead_row = (*entry.dead)(_resume_row);
        if (!dead_row) {
          break;
        }
        _resume_row = *dead_row + 1;
        _key_scratch.assign(reinterpret_cast<const char*>(prefix.data()),
                            prefix.size());
        primary_key::AppendSigned(_key_scratch, *dead_row);
        if (!_terms) {
          _terms = _pk_field->iterator();
        }
        const auto res = _terms->seek_ge(irs::bytes_view{
          reinterpret_cast<const irs::byte_type*>(_key_scratch.data()),
          _key_scratch.size()});
        if (res == irs::SeekResult::End) {
          break;
        }
        if (res == irs::SeekResult::NotFound) {
          const auto term = _terms->value();
          if (!term.starts_with(prefix)) {
            break;  // no terms of this file at or above the dead row
          }
          // An alive row's term: gallop the cursor to it and re-check.
          SDB_ASSERT(term.size() == prefix.size() + sizeof(int64_t));
          _resume_row = primary_key::ReadSigned<int64_t>(std::string_view{
            reinterpret_cast<const char*>(term.data()) + prefix.size(),
            sizeof(int64_t)});
          continue;
        }
        _postings = _terms->postings(irs::IndexFeatures::None);
        break;
      }
      if (!_postings) {
        NextEntry();
      }
      continue;
    }
    // Whole file: seek the prefix once, then walk -- every term under it
    // dies.
    if (!_terms) {
      _terms = _pk_field->iterator();
      if (_terms->seek_ge(prefix) == irs::SeekResult::End) {
        NextEntry();
        continue;
      }
    } else if (!_terms->next()) {
      NextEntry();
      continue;
    }
    if (!_terms->value().starts_with(prefix)) {
      NextEntry();
      continue;
    }
    _postings = _terms->postings(irs::IndexFeatures::None);
  }
}

}  // namespace sdb::connector
