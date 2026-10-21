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

#include <algorithm>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/memory.hpp>
#include <limits>
#include <string>

#include "connector/column_id.h"
#include "connector/primary_key.h"
#include "connector/term_dict.h"
#include "server/utils/primary_key.h"

namespace sdb::connector {
namespace {

// A removal is asked for a document stream and for nothing else -- the index
// writer drives `PlanLeadDocs` and applies its own accumulating mask to what
// comes back. So this is the one plan it has; the other nine say that this
// query kind has none, which is what a caller that wanted a count or a score
// would have to be told.
template<typename Filter>
class SearchRemoveQuery : public irs::QueryBuilder {
 public:
  SearchRemoveQuery(const irs::SubReader& segment, const Filter& filter,
                    const irs::DocumentMaskBuilder* pending)
    : irs::QueryBuilder{segment}, _filter{filter}, _pending{pending} {}

  irs::lead::Node::ptr PlanLead(const irs::detail::ScoredCtx&) const final {
    return _filter.MakeLead(_segment, _pending);
  }

  irs::count::Root::ptr PlanCount(const irs::count::Context&) const final {
    return {};
  }
  irs::docs::Root::ptr PlanDocs(const irs::docs::Context&) const final {
    return {};
  }
  irs::hits::Root::ptr PlanScored(const irs::hits::Context&) const final {
    return {};
  }
  irs::top::Root::ptr PlanTop(const irs::top::Context&) const final {
    return {};
  }
  irs::probe::Node::ptr PlanProbe(const irs::detail::ScoredCtx&,
                                  uint64_t) const final {
    return {};
  }
  irs::fill::Node::ptr PlanFill(const irs::detail::ScoredCtx&,
                                irs::ScoreMergeType) const final {
    return {};
  }

  void Visit(irs::PreparedStateVisitor&, irs::score_t) const final {}

  irs::score_t Boost() const noexcept final { return irs::kNoBoost; }

 private:
  const Filter& _filter;
  const irs::DocumentMaskBuilder* _pending;
};

}  // namespace

irs::QueryBuilder::ptr SearchRemoveFilter::PrepareSegment(
  const irs::SubReader& segment, const irs::PrepareContext& ctx) const {
  if (_pks.empty()) {
    return irs::QueryBuilder::Empty();
  }
  return irs::memory::make_tracked<SearchRemoveQuery<SearchRemoveFilter>>(
    ctx.memory, segment, *this, ctx.pending_docs_mask);
}

irs::lead::Node::ptr SearchRemoveFilter::MakeLead(
  const irs::SubReader& segment,
  const irs::DocumentMaskBuilder* pending) const {
  _segment_mask = segment.MaskedDocs();
  _pending = pending;
  _pk_field = segment.field(_pk_field_id);
  SDB_ASSERT(_pk_field);
  _pos = 0;
  auto& self = const_cast<SearchRemoveFilter&>(*this);
  self._doc = irs::doc_limits::invalid();
  return irs::memory::to_managed<irs::lead::Node>(self);
}

irs::doc_id_t SearchRemoveFilter::Next() {
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
      if (_segment_mask.Contains(found_doc) ||
          (_pending != nullptr && _pending->Contains(found_doc))) {
        return true;  // skip deleted, including by this batch's earlier queries
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
    ctx.memory, segment, *this, ctx.pending_docs_mask);
}

irs::lead::Node::ptr SearchRemovePrefixFilter::MakeLead(
  const irs::SubReader& segment,
  const irs::DocumentMaskBuilder* pending) const {
  _segment_mask = segment.MaskedDocs();
  _pending = pending;
  _pk_field = segment.field(_pk_field_id);
  SDB_ASSERT(_pk_field);
  _terms.reset();
  _postings.reset();
  _docs_at = 0;
  _docs_size = 0;
  _pos = 0;
  _resume_row = std::numeric_limits<int64_t>::min();
  auto& self = const_cast<SearchRemovePrefixFilter&>(*this);
  self._doc = irs::doc_limits::invalid();
  return irs::memory::to_managed<irs::lead::Node>(self);
}

irs::doc_id_t SearchRemovePrefixFilter::Next() {
  while (true) {
    if (_postings) {
      while (true) {
        if (_docs_at == _docs_size) {
          _docs_size = _postings->NextDocs(_docs.data(), nullptr);
          _docs_at = 0;
          if (_docs_size == 0) {
            break;
          }
        }
        const auto doc = _docs[_docs_at++];
        if (_segment_mask.Contains(doc) ||
            (_pending != nullptr && _pending->Contains(doc))) {
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
      // term, a landed alive term gallops the cursor forward. Only a seek that
      // lands on the term leaves the iterator usable: the other outcomes
      // advance it internally and leave its floor-block state stale, so the
      // iterator is dropped and the next dead row starts from a fresh one.
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
          _terms.reset();
          break;
        }
        if (res == irs::SeekResult::NotFound) {
          const auto term = _terms->value();
          if (!term.starts_with(prefix)) {
            _terms.reset();
            break;  // no terms of this file at or above the dead row
          }
          // An alive row's term: gallop the cursor to it and re-check.
          SDB_ASSERT(term.size() == prefix.size() + sizeof(int64_t));
          _resume_row = primary_key::ReadSigned<int64_t>(std::string_view{
            reinterpret_cast<const char*>(term.data()) + prefix.size(),
            sizeof(int64_t)});
          _terms.reset();
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

namespace {

enum class Coverage : uint8_t {
  None,
  All,
  Some,
};

constexpr int64_t kScannedRangeRows = 4096;

std::span<const search::RowRange>::iterator RangeEndingAfter(
  std::span<const search::RowRange> rows, int64_t row) {
  return absl::c_upper_bound(rows, row,
                             [](int64_t value, const search::RowRange& range) {
                               return value < range.end;
                             });
}

Coverage Classify(const duckdb::BaseStatistics& stats,
                  std::span<const search::RowRange> rows) {
  if (!duckdb::NumericStats::HasMinMax(stats)) {
    return Coverage::Some;
  }
  const auto min = duckdb::NumericStats::GetMin<int64_t>(stats);
  const auto max = duckdb::NumericStats::GetMax<int64_t>(stats);
  const auto range = RangeEndingAfter(rows, min);
  if (range == rows.end() || range->begin > max) {
    return Coverage::None;
  }
  if (range->begin <= min && max < range->end) {
    return Coverage::All;
  }
  return Coverage::Some;
}

}  // namespace

irs::QueryBuilder::ptr SearchRowRangeFilter::PrepareSegment(
  const irs::SubReader& segment, const irs::PrepareContext& ctx) const {
  return irs::memory::make_tracked<SearchRemoveQuery<SearchRowRangeFilter>>(
    ctx.memory, segment, *this, ctx.pending_docs_mask);
}

irs::lead::Node::ptr SearchRowRangeFilter::MakeLead(
  const irs::SubReader& segment, const irs::DocumentMaskBuilder*) const {
  _ranges.clear();
  _range = 0;
  const auto* store = segment.GetColReader();
  SDB_ASSERT(store);
  const auto* column = store->Column(kGeneratedPKId);
  SDB_ASSERT(column);
  SDB_ASSERT(column->Type().InternalType() == duckdb::PhysicalType::INT64);
  Collect(segment, *store, *column);
  auto& self = const_cast<SearchRowRangeFilter&>(*this);
  self._doc = irs::doc_limits::invalid();
  return irs::memory::to_managed<irs::lead::Node>(self);
}

void SearchRowRangeFilter::Collect(const irs::SubReader& segment,
                                   const irs::ColReader& store,
                                   const irs::ColumnReader& column) const {
  const std::span<const search::RowRange> deleted{_rows};
  const uint64_t rows = std::min(column.RowCount(), segment.docs_count());
  const auto add = [&](uint64_t begin, uint64_t end) {
    const auto first =
      static_cast<irs::doc_id_t>(irs::doc_limits::min() + begin);
    const auto last = static_cast<irs::doc_id_t>(irs::doc_limits::min() + end);
    if (!_ranges.empty() && _ranges.back().end == first) {
      _ranges.back().end = last;
    } else {
      _ranges.emplace_back(first, last);
    }
  };
  auto hit = deleted.end();
  const auto contains = [&](int64_t rowid) {
    if (hit != deleted.end() && hit->begin <= rowid && rowid < hit->end) {
      return true;
    }
    const auto range = RangeEndingAfter(deleted, rowid);
    if (range == deleted.end() || rowid < range->begin) {
      return false;
    }
    hit = range;
    return true;
  };
  switch (Classify(column.MergedStatistics(), deleted)) {
    case Coverage::None:
      return;
    case Coverage::All:
      add(0, rows);
      return;
    case Coverage::Some:
      break;
  }
  std::unique_ptr<irs::ReadContext> ctx;
  std::unique_ptr<irs::ColumnReader::VectorScratch> scratch;
  irs::ColumnReader::ScanState scan;
  uint64_t scanned = 0;
  irs::BlockWindow window;
  for (uint64_t row = 0; row < rows; row = window.end) {
    window = column.Locate(row, window);
    const uint64_t end = std::min<uint64_t>(window.end, rows);
    const auto coverage =
      Classify(column.RowGroupStatistics(window.block), deleted);
    if (coverage == Coverage::All) {
      add(row, end);
    }
    if (coverage != Coverage::Some) {
      continue;
    }
    if (!ctx) {
      ctx = std::make_unique<irs::ReadContext>(store);
      scan = column.InitScan(*ctx);
      scratch =
        std::make_unique<irs::ColumnReader::VectorScratch>(column.Type());
    }
    if (row > scanned) {
      column.Skip(scan, row - scanned);
      scanned = row;
    }
    while (scanned < end) {
      const auto count =
        std::min<uint64_t>(end - scanned, STANDARD_VECTOR_SIZE);
      auto& values = scratch->Reset();
      column.Scan(scan, values, count);
      duckdb::UnifiedVectorFormat format;
      values.ToUnifiedFormat(format);
      const auto* data = duckdb::UnifiedVectorFormat::GetData<int64_t>(format);
      for (uint64_t i = 0; i != count; ++i) {
        const auto index = format.sel->get_index(i);
        if (format.validity.RowIsValid(index) && contains(data[index])) {
          add(scanned + i, scanned + i + 1);
        }
      }
      scanned += count;
    }
  }
}

irs::doc_id_t SearchRowRangeFilter::Next() {
  while (_range != _ranges.size()) {
    auto& range = _ranges[_range];
    if (range.begin != range.end) {
      return _doc = range.begin++;
    }
    ++_range;
  }
  return _doc = irs::doc_limits::eof();
}

irs::QueryBuilder::ptr SearchRemoveUnion::PrepareSegment(
  const irs::SubReader& segment, const irs::PrepareContext& ctx) const {
  return irs::memory::make_tracked<SearchRemoveQuery<SearchRemoveUnion>>(
    ctx.memory, segment, *this, ctx.pending_docs_mask);
}

irs::lead::Node::ptr SearchRemoveUnion::MakeLead(
  const irs::SubReader& segment,
  const irs::DocumentMaskBuilder* pending) const {
  _prepared.clear();
  _leads.clear();
  _heads.clear();
  for (const auto& filter : _filters) {
    auto prepared =
      filter->PrepareSegment(segment, {.pending_docs_mask = pending});
    if (!prepared) {
      continue;
    }
    auto lead = prepared->PlanLead({});
    if (!lead) {
      continue;
    }
    _heads.push_back(lead->Next());
    _prepared.push_back(std::move(prepared));
    _leads.push_back(std::move(lead));
  }
  return irs::memory::to_managed<irs::lead::Node>(
    const_cast<SearchRemoveUnion&>(*this));
}

irs::doc_id_t SearchRemoveUnion::Next() {
  if (_heads.empty()) {
    return irs::doc_limits::eof();
  }
  const auto doc = *absl::c_min_element(_heads);
  if (irs::doc_limits::eof(doc)) {
    return doc;
  }
  for (size_t i = 0; i != _heads.size(); ++i) {
    if (_heads[i] == doc) {
      _heads[i] = _leads[i]->Next();
    }
  }
  return doc;
}

std::shared_ptr<const irs::Filter> MakeRowRangeRemoval(
  std::span<const search::RowRange> rows) {
  SDB_ASSERT(absl::c_all_of(rows, [](const search::RowRange& range) {
    return range.begin < range.end;
  }));
  SDB_ASSERT(absl::c_adjacent_find(
               rows, [](const search::RowRange& l, const search::RowRange& r) {
                 return l.end >= r.begin;
               }) == rows.end());
  const auto scanned = [](const search::RowRange& range) {
    return range.end - range.begin >= kScannedRangeRows;
  };
  std::vector<search::RowRange> scanned_ranges;
  size_t looked_up = 0;
  for (const auto& range : rows) {
    if (scanned(range)) {
      scanned_ranges.push_back(range);
    } else {
      looked_up += range.end - range.begin;
    }
  }
  std::vector<std::shared_ptr<const irs::Filter>> removals;
  if (looked_up != 0) {
    auto removal =
      std::make_shared<SearchRemoveFilter>(looked_up, term_dict::kPKFieldId);
    std::string key;
    for (const auto& range : rows) {
      if (scanned(range)) {
        continue;
      }
      for (auto row = range.begin; row != range.end; ++row) {
        key.clear();
        primary_key::AppendGenerated(key, static_cast<uint64_t>(row));
        removal->Add(key);
      }
    }
    removals.push_back(std::move(removal));
  }
  if (!scanned_ranges.empty()) {
    removals.push_back(
      std::make_shared<SearchRowRangeFilter>(std::move(scanned_ranges)));
  }
  switch (removals.size()) {
    case 0:
      return nullptr;
    case 1:
      return std::move(removals.front());
    default:
      return std::make_shared<SearchRemoveUnion>(std::move(removals));
  }
}

}  // namespace sdb::connector
