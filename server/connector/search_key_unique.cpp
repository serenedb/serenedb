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

#include "connector/search_key_unique.h"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <memory>
#include <string_view>
#include <utility>

#include "connector/term_dict.h"

namespace sdb::connector {
namespace {

// A key is only found again under what the sink wrote, so every column is read
// the way the sink reads it: through the same dispatch, off the same vector,
// into the same member and bytes. A type the sink does not invert as one value
// has no key component to build, so it is refused by name. `assign` names the
// component each row's value belongs in, so a caller keying a buffer of rows
// and one filling in a column of candidates read a value the same way.
template<typename Assign>
void EncodeColumn(duckdb::Vector& column, const primary_key::KeySlot& slot,
                  duckdb::idx_t first_row, duckdb::idx_t count,
                  Assign&& assign) {
  duckdb::UnifiedVectorFormat format;
  column.ToUnifiedFormat(count, format);
  const auto encode = [&]<duckdb::LogicalTypeId Kind>() {
    using enum duckdb::LogicalTypeId;
    for (duckdb::idx_t row = first_row; row != count; ++row) {
      const auto idx = format.sel->get_index(row);
      KeyTerm& component = assign(row);
      if constexpr (Kind == VARCHAR || Kind == BLOB) {
        const auto& value =
          duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(format)[idx];
        component.dict = {.layout = slot.layout,
                          .member = irs::DictMember::Strings};
        component.term.assign(irs::ViewCast<irs::byte_type>(
          std::string_view{value.GetData(), value.GetSize()}));
      } else if constexpr (Kind == BOOLEAN) {
        const auto* data = duckdb::UnifiedVectorFormat::GetData<bool>(format);
        component.dict = {.layout = slot.layout,
                          .member = irs::BoolMember(data[idx])};
        component.term.clear();
      } else {
        const auto value = term_dict::PromoteNumericValue(
          term_dict::ExtractNumericValue<Kind>(format, idx));
        irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
        component.dict = {.layout = slot.layout,
                          .member = irs::DictMember::Numbers};
        component.term.assign(
          irs::numeric_utils::EncodeNumericTerm(buf, value));
      }
    }
  };
  if (!term_dict::DispatchValueKind(term_dict::IndexedKind(slot.type),
                                    encode)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("primary key column \"", slot.name, "\" of type ",
              slot.type.ToString(), " is not indexed as a single value"),
      ERR_HINT("A search table enforces its primary key through the index, so "
               "every key column must be indexed as one value."));
  }
}

constexpr size_t kGatherBatch = STANDARD_VECTOR_SIZE;

// How many candidates the walk carries before settling them. Counted rather
// than measured: a candidate's remaining columns are only read when it settles,
// so at the point of the decision there is nothing to weigh. Every candidate
// still costs its key's spine, so the count bounds what matters.
constexpr size_t kCandidateFlush = size_t{1} << 18;

duckdb::idx_t RowOf(irs::doc_id_t doc) noexcept {
  return static_cast<duckdb::idx_t>(doc - irs::doc_limits::min());
}

// A key the seed walk could not rule out, carried until its remaining columns
// are known.
struct Candidate {
  KeyTerms key;
  // Whether published state holds the seed term, so the whole key still has to
  // be put to the probe.
  bool published = false;
};

// One source of this transaction's keys, walked in seed-term order. A source is
// walked exactly once, so every key it holds is certified.
class KeySource {
 public:
  virtual ~KeySource() = default;

  // Positions on the next seed term. False once the source is exhausted.
  virtual bool Next() = 0;

  // The seed term this source currently sits on.
  virtual const KeyTerm& Seed() const = 0;

  // Live positions carrying the current seed term.
  virtual size_t Count() const = 0;

  // Whether `Collect` leaves the non-seed columns for `Finish` to fill in. A
  // group no source defers is settled during the walk and never buffered.
  virtual bool Defers() const = 0;

  // Appends this position's keys as candidates.
  virtual void Collect(bool published, std::vector<Candidate>& out) = 0;

  // Fills in the columns `Collect` left behind, for every candidate this
  // source appended. One pass over each column, however many seed terms the
  // walk collected, which is what keeps the cost off the number of collisions.
  virtual void Finish(std::vector<Candidate>& out) = 0;
};

// Equal keys sort adjacent, and only a key already in a group can repeat, so
// one ordering settles every group at once.
std::optional<KeyTerms> FirstDuplicate(std::span<Candidate> candidates,
                                       UniqueProbe& probe) {
  absl::c_sort(candidates, [](const Candidate& lhs, const Candidate& rhs) {
    return lhs.key < rhs.key;
  });
  for (size_t i = 1; i < candidates.size(); ++i) {
    if (candidates[i].key == candidates[i - 1].key) {
      return candidates[i].key;
    }
  }
  for (const auto& candidate : candidates) {
    if (candidate.published && probe.Exists(candidate.key)) {
      return candidate.key;
    }
  }
  return std::nullopt;
}

// Reads back what the walk deferred and puts the whole window to the probe,
// leaving it empty. Equal keys share a seed term and a seed term is collected
// as one group, so a window cut on a group boundary never separates two halves
// of a duplicate.
std::optional<KeyTerms> Settle(
  std::span<const std::unique_ptr<KeySource>> sources,
  std::vector<Candidate>& candidates, UniqueProbe& probe) {
  if (candidates.empty()) {
    return std::nullopt;
  }
  for (const auto& source : sources) {
    source->Finish(candidates);
  }
  auto duplicate = FirstDuplicate(candidates, probe);
  candidates.clear();
  return duplicate;
}

class SegmentKeySource final : public KeySource {
 public:
  SegmentKeySource(const irs::SubReader& segment,
                   std::span<const primary_key::KeySlot> slots, size_t seed,
                   std::span<const irs::TermReader* const> seed_fields)
    : _segment{&segment},
      _slots{slots},
      _seed_index{seed},
      _mask{segment.MaskedDocs()},
      _fields{seed_fields.begin(), seed_fields.end()} {
    // The seed column's dictionaries are walked one after another, so sorting
    // them by dictionary is what makes this source's seeds ascend -- which the
    // merge below relies on to compare sources against each other.
    absl::c_sort(_fields, [](const auto* lhs, const auto* rhs) {
      return lhs->meta().dict < rhs->meta().dict;
    });
  }

  bool Next() final {
    while (_current != _fields.size()) {
      if (!_terms) {
        _terms = _fields[_current]->iterator();
        if (!_terms) {
          ++_current;
          continue;
        }
      }
      while (_terms->next()) {
        const auto term = _terms->value();
        _docs.clear();
        ForEachLiveDoc(*_fields[_current], term, _mask, nullptr,
                       [&](irs::doc_id_t doc) {
                         _docs.push_back(doc);
                         return true;
                       });
        if (_docs.empty()) {
          continue;
        }
        _seed.dict = _fields[_current]->meta().dict;
        _seed.term.assign(term);
        return true;
      }
      _terms.reset();
      ++_current;
    }
    return false;
  }

  const KeyTerm& Seed() const final { return _seed; }

  size_t Count() const final { return _docs.size(); }

  bool Defers() const final { return _slots.size() > 1; }

  void Collect(bool published, std::vector<Candidate>& out) final {
    const bool defers = Defers();
    for (const auto doc : _docs) {
      if (defers) {
        _pending.push_back({.doc = doc, .slot = out.size()});
      }
      auto& candidate = out.emplace_back();
      candidate.key.resize(_slots.size());
      candidate.key[_seed_index] = _seed;
      candidate.published = published;
    }
  }

  void Finish(std::vector<Candidate>& out) final {
    if (_pending.empty()) {
      return;
    }
    // The gather is a forward cursor over distinct rows: it skips ahead, never
    // back, and never twice to the same place. A seed column holding several
    // terms for one document -- a list, or text an analyzer split -- puts that
    // document under each of them, so one row can owe its value to more than
    // one candidate.
    absl::c_sort(_pending, [](const Pending& lhs, const Pending& rhs) {
      return lhs.doc < rhs.doc;
    });
    std::vector<Run> runs;
    for (size_t i = 0; i != _pending.size();) {
      auto& run = runs.emplace_back(_pending[i].doc, i, i);
      while (run.end != _pending.size() && _pending[run.end].doc == run.doc) {
        ++run.end;
      }
      i = run.end;
    }

    const auto* columns = _segment->GetColReader();
    SDB_ASSERT(columns);
    irs::ReadContext ctx{*columns};
    duckdb::SelectionVector sel{kGatherBatch};
    std::vector<KeyTerm> terms;

    for (size_t f = 0; f != _slots.size(); ++f) {
      if (f == _seed_index) {
        continue;
      }
      const auto* column = columns->Column(_slots[f].column);
      // A search table's columns and their types are fixed at CREATE TABLE:
      // ALTER TABLE adds, drops or retypes none of them, and a primary key
      // cannot be declared later, so every segment carries every key column.
      SDB_ASSERT(column);
      SDB_ASSERT(column->Type() == _slots[f].type);
      auto state = column->InitScan(ctx);
      irs::ColumnReader::VectorScratch scratch{column->Type()};

      for (size_t i = 0; i < runs.size(); i += kGatherBatch) {
        const auto hits = std::min(kGatherBatch, runs.size() - i);
        const auto anchor = RowOf(runs[i].doc);
        for (size_t k = 0; k != hits; ++k) {
          sel.set_index(k, RowOf(runs[i + k].doc) - anchor);
        }
        auto& values = scratch.Reset();
        column->GatherScatter(state, anchor, sel, hits, values, 0);
        terms.resize(hits);
        EncodeColumn(values, _slots[f], 0, hits,
                     [&](duckdb::idx_t row) -> KeyTerm& { return terms[row]; });
        for (size_t k = 0; k != hits; ++k) {
          const auto& run = runs[i + k];
          for (size_t p = run.begin; p != run.end; ++p) {
            auto& term = out[_pending[p].slot].key[f];
            if (p + 1 == run.end) {
              term = std::move(terms[k]);
            } else {
              term = terms[k];
            }
          }
        }
      }
    }
    _pending.clear();
  }

 private:
  struct Pending {
    irs::doc_id_t doc;
    size_t slot;
  };

  // The candidates one row owes its value to: `_pending[begin, end)`.
  struct Run {
    irs::doc_id_t doc;
    size_t begin;
    size_t end;
  };

  const irs::SubReader* _segment;
  std::span<const primary_key::KeySlot> _slots;
  size_t _seed_index;
  irs::DocumentMask::Iterator _mask;
  std::vector<const irs::TermReader*> _fields;
  size_t _current = 0;
  irs::SeekTermIterator::ptr _terms;
  KeyTerm _seed;
  std::vector<irs::doc_id_t> _docs;
  std::vector<Pending> _pending;
};

// Reads its keys where they lie and orders an index instead of them, because a
// commit puts the same key set to the probe twice -- once before it takes the
// commit lock and once under it -- and the second pass cannot afford to copy
// it. Only a key the walk actually collects is copied, which is one that
// collided; the rest are never touched.
class RowKeySource final : public KeySource {
 public:
  RowKeySource(std::span<const KeyTerms> keys, size_t seed)
    : _keys{keys}, _seed_index{seed}, _order(keys.size()) {
    absl::c_iota(_order, size_t{0});
    absl::c_sort(_order, [keys, seed](size_t lhs, size_t rhs) {
      if (keys[lhs][seed] != keys[rhs][seed]) {
        return keys[lhs][seed] < keys[rhs][seed];
      }
      return keys[lhs] < keys[rhs];
    });
  }

  bool Next() final {
    _begin = _end;
    if (_begin == _order.size()) {
      return false;
    }
    while (_end != _order.size() && Term(_end) == Term(_begin)) {
      ++_end;
    }
    return true;
  }

  const KeyTerm& Seed() const final { return Term(_begin); }

  size_t Count() const final { return _end - _begin; }

  bool Defers() const final { return false; }

  void Collect(bool published, std::vector<Candidate>& out) final {
    for (size_t i = _begin; i != _end; ++i) {
      out.push_back({.key = _keys[_order[i]], .published = published});
    }
  }

  void Finish(std::vector<Candidate>&) final {}

 private:
  const KeyTerm& Term(size_t position) const noexcept {
    return _keys[_order[position]][_seed_index];
  }

  std::span<const KeyTerms> _keys;
  size_t _seed_index;
  std::vector<size_t> _order;
  size_t _begin = 0;
  size_t _end = 0;
};

// Which key column orders the walk. Per-field dictionaries carry no order over
// composite keys, so one field leads and the rest are read only where that one
// already collides. The most distinct terms means the fewest documents per
// term, hence the fewest keys to resolve -- and it keeps a boolean, whose whole
// column is two terms, from leading whenever anything else can.
size_t ChooseSeedField(std::span<const irs::SubReader* const> segments,
                       std::span<const primary_key::KeySlot> slots) {
  SDB_ASSERT(!slots.empty());
  size_t seed = 0;
  size_t widest = 0;
  for (size_t f = 0; f != slots.size(); ++f) {
    size_t terms = 0;
    for (const auto* segment : segments) {
      for (const auto* field : segment->fields(slots[f].column)) {
        terms += field->size();
      }
    }
    if (terms > widest) {
      widest = terms;
      seed = f;
    }
  }
  return seed;
}

std::unique_ptr<KeySource> MakeSegmentKeySource(
  const irs::SubReader& segment, std::span<const primary_key::KeySlot> slots,
  size_t seed) {
  const auto fields = segment.fields(slots[seed].column);
  if (fields.empty()) {
    return nullptr;
  }
  return std::make_unique<SegmentKeySource>(segment, slots, seed, fields);
}

}  // namespace

void EncodeChunkKeys(duckdb::DataChunk& chunk,
                     std::span<const primary_key::KeySlot> slots,
                     std::vector<KeyTerms>& keys, duckdb::idx_t first_row) {
  const auto count = chunk.size();
  if (first_row >= count) {
    return;
  }
  const auto first_key = keys.size();
  keys.resize(first_key + (count - first_row), KeyTerms(slots.size()));
  for (size_t term = 0; term != slots.size(); ++term) {
    EncodeColumn(chunk.data[slots[term].input_col_idx], slots[term], first_row,
                 count, [&](duckdb::idx_t row) -> KeyTerm& {
                   return keys[first_key + (row - first_row)][term];
                 });
  }
}

void AppendRowKeys(duckdb::ColumnDataCollection& rows,
                   std::span<const primary_key::KeySlot> slots,
                   duckdb::idx_t first_row, std::vector<KeyTerms>& keys) {
  duckdb::ColumnDataScanState state;
  rows.InitializeScan(state);
  duckdb::DataChunk chunk;
  rows.InitializeScanChunk(state, chunk);
  duckdb::idx_t seen = 0;
  while (true) {
    chunk.Reset();
    rows.Scan(state, chunk);
    if (chunk.size() == 0) {
      break;
    }
    const auto next = seen + chunk.size();
    if (next > first_row) {
      EncodeChunkKeys(chunk, slots, keys,
                      first_row > seen ? first_row - seen : 0);
    }
    seen = next;
  }
}

std::optional<KeyTerms> FindDuplicateKey(
  std::span<const irs::SubReader* const> segments,
  std::span<const KeyTerms> keys, std::span<const primary_key::KeySlot> slots,
  UniqueProbe& probe) {
  const auto seed = ChooseSeedField(segments, slots);

  std::vector<std::unique_ptr<KeySource>> sources;
  sources.reserve(segments.size() + 1);
  for (const auto* segment : segments) {
    if (auto source = MakeSegmentKeySource(*segment, slots, seed)) {
      sources.push_back(std::move(source));
    }
  }
  if (!keys.empty()) {
    sources.push_back(std::make_unique<RowKeySource>(keys, seed));
  }

  // The sources outlive the walk: a source that ran out still owns candidates
  // the walk collected, and only it can finish them.
  std::vector<KeySource*> active;
  active.reserve(sources.size());
  for (const auto& source : sources) {
    if (source->Next()) {
      active.push_back(source.get());
    }
  }

  std::vector<Candidate> candidates;
  std::vector<size_t> matched;
  while (!active.empty()) {
    // A strictly smaller seed restarts the tally, an equal one joins it. All of
    // it settled before anything advances: `least` points into a source's own
    // term buffer, which `Next` overwrites, so a later comparison against it
    // would match whatever that source moved on to.
    const KeyTerm* least = nullptr;
    matched.clear();
    size_t positions = 0;
    for (size_t i = 0; i != active.size(); ++i) {
      const auto& seed_term = active[i]->Seed();
      if (matched.empty() || seed_term < *least) {
        least = &seed_term;
        matched.assign(1, i);
        positions = active[i]->Count();
      } else if (seed_term == *least) {
        matched.push_back(i);
        positions += active[i]->Count();
      }
    }

    // Published state is consulted by the seed column alone first: a miss there
    // rules the whole key out without reading the other columns.
    const bool published = probe.MayHold(seed, *least);
    if (positions > 1 || published) {
      const auto group = candidates.size();
      bool deferred = false;
      for (const auto i : matched) {
        deferred = deferred || active[i]->Defers();
        active[i]->Collect(published, candidates);
      }
      if (!deferred) {
        if (auto duplicate =
              FirstDuplicate(std::span{candidates}.subspan(group), probe)) {
          return duplicate;
        }
        candidates.resize(group);
      } else if (candidates.size() >= kCandidateFlush) {
        if (auto duplicate = Settle(sources, candidates, probe)) {
          return duplicate;
        }
      }
    }

    bool dropped = false;
    for (const auto i : matched) {
      if (!active[i]->Next()) {
        active[i] = nullptr;
        dropped = true;
      }
    }
    if (dropped) {
      std::erase(active, nullptr);
    }
  }

  return Settle(sources, candidates, probe);
}

}  // namespace sdb::connector
