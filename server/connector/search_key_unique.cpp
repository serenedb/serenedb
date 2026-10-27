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
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <utility>

#include "connector/search_filter_builder.hpp"
#include "connector/search_remove_filter.hpp"
#include "connector/term_dict.h"

namespace sdb::connector {
namespace {

constexpr size_t kGatherBatch = STANDARD_VECTOR_SIZE;

// How many candidates the walk carries before settling them. Counted rather
// than measured: a candidate's remaining columns are only read when it settles,
// so at the point of the decision there is nothing to weigh. Every candidate
// still costs its key's spine, so the count bounds what matters.
constexpr size_t kCandidateFlush = size_t{1} << 18;

duckdb::idx_t RowOf(irs::doc_id_t doc) noexcept {
  return static_cast<duckdb::idx_t>(doc - irs::doc_limits::min());
}

// Only a full-precision term identifies a single numeric value; the coarser
// buckets of a precision-step trie match whole ranges, so a key built from one
// would conflict with unrelated neighbours.
std::optional<irs::byte_type> FullPrecisionMagic(
  duckdb::LogicalTypeId type_id) {
  switch (term_dict::Classify(type_id)) {
    case term_dict::Kind::NumericI32:
      return irs::numeric_utils::encode_traits<uint32_t>::TYPE_MAGIC;
    case term_dict::Kind::NumericI64:
      return irs::numeric_utils::encode_traits<uint64_t>::TYPE_MAGIC;
    case term_dict::Kind::NumericF32:
      return irs::numeric_utils::encode_traits<float_t>::TYPE_MAGIC;
    case term_dict::Kind::NumericF64:
      return irs::numeric_utils::encode_traits<double_t>::TYPE_MAGIC;
    default:
      return std::nullopt;
  }
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
  virtual irs::bytes_view Seed() const = 0;

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
                                       UniqueProbe& probe);

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

class SegmentKeySource final : public KeySource {
 public:
  SegmentKeySource(const irs::SubReader& segment,
                   std::span<const primary_key::KeySlot> slots, size_t seed,
                   const irs::TermReader& seed_field)
    : _segment{&segment},
      _slots{slots},
      _seed_index{seed},
      _seed_field{&seed_field},
      _mask{segment.MaskedDocs()},
      _magic{FullPrecisionMagic(slots[seed].type.id())},
      _terms{seed_field.iterator()} {}

  bool Next() final {
    while (_terms->next()) {
      const auto term = _terms->value();
      if (_magic && (term.empty() || term.front() != *_magic)) {
        continue;
      }
      _docs.clear();
      ForEachLiveDoc(*_seed_field, term, _mask, nullptr,
                     [&](irs::doc_id_t doc) {
                       _docs.push_back(doc);
                       return true;
                     });
      if (_docs.empty()) {
        continue;
      }
      _seed.assign(term);
      return true;
    }
    return false;
  }

  irs::bytes_view Seed() const final { return _seed; }

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
      const SearchColumnInfo info{.field_id = _slots[f].column,
                                  .logical_type = _slots[f].type};
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
        for (size_t k = 0; k != hits; ++k) {
          const auto value = values.GetValue(k);
          SDB_ASSERT(!value.IsNull());
          irs::TermClause clause;
          if (const auto status = SetupTermClause(clause, info, value);
              !status.ok()) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                            ERR_MSG(status.message()));
          }
          const auto& run = runs[i + k];
          for (size_t p = run.begin; p != run.end; ++p) {
            auto& term = out[_pending[p].slot].key[f];
            if (p + 1 == run.end) {
              term = std::move(clause.term);
            } else {
              term = clause.term;
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
  const irs::TermReader* _seed_field;
  irs::DocumentMask::Iterator _mask;
  std::optional<irs::byte_type> _magic;
  irs::SeekTermIterator::ptr _terms;
  irs::bstring _seed;
  std::vector<irs::doc_id_t> _docs;
  std::vector<Pending> _pending;
};

class RowKeySource final : public KeySource {
 public:
  RowKeySource(duckdb::ColumnDataCollection& rows,
               std::span<const primary_key::KeySlot> slots, size_t seed)
    : _seed_index{seed} {
    for (auto& chunk : rows.Chunks()) {
      for (duckdb::idx_t row = 0; row != chunk.size(); ++row) {
        KeyTerms key(slots.size());
        for (size_t i = 0; i != slots.size(); ++i) {
          const SearchColumnInfo info{.field_id = slots[i].column,
                                      .logical_type = slots[i].type};
          irs::TermClause clause;
          const auto status = SetupTermClause(
            clause, info, chunk.GetValue(slots[i].input_col_idx, row));
          if (!status.ok()) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                            ERR_MSG(status.message()));
          }
          key[i] = std::move(clause.term);
        }
        _keys.push_back(std::move(key));
      }
    }
    absl::c_sort(_keys, [seed](const KeyTerms& lhs, const KeyTerms& rhs) {
      if (lhs[seed] != rhs[seed]) {
        return lhs[seed] < rhs[seed];
      }
      return lhs < rhs;
    });
  }

  bool Next() final {
    _begin = _end;
    if (_begin == _keys.size()) {
      return false;
    }
    while (_end != _keys.size() &&
           _keys[_end][_seed_index] == _keys[_begin][_seed_index]) {
      ++_end;
    }
    return true;
  }

  irs::bytes_view Seed() const final { return _keys[_begin][_seed_index]; }

  size_t Count() const final { return _end - _begin; }

  bool Defers() const final { return false; }

  void Collect(bool published, std::vector<Candidate>& out) final {
    for (size_t i = _begin; i != _end; ++i) {
      // The walk never returns to a position, so the key travels rather than
      // being copied.
      out.push_back({.key = std::move(_keys[i]), .published = published});
    }
  }

  void Finish(std::vector<Candidate>&) final {}

 private:
  std::vector<KeyTerms> _keys;
  size_t _seed_index;
  size_t _begin = 0;
  size_t _end = 0;
};

// Which key column orders the walk. Per-field dictionaries carry no order over
// composite keys, so one field leads and the rest are read only where that one
// already collides. The most distinct terms means the fewest documents per
// term, hence the fewest keys to resolve.
size_t ChooseSeedField(std::span<const irs::SubReader* const> segments,
                       std::span<const primary_key::KeySlot> slots) {
  SDB_ASSERT(!slots.empty());
  size_t seed = 0;
  size_t widest = 0;
  for (size_t f = 0; f != slots.size(); ++f) {
    size_t terms = 0;
    for (const auto* segment : segments) {
      if (const auto* field = segment->field(slots[f].column)) {
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
  const auto* seed_field = segment.field(slots[seed].column);
  if (seed_field == nullptr) {
    return nullptr;
  }
  return std::make_unique<SegmentKeySource>(segment, slots, seed, *seed_field);
}

std::unique_ptr<KeySource> MakeRowKeySource(
  duckdb::ColumnDataCollection& rows,
  std::span<const primary_key::KeySlot> slots, size_t seed) {
  return std::make_unique<RowKeySource>(rows, slots, seed);
}

}  // namespace

std::optional<KeyTerms> FindDuplicateKey(
  std::span<const irs::SubReader* const> segments,
  duckdb::ColumnDataCollection* rows,
  std::span<const primary_key::KeySlot> slots, UniqueProbe& probe) {
  const auto seed = ChooseSeedField(segments, slots);

  std::vector<std::unique_ptr<KeySource>> sources;
  sources.reserve(segments.size() + 1);
  for (const auto* segment : segments) {
    if (auto source = MakeSegmentKeySource(*segment, slots, seed)) {
      sources.push_back(std::move(source));
    }
  }
  if (rows != nullptr) {
    sources.push_back(MakeRowKeySource(*rows, slots, seed));
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
    // it settled before anything advances: `least` views a source's own term
    // buffer, which `Next` overwrites, so a later comparison against it would
    // match whatever that source moved on to.
    irs::bytes_view least;
    matched.clear();
    size_t positions = 0;
    for (size_t i = 0; i != active.size(); ++i) {
      const auto seed_term = active[i]->Seed();
      if (matched.empty() || seed_term < least) {
        least = seed_term;
        matched.assign(1, i);
        positions = active[i]->Count();
      } else if (seed_term == least) {
        matched.push_back(i);
        positions += active[i]->Count();
      }
    }

    // Published state is consulted by the seed column alone first: a miss there
    // rules the whole key out without reading the other columns.
    const bool published = probe.MayHold(seed, least);
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
