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

#include "iresearch/index/inverter/columnar_flush.hpp"

#include <absl/strings/str_cat.h>

#include <bit>
#include <duckdb/common/bswap.hpp>

#include "iresearch/error/error.hpp"

namespace irs {
namespace {

constexpr size_t kRadixThreshold = 2048;
constexpr uint32_t kRekeyMinShared = 4;
constexpr uint32_t kRekeyMaxSkip = 64;

uint64_t PrefixKey(const duckdb::string_t& term) noexcept {
  uint64_t key;
  std::memcpy(&key, term.GetData(), sizeof key);
  return duckdb::BSwapIfLE(key);
}

uint64_t PrefixKeyAt(const duckdb::string_t& term, uint32_t skip) noexcept {
  const uint32_t size = term.GetSize();
  if (size <= skip) {
    return 0;
  }
  uint64_t key = 0;
  std::memcpy(&key, term.GetData() + skip,
              std::min<uint32_t>(sizeof key, size - skip));
  return duckdb::BSwapIfLE(key);
}

// A flush of a desynced log (a mid-push allocation throw left the columns at
// unequal lengths) must fail, never write: every column read is bounded by
// the reader's out-of-line refill (an exhausted column throws instead of
// dereferencing an empty span, at most once per 1024 values) and Scatter
// cross-checks the final per-column totals. The inlined hot path carries no
// added checks -- it got smaller (the refill used to inline into every lane
// copy of the fused loops).
[[noreturn]] void ThrowDesyncedLog(field_id field, std::string_view what) {
  throw IndexError{absl::StrCat("occurrence log desynced in field '", field,
                                "' (", what, "), aborting segment flush")};
}

class LogColumnReader {
 public:
  LogColumnReader(const LogColumn& col, field_id field) noexcept
    : _cursor{col}, _field{field} {}

  IRS_FORCE_INLINE uint32_t Read() {
    if (_idx == _cur.size()) [[unlikely]] {
      Refill();
    }
    return _cur[_idx++];
  }

 private:
  IRS_NO_INLINE void Refill() {
    _cur = _cursor.Next();
    _idx = 0;
    if (_cur.empty()) [[unlikely]] {
      ThrowDesyncedLog(_field, "column exhausted");
    }
  }

  LogColumn::Cursor _cursor;
  std::span<const uint32_t> _cur;
  size_t _idx = 0;
  field_id _field;
};

struct NoColumn {
  IRS_FORCE_INLINE uint32_t Read() noexcept { return 0; }
};

// Write side of the term-major scatter output: a column is a flat array of
// fixed 64K-value block pointers (consumers are doc-at-a-time iterators, so
// contiguity is never needed). A null column is a layout the field does not
// index -- its Set is never reached (guarded by the same `if constexpr`).
struct OutColumn {
  uint32_t** blocks = nullptr;

  IRS_FORCE_INLINE void Set(uint64_t i, uint32_t value) noexcept {
    blocks[i >> ScatterView::kBlockShift][i & ScatterView::kBlockMask] = value;
  }
};

}  // namespace

void ScatteredField::RadixSortByKey() {
  auto& src = _s->ranked;
  auto& dst = _s->ranked_alt;
  const size_t n = src.size();
  if (n < 2) {
    return;
  }
  dst.resize(n);
  constexpr size_t kBuckets = 65536;
  auto& counts = _s->radix_counts;
  counts.assign(4 * kBuckets, 0);
  for (size_t i = 0; i < n; ++i) {
    const auto key = src[i].key;
    ++counts[static_cast<uint16_t>(key)];
    ++counts[kBuckets + static_cast<uint16_t>(key >> 16)];
    ++counts[2 * kBuckets + static_cast<uint16_t>(key >> 32)];
    ++counts[3 * kBuckets + static_cast<uint16_t>(key >> 48)];
  }
  const auto probe = src[0].key;
  for (unsigned pass = 0; pass < 4; ++pass) {
    const unsigned shift = 16 * pass;
    auto* const c = counts.data() + pass * kBuckets;
    if (c[static_cast<uint16_t>(probe >> shift)] == n) {
      continue;
    }
    uint32_t sum = 0;
    for (size_t b = 0; b < kBuckets; ++b) {
      const auto k = c[b];
      c[b] = sum;
      sum += k;
    }
    for (size_t i = 0; i < n; ++i) {
      dst[c[static_cast<uint16_t>(src[i].key >> shift)]++] = src[i];
    }
    std::swap(src, dst);
  }
}

void ScatteredField::Reset(const FieldInverter& field) {
  _field = &field;
  _all_inline = field.Log().Size() == 0;
  // Inline docs exist only for append-only (unique) dictionaries, which
  // never log: a field is either all-log or all-inline.
  SDB_ASSERT(field.InlineDocs().empty() || _all_inline);
  // Histogram counts and scatter cursors are u32; segment flush thresholds
  // keep any real log far below 4B occurrences, so overflow must abort the
  // flush instead of silently truncating region cursors.
  if (field.Log().Size() > std::numeric_limits<uint32_t>::max()) [[unlikely]] {
    ThrowDesyncedLog(field.Meta().id, "occurrence count overflows u32");
  }

  _identity = _all_inline && InlineFillSorted();
  if (_identity) {
    _s->ranked.clear();
    _s->term_starts.clear();
    _s->docs.clear();
    _s->pos.clear();
    _s->offs_start.clear();
    _s->offs_end.clear();
    return;
  }

  if (!_all_inline) {
    BuildHistogram(field.Log().TermIds(), field.Dictionary().Entries().size());
  }
  const auto fold_hint = RankLiveTerms(field.Dictionary().Entries());

  _s->term_starts.clear();
  if (field.UniqueTerms()) {
    FoldDuplicateTerms(field.Dictionary().Entries(), fold_hint);
  }

  if (_all_inline) {
    _s->docs.clear();
    _s->pos.clear();
    _s->offs_start.clear();
    _s->offs_end.clear();
    return;
  }
  const auto nocc = PrefixSums();
  SDB_ASSERT(nocc == field.Log().Size());
  field.VisitLog([&](const auto& log) { Scatter(log, nocc); });
}

bool ScatteredField::InlineFillSorted() const noexcept {
  const auto entries = _field->Dictionary().Entries();
  const size_t n = _field->InlineDocs().size();
  SDB_ASSERT(n <= entries.size());
  for (size_t i = 1; i < n; ++i) {
    if (!(entries[i - 1].term < entries[i].term)) {
      return false;
    }
  }
  return true;
}

// Duplicate entries of an append-only dictionary rank adjacent; one pass
// finds whether any exist and, only then, materializes the fold starts.
void ScatteredField::FoldDuplicateTerms(
  std::span<const TermDictionary::Entry> entries,
  std::optional<size_t> first_dup_hint) {
  const auto& ranked = _s->ranked;
  const auto same_term = [&](size_t r) {
    return ranked[r].key == ranked[r - 1].key &&
           entries[ranked[r].id].term == entries[ranked[r - 1].id].term;
  };
  size_t first_dup = 0;
  if (first_dup_hint) {
    first_dup = *first_dup_hint;
  } else {
    for (size_t r = 1; r < ranked.size(); ++r) {
      if (same_term(r)) {
        first_dup = r;
        break;
      }
    }
  }
  if (!first_dup) {
    return;
  }
  auto& starts = _s->term_starts;
  starts.reserve(ranked.size() + 1);
  for (size_t r = 0; r < first_dup; ++r) {
    starts.push_back(static_cast<uint32_t>(r));
  }
  for (size_t r = first_dup + 1; r < ranked.size(); ++r) {
    if (!same_term(r)) {
      starts.push_back(static_cast<uint32_t>(r));
    }
  }
  starts.push_back(static_cast<uint32_t>(ranked.size()));
}

// _cursors[id] is a histogram here, then the id's region start after
// PrefixSums, then its region end once Scatter has run.
void ScatteredField::BuildHistogram(const LogColumn& term_ids, size_t vocab) {
  auto& cursors = _s->cursors;
  cursors.assign(vocab, 0);
  LogColumn::Cursor ids{term_ids};
  for (auto vals = ids.Next(); !vals.empty(); vals = ids.Next()) {
    for (const auto id : vals) {
      ++cursors[id];
    }
  }
}

// Zero-occ entries are legal leftovers of a rejected batch (resolved but
// never recorded) and must not surface as terms. Order matches
// Postings::get_sorted_postings (duckdb::string_t less): strict prefix-key
// inequality implies strict string_t inequality (keys are byte-lexicographic,
// zero-padded), so after the radix only equal-key runs compare term bytes.
std::optional<size_t> ScatteredField::RankLiveTerms(
  std::span<const TermDictionary::Entry> entries) {
  auto& ranked = _s->ranked;
  ranked.clear();
  auto& cursors = _s->cursors;
  // Id order is first-occurrence order; PK-shaped columns intern ascending
  // keys, so their fill comes out already key-sorted -- detect that here and
  // skip the sort entirely (equal-key runs still get the term tie-break).
  uint64_t prev_key = 0;
  uint64_t min_key = std::numeric_limits<uint64_t>::max();
  uint64_t max_key = 0;
  bool key_sorted = true;
  const auto note_live = [&](uint32_t i) {
    const auto key = PrefixKey(entries[i].term);
    key_sorted &= key >= prev_key;
    prev_key = key;
    min_key = std::min(min_key, key);
    max_key = std::max(max_key, key);
    ranked.push_back({key, i});
  };
  if (_all_inline) {
    const auto n = static_cast<uint32_t>(_field->InlineDocs().size());
    ranked.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
      note_live(i);
    }
  } else {
    ranked.reserve(entries.size());
    for (uint32_t i = 0; i < entries.size(); ++i) {
      if (cursors[i] != 0) {
        note_live(i);
      }
    }
  }
  if (ranked.size() < 2) {
    return 0;
  }
  RekeyPastSharedPrefix(entries, min_key, max_key, key_sorted);
  const auto by_term = [&](const ScatterScratch::RankedTerm& lhs,
                           const ScatterScratch::RankedTerm& rhs) {
    const auto& lt = entries[lhs.id].term;
    const auto& rt = entries[rhs.id].term;
    return lt != rt ? lt < rt : lhs.id < rhs.id;
  };
  if (!key_sorted) {
    if (ranked.size() < kRadixThreshold) {
      // The precomputed keys settle most comparisons without touching the
      // entries; strict key inequality implies strict term inequality.
      std::sort(ranked.begin(), ranked.end(),
                [&](const ScatterScratch::RankedTerm& lhs,
                    const ScatterScratch::RankedTerm& rhs) {
                  return lhs.key != rhs.key ? lhs.key < rhs.key
                                            : by_term(lhs, rhs);
                });
      return std::nullopt;
    }
    RadixSortByKey();
  } else {
    // Key-sorted fill: one adjacency pass settles the equal-key runs
    // (in-order or not) and finds the first duplicate as a byproduct, so a
    // clean fill skips both the tie-break loop and the fold's detect scan.
    const bool want_dups = _field->UniqueTerms();
    size_t first_dup = 0;
    bool ordered = true;
    for (size_t r = 1, n = ranked.size(); r < n; ++r) {
      if (ranked[r].key != ranked[r - 1].key) {
        continue;
      }
      if (by_term(ranked[r], ranked[r - 1])) {
        ordered = false;
        break;
      }
      if (want_dups && !first_dup &&
          entries[ranked[r].id].term == entries[ranked[r - 1].id].term) {
        first_dup = r;
      }
    }
    if (ordered) {
      return first_dup;
    }
  }
  for (size_t lo = 0, n = ranked.size(); lo < n;) {
    size_t hi = lo + 1;
    while (hi < n && ranked[hi].key == ranked[lo].key) {
      ++hi;
    }
    if (hi - lo > 1 &&
        !std::is_sorted(ranked.begin() + lo, ranked.begin() + hi, by_term)) {
      std::sort(ranked.begin() + lo, ranked.begin() + hi, by_term);
    }
    lo = hi;
  }
  return std::nullopt;
}

// A shared key prefix (URLs, "pk_"-style ids) starves both the radix and
// the sorted-fill fast path of discrimination: whole 100K-term groups
// collapse into one equal-key run. Re-key past the common prefix (its
// width comes free from min/max) until keys discriminate again; bounded
// loads only -- past offset 0 the fixed 8-byte read has no in-bounds
// guarantee.
void ScatteredField::RekeyPastSharedPrefix(
  std::span<const TermDictionary::Entry> entries, uint64_t min_key,
  uint64_t max_key, bool& key_sorted) {
  auto& ranked = _s->ranked;
  for (uint32_t skip = 0; skip < kRekeyMaxSkip;) {
    const uint64_t diff = min_key ^ max_key;
    const auto shared =
      diff ? static_cast<uint32_t>(std::countl_zero(diff)) / 8 : 8;
    if (shared < kRekeyMinShared || (!diff && !max_key)) {
      break;
    }
    skip += shared;
    uint64_t prev_key = 0;
    min_key = std::numeric_limits<uint64_t>::max();
    max_key = 0;
    key_sorted = true;
    for (auto& term : ranked) {
      const auto key = PrefixKeyAt(entries[term.id].term, skip);
      term.key = key;
      key_sorted &= key >= prev_key;
      prev_key = key;
      min_key = std::min(min_key, key);
      max_key = std::max(max_key, key);
    }
  }
}

uint64_t ScatteredField::PrefixSums() {
  auto& cursors = _s->cursors;
  auto& bounds = _s->bounds;
  bounds.clear();
  bounds.reserve(_s->ranked.size() + 1);
  uint64_t sum = 0;
  for (const auto& term : _s->ranked) {
    const auto n = cursors[term.id];
    cursors[term.id] = static_cast<uint32_t>(sum);
    bounds.push_back(static_cast<uint32_t>(sum));
    sum += n;
  }
  bounds.push_back(static_cast<uint32_t>(sum));
  return sum;
}

uint32_t** ScatteredField::AssignBlocks(ManagedVector<uint32_t*>& col,
                                        size_t nblocks, size_t& next) {
  auto& pool = _s->blocks;
  col.resize(nblocks);
  for (auto& block : col) {
    if (next == pool.size()) {
      pool.push_back(
        _mem->allocator.Allocate(ScatterView::kBlockValues * sizeof(uint32_t)));
    }
    block = reinterpret_cast<uint32_t*>(pool[next++].get());
  }
  return col.data();
}

template<typename Log>
void ScatteredField::Scatter(const Log& log, uint64_t nocc) {
  constexpr auto kLayout = Log::kLayout;
  constexpr bool kPos = kLayout != TokenLayout::Terms;
  constexpr bool kOffs = kLayout == TokenLayout::TermsPosOffs;

  // The log's columns advance together only if no push ever failed midway
  // (an allocation throw between two column pushes leaves them desynced, and
  // Rollback keeps the writer when earlier transactions committed docs).
  // Cross-checks below ride state the scatter loop maintains anyway, so a
  // desynced log fails the flush instead of writing garbage postings.
  const uint64_t nids = log.TermIds().Size();
  if constexpr (kOffs) {
    if (log.OffsDelta().Size() != nids || log.OffsLen().Size() != nids)
      [[unlikely]] {
      ThrowDesyncedLog(_field->Meta().id, "offset columns disagree with ids");
    }
  }

  _s->docs.clear();
  _s->pos.clear();
  _s->offs_start.clear();
  _s->offs_end.clear();
  if (!nocc) {
    return;
  }

  // Carve one block-column per indexed lane from the shared pool.
  const size_t nblocks =
    (nocc + ScatterView::kBlockMask) >> ScatterView::kBlockShift;
  size_t next = 0;
  OutColumn docs{AssignBlocks(_s->docs, nblocks, next)};
  OutColumn positions;
  OutColumn offs_start;
  OutColumn offs_end;
  if constexpr (kPos) {
    positions.blocks = AssignBlocks(_s->pos, nblocks, next);
  }
  if constexpr (kOffs) {
    offs_start.blocks = AssignBlocks(_s->offs_start, nblocks, next);
    offs_end.blocks = AssignBlocks(_s->offs_end, nblocks, next);
  }

  // Log readers; absent lanes read as a zero stub so the token loop stays
  // branch-free (guarded by the same `if constexpr` that skips their output).
  auto* const cursors = _s->cursors.data();
  const auto field = _field->Meta().id;
  LogColumnReader term_ids{log.TermIds(), field};
  auto pos_col = [&] {
    if constexpr (kPos) {
      return LogColumnReader{log.Pos(), field};
    } else {
      return NoColumn{};
    }
  }();
  auto delta_col = [&] {
    if constexpr (kOffs) {
      return LogColumnReader{log.OffsDelta(), field};
    } else {
      return NoColumn{};
    }
  }();
  auto len_col = [&] {
    if constexpr (kOffs) {
      return LogColumnReader{log.OffsLen(), field};
    } else {
      return NoColumn{};
    }
  }();

  // Walk the log doc-major (runs + per-doc token counts) and scatter each
  // occurrence into its term's region. `pos_of` is chosen once per doc (its
  // source is doc-invariant): dense docs reconstruct the within-doc ordinal,
  // promoted docs read the pos column. Offset starts are a within-doc delta
  // stream summed per doc.
  LogColumnReader doc_tokens{log.DocTokens(), field};
  const uint64_t doc_slots = log.DocTokens().Size();
  size_t doc_idx = 0;
  uint64_t consumed = 0;
  [[maybe_unused]] uint64_t pos_consumed = 0;
  for (const auto& run : log.Runs()) {
    for (uint32_t k = 0; k < run.ndocs; ++k, ++doc_idx) {
      const doc_id_t doc = run.first_doc + k;
      const uint32_t ntokens = doc_tokens.Read();
      consumed += ntokens;
      [[maybe_unused]] uint32_t offs = 0;
      const auto emit = [&](auto&& pos_of) {
        for (uint32_t j = 0; j < ntokens; ++j) {
          const auto c = cursors[term_ids.Read()]++;
          docs.Set(c, doc);
          if constexpr (kPos) {
            positions.Set(c, pos_of(j));
          }
          if constexpr (kOffs) {
            offs += delta_col.Read();
            offs_start.Set(c, offs);
            offs_end.Set(c, offs + len_col.Read());
          }
        }
      };
      if constexpr (kPos) {
        if (log.DocExplicit(doc_idx)) {
          pos_consumed += ntokens;
          uint32_t pos = 0;
          emit([&](uint32_t) {
            pos += pos_col.Read();
            return pos;
          });
        } else {
          emit([](uint32_t j) { return j + 1; });
        }
      } else {
        emit([](uint32_t) { return uint32_t{0}; });
      }
    }
  }
  if (doc_idx != doc_slots) [[unlikely]] {
    ThrowDesyncedLog(_field->Meta().id, "runs disagree with doc slots");
  }
  if (consumed != nids) [[unlikely]] {
    ThrowDesyncedLog(_field->Meta().id, "ids outlive the doc slots");
  }
  if constexpr (kPos) {
    if (pos_consumed != log.Pos().Size()) [[unlikely]] {
      ThrowDesyncedLog(_field->Meta().id, "pos column disagrees with ids");
    }
  }
  for (size_t r = 0, n = _s->ranked.size(); r < n; ++r) {
    SDB_ASSERT(_s->cursors[_s->ranked[r].id] == _s->bounds[r + 1]);
  }
}

}  // namespace irs
