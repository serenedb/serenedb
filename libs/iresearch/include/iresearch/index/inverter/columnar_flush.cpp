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

#include <absl/algorithm/container.h>

#include <duckdb/common/bswap.hpp>

#include "iresearch/formats/index/burst_trie.hpp"

namespace irs {
namespace {

constexpr size_t kRadixThreshold = 2048;

uint64_t PrefixKey(const duckdb::string_t& term) noexcept {
  uint64_t key;
  std::memcpy(&key, term.GetData(), sizeof key);
  return duckdb::BSwapIfLE(key);
}

class LogColumnReader {
 public:
  explicit LogColumnReader(const LogColumn& col) noexcept : _cursor{col} {}

  IRS_FORCE_INLINE uint32_t Read() noexcept {
    if (_idx == _cur.size()) [[unlikely]] {
      _cur = _cursor.Next(std::numeric_limits<size_t>::max());
      _idx = 0;
      SDB_ASSERT(!_cur.empty());
    }
    return _cur[_idx++];
  }

 private:
  LogColumn::Cursor _cursor;
  std::span<const uint32_t> _cur;
  size_t _idx = 0;
};

struct NoColumn {
  IRS_FORCE_INLINE uint32_t Read() noexcept { return 0; }
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
  _layout = field.Layout();
  SDB_ASSERT(field.Log().Size() <= std::numeric_limits<uint32_t>::max());

  BuildHistogram(field.Log().TermIds(), field.Dictionary().Entries().size());
  const auto ninline = RankLiveTerms(field.Dictionary().Entries());
  const auto nocc = PrefixSums();
  SDB_ASSERT(nocc == field.Log().Size() + ninline);

  field.VisitLog([&](const auto& log) { Scatter(log, nocc); });
}

// _cursors[id] is a histogram here, then the id's region start after
// PrefixSums, then its region end once Scatter has run.
void ScatteredField::BuildHistogram(const LogColumn& term_ids, size_t vocab) {
  auto& cursors = _s->cursors;
  cursors.assign(vocab, 0);
  LogColumn::Cursor ids{term_ids};
  for (auto vals = ids.Next(std::numeric_limits<size_t>::max()); !vals.empty();
       vals = ids.Next(std::numeric_limits<size_t>::max())) {
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
uint64_t ScatteredField::RankLiveTerms(
  std::span<const TermDictionary::Entry> entries) {
  auto& ranked = _s->ranked;
  ranked.clear();
  ranked.reserve(entries.size());
  auto& cursors = _s->cursors;
  uint64_t ninline = 0;
  for (uint32_t i = 0; i < entries.size(); ++i) {
    // Inline first occurrences count into the term's region: the scatter
    // emits them ahead of the log's, so one contiguous region serves every
    // consumer and no read-side merge exists.
    uint32_t n = 0;
    while (n < TermDictionary::kInlineOccs && entries[i].inline_docs[n]) {
      ++n;
    }
    ninline += n;
    if ((cursors[i] += n) != 0) {
      ranked.push_back({PrefixKey(entries[i].term), i});
    }
  }
  const auto by_term = [&](const ScatterScratch::RankedTerm& lhs,
                           const ScatterScratch::RankedTerm& rhs) {
    const auto& lt = entries[lhs.id].term;
    const auto& rt = entries[rhs.id].term;
    return lt != rt ? lt < rt : lhs.id < rhs.id;
  };
  if (ranked.size() < kRadixThreshold) {
    // The precomputed keys settle most comparisons without touching the
    // entries; strict key inequality implies strict term inequality.
    std::sort(ranked.begin(), ranked.end(),
              [&](const ScatterScratch::RankedTerm& lhs,
                  const ScatterScratch::RankedTerm& rhs) {
                return lhs.key != rhs.key ? lhs.key < rhs.key
                                          : by_term(lhs, rhs);
              });
    return ninline;
  }
  RadixSortByKey();
  for (size_t lo = 0, n = ranked.size(); lo < n;) {
    size_t hi = lo + 1;
    while (hi < n && ranked[hi].key == ranked[lo].key) {
      ++hi;
    }
    if (hi - lo > 1) {
      std::sort(ranked.begin() + lo, ranked.begin() + hi, by_term);
    }
    lo = hi;
  }
  return ninline;
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
    bounds.push_back(sum);
    sum += n;
  }
  bounds.push_back(sum);
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
  _s->docs.clear();
  _s->pos.clear();
  _s->offs_start.clear();
  _s->offs_end.clear();
  if (!nocc) {
    return;
  }
  const size_t nblocks =
    (nocc + ScatterView::kBlockMask) >> ScatterView::kBlockShift;
  size_t next = 0;
  auto* const cursors = _s->cursors.data();
  auto* const out_docs = AssignBlocks(_s->docs, nblocks, next);
  uint32_t** out_pos = nullptr;
  uint32_t** out_os = nullptr;
  uint32_t** out_oe = nullptr;
  if constexpr (kLayout != TokenLayout::Terms) {
    out_pos = AssignBlocks(_s->pos, nblocks, next);
  }
  if constexpr (kLayout == TokenLayout::TermsPosOffs) {
    out_os = AssignBlocks(_s->offs_start, nblocks, next);
    out_oe = AssignBlocks(_s->offs_end, nblocks, next);
  }

  LogColumnReader term_ids{log.TermIds()};
  auto pos = [&] {
    if constexpr (kLayout != TokenLayout::Terms) {
      return LogColumnReader{log.Pos()};
    } else {
      return NoColumn{};
    }
  }();
  auto offs_start = [&] {
    if constexpr (kLayout == TokenLayout::TermsPosOffs) {
      return LogColumnReader{log.OffsStart()};
    } else {
      return NoColumn{};
    }
  }();
  auto offs_len = [&] {
    if constexpr (kLayout == TokenLayout::TermsPosOffs) {
      return LogColumnReader{log.OffsLen()};
    } else {
      return NoColumn{};
    }
  }();

  if constexpr (kLayout == TokenLayout::Terms) {
    const auto entries = _field->Dictionary().Entries();
    for (const auto& term : _s->ranked) {
      for (const auto doc : entries[term.id].inline_docs) {
        if (!doc) {
          break;
        }
        const auto c = cursors[term.id]++;
        out_docs[c >> ScatterView::kBlockShift][c & ScatterView::kBlockMask] =
          doc;
      }
    }
  }

  LogColumnReader doc_tokens{log.DocTokens()};
  size_t doc_idx = 0;
  for (const auto& run : log.Runs()) {
    for (uint32_t k = 0; k < run.ndocs; ++k) {
      const doc_id_t doc = run.first_doc + k;
      const uint32_t ntokens = doc_tokens.Read();
      ++doc_idx;
      if constexpr (kLayout == TokenLayout::Terms) {
        for (uint32_t j = 0; j < ntokens; ++j) {
          const auto c = cursors[term_ids.Read()]++;
          out_docs[c >> ScatterView::kBlockShift][c & ScatterView::kBlockMask] =
            doc;
        }
      } else {
        // The position source is doc-invariant: split the loop instead of
        // branching per occurrence (dense docs reconstruct the within-doc
        // ordinal, promoted docs read the pos column).
        const auto scatter = [&](auto&& pos_of) {
          for (uint32_t j = 0; j < ntokens; ++j) {
            const auto c = cursors[term_ids.Read()]++;
            const auto hi = c >> ScatterView::kBlockShift;
            const auto lo = c & ScatterView::kBlockMask;
            out_docs[hi][lo] = doc;
            out_pos[hi][lo] = pos_of(j);
            if constexpr (kLayout == TokenLayout::TermsPosOffs) {
              const auto os = offs_start.Read();
              out_os[hi][lo] = os;
              out_oe[hi][lo] = os + offs_len.Read();
            }
          }
        };
        if (log.DocExplicit(doc_idx - 1)) {
          scatter([&](uint32_t) { return pos.Read(); });
        } else {
          scatter([](uint32_t j) { return j + 1; });
        }
      }
    }
  }
  SDB_ASSERT(doc_idx == log.DocTokens().Size());
}

FieldsInverter::FieldsInverter(InverterMemory mem)
  : _mem{mem},
    _arena{mem.allocator},
    _fields{ManagedTypedAllocator<FieldInverter>{mem.rm}} {}

FieldsInverter::~FieldsInverter() = default;

void FieldsInverter::Flush(burst_trie::FieldWriter& fw, FlushState& state,
                           std::span<const BasicTermReader* const> extra) {
  if (!_scatter) {
    _scatter = std::make_unique<ScatterScratch>(_mem.rm);
  }
  IndexFeatures index_features{IndexFeatures::None};

  ManagedVector<const FieldInverter*> sorted_fields{
    ManagedTypedAllocator<const FieldInverter*>{_mem.rm}};
  sorted_fields.reserve(_fields.size());
  for (auto& entry : _fields) {
    sorted_fields.push_back(&entry);
    index_features |= static_cast<IndexFeatures>(entry.Meta().index_features);
  }
  for (const auto* reader : extra) {
    index_features |= reader->properties().index_features;
  }
  state.index_features = index_features;

  absl::c_sort(sorted_fields,
               [](const FieldInverter* lhs, const FieldInverter* rhs) noexcept {
                 return lhs->Meta().id < rhs->Meta().id;
               });

  ManagedVector<const BasicTermReader*> sorted_extra{
    ManagedTypedAllocator<const BasicTermReader*>{_mem.rm}};
  sorted_extra.assign(extra.begin(), extra.end());
  absl::c_sort(sorted_extra,
               [](const BasicTermReader* lhs, const BasicTermReader* rhs) {
                 return lhs->id() < rhs->id();
               });

  ScatteredField scattered{_mem, *_scatter};
  ColumnarTermReader terms;

  fw.prepare(state);
  size_t fi = 0;
  size_t ei = 0;
  const size_t fn = sorted_fields.size();
  const size_t en = sorted_extra.size();
  while (fi < fn || ei < en) {
    const bool take_field =
      ei >= en ||
      (fi < fn && sorted_fields[fi]->Meta().id < sorted_extra[ei]->id());
    if (take_field) {
      scattered.Reset(*sorted_fields[fi]);
      ++fi;
      if (scattered.TermCount() == 0) {
        continue;
      }
      terms.Reset(scattered);
      fw.write(terms);
    } else {
      fw.write(*sorted_extra[ei]);
      ++ei;
    }
  }
  fw.end();
  _scatter->Release();
}

}  // namespace irs
