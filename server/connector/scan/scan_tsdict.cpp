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

#include <array>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/count/term_counts.hpp>
#include <iresearch/search/detail/lazy_bitset.hpp>
#include <iresearch/search/detail/resolve.hpp>
#include <iresearch/search/docs/make.hpp>
#include <iresearch/search/queries/term_state.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {

struct TsDictLocalState : public ScanLocalState {
  struct FieldState {
    irs::field_id field_id = irs::field_limits::invalid();
    irs::field_id null_field_id = irs::field_limits::invalid();
    duckdb::idx_t term_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t term_raw_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t count_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t freq_slot = duckdb::DConstants::INVALID_INDEX;
    duckdb::idx_t score_slot = duckdb::DConstants::INVALID_INDEX;
    TsDictTermUses term_uses = TsDictTermUses::None;
    const irs::Filter* having_filter = nullptr;
  };

  enum class CountMode {
    Meta,
    Masked,
    Where,
  };

  std::vector<FieldState> fields;
  CountMode count_mode = CountMode::Meta;
  const irs::QueryBuilder* where_query = nullptr;

  void StartSegment(const irs::SubReader& seg, uint32_t seg_idx,
                    ScanGlobalState& g);
  duckdb::idx_t EmitChunk(ScanGlobalState& g, duckdb::DataChunk& output,
                          duckdb::idx_t output_start);
  uint32_t LiveDocs(irs::TermIterator& it, bool count_all);

 private:
  bool NextField();
  irs::TermIterator::ptr MakeDictSource(const FieldState& field,
                                        const irs::TermReader& reader);
  irs::TermIterator::ptr MakeTermSource(const FieldState& field,
                                        const irs::TermReader& reader);
  duckdb::idx_t EmitField(duckdb::DataChunk& output, duckdb::idx_t output_start,
                          duckdb::idx_t capacity);
  duckdb::idx_t AppendNullRow(duckdb::DataChunk& output,
                              const FieldState& field, duckdb::idx_t row);
  irs::detail::LazyBitset& Live();
  uint32_t WalkLive(const irs::TermReader& reader, irs::TermIterator& it,
                    bool count_all);
  uint32_t NullDocs(const irs::TermReader& reader, bool count_all);
  void BindTermCounts(const irs::TermReader& reader);

  const irs::SubReader* _seg = nullptr;
  const irs::TermReader* _reader = nullptr;
  ColFilterVerify _col_verify;
  std::unique_ptr<irs::detail::LazyBitset> _live;
  irs::count::TermCounts::ptr _term_counts;
  irs::QueryBuilder::ptr _all_query;
  bool _null_pending = false;
  const FieldState* _field = nullptr;
  const FieldState* _next_field = nullptr;
  CountMode _cursor_mode = CountMode::Meta;
  irs::TermIterator::ptr _cursor;

 public:
  void StartUnit(ScanGlobalState& g);
  void CountUnit(ScanGlobalState& g);
  void BeginEmit(ScanGlobalState& g);
  uint32_t TermCount(irs::TermIterator& it, bool count_all);

  bool counting = false;
  bool emitting = false;

 private:
  uint32_t FieldIndex() const noexcept;
  ScanGlobalState::TsDictCounts* CountsFor(ScanGlobalState& g) const;

  irs::DocRange _range;
  ScanGlobalState* _g = nullptr;
  const ScanGlobalState::TsDictCounts* _counts = nullptr;
  const FieldState* _emit_fields = nullptr;
  uint32_t _seg_idx = 0;
  uint32_t _term_ordinal = 0;
  bool _from_counts = false;
};

namespace {

constexpr uint32_t kPlanBatch = STANDARD_VECTOR_SIZE;

class MinMaxTermsIterator : public irs::TermIterator {
 public:
  MinMaxTermsIterator(const std::array<irs::bytes_view, 2>& terms,
                      size_t count) noexcept
    : _terms{terms}, _count{count} {}

  bool next() final {
    if (_next == _count) {
      return false;
    }
    ++_next;
    return true;
  }

  irs::bytes_view value() const noexcept final { return _terms[_next - 1]; }

  const irs::PostingMeta& cookie() const noexcept final { SDB_UNREACHABLE(); }

  irs::TermPostings::ptr postings(irs::IndexFeatures) const final {
    return irs::TermPostings::empty();
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id) noexcept final {
    return nullptr;
  }

 private:
  std::array<irs::bytes_view, 2> _terms;
  size_t _count;
  size_t _next = 0;
};

void BuildTsDictSlots(TsDictLocalState& lstate,
                      duckdb::TableFunctionInitInput& input,
                      const ScanBindData& bd) {
  using duckdb::DConstants;
  using Req = TsDictRequest;
  using Field = TsDictLocalState::FieldState;

  struct SlotKind {
    catalog::ColumnId cat;
    duckdb::idx_t Req::* req;
    duckdb::idx_t Field::* slot;
    size_t next = 0;
  };
  std::array<SlotKind, 5> kinds{{
    {catalog::kInvertedIndexTermId, &Req::term_col_idx, &Field::term_slot},
    {catalog::kInvertedIndexTermRawId, &Req::term_raw_col_idx,
     &Field::term_raw_slot},
    {catalog::kInvertedIndexTermCountId, &Req::count_col_idx,
     &Field::count_slot},
    {catalog::kInvertedIndexTermFreqId, &Req::freq_col_idx, &Field::freq_slot},
    {catalog::kInvertedIndexTermScoreId, &Req::score_col_idx,
     &Field::score_slot},
  }};

  duckdb::idx_t out_slot = 0;
  for (auto col_id : input.column_ids) {
    if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID ||
        col_id >= duckdb::VIRTUAL_COLUMN_START) {
      ++out_slot;
      continue;
    }
    if (col_id >= bd.columns.ids.size()) {
      continue;
    }
    const auto cat = bd.columns.ids[col_id];
    for (auto& kind : kinds) {
      if (cat != kind.cat) {
        continue;
      }
      while (bd.ts_dict.requests[kind.next].*kind.req ==
             DConstants::INVALID_INDEX) {
        ++kind.next;
      }
      lstate.fields[kind.next++].*kind.slot = out_slot;
      break;
    }
    ++out_slot;
  }
}

struct TsDictEmitContext {
  duckdb::string_t* term_data;
  duckdb::string_t* raw_data;
  int32_t* count_data;
  int64_t* freq_data;
  float* score_data;
  duckdb::Vector* term_vec;
  duckdb::Vector* raw_vec;
  bool needs_meta;
  const irs::TermBoost* boost;
  TsDictLocalState* state;
  TsDictLocalState::CountMode count_mode;
  duckdb::idx_t row;
  duckdb::idx_t end_row;
};

struct TsDictEmitter {
  explicit TsDictEmitter(TsDictEmitContext ctx) noexcept : ctx{ctx} {}

  void Emit(irs::bytes_view term, uint32_t docs) {
    const auto row = ctx.row;
    const auto* p = reinterpret_cast<const char*>(term.data());
    if (ctx.term_data) {
      ctx.term_data[row] =
        duckdb::StringVector::AddString(*ctx.term_vec, p, term.size());
    }
    if (ctx.raw_data) {
      ctx.raw_data[row] =
        duckdb::StringVector::AddStringOrBlob(*ctx.raw_vec, p, term.size());
    }
    if (ctx.count_data) {
      ctx.count_data[row] = static_cast<int32_t>(docs);
    }
    if (ctx.freq_data) {
      ctx.freq_data[row] = static_cast<int64_t>(meta->freq);
    }
    if (ctx.score_data) {
      ctx.score_data[row] = ctx.boost ? ctx.boost->value : irs::kNoBoost;
    }
    ++ctx.row;
  }

  uint32_t LiveDocs(irs::TermIterator& it) const {
    if (ctx.count_mode == TsDictLocalState::CountMode::Meta) {
      return meta ? meta->docs_count : 1;
    }
    return ctx.state->TermCount(it, ctx.count_data != nullptr);
  }

  void OnTerm(irs::TermIterator& it) {
    meta = ctx.needs_meta ? &it.cookie() : nullptr;
    const auto live_docs = LiveDocs(it);
    if (live_docs != 0) {
      Emit(it.value(), live_docs);
    }
  }

  TsDictEmitContext ctx;
  const irs::PostingMeta* meta = nullptr;
};

}  // namespace

void TsDictLocalState::StartSegment(const irs::SubReader& seg, uint32_t seg_idx,
                                    ScanGlobalState& g) {
  _seg = &seg;
  _term_counts.reset();
  _live.reset();
  _all_query = {};
  Classify(g, seg_idx);
  if (seg_cls.segment_dead) {
    _next_field = nullptr;
    return;
  }
  _col_verify.Begin(seg, seg_cls.active, *g.client_context, filter_states);
  count_mode = seg.live_docs_count() != seg.docs_count() ? CountMode::Masked
                                                         : CountMode::Meta;
  where_query = nullptr;
  _next_field = fields.empty() ? nullptr : fields.data();
  if (g.filter) {
    where_query = &EnsureSegmentQuery(g, *this, seg_idx);
    if (irs::QueryBuilder::IsEmpty(*where_query)) {
      _next_field = nullptr;
      return;
    }
    count_mode = CountMode::Where;
  }
  if (!_col_verify.Empty() && count_mode == CountMode::Meta) {
    count_mode = CountMode::Masked;
  }
  if (count_mode != CountMode::Meta && where_query == nullptr) {
    _all_query = MatchAllFilter().PrepareSegment(seg, {});
  }
}

void TsDictLocalState::BindTermCounts(const irs::TermReader& reader) {
  _term_counts = {};
  if (count_mode == CountMode::Meta || !_col_verify.Empty()) {
    return;
  }
  if (irs::detail::DocOf(reader) == nullptr) {
    return;
  }
  _term_counts = irs::count::MakeTermCounts(Live(), reader, reader.size());
}

irs::detail::LazyBitset& TsDictLocalState::Live() {
  if (!_live) {
    const auto& query = where_query != nullptr ? *where_query : *_all_query;
    SDB_ASSERT(!irs::QueryBuilder::IsEmpty(query));
    auto node = query.PlanFill({}, irs::ScoreMergeType::Noop);
    EnsurePlanned(node != nullptr);
    const auto* removals = _seg->docs_mask();
    if (auto* folded = node->Folded(); folded != nullptr) {
      _live =
        std::make_unique<irs::detail::LazyBitset>(std::move(*folded), removals);
    } else {
      _live = std::make_unique<irs::detail::LazyBitset>(
        std::move(node), static_cast<irs::doc_id_t>(_seg->docs_count()),
        removals);
    }
  }
  return *_live;
}

uint32_t TsDictLocalState::WalkLive(const irs::TermReader& reader,
                                    irs::TermIterator& it, bool count_all) {
  auto postings = irs::docs::MakePosting(
    irs::detail::PostingClause{irs::TermState{&reader, it.cookie()}}, *_seg,
    {});
  SDB_ASSERT(postings);
  _col_verify.Rewind();
  auto& live = Live();
  uint32_t total = 0;
  irs::SlackBuf<irs::doc_id_t, kPlanBatch, irs::doc_limits::kDocsSlack> docs;
  const auto stop = std::min<irs::doc_id_t>(
    _range.end,
    irs::doc_limits::min() + static_cast<irs::doc_id_t>(_seg->docs_count()));
  for (auto at = _range.begin; at < stop;) {
    const auto upto = static_cast<irs::doc_id_t>(
      std::min<uint64_t>(uint64_t{at} + kPlanBatch, stop));
    const auto read = postings->Run(at, upto, docs.data());
    at = upto;
    if (read == 0) {
      continue;
    }
    uint32_t n = 0;
    for (uint32_t i = 0; i != read; ++i) {
      if (live.Contains(docs[i])) {
        docs[n++] = docs[i];
      }
    }
    if (n == 0) {
      continue;
    }
    total += static_cast<uint32_t>(_col_verify.Narrow(docs.data(), nullptr, n));
    if (!count_all && total != 0) {
      return 1;
    }
  }
  return total;
}

uint32_t TsDictLocalState::NullDocs(const irs::TermReader& reader,
                                    bool count_all) {
  auto it = reader.iterator();
  if (!it || !it->next()) {
    return 0;
  }
  if (_col_verify.Empty() && irs::detail::DocOf(reader) != nullptr) {
    if (auto counts =
          irs::count::MakeTermCounts(Live(), reader, reader.size())) {
      const auto& term = it->cookie();
      return count_all ? static_cast<uint32_t>(
                           counts->Count(term, _range.begin, _range.end))
                       : static_cast<uint32_t>(
                           counts->Any(term, _range.begin, _range.end));
    }
  }
  return WalkLive(reader, *it, count_all);
}

uint32_t TsDictLocalState::FieldIndex() const noexcept {
  return static_cast<uint32_t>(_field - fields.data());
}

ScanGlobalState::TsDictCounts* TsDictLocalState::CountsFor(
  ScanGlobalState& g) const {
  if (g.ts_dict_counts.empty()) {
    return nullptr;
  }
  auto& per_field = g.ts_dict_counts[_seg_idx];
  const auto field = FieldIndex();
  return field < per_field.size() ? &per_field[field] : nullptr;
}

void TsDictLocalState::StartUnit(ScanGlobalState& g) {
  _g = &g;
  _range = g.RangeOf(unit);
  _seg_idx = unit.seg;
  emitting = false;
  StartSegment((*g.reader)[unit.seg], unit.seg, g);
  _emit_fields = _next_field;
  counting = !unit.whole && count_mode != CountMode::Meta &&
             _emit_fields != nullptr && _seg_idx < g.ts_dict_counts.size() &&
             !g.ts_dict_counts[_seg_idx].empty();
  if (!unit.whole && !counting) {
    _next_field = nullptr;
  }
}

void TsDictLocalState::CountUnit(ScanGlobalState& g) {
  while (NextField()) {
    auto* slot = CountsFor(g);
    if (slot == nullptr || !_cursor) {
      continue;
    }
    const bool count_all =
      _field->count_slot != duckdb::DConstants::INVALID_INDEX;
    uint32_t ordinal = 0;
    while (_cursor->next()) {
      const auto live = LiveDocs(*_cursor, count_all);
      if (live != 0 && ordinal < slot->terms) {
        slot->Term(ordinal).fetch_add(live, std::memory_order_relaxed);
      }
      ++ordinal;
    }
    _cursor.reset();
    if (_null_pending) {
      const auto* nulls = _seg->field(_field->null_field_id);
      if (nulls != nullptr) {
        slot->Nulls().fetch_add(NullDocs(*nulls, count_all),
                                std::memory_order_relaxed);
      }
      _null_pending = false;
    }
  }
}

void TsDictLocalState::BeginEmit(ScanGlobalState& g) {
  _g = &g;
  _from_counts = counting;
  emitting = true;
  counting = false;
  _range = {};
  _live.reset();
  _term_counts.reset();
  _cursor.reset();
  _field = nullptr;
  _next_field = _emit_fields;
  _term_ordinal = 0;
  _counts = nullptr;
}

uint32_t TsDictLocalState::TermCount(irs::TermIterator& it, bool count_all) {
  if (!emitting || !_from_counts) {
    return LiveDocs(it, count_all);
  }
  const auto ordinal = _term_ordinal++;
  if (_counts == nullptr || ordinal >= _counts->terms) {
    return 0;
  }
  return static_cast<uint32_t>(
    _counts->Term(ordinal).load(std::memory_order_relaxed));
}

uint32_t TsDictLocalState::LiveDocs(irs::TermIterator& it, bool count_all) {
  if (_term_counts) {
    const auto& term = it.cookie();
    return count_all ? static_cast<uint32_t>(
                         _term_counts->Count(term, _range.begin, _range.end))
                     : static_cast<uint32_t>(
                         _term_counts->Any(term, _range.begin, _range.end));
  }
  SDB_ASSERT(_reader != nullptr);
  return WalkLive(*_reader, it, count_all);
}

irs::TermIterator::ptr TsDictLocalState::MakeDictSource(
  const FieldState& field, const irs::TermReader& reader) {
  if (field.having_filter == nullptr) {
    return reader.iterator();
  }
  auto cursor = field.having_filter->CompileTermIterator(reader);
  SDB_ENSURE(cursor,
             "ts_dict: claimed having filter failed to compile a term "
             "iterator");
  return cursor;
}

irs::TermIterator::ptr TsDictLocalState::MakeTermSource(
  const FieldState& field, const irs::TermReader& reader) {
  if (field.having_filter) {
    return MakeDictSource(field, reader);
  }
  const bool max_only = field.term_uses == TsDictTermUses::Max;
  const bool min_max =
    field.term_uses == (TsDictTermUses::Min | TsDictTermUses::Max);
  if ((max_only || min_max) && count_mode != CountMode::Masked) {
    std::array<irs::bytes_view, 2> terms;
    size_t count = 0;
    const auto max = reader.max();
    if (count_mode == CountMode::Meta) {
      if (min_max) {
        terms[count++] = reader.min();
      }
      terms[count++] = max;
    } else {
      auto it = reader.iterator();
      const auto pin = [&](irs::bytes_view term) {
        if (!it->seek(term) || LiveDocs(*it, false) == 0) {
          return false;
        }
        terms[count++] = term;
        return true;
      };
      if (!(min_max ? pin(reader.min()) && pin(max) : pin(max))) {
        count = 0;
      }
    }
    if (count != 0) {
      _cursor_mode = CountMode::Meta;
      return irs::memory::make_managed<MinMaxTermsIterator>(terms, count);
    }
  }
  return MakeDictSource(field, reader);
}

bool TsDictLocalState::NextField() {
  while (_next_field) {
    const auto& field = *_next_field++;
    if (_next_field == fields.data() + fields.size()) {
      _next_field = nullptr;
    }
    _field = &field;
    _null_pending = irs::field_limits::valid(field.null_field_id);
    _cursor_mode = count_mode;
    _cursor.reset();
    _term_ordinal = 0;
    _counts = nullptr;
    _reader = nullptr;
    if (const auto* reader = _seg->field(field.field_id);
        reader && reader->size() != 0) {
      _reader = reader;
      if (emitting || counting) {
        _counts = _g != nullptr ? CountsFor(*_g) : nullptr;
        if (!emitting) {
          BindTermCounts(*reader);
        }
        _cursor = MakeDictSource(field, *reader);
      } else {
        BindTermCounts(*reader);
        _cursor = MakeTermSource(field, *reader);
      }
    }
    if (_cursor || _null_pending) {
      return true;
    }
  }
  return false;
}

duckdb::idx_t TsDictLocalState::EmitField(duckdb::DataChunk& output,
                                          duckdb::idx_t output_start,
                                          duckdb::idx_t capacity) {
  using duckdb::DConstants;

  if (!_cursor && !_null_pending) {
    return 0;
  }
  const auto& field = *_field;
  const auto vec = [&](duckdb::idx_t slot) -> duckdb::Vector* {
    return slot == DConstants::INVALID_INDEX ? nullptr : &output.data[slot];
  };
  const auto data = [&]<typename T>(duckdb::idx_t slot) -> T* {
    auto* v = vec(slot);
    return v ? duckdb::FlatVector::GetDataMutable<T>(*v) : nullptr;
  };

  auto* term_vec = vec(field.term_slot);
  auto* raw_vec = vec(field.term_raw_slot);
  auto* term_data = data.operator()<duckdb::string_t>(field.term_slot);
  auto* raw_data = data.operator()<duckdb::string_t>(field.term_raw_slot);
  auto* count_data = data.operator()<int32_t>(field.count_slot);
  auto* freq_data = data.operator()<int64_t>(field.freq_slot);
  auto* score_data = data.operator()<float>(field.score_slot);

  const bool min_only = field.term_uses == TsDictTermUses::Min;
  const auto field_capacity = min_only ? duckdb::idx_t{1} : capacity;

  duckdb::idx_t n = 0;
  if (_cursor && field_capacity != 0) {
    TsDictEmitter emitter{TsDictEmitContext{
      .term_data = term_data,
      .raw_data = raw_data,
      .count_data = count_data,
      .freq_data = freq_data,
      .score_data = score_data,
      .term_vec = term_vec,
      .raw_vec = raw_vec,
      .needs_meta = count_data != nullptr || freq_data != nullptr,
      .boost = score_data ? irs::get<irs::TermBoost>(*_cursor) : nullptr,
      .state = this,
      .count_mode = _cursor_mode,
      .row = output_start,
      .end_row = output_start + field_capacity}};
    while (emitter.ctx.row < emitter.ctx.end_row) {
      if (!_cursor->next()) {
        _cursor.reset();
        break;
      }
      emitter.OnTerm(*_cursor);
    }
    n = emitter.ctx.row - output_start;
  }

  if (min_only && n != 0) {
    _cursor.reset();
  }
  if (_null_pending && n < field_capacity && !_cursor) {
    _null_pending = false;
    n += AppendNullRow(output, field, output_start + n);
  }
  if (n == 0) {
    return 0;
  }

  for (const auto& other : fields) {
    if (&other == _field) {
      continue;
    }
    for (const auto slot :
         {other.term_slot, other.term_raw_slot, other.count_slot,
          other.freq_slot, other.score_slot}) {
      if (slot == DConstants::INVALID_INDEX) {
        continue;
      }
      auto& validity = duckdb::FlatVector::ValidityMutable(output.data[slot]);
      for (duckdb::idx_t i = 0; i < n; ++i) {
        validity.SetInvalid(output_start + i);
      }
    }
  }
  return n;
}

duckdb::idx_t TsDictLocalState::AppendNullRow(duckdb::DataChunk& output,
                                              const FieldState& field,
                                              duckdb::idx_t row) {
  const auto* reader = _seg->field(field.null_field_id);
  if (reader == nullptr) {
    return 0;
  }
  uint32_t nulls = 0;
  if (emitting && _from_counts) {
    nulls = _counts == nullptr ? 0
                               : static_cast<uint32_t>(_counts->Nulls().load(
                                   std::memory_order_relaxed));
  } else if (count_mode == CountMode::Meta) {
    nulls = static_cast<uint32_t>(reader->docs_count());
  } else {
    nulls =
      NullDocs(*reader, field.count_slot != duckdb::DConstants::INVALID_INDEX);
  }
  if (nulls == 0) {
    return 0;
  }
  const auto set_null = [&](duckdb::idx_t slot) {
    if (slot != duckdb::DConstants::INVALID_INDEX) {
      duckdb::FlatVector::SetNull(output.data[slot], row, true);
    }
  };
  set_null(field.term_slot);
  set_null(field.term_raw_slot);
  set_null(field.freq_slot);
  set_null(field.score_slot);
  if (field.count_slot != duckdb::DConstants::INVALID_INDEX) {
    duckdb::FlatVector::GetDataMutable<int32_t>(
      output.data[field.count_slot])[row] = static_cast<int32_t>(nulls);
  }
  return 1;
}

duckdb::idx_t TsDictLocalState::EmitChunk(ScanGlobalState& g,
                                          duckdb::DataChunk& output,
                                          duckdb::idx_t output_start) {
  const auto capacity = STANDARD_VECTOR_SIZE - output_start;
  do {
    if (const auto n = EmitField(output, output_start, capacity); n != 0) {
      g.produced_rows.fetch_add(n, std::memory_order_relaxed);
      return n;
    }
  } while (NextField());
  return 0;
}

void BuildTsDictCounts(ScanGlobalState& g) {
  const auto& reqs = g.Bind().ts_dict.requests;
  const auto& reader = *g.reader;
  g.ts_dict_counts.resize(reader.size());
  for (const auto seg : g.segment_order) {
    if (g.Segment(seg).claim.load(std::memory_order_relaxed) !=
        SegmentWork::kSplit) {
      continue;
    }
    auto& per_field = g.ts_dict_counts[seg];
    per_field.resize(reqs.size());
    for (size_t i = 0; i != reqs.size(); ++i) {
      const auto* terms = reader[seg].field(reqs[i].field_id);
      per_field[i].Reset(
        terms == nullptr ? 0 : static_cast<uint32_t>(terms->size()));
    }
  }
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> MakeTsDictLocal(
  ScanGlobalState& g, duckdb::TableFunctionInitInput& input) {
  auto lstate = duckdb::make_uniq<TsDictLocalState>();
  const auto& ss = g.Bind();
  lstate->fields.resize(ss.ts_dict.requests.size());
  for (size_t i = 0; i < ss.ts_dict.requests.size(); ++i) {
    lstate->fields[i].field_id = ss.ts_dict.requests[i].field_id;
    lstate->fields[i].null_field_id = ss.ts_dict.requests[i].null_field_id;
    lstate->fields[i].term_uses = ss.ts_dict.requests[i].term_uses;
    lstate->fields[i].having_filter =
      ss.ts_dict.requests[i].having_filter.get();
  }
  BuildTsDictSlots(*lstate, input, ss);
  return lstate;
}

void RunTsDictScan(duckdb::ClientContext&, ScanGlobalState& g,
                   duckdb::LocalTableFunctionState& lstate,
                   duckdb::DataChunk& output) {
  auto& l = lstate.Cast<TsDictLocalState>();
  for (;;) {
    duckdb::idx_t collected = 0;
    bool exhausted = false;
    while (collected < STANDARD_VECTOR_SIZE) {
      const auto added = l.EmitChunk(g, output, collected);
      SDB_ASSERT(collected + added <= STANDARD_VECTOR_SIZE);
      collected += added;
      if (added != 0) {
        continue;
      }
      if (l.has_unit) {
        const bool split = !l.unit.whole;
        const bool segment_done = FinishUnit(g, l);
        if (segment_done) {
          FinishSegments(g, 1);
        }
        if (segment_done && split) {
          l.BeginEmit(g);
          continue;
        }
      }
      if (!ClaimUnit(g, l)) {
        exhausted = true;
        break;
      }
      l.StartUnit(g);
      if (l.counting) {
        l.CountUnit(g);
      }
    }
    if (collected != 0 || exhausted) {
      output.SetChildCardinality(collected);
      return;
    }
    output.Reset();
  }
}

}  // namespace sdb::connector
