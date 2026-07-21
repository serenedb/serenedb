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

#include "connector/functions/ts_highlight.h"

#include <absl/algorithm/container.h>
#include <unicode/brkiter.h>
#include <unicode/locid.h>
#include <unicode/ubrk.h>
#include <unicode/utext.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/function_binder.hpp>
#include <duckdb/function/function_set.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <span>

#include "basics/assert.h"
#include "connector/common.h"
#include "connector/functions/search.h"
#include "connector/functions/ts_common.hpp"
#include "connector/highlight/highlight_options.h"
#include "connector/highlight/highlight_types.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

struct UTextDeleter {
  void operator()(UText* p) const noexcept { utext_close(p); }
};
using UTextPtr = std::unique_ptr<UText, UTextDeleter>;

std::unique_ptr<icu::BreakIterator> CreateSentenceIterator() {
  UErrorCode err = U_ZERO_ERROR;
  std::unique_ptr<icu::BreakIterator> bi{
    icu::BreakIterator::createSentenceInstance(icu::Locale::getRoot(), err)};
  if (U_FAILURE(err) || !bi) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("ts_highlight: failed to create sentence iterator (icu err ", err,
              ")"));
  }
  return bi;
}

std::unique_ptr<icu::BreakIterator> CreateWordIterator() {
  UErrorCode err = U_ZERO_ERROR;
  std::unique_ptr<icu::BreakIterator> bi{
    icu::BreakIterator::createWordInstance(icu::Locale::getRoot(), err)};
  if (U_FAILURE(err) || !bi) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("ts_highlight: failed to create word iterator (icu err ", err,
              ")"));
  }
  return bi;
}

void BindIterator(icu::BreakIterator& bi, UTextPtr& utext,
                  std::string_view doc) {
  UErrorCode err = U_ZERO_ERROR;
  auto* p = utext_openUTF8(utext.get(), doc.data(),
                           static_cast<int64_t>(doc.size()), &err);
  if (U_FAILURE(err) || !p) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("ts_highlight: failed to open UTF-8 text (icu err ", err, ")"));
  }
  if (!utext) {
    utext.reset(p);
  }
  bi.setText(p, err);
  if (U_FAILURE(err)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("ts_highlight: BreakIterator setText failed (icu err ", err,
              ")"));
  }
}

struct TsHighlightBindData final : public duckdb::FunctionData {
  highlight::HighlightOptions options;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<TsHighlightBindData>(*this);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    return options == other.Cast<TsHighlightBindData>().options;
  }
};

struct HighlightState {
  std::unique_ptr<icu::BreakIterator> sentence_iter;
  std::unique_ptr<icu::BreakIterator> word_iter;
  UTextPtr utext;
  std::vector<highlight::DocToken> tokens;
  std::vector<highlight::Passage> passages;
};

struct TsHighlightLocalState final : public duckdb::FunctionLocalState {
  HighlightState state;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitTsHighlightLocalState(
  duckdb::ExpressionState& /*state*/,
  const duckdb::BoundFunctionExpression& /*expr*/,
  duckdb::FunctionData* /*bind_data*/) {
  auto local = duckdb::make_uniq<TsHighlightLocalState>();
  local->state.sentence_iter = CreateSentenceIterator();
  local->state.word_iter = CreateWordIterator();
  return local;
}

const TsHighlightBindData& GetBindData(duckdb::ExpressionState& state) {
  return state.expr.Cast<duckdb::BoundFunctionExpression>()
    .BindInfo()
    ->Cast<TsHighlightBindData>();
}

// Sliding-window pick of the max_words tokens with the most hits.
std::span<const highlight::DocToken> ClipToMaxWords(
  std::span<const highlight::DocToken> tokens, size_t max_words) noexcept {
  if (tokens.size() <= max_words) {
    return tokens;
  }
  size_t best_lo = 0;
  size_t cur_count = 0;
  for (size_t i = 0; i < max_words; ++i) {
    if (tokens[i].is_hit) {
      ++cur_count;
    }
  }
  size_t best_count = cur_count;
  for (size_t i = max_words; i < tokens.size(); ++i) {
    if (tokens[i].is_hit) {
      ++cur_count;
    }
    if (tokens[i - max_words].is_hit) {
      --cur_count;
    }
    if (cur_count > best_count) {
      best_count = cur_count;
      best_lo = i - max_words + 1;
    }
  }
  return tokens.subspan(best_lo, max_words);
}

class HitsView {
 public:
  HitsView(const duckdb::list_entry_t& entry,
           const duckdb::UnifiedVectorFormat& fmt, const int32_t* data)
    : _fmt{&fmt}, _data{data}, _offset{entry.offset}, _count{entry.length / 2} {
    if (entry.length % 2 != 0) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_highlight: offsets array must contain an even number of "
                "elements"));
    }
  }

  size_t size() const noexcept { return _count; }
  bool empty() const noexcept { return _count == 0; }

  highlight::HitRange operator[](size_t i) const noexcept {
    const auto [start, end] = GetIndexes(i);
    return {static_cast<uint32_t>(_data[start]),
            static_cast<uint32_t>(_data[end])};
  }

  highlight::HitRange at(size_t i) const {
    const auto [start, end] = GetIndexes(i);
    if (!_fmt->validity.RowIsValid(start) || !_fmt->validity.RowIsValid(end)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_highlight: NULL element in offsets array"));
    }
    return {static_cast<uint32_t>(_data[start]),
            static_cast<uint32_t>(_data[end])};
  }

 private:
  std::pair<duckdb::idx_t, duckdb::idx_t> GetIndexes(size_t i) const noexcept {
    auto* sel = _fmt->sel;
    const auto base = _offset + 2 * i;
    return std::pair{sel->get_index(base), sel->get_index(base + 1)};
  }

  const duckdb::UnifiedVectorFormat* _fmt;
  const int32_t* _data;
  duckdb::idx_t _offset;
  size_t _count;
};

void ValidateHits(std::string_view doc, const HitsView& view) {
  const auto doc_size =
    std::min<uint32_t>(doc.size(), std::numeric_limits<uint32_t>::max());
  uint32_t prev_start = 0;
  bool has_prev = false;
  for (size_t i = 0; i < view.size(); ++i) {
    const auto h = view.at(i);
    if (h.first > h.second) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_highlight: malformed offset pair (start=", h.first,
                ", end=", h.second, "); expected non-negative start <= end"));
    }
    if (h.first > doc_size) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("ts_highlight: offset start (", h.first,
                              ") past document size (", doc_size, ")"));
    }
    if (has_prev && h.first < prev_start) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_highlight: offsets array must be sorted ascending by "
                "start; saw start=",
                h.first, " after start=", prev_start));
    }
    prev_start = h.first;
    has_prev = true;
  }
}

void Append(char* dst, size_t& pos, std::string_view src) {
  memcpy(dst + pos, src.data(), src.size());
  pos += src.size();
}

// First max_words words of doc (or the whole doc if it has no words).
std::string_view DiscoverPrefix(std::string_view doc, icu::BreakIterator& bi,
                                UTextPtr& utext, size_t max_words) {
  if (max_words == 0) {
    return doc;
  }
  BindIterator(bi, utext, doc);

  // Skip leading non-word boundaries; `prev` ends at first word's start.
  int32_t prev = bi.first();
  int32_t cur = bi.next();
  while (cur != icu::BreakIterator::DONE &&
         bi.getRuleStatus() < UBRK_WORD_NONE_LIMIT) {
    prev = cur;
    cur = bi.next();
  }
  if (cur == icu::BreakIterator::DONE) {
    return doc;
  }

  const auto first_start = static_cast<size_t>(prev);
  size_t last_end = static_cast<size_t>(cur);
  for (size_t remaining = max_words - 1;
       remaining > 0 && (cur = bi.next()) != icu::BreakIterator::DONE;) {
    if (bi.getRuleStatus() >= UBRK_WORD_NONE_LIMIT) {
      last_end = static_cast<size_t>(cur);
      --remaining;
    }
  }

  return doc.substr(first_start, last_end - first_start);
}

void TokenizeSentence(std::vector<highlight::DocToken>& out,
                      icu::BreakIterator& word_iter, UTextPtr& utext,
                      std::string_view doc, highlight::SentenceRange sentence,
                      HitsView hits, size_t hit_lo, size_t hit_hi) {
  auto slice =
    doc.substr(sentence.byte_start, sentence.byte_end - sentence.byte_start);
  out.clear();
  BindIterator(word_iter, utext, slice);
  size_t hit_cursor = hit_lo;
  highlight::HitRange hit;
  if (hit_cursor < hit_hi) {
    hit = hits[hit_cursor];
  }
  int32_t prev = word_iter.first();
  for (int32_t cur = word_iter.next(); cur != icu::BreakIterator::DONE;
       cur = word_iter.next()) {
    if (word_iter.getRuleStatus() >= UBRK_WORD_NONE_LIMIT) {
      const auto start = static_cast<uint32_t>(prev) + sentence.byte_start;
      const auto end = static_cast<uint32_t>(cur) + sentence.byte_start;
      while (hit_cursor < hit_hi && hit.second <= start) {
        if (++hit_cursor < hit_hi) {
          hit = hits[hit_cursor];
        }
      }
      const bool is_hit = hit_cursor < hit_hi && hit.first < end;
      out.emplace_back(start, end, is_hit);
    }
    prev = cur;
  }
}

void WriteRenderTokens(char* dst, size_t& pos, std::string_view doc,
                       std::span<const highlight::DocToken> tokens,
                       const highlight::HighlightOptions& opts) {
  if (tokens.empty()) {
    return;
  }
  uint32_t cursor = tokens.front().byte_start;
  bool in_run = false;
  uint32_t run_end = 0;
  auto close_run = [&] {
    if (in_run) {
      Append(dst, pos, doc.substr(cursor, run_end - cursor));
      Append(dst, pos, opts.stop_sel);
      cursor = run_end;
      in_run = false;
    }
  };
  for (const auto& token : tokens) {
    if (token.is_hit) {
      if (!in_run) {
        Append(dst, pos, doc.substr(cursor, token.byte_start - cursor));
        Append(dst, pos, opts.start_sel);
        cursor = token.byte_start;
        in_run = true;
      }
      run_end = token.byte_end;
    } else {
      close_run();
    }
  }
  close_run();
  const auto last_end = tokens.back().byte_end;
  if (cursor < last_end) {
    Append(dst, pos, doc.substr(cursor, last_end - cursor));
  }
}

// Scoring biases earlier-in-doc passages and earlier-in-passage hits.
double NormForPassage(uint32_t passage_start) noexcept {
  return 1.0 + 1.0 / std::sqrt(1.0 + 0.05 * passage_start);
}
double SloppyWeight(uint32_t hit_offset_in_passage) noexcept {
  return 1.0 / (1.0 + static_cast<double>(hit_offset_in_passage));
}

std::span<const highlight::Passage> GetPassages(
  icu::BreakIterator& sentence_iter, UTextPtr& utext, std::string_view doc,
  HitsView view, size_t max_fragments,
  std::vector<highlight::Passage>& passages) {
  passages.clear();
  if (view.empty()) {
    return {};
  }
  passages.reserve(
    std::min<size_t>(view.size(), std::max<size_t>(1, max_fragments)));
  BindIterator(sentence_iter, utext, doc);
  const auto doc_size =
    std::min<uint32_t>(doc.size(), std::numeric_limits<uint32_t>::max());

  for (size_t cursor = 0; cursor < view.size();) {
    highlight::Passage passage;
    const auto hit_start = view[cursor].first;
    const auto start =
      sentence_iter.preceding(static_cast<int32_t>(hit_start + 1));
    passage.range.byte_start =
      start == icu::BreakIterator::DONE ? 0 : static_cast<uint32_t>(start);
    const auto end = sentence_iter.next();
    passage.range.byte_end =
      end == icu::BreakIterator::DONE ? doc_size : static_cast<uint32_t>(end);
    passage.start = static_cast<uint32_t>(cursor);
    double sum = 0.0;
    for (; cursor < view.size(); ++cursor) {
      const auto h = view[cursor];
      if (h.first >= passage.range.byte_end) {
        break;
      }
      sum += SloppyWeight(h.first - passage.range.byte_start);
    }
    SDB_ASSERT(cursor > passage.start);
    if (cursor == passage.start) {
      ++cursor;
      continue;
    }
    passage.end = static_cast<uint32_t>(cursor);
    passage.score = sum * NormForPassage(passage.range.byte_start);
    passages.emplace_back(passage);
  }

  if (auto k = std::max<size_t>(1, max_fragments); passages.size() > k) {
    absl::c_partial_sort(
      passages, passages.begin() + k,
      [](const auto& lhs, const auto& rhs) { return lhs.score > rhs.score; });
    passages.resize(k);
  }
  absl::c_sort(passages, [](const auto& lhs, const auto& rhs) {
    return lhs.range.byte_start < rhs.range.byte_start;
  });
  return passages;
}

duckdb::string_t RenderHighlightAll(duckdb::Vector& result,
                                    std::string_view doc, HitsView view,
                                    const highlight::HighlightOptions& opts) {
  const auto upper =
    doc.size() + view.size() * (opts.start_sel.size() + opts.stop_sel.size());
  auto target = duckdb::StringVector::EmptyString(result, upper);
  char* dst = target.GetDataWriteable();
  size_t pos = 0;
  uint32_t cursor = 0;
  for (size_t i = 0; i < view.size();) {
    auto [start, end] = view[i++];
    while (i < view.size() && view[i].first <= end) {
      end = std::max(end, view[i++].second);
    }
    Append(dst, pos, doc.substr(cursor, start - cursor));
    Append(dst, pos, opts.start_sel);
    Append(dst, pos, doc.substr(start, end - start));
    Append(dst, pos, opts.stop_sel);
    cursor = end;
  }
  if (cursor < doc.size()) {
    Append(dst, pos, doc.substr(cursor));
  }
  target.SetSizeAndFinalize(pos, upper);
  return target;
}

duckdb::string_t RenderPrefix(duckdb::Vector& result, std::string_view doc,
                              HighlightState& state, size_t max_words) {
  const auto prefix =
    DiscoverPrefix(doc, *state.word_iter, state.utext, max_words);
  auto target = duckdb::StringVector::EmptyString(result, prefix.size());
  memcpy(target.GetDataWriteable(), prefix.data(), prefix.size());
  target.Finalize();
  return target;
}

duckdb::string_t RenderPassages(duckdb::Vector& result, std::string_view doc,
                                HighlightState& state,
                                std::span<const highlight::Passage> passages,
                                HitsView view,
                                const highlight::HighlightOptions& opts) {
  // (passages.size() - 1) below underflows on empty input.
  SDB_ASSERT(!passages.empty());
  const size_t wrap_size = opts.start_sel.size() + opts.stop_sel.size();
  size_t upper = (passages.size() - 1) * opts.fragment_delim.size();
  for (const auto& passage : passages) {
    upper += (passage.range.byte_end - passage.range.byte_start) +
             opts.max_words * wrap_size;
  }

  auto target = duckdb::StringVector::EmptyString(result, upper);
  char* dst = target.GetDataWriteable();
  size_t pos = 0;
  for (size_t p = 0; p < passages.size(); ++p) {
    if (p > 0) {
      Append(dst, pos, opts.fragment_delim);
    }
    TokenizeSentence(state.tokens, *state.word_iter, state.utext, doc,
                     passages[p].range, view, passages[p].start,
                     passages[p].end);
    auto clipped = ClipToMaxWords(state.tokens, opts.max_words);
    WriteRenderTokens(dst, pos, doc, clipped, opts);
  }
  target.SetSizeAndFinalize(pos, upper);
  return target;
}

void RenderChunk(HighlightState& state, duckdb::DataChunk& args,
                 const highlight::HighlightOptions& opts,
                 duckdb::Vector& result) {
  const auto count = args.size();

  duckdb::UnifiedVectorFormat doc_format;
  duckdb::UnifiedVectorFormat list_format;
  args.data[0].ToUnifiedFormat(count, doc_format);
  args.data[1].ToUnifiedFormat(count, list_format);
  const auto* doc_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(doc_format);
  const auto* list_entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(list_format);

  auto& list_child = duckdb::ListVector::GetEntry(args.data[1]);
  const auto child_size = duckdb::ListVector::GetListSize(args.data[1]);
  duckdb::UnifiedVectorFormat child_format;
  list_child.ToUnifiedFormat(child_size, child_format);
  const auto* child_data =
    duckdb::UnifiedVectorFormat::GetData<int32_t>(child_format);

  auto& result_validity = duckdb::FlatVector::ValidityMutable(result);
  auto* result_data =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(result);

  for (size_t i = 0; i < count; ++i) {
    const auto doc_idx = doc_format.sel->get_index(i);
    const auto list_idx = list_format.sel->get_index(i);
    if (!doc_format.validity.RowIsValid(doc_idx) ||
        !list_format.validity.RowIsValid(list_idx)) {
      result_validity.SetInvalid(i);
      continue;
    }
    const auto doc = AsView(doc_data[doc_idx]);
    const HitsView view{list_entries[list_idx], child_format, child_data};
    ValidateHits(doc, view);

    if (opts.highlight_all) {
      result_data[i] = RenderHighlightAll(result, doc, view, opts);
      continue;
    }

    auto passages = GetPassages(*state.sentence_iter, state.utext, doc, view,
                                opts.max_fragments, state.passages);

    result_data[i] =
      passages.empty()
        ? RenderPrefix(result, doc, state, opts.max_words)
        : RenderPassages(result, doc, state, passages, view, opts);
  }
}

// POSTINGS form: doc + LIST<INTEGER> offsets [+ options]. Sugar forms
// rewrite into this at bind time via the hooks below.
void TsHighlightOffsets(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  auto& local_state = duckdb::ExecuteFunctionState::GetFunctionState(state)
                        ->Cast<TsHighlightLocalState>();
  auto& bind = GetBindData(state);
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  RenderChunk(local_state.state, args, bind.options, result);
}

duckdb::unique_ptr<duckdb::FunctionData> TsHighlightBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto bind = duckdb::make_uniq<TsHighlightBindData>();
  auto& args = input.GetArguments();

  if (args.size() == 3) {
    if (!args[2]->IsFoldable()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_highlight: options must be a constant expression"));
    }
    auto val = duckdb::ExpressionExecutor::EvaluateScalar(context, *args[2]);
    if (!val.IsNull()) {
      bind->options =
        highlight::ParseHighlightOptions(duckdb::StringValue::Get(val));
    }
  }
  return bind;
}

duckdb::ScalarFunction MakeOffsetsFn(duckdb::vector<duckdb::LogicalType> args) {
  duckdb::ScalarFunction f{
    std::move(args),
    duckdb::LogicalType::VARCHAR,
    TsHighlightOffsets,
    TsHighlightBind,
  };
  f.SetInitStateCallback(InitTsHighlightLocalState);
  f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  return f;
}

// Only runs if DuckDB ever invokes a sugar overload without firing
// its bind_expression rewrite hook -- a clean failure mode.
void TsHighlightStubFn(duckdb::DataChunk& /*args*/,
                       duckdb::ExpressionState& /*state*/,
                       duckdb::Vector& /*result*/) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_INTERNAL_ERROR),
    ERR_MSG("ts_highlight() bind-time rewrite did not fire"),
    ERR_HINT("This is an internal error; report with a reproducer."));
}

// Rewrite sugar forms into POSTINGS at bind time:
//   virtual-col:  ts_highlight(col [, opts])
//     -> ts_highlight(col, ts_offsets(col [, maxoffsets]) [, opts])
//   dict-standalone: ts_highlight(dict, body, filter [, opts])
//     -> ts_highlight(body, ts_offsets(dict, body, filter [, maxoffsets])
//                     [, opts])
duckdb::unique_ptr<duckdb::Expression> RewriteToPostings(
  duckdb::FunctionBindExpressionInput& input) {
  auto& children = input.children;
  const bool dict_standalone = (children.size() == 3 || children.size() == 4);
  const size_t body_idx = dict_standalone ? 1 : 0;
  const size_t opts_idx = (children.size() == 4 || children.size() == 2)
                            ? children.size() - 1
                            : children.size();

  // `ts_highlight(<literal>, NULL)` binds to the (ANY, VARCHAR) sugar
  // (ANY matches NULL without a cast) instead of POSTINGS. Rebind it
  // to POSTINGS so SPECIAL_HANDLING returns NULL. The 1-arg literal
  // form stays on the original path -- it has no column to derive
  // offsets from and fails downstream with a clear error.
  if (!dict_standalone && children.size() == 2 &&
      children[body_idx]->GetExpressionClass() !=
        duckdb::ExpressionClass::BOUND_COLUMN_REF) {
    duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> postings_args;
    postings_args.emplace_back(std::move(children[body_idx]));
    auto list_type = duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER);
    postings_args.emplace_back(duckdb::BoundCastExpression::AddCastToType(
      input.context, std::move(children[opts_idx]), list_type));
    duckdb::FunctionBinder binder(input.context);
    duckdb::ErrorData err;
    auto bound = binder.BindScalarFunction(duckdb::Identifier{DEFAULT_SCHEMA},
                                           duckdb::Identifier{kTsHighlight},
                                           std::move(postings_args), err);
    if (!bound) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("ts_highlight(): non-column doc requires POSTINGS form "
                "ts_highlight(doc, offsets_list[, options]): ",
                err.RawMessage()));
    }
    return bound;
  }

  // 0 leaves the limit off the synthesised call; ts_offsets() then
  // applies its own default cap.
  uint32_t max_offsets = 0;
  if (opts_idx < children.size() && children[opts_idx]->IsFoldable()) {
    auto v = duckdb::ExpressionExecutor::EvaluateScalar(input.context,
                                                        *children[opts_idx]);
    if (!v.IsNull()) {
      max_offsets =
        highlight::ParseHighlightOptions(duckdb::StringValue::Get(v))
          .max_offsets;
    }
  }

  const size_t offsets_arity = dict_standalone ? 3 : 1;
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> offsets_args;
  offsets_args.reserve(offsets_arity + (max_offsets > 0 ? 1 : 0));
  for (size_t i = 0; i < offsets_arity; ++i) {
    offsets_args.emplace_back(children[i]->Copy());
  }
  if (max_offsets > 0) {
    offsets_args.emplace_back(
      duckdb::make_uniq<duckdb::BoundConstantExpression>(
        duckdb::Value::INTEGER(static_cast<int32_t>(max_offsets))));
  }

  duckdb::FunctionBinder offsets_binder(input.context);
  duckdb::ErrorData offsets_err;
  auto offsets_call = offsets_binder.BindScalarFunction(
    duckdb::Identifier{DEFAULT_SCHEMA}, duckdb::Identifier{kOffsets},
    std::move(offsets_args), offsets_err);
  if (!offsets_call) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_highlight(): failed to synthesize ts_offsets() call: ",
              offsets_err.RawMessage()));
  }

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> postings_args;
  postings_args.reserve(opts_idx < children.size() ? 3 : 2);
  postings_args.emplace_back(std::move(children[body_idx]));
  postings_args.emplace_back(std::move(offsets_call));
  if (opts_idx < children.size()) {
    postings_args.emplace_back(std::move(children[opts_idx]));
  }

  duckdb::FunctionBinder binder(input.context);
  duckdb::ErrorData err;
  auto bound = binder.BindScalarFunction(duckdb::Identifier{DEFAULT_SCHEMA},
                                         duckdb::Identifier{kTsHighlight},
                                         std::move(postings_args), err);
  if (!bound) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_highlight(): failed to rewrite to POSTINGS "
                            "form: ",
                            err.RawMessage()));
  }
  return bound;
}

duckdb::ScalarFunction MakeSugarFn(duckdb::vector<duckdb::LogicalType> args) {
  duckdb::ScalarFunction f{std::move(args), duckdb::LogicalType::VARCHAR,
                           TsHighlightStubFn};
  f.SetBindExpressionCallback(RewriteToPostings);
  f.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  return f;
}

}  // namespace

void RegisterTsHighlight(duckdb::ExtensionLoader& loader) {
  duckdb::ScalarFunctionSet set{duckdb::Identifier{kTsHighlight}};
  // POSTINGS: ts_highlight(doc, offsets[] [, opts]).
  set.AddFunction(
    MakeOffsetsFn({duckdb::LogicalType::VARCHAR,
                   duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER)}));
  set.AddFunction(
    MakeOffsetsFn({duckdb::LogicalType::VARCHAR,
                   duckdb::LogicalType::LIST(duckdb::LogicalType::INTEGER),
                   duckdb::LogicalType::VARCHAR}));
  // Dict-standalone: ts_highlight(dict, body, filter [, opts]).
  set.AddFunction(
    MakeSugarFn({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                 MakeTSQueryType()}));
  set.AddFunction(
    MakeSugarFn({duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
                 MakeTSQueryType(), duckdb::LogicalType::VARCHAR}));
  // Virtual-column: ts_highlight(col [, opts]).
  set.AddFunction(MakeSugarFn({duckdb::LogicalType::ANY}));
  set.AddFunction(
    MakeSugarFn({duckdb::LogicalType::ANY, duckdb::LogicalType::VARCHAR}));
  loader.RegisterFunction(std::move(set));
}

}  // namespace sdb::connector
