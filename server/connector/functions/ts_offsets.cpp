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

#include "connector/functions/ts_offsets.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <algorithm>
#include <iresearch/analysis/sparse_ngram_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/search.h"
#include "connector/highlight/highlight_types.h"
#include "connector/highlight/memory_index.h"
#include "connector/offsets_collector.hpp"
#include "connector/offsets_writer.hpp"
#include "connector/search_filter_builder.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::FunctionData> OffsetsBindData::Copy() const {
  return duckdb::make_uniq<OffsetsBindData>(*this);
}

bool OffsetsBindData::Equals(const duckdb::FunctionData& other) const {
  const auto& o = other.Cast<OffsetsBindData>();
  return inverted_index == o.inverted_index && column_id == o.column_id &&
         dict_tokenizer == o.dict_tokenizer && limit == o.limit &&
         stored_filter == o.stored_filter;
}

namespace {

constexpr irs::field_id kStandaloneFieldId =
  catalog::Column::kMaxRealIdValue + 4;
constexpr catalog::Column::Id kStandaloneSyntheticColumnId{kStandaloneFieldId};

// How a tokenizer can supply the per-token offsets the offsets/highlight path
// re-derives in memory:
//   Direct      -- emits OffsAttr in non-decreasing start order; index as-is.
//   Sorted      -- emits OffsAttr but out of start order (sparse_ngram); the
//                  grams must be buffered and sorted before indexing.
//   Unsupported -- cannot produce offsets at all (e.g. union interleaves
//                  independent sub-tokenizers); offsets/highlight is not
//                  available and must error instead of crashing.
enum class OffsetSupport { Direct, Sorted, Unsupported };

OffsetSupport ClassifyOffsetSupport(const irs::analysis::Analyzer& analyzer) {
  const auto id = analyzer.type();
  if (id == irs::Type<irs::analysis::SparseNGramTokenizer>::id()) {
    return OffsetSupport::Sorted;
  }
  if (id == irs::Type<irs::analysis::UnionTokenizer>::id()) {
    return OffsetSupport::Unsupported;
  }
  return OffsetSupport::Direct;
}

// Adapts a tokenizer that emits OffsAttr out of start order (sparse_ngram) into
// a stream the indexer accepts: on reset it drains the inner tokenizer, buffers
// each gram with its byte range, and replays them sorted by start offset so the
// indexer's monotonic-offset invariant holds. Bounded per document, which is
// fine for the in-memory highlight index (not the streaming on-disk indexer).
class SortingOffsetTokenizer final : public irs::Tokenizer {
 public:
  explicit SortingOffsetTokenizer(irs::analysis::Analyzer& inner) noexcept
    : _inner{&inner} {}

  bool Reset(std::string_view value) {
    _idx = 0;
    _buf.clear();
    if (!_inner->reset(value)) {
      return false;
    }
    const auto* term = irs::get<irs::TermAttr>(*_inner);
    const auto* offs = irs::get<irs::OffsAttr>(*_inner);
    if (!term || !offs) {
      return false;
    }
    while (_inner->next()) {
      _buf.push_back({.term = term->value, .start = offs->start,
                      .end = offs->end});
    }
    std::sort(_buf.begin(), _buf.end(), [](const Entry& a, const Entry& b) {
      return a.start != b.start ? a.start < b.start : a.end < b.end;
    });
    return true;
  }

  bool next() final {
    if (_idx >= _buf.size()) {
      return false;
    }
    const auto& e = _buf[_idx++];
    std::get<irs::TermAttr>(_attrs).value = e.term;
    auto& offs = std::get<irs::OffsAttr>(_attrs);
    offs.start = e.start;
    offs.end = e.end;
    return true;
  }

  irs::Attribute* GetMutable(irs::TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

 private:
  struct Entry {
    irs::bytes_view term;
    uint32_t start;
    uint32_t end;
  };

  irs::analysis::Analyzer* _inner;
  // IncAttr defaults to 1, so replayed positions advance one per gram.
  std::tuple<irs::IncAttr, irs::TermAttr, irs::OffsAttr> _attrs;
  std::vector<Entry> _buf;
  size_t _idx{0};
};

struct IndexField {
  void Reset(catalog::Column::Id column_id,
             catalog::Tokenizer::TokenizerWrapper analyzer) {
    id = static_cast<irs::field_id>(column_id);
    tokenizer = std::move(analyzer);
    if (ClassifyOffsetSupport(*tokenizer) == OffsetSupport::Sorted) {
      sorter = std::make_unique<SortingOffsetTokenizer>(*tokenizer);
    } else {
      sorter.reset();
    }
  }

  irs::field_id Id() const noexcept { return id; }
  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Pos |
           irs::IndexFeatures::Offs;
  }
  irs::Tokenizer& GetTokens() const noexcept {
    if (sorter) {
      return *sorter;
    }
    return *tokenizer;
  }
  bool Write(irs::DataOutput&) const noexcept { return false; }
  void SetValue(std::string_view value) const {
    if (sorter) {
      sorter->Reset(value);
    } else {
      tokenizer->reset(value);
    }
  }

  irs::field_id id{irs::field_limits::invalid()};
  catalog::Tokenizer::TokenizerWrapper tokenizer;
  std::unique_ptr<SortingOffsetTokenizer> sorter;
};

struct OffsetsLocalState final : duckdb::FunctionLocalState {
  highlight::MemoryIndex memory_index;
  IndexField field;
};

auto& EnsureField(duckdb::ClientContext& context,
                  OffsetsLocalState& local_state, const OffsetsBindData& bind) {
  if (local_state.field.tokenizer) {
    return local_state.field;
  }

  auto column_id = kStandaloneSyntheticColumnId;
  catalog::Tokenizer::TokenizerWrapper wrapper;
  if (bind.IsStandalone()) {
    auto wrapper_or = bind.dict_tokenizer->GetTokenizer();
    SDB_ENSURE(wrapper_or.has_value(), ERROR_INTERNAL);
    wrapper = std::move(*wrapper_or);
  } else {
    auto snapshot = GetSereneDBContext(context).EnsureCatalogSnapshot();
    auto column_tokenizer = bind.inverted_index->GetTokenizer(
      snapshot, static_cast<irs::field_id>(bind.column_id));
    wrapper = std::move(column_tokenizer.analyzer);
    column_id = bind.column_id;
  }

  // Guard: a tokenizer that cannot produce offsets at all would index a field
  // with positions but no offsets, then the offset read path decodes offset
  // deltas that were never written -- a crash. Fail cleanly instead.
  if (ClassifyOffsetSupport(*wrapper) == OffsetSupport::Unsupported) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("ts_offsets()/ts_highlight() is not supported for this "
              "dictionary: its tokenizer does not produce text offsets"));
  }

  local_state.field.Reset(column_id, std::move(wrapper));
  return local_state.field;
}

}  // namespace

duckdb::unique_ptr<duckdb::FunctionLocalState> InitOffsetsLocalState(
  duckdb::ExpressionState& /*state*/,
  const duckdb::BoundFunctionExpression& /*expr*/,
  duckdb::FunctionData* /*bind_data*/) {
  return duckdb::make_uniq<OffsetsLocalState>();
}

void OffsetsScalarFn(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                     duckdb::Vector& result) {
  auto& expr = state.expr.Cast<duckdb::BoundFunctionExpression>();
  if (!expr.bind_info) {
    // Reached when neither the iresearch_plan rule nor
    // OffsetsStandaloneBind populated bind data (literal first arg, no
    // surrounding SearchScan, unresolved dict name, etc.). Same shape
    // as the function's pre-rewrite stub error.
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() requires an inverted index scan in the same "
              "sub-query"),
      ERR_HINT("Add `WHERE <col> @@ <tsquery>` on the column being looked "
               "up, or use the standalone ts_offsets(dict, body, filter) "
               "form."));
  }
  auto& bind = expr.bind_info->Cast<OffsetsBindData>();
  auto& local_state = duckdb::ExecuteFunctionState::GetFunctionState(state)
                        ->Cast<OffsetsLocalState>();
  const auto count = args.size();

  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  auto& child = duckdb::ListVector::GetChildMutable(result);
  child.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  duckdb::ListVector::SetListSize(result, 0);

  if (count == 0) {
    return;
  }

  auto& field = EnsureField(state.GetContext(), local_state, bind);

  SDB_ASSERT(args.ColumnCount() >= 2);
  auto& body = args.data[1];
  duckdb::UnifiedVectorFormat fmt;
  body.ToUnifiedFormat(count, fmt);

  auto segment = local_state.memory_index.IndexChunk(count, [&](auto& doc) {
    const auto* data =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
    for (duckdb::idx_t r = 0; r < count; ++r) {
      const auto idx = fmt.sel->get_index(r);
      if (fmt.validity.RowIsValid(idx)) {
        const auto& s = data[idx];
        if (s.GetSize() > 0) {
          field.SetValue(std::string_view{s.GetData(), s.GetSize()});
          doc.Insert(field);
        }
      }
      doc.NextDocument();
    }
  });

  auto query = bind.stored_filter->prepare({.index = *segment});
  if (!query) {
    return;
  }

  FieldEntry entry{.id = field.Id()};
  OffsetsCollector visitor{std::span{&entry, 1}};
  query->visit(*segment, visitor, irs::kNoBoost);

  std::vector<highlight::HitRange> offsets;
  for (duckdb::idx_t r = 0; r < count; ++r) {
    const auto target = static_cast<irs::doc_id_t>(irs::doc_limits::min() + r);
    FillRowOffsets(entry.state, *segment, target, bind.limit, offsets);
    WriteRowOffsets(result, r, offsets);
  }
}

namespace {

bool EvalFoldableVarchar(duckdb::ClientContext& context,
                         const duckdb::Expression& expr, const char* arg_name,
                         std::string& out) {
  if (!expr.IsFoldable()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): ", arg_name, " must be a constant expression"));
  }
  auto val = duckdb::ExpressionExecutor::EvaluateScalar(context, expr);
  if (val.IsNull()) {
    return false;
  }
  out = duckdb::StringValue::Get(val);
  return true;
}

int64_t EvalOptionalLimit(duckdb::ClientContext& context,
                          const duckdb::Expression& expr) {
  if (!expr.IsFoldable()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): limit must be a constant integer expression"));
  }
  auto val = duckdb::ExpressionExecutor::EvaluateScalar(context, expr);
  if (val.IsNull()) {
    return 0;
  }
  const auto raw = val.GetValue<int32_t>();
  if (raw < 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_offsets(): limit must be >= 0"));
  }
  return raw;
}

}  // namespace

std::shared_ptr<irs::Filter> BuildFilterFromTSQuery(
  duckdb::ClientContext& context, const duckdb::Expression& tsquery_expr,
  catalog::Column::Id column_id,
  const std::shared_ptr<catalog::Tokenizer>& dict_tokenizer) {
  static constexpr duckdb::idx_t kSyntheticTableIdx = 0;
  static constexpr duckdb::idx_t kSyntheticColumnIdx = 0;

  auto body_ref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
    "synthetic_body", duckdb::LogicalType::VARCHAR,
    duckdb::ColumnBinding{duckdb::TableIndex{kSyntheticTableIdx},
                          duckdb::ProjectionIndex{kSyntheticColumnIdx}});

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> at_at_children;
  at_at_children.push_back(std::move(body_ref));
  at_at_children.push_back(tsquery_expr.Copy());

  duckdb::ScalarFunction at_at(std::string{kTSQueryMatch}, {},
                               duckdb::LogicalType::BOOLEAN, nullptr);
  duckdb::BoundScalarFunction bound_at_at(at_at);
  bound_at_at.SetName(std::string{kTSQueryMatch});
  auto match_expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    std::move(bound_at_at), std::move(at_at_children), nullptr);

  auto column_getter =
    [column_id, dict_tokenizer](const duckdb::BoundColumnRefExpression& ref)
    -> std::optional<SearchColumnInfo> {
    if (ref.binding.table_index != duckdb::TableIndex{kSyntheticTableIdx} ||
        ref.binding.column_index !=
          duckdb::ProjectionIndex{kSyntheticColumnIdx}) {
      return std::nullopt;
    }
    auto wrapper_or = dict_tokenizer->GetTokenizer();
    if (!wrapper_or.has_value()) {
      return std::nullopt;
    }
    SearchColumnInfo info;
    info.field_id = static_cast<irs::field_id>(column_id);
    info.logical_type = duckdb::LogicalType::VARCHAR;
    info.tokenizer.analyzer = std::move(*wrapper_or);
    info.tokenizer.features = irs::IndexFeatures::Freq |
                              irs::IndexFeatures::Pos |
                              irs::IndexFeatures::Offs;
    return info;
  };

  auto root = std::make_shared<irs::And>();
  duckdb::unique_ptr<duckdb::Expression> match_owner = std::move(match_expr);
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts{
    &match_owner, 1};
  auto result = MakeSearchFilter(*root, conjuncts, column_getter, context);
  if (!result.ok()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("failed to build filter from tsquery: ", result.errorMessage()));
  }
  return root;
}

duckdb::unique_ptr<duckdb::FunctionData> OffsetsStandaloneBind(
  duckdb::BindScalarFunctionInput& input) {
  auto& context = input.GetClientContext();
  auto& arguments = input.GetArguments();
  SDB_ASSERT(arguments.size() == 3 || arguments.size() == 4);

  std::string dict_name;
  if (!EvalFoldableVarchar(context, *arguments[0], "dict", dict_name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("ts_offsets(): dict must not be NULL"));
  }

  auto dict = ResolveCatalogTokenizer(context, dict_name);
  if (!dict) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets(): text search dictionary not found: ", dict_name));
  }

  auto bind = duckdb::make_uniq<OffsetsBindData>();
  bind->dict_tokenizer = dict;
  bind->stored_filter = BuildFilterFromTSQuery(
    context, *arguments[2], kStandaloneSyntheticColumnId, dict);
  // Omitted arg => default cap; explicit 0 => unlimited.
  constexpr size_t kDefaultOffsetsLimit = 1 << 12;
  bind->limit = kDefaultOffsetsLimit;
  if (arguments.size() == 4) {
    const auto raw_limit =
      static_cast<size_t>(EvalOptionalLimit(context, *arguments[3]));
    bind->limit =
      raw_limit > 0 ? raw_limit : std::numeric_limits<size_t>::max();
  }

  arguments[2] = duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value{MakeTSQueryType()});
  return bind;
}

}  // namespace sdb::connector
