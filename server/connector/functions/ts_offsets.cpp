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

#include <absl/algorithm/container.h>

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/analysis/batch/token_batch.hpp>
#include <iresearch/analysis/batch/token_sinks.hpp>
#include <iresearch/analysis/sparse_ngram_tokenizer.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/filter_optimizer.hpp>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/table_options.h"
#include "connector/common.h"
#include "connector/duckdb_client_state.h"
#include "connector/functions/search.h"
#include "connector/highlight/highlight_types.h"
#include "connector/highlight/memory_index.h"
#include "connector/inverter_sink.hpp"
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

class SortingOffsetTokenizer final
  : public irs::analysis::TypedTokenizer<SortingOffsetTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "sorting_offset_tokenizer";
  }

  explicit SortingOffsetTokenizer(catalog::Tokenizer::TokenizerWrapper inner)
    : _inner{std::move(inner)} {}

  template<irs::TokenLayout Layout>
  bool DoFill(std::string_view value, irs::TokenEmitter& sink) {
    _buf.clear();
    irs::TokenCollector collector{irs::TokenLayout::TermsPosOffs};
    if (!irs::AnalyzeValue(*_inner, value, collector)) {
      return false;
    }
    for (auto& tok : collector.tokens) {
      _buf.emplace_back(tok.offs_start, tok.offs_end, std::move(tok.term));
    }
    absl::c_sort(_buf);
    for (const auto& [start, end, term] : _buf) {
      sink.EmitInterned<Layout>(irs::bytes_view{term}, start, end);
    }
    return true;
  }

 private:
  using Gram = std::tuple<uint32_t, uint32_t, irs::bstring>;

  catalog::Tokenizer::TokenizerWrapper _inner;
  std::vector<Gram> _buf;
};

catalog::Tokenizer::TokenizerWrapper EnsureOffsets(
  catalog::Tokenizer::TokenizerWrapper tokenizer) {
  const auto id = tokenizer->type();
  if (id == irs::Type<irs::analysis::UnionTokenizer>::id()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("ts_offsets()/ts_highlight() is not supported for this "
              "dictionary: its tokenizer does not produce text offsets"));
  }
  if (id == irs::Type<irs::analysis::SparseNGramTokenizer>::id()) {
    return {new SortingOffsetTokenizer(std::move(tokenizer)),
            catalog::Tokenizer::Deleter{}};
  }
  return tokenizer;
}

constexpr auto kOffsetsFeatures =
  irs::IndexFeatures::Freq | irs::IndexFeatures::Pos | irs::IndexFeatures::Offs;

struct IndexField {
  void Reset(catalog::Column::Id column_id,
             catalog::Tokenizer::TokenizerWrapper analyzer) {
    id = static_cast<irs::field_id>(column_id);
    tokens = EnsureOffsets(std::move(analyzer));
  }

  irs::field_id Id() const noexcept { return id; }

  irs::field_id id{irs::field_limits::invalid()};
  catalog::Tokenizer::TokenizerWrapper tokens;
};

struct OffsetsLocalState final : duckdb::FunctionLocalState {
  highlight::MemoryIndex memory_index;
  IndexField field;
  InverterSink inverter_sink;
};

auto& EnsureField(duckdb::ClientContext& context,
                  OffsetsLocalState& local_state, const OffsetsBindData& bind) {
  if (local_state.field.tokens) {
    return local_state.field;
  }

  auto column_id = kStandaloneSyntheticColumnId;
  catalog::Tokenizer::TokenizerWrapper wrapper;
  if (bind.IsStandalone()) {
    wrapper = bind.dict_tokenizer->GetTokenizer();
  } else {
    auto snapshot = GetSereneDBContext(context).CatalogSnapshot();
    auto column_tokenizer = bind.inverted_index->GetTokenizer(
      snapshot, static_cast<irs::field_id>(bind.column_id));
    wrapper = std::move(column_tokenizer.analyzer);
    column_id = bind.column_id;
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
  if (!expr.BindInfo()) {
    // Reached when neither the iresearch_plan rule nor
    // OffsetsStandaloneBind populated bind data (literal first arg, no
    // surrounding search scan, unresolved dict name, etc.). Same shape
    // as the function's pre-rewrite stub error.
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("ts_offsets() requires an inverted index scan in the same "
              "sub-query"),
      ERR_HINT("Add `WHERE <col> @@ <tsquery>` on the column being looked "
               "up, or use the standalone ts_offsets(dict, body, filter) "
               "form."));
  }
  auto& bind = expr.BindInfo()->Cast<OffsetsBindData>();
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
    auto* slot = doc.Field(field.Id(), kOffsetsFeatures);
    SDB_ASSERT(slot);
    auto& inverter_sink = local_state.inverter_sink;
    inverter_sink.Reset();

    const auto* data =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
    auto& w = inverter_sink.Bind(doc, *slot);
    irs::doc_id_t d = doc.DocId();
    for (duckdb::idx_t r = 0; r < count; ++r, ++d) {
      const auto idx = fmt.sel->get_index(r);
      if (!fmt.validity.RowIsValid(idx)) {
        continue;
      }
      const auto& s = data[idx];
      if (s.GetSize() > 0) {
        w.BeginValue(d);
        field.tokens->Fill(AsView(s), w, irs::TokenLayout::TermsPosOffs);
        w.EndValue();
      }
    }
    inverter_sink.Flush();
  });

  auto collector = bind.stored_filter->MakeCollector(nullptr);
  auto query = bind.stored_filter->PrepareSegment(
    *segment, {.collector = collector.get()});
  if (!query) {
    return;
  }

  FieldEntry entry{.id = field.Id()};
  OffsetsCollector visitor{std::span{&entry, 1}};
  query->Visit(visitor, irs::kNoBoost);

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
    duckdb::Identifier{"synthetic_body"}, duckdb::LogicalType::VARCHAR,
    duckdb::ColumnBinding{duckdb::TableIndex{kSyntheticTableIdx},
                          duckdb::ProjectionIndex{kSyntheticColumnIdx}});

  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> at_at_children;
  at_at_children.push_back(std::move(body_ref));
  at_at_children.push_back(tsquery_expr.Copy());

  duckdb::ScalarFunction at_at(duckdb::Identifier{kTSQueryMatch}, {},
                               duckdb::LogicalType::BOOLEAN, nullptr);
  duckdb::BoundScalarFunction bound_at_at(at_at);
  bound_at_at.SetName(duckdb::Identifier{kTSQueryMatch});
  auto match_expr = duckdb::make_uniq<duckdb::BoundFunctionExpression>(
    std::move(bound_at_at), std::move(at_at_children), nullptr);

  auto column_getter =
    [column_id, dict_tokenizer](const duckdb::BoundColumnRefExpression& ref)
    -> std::optional<SearchColumnInfo> {
    if (ref.Binding().table_index != duckdb::TableIndex{kSyntheticTableIdx} ||
        ref.Binding().column_index !=
          duckdb::ProjectionIndex{kSyntheticColumnIdx}) {
      return std::nullopt;
    }
    SearchColumnInfo info;
    info.field_id = static_cast<irs::field_id>(column_id);
    info.logical_type = duckdb::LogicalType::VARCHAR;
    info.tokenizer.analyzer = dict_tokenizer->GetTokenizer();
    info.tokenizer.features = irs::IndexFeatures::Freq |
                              irs::IndexFeatures::Pos |
                              irs::IndexFeatures::Offs;
    return info;
  };

  auto root = std::make_unique<irs::And>();
  duckdb::unique_ptr<duckdb::Expression> match_owner = std::move(match_expr);
  std::span<const duckdb::unique_ptr<duckdb::Expression>> conjuncts{
    &match_owner, 1};
  if (auto s = MakeSearchFilter(*root, conjuncts, column_getter, context);
      !s.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG(s.message()));
  }
  irs::Filter::ptr filter = std::move(root);
  irs::Optimize(filter);
  return filter;
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
