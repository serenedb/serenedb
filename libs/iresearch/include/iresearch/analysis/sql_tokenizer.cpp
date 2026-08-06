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

#include "sql_tokenizer.hpp"

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/column_definition.hpp>
#include <duckdb/parser/column_list.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression_binder/check_binder.hpp>

#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

constexpr std::string_view kInputColumn = "input";

void ValidateParsed(duckdb::ParsedExpression& expr) {
  switch (expr.GetExpressionClass()) {
    case duckdb::ExpressionClass::SUBQUERY:
      THROW_SQL_ERROR(ERR_MSG("sql: subqueries are not allowed"));
    case duckdb::ExpressionClass::PARAMETER:
      THROW_SQL_ERROR(ERR_MSG("sql: parameters are not allowed"));
    case duckdb::ExpressionClass::FUNCTION: {
      auto& func = expr.Cast<duckdb::FunctionExpression>();
      const auto& name = func.GetQualifiedName();
      if ((!name.Catalog().empty() && name.Catalog() != SYSTEM_CATALOG) ||
          (!name.Schema().empty() && name.Schema() != DEFAULT_SCHEMA)) {
        THROW_SQL_ERROR(ERR_MSG("sql: function \"",
                                name.Name().GetIdentifierName(),
                                "\": only built-in functions are allowed"));
      }
      // Text-search functions (ts_lexize / ts_tokenize / ...) lease a tokenizer
      // and analyze their input, so calling one here would re-enter the
      // tokenizer machinery -- infinite recursion if it names this very
      // dictionary. None of the ts_* family is a legitimate token-producing
      // primitive; reject the whole prefix.
      if (name.Name().GetIdentifierName().starts_with("ts_")) {
        THROW_SQL_ERROR(
          ERR_MSG("sql: text-search function \"",
                  name.Name().GetIdentifierName(),
                  "\" is not allowed in a sql tokenizer expression (it would "
                  "recurse into the tokenizer)"));
      }
      func.SetQualifiedName(duckdb::Identifier{SYSTEM_CATALOG},
                            duckdb::Identifier{DEFAULT_SCHEMA}, name.Name());
    } break;
    default:
      break;
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expr, [](duckdb::ParsedExpression& child) { ValidateParsed(child); });
}

void VerifyFunctionsExist(duckdb::ClientContext& ctx,
                          const duckdb::ParsedExpression& expr) {
  if (expr.GetExpressionClass() == duckdb::ExpressionClass::FUNCTION) {
    const auto& name =
      expr.Cast<duckdb::FunctionExpression>().GetQualifiedName().Name();
    auto& sys = duckdb::Catalog::GetSystemCatalog(ctx);
    const auto exists = [&](duckdb::CatalogType type) {
      return sys
        .GetEntry(ctx, type, duckdb::Identifier{DEFAULT_SCHEMA}, name,
                  duckdb::OnEntryNotFound::RETURN_NULL)
        .get() != nullptr;
    };
    if (!exists(duckdb::CatalogType::SCALAR_FUNCTION_ENTRY) &&
        !exists(duckdb::CatalogType::MACRO_ENTRY) &&
        !exists(duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY)) {
      THROW_SQL_ERROR(ERR_MSG("sql: function \"", name.GetIdentifierName(),
                              "\": only built-in functions are allowed"));
    }
  }
  duckdb::ParsedExpressionIterator::EnumerateChildren(
    expr, [&](const duckdb::ParsedExpression& child) {
      VerifyFunctionsExist(ctx, child);
    });
}

}  // namespace

struct SqlTokenizer::Plan {
  duckdb::unique_ptr<duckdb::Expression> expr;
};

struct SqlTokenizer::Exec {
  explicit Exec(duckdb::ClientContext& ctx, const duckdb::Expression& expr)
    : executor{ctx}, result{expr.GetReturnType()} {
    executor.AddExpression(expr);
    const duckdb::LogicalType type = duckdb::LogicalType::VARCHAR;
    input.InitializeEmpty(std::span{&type, 1});
  }

  duckdb::ExpressionExecutor executor;
  duckdb::DataChunk input;
  duckdb::Vector result;
};

SqlTokenizer::SqlTokenizer(Options opts)
  : _expression{std::move(opts.expression)} {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> exprs;
  try {
    exprs = duckdb::Parser::ParseExpressionList(_expression);
  } catch (const std::exception& e) {
    THROW_SQL_ERROR(ERR_MSG("sql: ", e.what()));
  }
  if (exprs.size() != 1) {
    THROW_SQL_ERROR(ERR_MSG("sql: expected exactly one expression"));
  }
  ValidateParsed(*exprs[0]);
  _parsed = std::move(exprs[0]);
}

SqlTokenizer::~SqlTokenizer() = default;

Tokenizer::ptr SqlTokenizer::Make(Options opts) {
  return std::make_unique<SqlTokenizer>(std::move(opts));
}

void SqlTokenizer::Bind(duckdb::ClientContext& ctx) {
  if (!_plan) {
    const auto bind = [&] {
      VerifyFunctionsExist(ctx, *_parsed);
      auto expr = _parsed->Copy();
      auto binder = duckdb::Binder::CreateBinder(ctx);
      duckdb::ColumnList columns;
      columns.AddColumn(duckdb::ColumnDefinition{
        duckdb::Identifier{kInputColumn}, duckdb::LogicalType::VARCHAR});
      duckdb::physical_index_set_t bound_columns;
      duckdb::CheckBinder check_binder{*binder, ctx,
                                       duckdb::Identifier{type_name()},
                                       columns, bound_columns};
      check_binder.target_type =
        duckdb::LogicalType{duckdb::LogicalTypeId::INVALID};
      duckdb::unique_ptr<duckdb::Expression> bound;
      try {
        bound = check_binder.Bind(expr);
      } catch (const std::exception& e) {
        THROW_SQL_ERROR(ERR_MSG("sql: ", e.what()));
      }
      if (bound->IsVolatile()) {
        THROW_SQL_ERROR(ERR_MSG("sql: volatile expressions are not allowed"));
      }
      const auto& type = bound->GetReturnType();
      if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
        _mode = Mode::Scalar;
      } else if (type.id() == duckdb::LogicalTypeId::LIST &&
                 duckdb::ListType::GetChildType(type).id() ==
                   duckdb::LogicalTypeId::VARCHAR) {
        _mode = Mode::List;
      } else {
        THROW_SQL_ERROR(
          ERR_MSG("sql: expression must return VARCHAR or LIST(VARCHAR), got ",
                  type.ToString()));
      }
      auto plan = std::make_unique<Plan>();
      plan->expr = std::move(bound);
      _plan = std::move(plan);
    };
    if (ctx.transaction.HasActiveTransaction()) {
      bind();
    } else {
      ctx.RunFunctionInTransaction(bind);
    }
  }
  _exec = std::make_unique<Exec>(ctx, *_plan->expr);
}

void SqlTokenizer::Unbind() noexcept { _exec.reset(); }

template<TokenLayout Layout>
void SqlTokenizer::FillSlice(std::span<const duckdb::string_t> values,
                             std::span<const doc_id_t> docs,
                             TokenSink& sink) {
  const auto count = values.size();
  duckdb::Vector in{
    duckdb::LogicalType::VARCHAR,
    reinterpret_cast<duckdb::data_ptr_t>(
      const_cast<duckdb::string_t*>(values.data())),
    count};
  _exec->input.data[0].Reference(in);
  _exec->input.SetCardinality(count);
  _exec->executor.ExecuteExpression(_exec->input, _exec->result);

  auto& result = _exec->result;
  duckdb::UnifiedVectorFormat fmt;
  result.ToUnifiedFormat(count, fmt);

  const auto emit = [&](duckdb::string_t term) {
    const uint32_t size = term.GetSize();
    if (size <= duckdb::string_t::INLINE_LENGTH) {
      sink.Emit<Layout>(term);
    } else {
      const char* const data = term.GetData();
      sink.Emit<Layout>(size, [&](byte_type* mem) IRS_FORCE_INLINE {
        std::memcpy(mem, data, size);
        return size;
      });
    }
  };

  if (_mode == Mode::Scalar) {
    const auto* data =
      duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
    for (size_t v = 0; v < count; ++v) {
      const auto idx = fmt.sel->get_index(v);
      const bool valid = fmt.validity.RowIsValid(idx);
      sink.BeginValue(docs[v], valid ? data[idx].GetSize() : 0);
      if (valid) {
        emit(data[idx]);
      }
      sink.EndValue();
    }
    return;
  }

  const auto* entries =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(fmt);
  auto& child = duckdb::ListVector::GetEntry(result);
  const auto child_size = duckdb::ListVector::GetListSize(result);
  duckdb::UnifiedVectorFormat child_fmt;
  child.ToUnifiedFormat(child_size, child_fmt);
  const auto* child_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(child_fmt);
  for (size_t v = 0; v < count; ++v) {
    sink.BeginValue(docs[v], 0);
    const auto idx = fmt.sel->get_index(v);
    if (fmt.validity.RowIsValid(idx)) {
      const auto& entry = entries[idx];
      for (duckdb::idx_t k = 0; k < entry.length; ++k) {
        const auto child_idx = child_fmt.sel->get_index(entry.offset + k);
        if (child_fmt.validity.RowIsValid(child_idx)) {
          emit(child_data[child_idx]);
        }
      }
    }
    sink.EndValue();
  }
}

void SqlTokenizer::DoFillColumn(std::span<const duckdb::string_t> values,
                                std::span<const doc_id_t> docs,
                                TokenSink& sink, TokenLayout layout) {
  SDB_ASSERT(layout != TokenLayout::TermsPosOffs);
  SDB_ASSERT(_exec);
  ResolveLayout(layout, [&]<TokenLayout Layout>() {
    for (size_t off = 0; off < values.size(); off += STANDARD_VECTOR_SIZE) {
      const auto n =
        std::min<size_t>(STANDARD_VECTOR_SIZE, values.size() - off);
      FillSlice<Layout>(values.subspan(off, n), docs.subspan(off, n), sink);
    }
  });
}

template<TokenLayout Layout>
bool SqlTokenizer::DoFill(duckdb::string_t raw, TokenSink& sink) {
  SDB_ASSERT(_exec);
  duckdb::string_t slot = raw;
  duckdb::Vector in{duckdb::LogicalType::VARCHAR,
                    reinterpret_cast<duckdb::data_ptr_t>(&slot), 1};
  _exec->input.data[0].Reference(in);
  _exec->input.SetCardinality(1);
  _exec->executor.ExecuteExpression(_exec->input, _exec->result);

  auto& result = _exec->result;
  duckdb::UnifiedVectorFormat fmt;
  result.ToUnifiedFormat(1, fmt);
  const auto idx = fmt.sel->get_index(0);
  if (!fmt.validity.RowIsValid(idx)) {
    return false;
  }

  const auto emit = [&](duckdb::string_t term) {
    const uint32_t size = term.GetSize();
    if (size <= duckdb::string_t::INLINE_LENGTH) {
      sink.Emit<Layout>(term);
    } else {
      const char* const data = term.GetData();
      sink.Emit<Layout>(size, [&](byte_type* mem) IRS_FORCE_INLINE {
        std::memcpy(mem, data, size);
        return size;
      });
    }
  };

  if (_mode == Mode::Scalar) {
    emit(duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt)[idx]);
    return true;
  }

  const auto& entry =
    duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(fmt)[idx];
  auto& child = duckdb::ListVector::GetEntry(result);
  const auto child_size = duckdb::ListVector::GetListSize(result);
  duckdb::UnifiedVectorFormat child_fmt;
  child.ToUnifiedFormat(child_size, child_fmt);
  const auto* child_data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(child_fmt);
  for (duckdb::idx_t k = 0; k < entry.length; ++k) {
    const auto child_idx = child_fmt.sel->get_index(entry.offset + k);
    if (child_fmt.validity.RowIsValid(child_idx)) {
      emit(child_data[child_idx]);
    }
  }
  return true;
}

template bool SqlTokenizer::DoFill<TokenLayout::Terms>(
  duckdb::string_t, TokenSink&);
template bool SqlTokenizer::DoFill<TokenLayout::TermsPos>(
  duckdb::string_t, TokenSink&);
template bool SqlTokenizer::DoFill<TokenLayout::TermsPosOffs>(
  duckdb::string_t, TokenSink&);

template class TypedTokenizer<SqlTokenizer>;

}  // namespace irs::analysis
