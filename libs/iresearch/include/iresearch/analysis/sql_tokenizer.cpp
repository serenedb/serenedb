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

#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/expression_executor_state.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/parser/column_definition.hpp>
#include <duckdb/parser/column_list.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression_binder/check_binder.hpp>
#include <memory>
#include <vector>

#include "pg/sql_exception_macro.h"

namespace irs::analysis {
namespace {

constexpr std::string_view kInputColumn = "input";
constexpr uint32_t kBatch = STANDARD_VECTOR_SIZE;

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
      if (name.Name().GetIdentifierName().starts_with("ts_")) {
        THROW_SQL_ERROR(ERR_MSG(
          "sql: text-search function \"", name.Name().GetIdentifierName(),
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

bool IsDirectCall(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& call = expr.Cast<duckdb::BoundFunctionExpression>();
  if (!call.Function().HasFunctionCallback()) {
    return false;
  }
  const bool propagates_null =
    call.Function().GetNullHandling() ==
    duckdb::FunctionNullHandling::DEFAULT_NULL_HANDLING;
  bool has_input = false;
  for (const auto& child : call.GetChildren()) {
    switch (child->GetExpressionClass()) {
      case duckdb::ExpressionClass::BOUND_REF:
        has_input = true;
        break;
      case duckdb::ExpressionClass::BOUND_CONSTANT: {
        const auto& value =
          child->Cast<duckdb::BoundConstantExpression>().GetValue();
        if (propagates_null && value.IsNull()) {
          return false;
        }
      } break;
      default:
        if (!IsDirectCall(*child)) {
          return false;
        }
        has_input = true;
        break;
    }
  }
  return has_input;
}

template<TokenLayout Layout>
void EmitTerm(TokenSink& sink, const duckdb::string_t& term) {
  sink.Emit<Layout>(term.GetData(), static_cast<uint32_t>(term.GetSize()));
}

struct ResultRows {
  explicit ResultRows(const duckdb::Vector& result) {
    result.ToUnifiedFormat(rows);
    if (result.GetType().id() != duckdb::LogicalTypeId::LIST) {
      terms = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(rows);
      return;
    }
    entries = duckdb::UnifiedVectorFormat::GetData<duckdb::list_entry_t>(rows);
    duckdb::ListVector::GetChild(result).ToUnifiedFormat(elements);
    terms = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(elements);
  }

  bool Valid(uint32_t row) const {
    return rows.validity.RowIsValid(rows.sel->get_index(row));
  }

  template<TokenLayout Layout>
  void Emit(uint32_t row, TokenSink& sink) const {
    const auto idx = rows.sel->get_index(row);
    if (!rows.validity.RowIsValid(idx)) {
      return;
    }
    if (!entries) {
      EmitTerm<Layout>(sink, terms[idx]);
      return;
    }
    const auto entry = entries[idx];
    for (duckdb::idx_t k = 0; k < entry.length; ++k) {
      const auto element = elements.sel->get_index(entry.offset + k);
      if (elements.validity.RowIsValid(element)) {
        EmitTerm<Layout>(sink, terms[element]);
      }
    }
  }

  duckdb::UnifiedVectorFormat rows;
  duckdb::UnifiedVectorFormat elements;
  const duckdb::string_t* terms = nullptr;
  const duckdb::list_entry_t* entries = nullptr;
};

}  // namespace

struct SqlTokenizer::Call {
  struct Node;

  struct Child {
    uint32_t arg;
    duckdb::VectorCache cache;
    std::unique_ptr<Node> node;
  };

  struct Node {
    Node(const duckdb::Expression& expr, duckdb::ExpressionState& node_state,
         const duckdb::Vector& input, duckdb::Allocator& allocator) {
      if (!IsDirectCall(expr)) {
        const duckdb::LogicalType type = duckdb::LogicalType::VARCHAR;
        args.InitializeEmpty(std::span{&type, 1});
        args.data[0].Reference(input);
        return;
      }
      const auto& call = expr.Cast<duckdb::BoundFunctionExpression>();
      function = call.Function().GetFunctionCallback();
      state = &node_state;
      const auto& exprs = call.GetChildren();
      duckdb::vector<duckdb::LogicalType> types;
      types.reserve(exprs.size());
      for (const auto& child : exprs) {
        types.push_back(child->GetReturnType());
      }
      args.InitializeEmpty(types);
      for (uint32_t i = 0; i < exprs.size(); ++i) {
        const auto& child = *exprs[i];
        switch (child.GetExpressionClass()) {
          case duckdb::ExpressionClass::BOUND_REF:
            args.data[i].Reference(input);
            break;
          case duckdb::ExpressionClass::BOUND_CONSTANT:
            args.data[i].Reference(
              child.Cast<duckdb::BoundConstantExpression>().GetValue(),
              duckdb::count_t(1));
            break;
          default:
            children.push_back(
              {i, duckdb::VectorCache{allocator, child.GetReturnType()},
               std::make_unique<Node>(child, *node_state.child_states[i], input,
                                      allocator)});
            break;
        }
      }
    }

    void Run(uint32_t count, duckdb::Vector& out) {
      for (auto& child : children) {
        auto& slot = args.data[child.arg];
        slot.ResetFromCache(child.cache);
        child.node->Run(count, slot);
      }
      args.SetChildCardinality(count);
      function(args, *state, out);
      duckdb::FlatVector::SetSize(out, count);
    }

    duckdb::scalar_function_t function;
    duckdb::ExpressionState* state = nullptr;
    duckdb::DataChunk args;
    std::vector<Child> children;
  };

  Call(duckdb::ClientContext& ctx, const duckdb::Expression& expr)
    : executor{ctx, expr},
      result_cache{executor.GetAllocator(), expr.GetReturnType()},
      result{result_cache},
      root{expr, *executor.GetStates()[0]->root_state,
           duckdb::Vector{duckdb::LogicalType::VARCHAR,
                          reinterpret_cast<duckdb::data_ptr_t>(values.data()),
                          kBatch},
           executor.GetAllocator()} {}

  void Run(uint32_t count) {
    result.ResetFromCache(result_cache);
    if (root.function) {
      root.Run(count, result);
      return;
    }
    root.args.SetChildCardinality(count);
    executor.ExecuteExpression(root.args, result);
  }

  size_t MemoryUsage() const noexcept {
    return sizeof(Call) + result.GetAllocationSize();
  }

  std::array<duckdb::string_t, kBatch> values;
  std::array<uint32_t, kBatch> rows;
  duckdb::ExpressionExecutor executor;
  duckdb::VectorCache result_cache;
  duckdb::Vector result;
  Node root;
};

SqlTokenizer::SqlTokenizer(Options opts) {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> exprs;
  try {
    exprs = duckdb::Parser::ParseExpressionList(opts.expression);
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

void SqlTokenizer::BindExpression(duckdb::ClientContext& ctx) {
  VerifyFunctionsExist(ctx, *_parsed);
  auto expr = _parsed->Copy();
  auto binder = duckdb::Binder::CreateBinder(ctx);
  duckdb::ColumnList columns;
  columns.AddColumn(duckdb::ColumnDefinition{duckdb::Identifier{kInputColumn},
                                             duckdb::LogicalType::VARCHAR});
  duckdb::physical_index_set_t bound_columns;
  duckdb::CheckBinder check_binder{
    *binder, ctx, duckdb::Identifier{type_name()}, columns, bound_columns};
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
  const bool list =
    type.id() == duckdb::LogicalTypeId::LIST &&
    duckdb::ListType::GetChildType(type).id() == duckdb::LogicalTypeId::VARCHAR;
  if (!list && type.id() != duckdb::LogicalTypeId::VARCHAR) {
    THROW_SQL_ERROR(
      ERR_MSG("sql: expression must return VARCHAR or LIST(VARCHAR), got ",
              type.ToString()));
  }
  _expr = std::move(bound);
  _parsed.reset();
}

TokenTraits SqlTokenizer::Traits() const noexcept {
  return {.unique = _expr != nullptr && _expr->GetReturnType().id() ==
                                          duckdb::LogicalTypeId::VARCHAR};
}

void SqlTokenizer::Bind(duckdb::ClientContext& ctx) {
  if (!_expr) {
    if (ctx.transaction.HasActiveTransaction()) {
      BindExpression(ctx);
    } else {
      ctx.RunFunctionInTransaction([&] { BindExpression(ctx); });
    }
  }
  _call = std::make_unique<Call>(ctx, *_expr);
}

void SqlTokenizer::Unbind() noexcept { _call.reset(); }

size_t SqlTokenizer::MemoryUsage() const noexcept {
  return _call ? _call->MemoryUsage() : 0;
}

bool SqlTokenizer::Fill(const duckdb::string_t& value, TokenSink& sink,
                        FillCtx ctx) {
  SDB_ASSERT(_call);
  auto& call = *_call;
  call.values[0] = value;
  call.Run(1);
  const ResultRows rows{call.result};
  if (!rows.Valid(0)) {
    return false;
  }
  ResolveLayout(ctx.layout,
                [&]<TokenLayout Layout>() { rows.Emit<Layout>(0, sink); });
  return true;
}

void SqlTokenizer::Fill(const duckdb::UnifiedVectorFormat& fmt, uint32_t count,
                        doc_id_t first_doc, TokenSink& sink, FillCtx ctx) {
  SDB_ASSERT(ctx.layout != TokenLayout::TermsPosOffs);
  SDB_ASSERT(_call);
  auto& call = *_call;
  const auto* data =
    duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(fmt);
  ResolveLayout(ctx.layout, [&]<TokenLayout Layout>() {
    for (uint32_t base = 0; base < count; base += kBatch) {
      uint32_t staged = 0;
      ForEachValidRow(fmt, duckdb::idx_t{base}, std::min(kBatch, count - base),
                      [&](uint32_t i, uint32_t idx) {
                        call.values[staged] = data[idx];
                        call.rows[staged] = base + i;
                        ++staged;
                        return true;
                      });
      if (staged == 0) {
        continue;
      }
      call.Run(staged);
      const ResultRows rows{call.result};
      for (uint32_t v = 0; v < staged; ++v) {
        sink.BeginValue(first_doc + call.rows[v], 0);
        rows.Emit<Layout>(v, sink);
        sink.EndValue();
      }
    }
  });
}

}  // namespace irs::analysis
