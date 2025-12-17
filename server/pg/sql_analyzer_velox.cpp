////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/sql_analyzer_velox.h"

#include <absl/functional/overload.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <axiom/logical_plan/Expr.h>
#include <axiom/logical_plan/ExprPrinter.h>
#include <axiom/logical_plan/LogicalPlanNode.h>
#include <axiom/logical_plan/Utils.h>
#include <axiom/optimizer/ConstantExprEvaluator.h>
#include <frozen/unordered_map.h>
#include <velox/core/PlanFragment.h>
#include <velox/core/QueryCtx.h>
#include <velox/exec/AggregateFunctionRegistry.h>
#include <velox/exec/WindowFunction.h>
#include <velox/expression/Expr.h>
#include <velox/expression/SignatureBinder.h>
#include <velox/functions/FunctionRegistry.h>
#include <velox/functions/prestosql/types/IPAddressType.h>
#include <velox/functions/prestosql/types/IPPrefixType.h>
#include <velox/functions/prestosql/types/JsonType.h>
#include <velox/functions/prestosql/types/TimestampWithTimeZoneType.h>
#include <velox/functions/prestosql/types/UuidType.h>
#include <velox/type/SimpleFunctionApi.h>
#include <velox/type/Type.h>
#include <velox/vector/FlatVector.h>

#include <algorithm>
#include <expected>
#include <iresearch/types.hpp>
#include <memory>

#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "basics/utf8_utils.hpp"
#include "catalog/function.h"
#include "catalog/object.h"
#include "catalog/sql_function_impl.h"
#include "catalog/sql_query_view.h"
#include "catalog/table.h"
#include "catalog/virtual_table.h"
#include "pg/pg_ast_visitor.h"
#include "pg/pg_list_utils.h"
#include "pg/sql_collector.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_statement.h"
#include "query/context.h"
#include "query/types.h"
#include "utils/query_string.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"

#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "parser/parse_node.h"
#include "parser/parse_target.h"
#include "postgres_deparse.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg {
namespace {
namespace lp = axiom::logical_plan;
namespace ve = velox::exec;
namespace vc = velox::core;

using pg::ParamIndex;

// Expression kinds for SQL analysis context tracking
// Based on PostgreSQL's ParseExprKind from parse_node.h
enum class ExprKind {
  None = EXPR_KIND_NONE,                     // not in an expression
  Other = EXPR_KIND_OTHER,                   // reserved for extensions
  JoinOn = EXPR_KIND_JOIN_ON,                // JOIN ON
  JoinUsing = EXPR_KIND_JOIN_USING,          // JOIN USING
  FromSubselect = EXPR_KIND_FROM_SUBSELECT,  // sub-SELECT in FROM clause
  FromFunction = EXPR_KIND_FROM_FUNCTION,    // function in FROM clause
  Where = EXPR_KIND_WHERE,                   // WHERE
  Having = EXPR_KIND_HAVING,                 // HAVING
  Filter = EXPR_KIND_FILTER,                 // FILTER
  WindowPartition =
    EXPR_KIND_WINDOW_PARTITION,          // window definition PARTITION BY
  WindowOrder = EXPR_KIND_WINDOW_ORDER,  // window definition ORDER BY
  WindowFrameRange =
    EXPR_KIND_WINDOW_FRAME_RANGE,  // window frame clause with RANGE
  WindowFrameRows =
    EXPR_KIND_WINDOW_FRAME_ROWS,  // window frame clause with ROWS
  WindowFrameGroups =
    EXPR_KIND_WINDOW_FRAME_GROUPS,         // window frame clause with GROUPS
  SelectTarget = EXPR_KIND_SELECT_TARGET,  // SELECT target list item
  InsertTarget = EXPR_KIND_INSERT_TARGET,  // INSERT target list item
  UpdateSource = EXPR_KIND_UPDATE_SOURCE,  // UPDATE assignment source item
  UpdateTarget = EXPR_KIND_UPDATE_TARGET,  // UPDATE assignment target item
  MergeWhen = EXPR_KIND_MERGE_WHEN,        // MERGE WHEN [NOT] MATCHED condition
  GroupBy = EXPR_KIND_GROUP_BY,            // GROUP BY
  OrderBy = EXPR_KIND_ORDER_BY,            // ORDER BY
  DistinctOn = EXPR_KIND_DISTINCT_ON,      // DISTINCT ON
  Limit = EXPR_KIND_LIMIT,                 // LIMIT
  Offset = EXPR_KIND_OFFSET,               // OFFSET
  Returning = EXPR_KIND_RETURNING,         // RETURNING
  Values = EXPR_KIND_VALUES,               // VALUES
  ValuesSingle = EXPR_KIND_VALUES_SINGLE,  // single-row VALUES (in INSERT only)
  CheckConstraint = EXPR_KIND_CHECK_CONSTRAINT,  // CHECK constraint for a table
  DomainCheck = EXPR_KIND_DOMAIN_CHECK,      // CHECK constraint for a domain
  ColumnDefault = EXPR_KIND_COLUMN_DEFAULT,  // default value for a table column
  FunctionDefault =
    EXPR_KIND_FUNCTION_DEFAULT,  // default parameter value for function
  IndexExpression = EXPR_KIND_INDEX_EXPRESSION,  // index expression
  IndexPredicate = EXPR_KIND_INDEX_PREDICATE,    // index predicate
  StatsExpression =
    EXPR_KIND_STATS_EXPRESSION,  // extended statistics expression
  AlterColTransform =
    EXPR_KIND_ALTER_COL_TRANSFORM,  // transform expr in ALTER COLUMN TYPE
  ExecuteParameter = EXPR_KIND_EXECUTE_PARAMETER,  // parameter value in EXECUTE
  TriggerWhen = EXPR_KIND_TRIGGER_WHEN,  // WHEN condition in CREATE TRIGGER
  Policy = EXPR_KIND_POLICY,             // USING or WITH CHECK expr in policy
  PartitionBound = EXPR_KIND_PARTITION_BOUND,  // partition bound expression
  PartitionExpression =
    EXPR_KIND_PARTITION_EXPRESSION,        // PARTITION BY expression
  CallArgument = EXPR_KIND_CALL_ARGUMENT,  // procedure argument in CALL
  CopyWhere = EXPR_KIND_COPY_WHERE,        // WHERE condition in COPY FROM
  GeneratedColumn =
    EXPR_KIND_GENERATED_COLUMN,      // generation expression for a column
  CycleMark = EXPR_KIND_CYCLE_MARK,  // cycle mark value
  AggregateOrder,                    // ORDER BY in aggregate function
  AggregateArgument,                 // arguments of aggregate function
  InsertSelect                       // SELECT in INSERT statement
};

constexpr lp::SpecialForm kSpecialFormPlaceholder{
  std::numeric_limits<std::underlying_type_t<lp::SpecialForm>>::max()};

// axiom special form; doesn't exist in velox
const containers::FlatHashMap<std::string_view, lp::SpecialForm> kSpecialForms{
  {"and", lp::SpecialForm::kAnd},
  {"cast", lp::SpecialForm::kCast},
  {"try_cast", lp::SpecialForm::kTryCast},
  {"coalesce", lp::SpecialForm::kCoalesce},
  {"if", lp::SpecialForm::kIf},
  {"or", lp::SpecialForm::kOr},
  {"switch", lp::SpecialForm::kSwitch},
  {"try", lp::SpecialForm::kTry},
  {"row_constructor", kSpecialFormPlaceholder},
  {"in", lp::SpecialForm::kIn},
};

velox::TypePtr ResolveFunction(const std::string& function_name,
                               const std::vector<velox::TypePtr>& arg_types,
                               std::vector<velox::TypePtr>* arg_coercions) {
  if (arg_coercions) {
    return velox::resolveFunctionOrCallableSpecialFormWithCoercions(
      function_name, arg_types, *arg_coercions);
  }
  return velox::resolveFunctionOrCallableSpecialForm(function_name, arg_types);
}

std::vector<velox::TypePtr> GetExprsTypes(
  std::span<const lp::ExprPtr> arg_exprs, bool use_pg_unknown = true) {
  return arg_exprs | std::views::transform([&](const auto& arg) {
           const auto& arg_type = arg->type();
           // String literal as pg_unknown
           if (use_pg_unknown && arg_type == velox::VARCHAR() &&
               arg->kind() == lp::ExprKind::kConstant) {
             return PG_UNKNOWN();
           }
           return arg->type();
         }) |
         std::ranges::to<std::vector>();
}

velox::TypePtr FixupReturnType(const velox::TypePtr& ret_type) {
  if (!ret_type) {
    return ret_type;
  }
  if (ret_type == PG_UNKNOWN()) {
    return velox::VARCHAR();
  }
  const auto& params = ret_type->parameters();
  const auto params_cnt = params.size();

  std::vector<velox::TypeParameter> new_params;
  bool changed = false;

  new_params.reserve(params_cnt);
  for (const auto& param : params) {
    if (param.kind != velox::TypeParameterKind::kType) {
      new_params.emplace_back(param);
      continue;
    }

    auto new_param_type = FixupReturnType(param.type);
    if (new_param_type != param.type) {
      changed = true;
      new_params.emplace_back(std::move(new_param_type), param.rowFieldName);
    } else {
      new_params.emplace_back(param);
    }
  }
  if (!changed) {
    return ret_type;
  }
  return velox::getType(ret_type->name(), std::move(new_params));
}
velox::TypePtr ResolveFunction(const std::string& function_name,
                               std::span<const lp::ExprPtr> arg_exprs,
                               std::vector<velox::TypePtr>* arg_coercions) {
  if (const auto ret_type = ResolveFunction(
        function_name, GetExprsTypes(arg_exprs), arg_coercions)) {
    return FixupReturnType(ret_type);
  }

  const auto ret_type = ResolveFunction(
    function_name, GetExprsTypes(arg_exprs, false), arg_coercions);
  return FixupReturnType(ret_type);
}

template<typename V>
lp::ExprPtr MakeConst(V v, velox::TypePtr type = nullptr) {
  auto variant = std::make_shared<const velox::Variant>(v);
  if (!type) {
    type = variant->inferType();
  }
  return std::make_shared<lp::ConstantExpr>(std::move(type),
                                            std::move(variant));
}

template<typename T>
std::shared_ptr<const T> MakePtrView(const T* ptr) {
  return std::shared_ptr<const T>(std::shared_ptr<const T>{}, ptr);
}

template<typename T>
std::shared_ptr<const T> MakePtrView(const std::shared_ptr<const T>& ptr) {
  return MakePtrView(ptr.get());
}

void AndToLeft(lp::ExprPtr& left, lp::ExprPtr right) {
  if (!left) {
    left = std::move(right);
    return;
  }
  left = std::make_shared<lp::SpecialFormExpr>(
    velox::BOOLEAN(), lp::SpecialForm::kAnd,
    std::vector<lp::ExprPtr>{std::move(left), std::move(right)});
}

template<typename T>
inline int ExprLocation(const T* node) noexcept {
  return ::exprLocation(reinterpret_cast<const Node*>(node));
}

template<typename T>
std::optional<T> TryGet(const A_Const& expr) {
  if (expr.isnull) {
    return {};
  }
  if constexpr (std::is_same_v<T, int>) {
    if (nodeTag(&expr.val) == T_Integer) {
      return intVal(&expr.val);
    }
  } else if constexpr (std::is_same_v<T, double>) {
    if (nodeTag(&expr.val) == T_Float) {
      return floatVal(&expr.val);
    }
  } else if constexpr (std::is_same_v<T, bool>) {
    if (nodeTag(&expr.val) == T_Boolean) {
      return boolVal(&expr.val);
    }
  } else if constexpr (std::is_same_v<T, std::string_view>) {
    if (nodeTag(&expr.val) == T_String) {
      return strVal(&expr.val);
    }
  } else {
    static_assert(false);
  }
  return {};
}

template<typename T>
std::optional<T> TryGet(const Node* node) {
  SDB_ASSERT(node);
  if (nodeTag(node) == T_A_Const) {
    return TryGet<T>(*castNode(A_Const, node));
  }
  return {};
}

template<typename T>
std::optional<T> TryGet(const List* list, size_t i) {
  if (i < list_length(list)) {
    return TryGet<T>(castNode(Node, list_nth(list, i)));
  }
  return {};
}

std::string_view ToAlias(std::string_view name) {
  return name.substr(0, name.find_last_of(query::kColumnSeparator));
}

std::string ToPgTypeString(const velox::Type& type) {
  return absl::AsciiStrToLower(type.toString());
}

// TODO: temporary solution, this works wrong for some types
std::string ToPgTypeString(const velox::TypePtr& type) {
  if (!type) {
    return "unknown";
  }
  return ToPgTypeString(*type);
}

std::string ToPgSignatureString(const std::vector<lp::ExprPtr>& args,
                                std::string_view sep) {
  const auto types = GetExprsTypes(args);
  return absl::StrJoin(types, sep, [](std::string* out, const auto& type) {
    out->append(ToPgTypeString(type));
  });
}

std::string ToPgOperatorString(std::string_view name,
                               const std::vector<lp::ExprPtr>& args) {
  return ToPgSignatureString(args, name);
}

std::string ToPgFunctionString(std::string_view name,
                               const std::vector<lp::ExprPtr>& args) {
  return absl::StrCat(name, "(", ToPgSignatureString(args, ", "), ")");
}

static const velox::TypePtr kUncastedParamPlaceholder =
  velox::ROW({"<uncasted_param>"}, velox::UNKNOWN());

class ResolveResult {
 public:
  enum class Status : uint8_t {
    Found,
    ColumnNotFound,
    TableNotFound,
    Ambiguous,
  };

  static ResolveResult Found(std::string_view column_name) {
    return {column_name, Status::Found};
  }

  static ResolveResult ColumnNotFound() { return {"", Status::ColumnNotFound}; }

  static ResolveResult TableNotFound(std::string_view table_name) {
    return {table_name, Status::TableNotFound};
  }

  static ResolveResult Ambiguous() { return {"", Status::Ambiguous}; }

  bool IsFound() { return _status == Status::Found; }

  bool IsColumnNotFound() { return _status == Status::ColumnNotFound; }

  bool IsTableNotFound() { return _status == Status::TableNotFound; }

  bool IsAmbiguous() { return _status == Status::Ambiguous; }

  std::string_view GetColumnName() {
    SDB_ASSERT(IsFound());
    return _data;
  }

  std::string_view GetTableName() {
    SDB_ASSERT(IsTableNotFound());
    return _data;
  }

 private:
  ResolveResult(std::string_view data, Status status)
    : _data{data}, _status{status} {}

  std::string_view _data;
  Status _status;
};

using AliasToName = containers::FlatHashMap<std::string_view, std::string_view>;

struct ColumnResolver {
  mutable AliasToName alias_to_name;
  mutable velox::RowTypePtr output = nullptr;

  ResolveResult ResolveColumn(const velox::RowTypePtr& type,
                              std::string_view alias) const {
    if (output != type) {
      if (!output || *output != *type) {
        alias_to_name.clear();
      }
      output = type;
    }
    return ResolveColumn(alias);
  }

  ResolveResult ResolveColumn(std::string_view alias) const {
    if (alias_to_name.empty()) {
      ComputeAliases();
    }
    if (auto it = alias_to_name.find(alias); it != alias_to_name.end()) {
      if (it->second.empty()) {
        return ResolveResult::Ambiguous();
      }
      return ResolveResult::Found(it->second);
    }
    return ResolveResult::ColumnNotFound();
  }

 private:
  void ComputeAliases() const {
    for (const auto& name : output->names()) {
      SDB_ASSERT(name.contains(query::kColumnSeparator));
      const auto alias = ToAlias(name);
      SDB_ASSERT(!alias.empty());
      SDB_ASSERT(!alias.contains(query::kColumnSeparator));
      auto [it, emplaced] = alias_to_name.try_emplace(alias, name);
      if (!emplaced) {
        it->second = {};  // mark as ambiguous
      }
    }
  }
};

struct AliasResolver : ColumnResolver {
  bool HasTables() const { return !_tables.empty(); }

  void ClearTables() { _tables.clear(); }

  void CreateTable(std::string_view alias, velox::RowTypePtr type) {
    auto [it, emplaced] = _tables.try_emplace(alias);
    SDB_ASSERT(emplaced);
    it->second.output = std::move(type);
  }

  const velox::RowType* GetTableOutput(std::string_view table_name) const {
    if (const auto* table = GetTable(table_name)) {
      return table->output.get();
    }
    return nullptr;
  }

  ResolveResult Resolve(const velox::RowTypePtr& type, std::string_view alias) {
    auto [table_alias, column_alias] = SplitTo(alias);
    if (column_alias.empty()) {
      return ResolveColumn(type, alias);
    }
    if (auto* table = GetTable(table_alias)) {
      return table->ResolveColumn(column_alias);
    }
    return ResolveResult::TableNotFound(table_alias);
  }

  void AddTables(AliasResolver&& other) {
    for (auto& [name, resolver] : other._tables) {
      auto [_, emplaced] = _tables.try_emplace(name, std::move(resolver));
      if (!emplaced) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DUPLICATE_TABLE),
          ERR_MSG("table name \"", name, "\" specified more than once"));
      }
    }
  }

  auto TableWithOutputViews() {
    return _tables |
           std::views::transform(
             [](auto& pair) -> std::pair<std::string_view, velox::RowTypePtr&> {
               return {pair.first, pair.second.output};
             });
  }

 private:
  const ColumnResolver* GetTable(std::string_view name) const {
    if (auto it = _tables.find(name); it != _tables.end()) {
      return &it->second;
    }
    return nullptr;
  }

  std::pair<std::string_view, std::string_view> SplitTo(
    std::string_view alias) const {
    return absl::StrSplit(alias, '.');
  }

  containers::FlatHashMap<std::string_view, ColumnResolver> _tables;
};

struct State;

using ColumnRefHook =
  std::function<lp::ExprPtr(std::string_view, const ColumnRef&)>;

struct CTE {
  const CommonTableExpr* node = nullptr;
  // ^ we could use LogicalPlanNode, but axiom renaming logic
  // doesn't consider DAG; so we reprocess CTE each time to make it tree
};

struct State {
  lp::LogicalPlanNodePtr root;

  // names 'hooks' were taken from pg source code
  // they're just functors which we call before/after
  // processing ColumnRef node
  ColumnRefHook pre_columnref_hook;
  ColumnRefHook post_columnref_hook;
  velox::RowTypePtr lookup_columns{nullptr};

  struct Column {
    const velox::TypePtr type;

    // TODO: use string_view
    std::string name;
  };
  containers::FlatHashMap<const FuncCall*, Column> aggregate_or_window;
  // str repr of groupby key -> Column
  containers::FlatHashMap<std::string, Column> groupped_columns;

  // inside of the function refers to passed through parameters
  using FuncParamToExpr =
    containers::FlatHashMap<std::string_view, lp::ExprPtr>;
  const FuncParamToExpr* func_params{nullptr};

  bool has_aggregate{false};
  bool has_groupby{false};
  bool is_sublink{false};

  State* parent{};
  ExprKind expr_kind{};
  AliasResolver resolver;

  containers::FlatHashMap<std::string_view, CTE> ctes;
  const Node* pgsql_node = nullptr;
  const List* options = nullptr;  // list of DefElem

  void Project(UniqueIdGenerator& id_generator, std::vector<std::string> names,
               std::vector<lp::ExprPtr> exprs) {
    SDB_ASSERT(root);
    const auto& input_type = *root->outputType();
    if (!std::ranges::equal(
          std::views::zip(names, exprs), input_type.names(),
          [](const auto& l, const auto& r_name) {
            const auto& [l_name, l_expr] = l;
            return l_expr->isInputReference() &&
                   l_expr->template as<lp::InputReferenceExpr>()->name() ==
                     r_name &&
                   ToAlias(l_name) == ToAlias(r_name);
          })) {
      root = std::make_shared<lp::ProjectNode>(
        id_generator.NextPlanId(), std::move(root), std::move(names),
        std::move(exprs));
    }
  }

  lp::ExprPtr MaybeFuncParam(std::string_view name) {
    if (!func_params) {
      return nullptr;
    }

    if (auto it = func_params->find(name); it != func_params->end()) {
      return it->second;
    }

    return nullptr;
  }

  State MakeChild() {
    State child;
    child.parent = this;
    child.expr_kind = expr_kind;
    child.func_params = func_params;
    return child;
  }
};

const CTE* GetCTE(const State* state, std::string_view name) {
  for (; state; state = state->parent) {
    if (auto it = state->ctes.find(name); it != state->ctes.end()) {
      return &it->second;
    }
  }
  return nullptr;
}

inline void CheckStackOverflow() {
  if (stack_is_too_deep()) {
    THROW_SQL_ERROR((ERR_CODE(ERRCODE_STATEMENT_TOO_COMPLEX),
                     ERR_MSG("stack depth limit exceeded")));
  }
}

class TargetList {
 public:
  void PushBack(std::string_view alias, lp::ExprPtr expr) {
    SDB_ASSERT(expr);
    SDB_ASSERT(!alias.contains(query::kColumnSeparator));
    _entries.emplace_back(expr, alias);
    auto [it, emplaced] = _alias_to_expr.try_emplace(alias, std::move(expr));
    if (!emplaced && !query::Equals(it->second.get(), expr.get())) {
      // TODO: it->second can be "lazy string": variant<ExprPtr, std::string>
      // in Equals we convert it into text and we don't want to do it twice
      it->second = nullptr;  // ambiguous
    }
  }

  void ExpandStar(const velox::RowType& output) {
    for (const auto& [type, name] :
         std::views::zip(output.children(), output.names())) {
      auto expr = std::make_shared<lp::InputReferenceExpr>(type, name);
      PushBack(ToAlias(expr->name()), std::move(expr));
    }
  }

  enum class ResolveError : uint8_t {
    ColumnNotFound,
    Ambiguous,
  };

  std::expected<lp::ExprPtr, ResolveError> Resolve(
    std::string_view name) const {
    if (auto it = _alias_to_expr.find(name); it != _alias_to_expr.end()) {
      if (IsAmbiguous(it->second)) {
        return std::unexpected(ResolveError::Ambiguous);
      }

      return it->second;
    }
    return std::unexpected(ResolveError::ColumnNotFound);
  }

  struct Entry {
    lp::ExprPtr expr;
    std::string_view alias;
  };

  std::vector<Entry> GetEntries() && { return std::move(_entries); }

 private:
  bool IsAmbiguous(const lp::ExprPtr& expr) const { return expr == nullptr; }

  std::vector<Entry> _entries;
  containers::FlatHashMap<std::string_view, lp::ExprPtr> _alias_to_expr;
};

class SqlAnalyzer {
 public:
  explicit SqlAnalyzer(const QueryString& query_sting, const Objects& objects,
                       UniqueIdGenerator& id_generator,
                       query::QueryContext& query_ctx,
                       pg::Params& params) noexcept
    : _objects{objects},
      _query_string{query_sting},
      _id_generator{id_generator},
      _query_ctx{*query_ctx.velox_query_ctx},
      _memory_pool{*query_ctx.query_memory_pool},
      _params{params} {}

  VeloxQuery ProcessRoot(State& state, const Node& node);

 private:
  SqlCommandType ProcessStmt(State& state, const Node& node);

  void ProcessSelectStmt(State& state, const SelectStmt& stmt);
  void ProcessInsertStmt(State& state, const InsertStmt& stmt);
  void ProcessUpdateStmt(State& state, const UpdateStmt& stmt);
  void ProcessDeleteStmt(State& state, const DeleteStmt& stmt);
  void ProcessMergeStmt(State& state, const MergeStmt& stmt);
  void ProcessCreateFunctionStmt(State& state, const CreateFunctionStmt& stmt);
  void ProcessCreateViewStmt(State& state, const ViewStmt& stmt);
  void ProcessCreateStmt(State& state, const CreateStmt& stmt);
  void ProcessCallStmt(State& state, const CallStmt& stmt);

  void ProcessValuesList(State& state, const List* list);
  void ProcessPipeline(State& state, const SelectStmt& stmt);
  void ProcessPipelineSet(State& state, const SelectStmt& stmt);

  void ProcessWithClause(State& state, const WithClause* clause);
  std::string_view ProcessTableColumns(State* parent, const RangeVar* node,
                                       const velox::RowTypePtr& row_type,
                                       std::vector<std::string>& column_names);

  void ProcessFromList(State& state, const List* list);
  State ProcessFromNode(State* parent, const Node* node);
  State ProcessRangeVar(State* parent, const RangeVar* node);
  State ProcessJoinExpr(State* parent, const JoinExpr* node);

  using JoinUsingReturn =
    std::tuple<lp::ExprPtr, std::vector<std::string>, std::vector<lp::ExprPtr>>;

  template<typename UsingList>
  JoinUsingReturn ProcessJoinUsingClause(State& l_state, State& r_state,
                                         const UsingList& using_list);
  State ProcessRangeSubselect(State* parent, const RangeSubselect* node);
  State ProcessRangeFunction(State* parent, const RangeFunction* node);

  std::optional<State> MaybeCTE(State* parent, std::string_view name,
                                const RangeVar* node);
  State ProcessView(State* parent, std::string_view view_name,
                    const SqlQueryView& view, const RangeVar* node);
  State ProcessTable(State* parent, std::string_view schema_name,
                     std::string_view table_name,
                     const Objects::ObjectData& object, const RangeVar* node);

  State ProcessSystemTable(State* parent, std::string_view name,
                           catalog::VirtualTableSnapshot& snapshot,
                           const RangeVar* node);

  void ProcessFilterNode(State& state, const Node* node, ExprKind expr_kind);

  struct CollectedAggregates {
    std::vector<lp::AggregateExprPtr> aggregates;
    std::vector<std::string> names;
  };
  CollectedAggregates CollectAggregateFunctions(State& state,
                                                const List* target_list,
                                                const List* orderby_list,
                                                const Node* having);
  struct CollectedWindows {
    std::vector<lp::WindowExprPtr> windows;
    std::vector<std::string> names;
  };
  CollectedWindows CollectTargetListWindowFunctions(State& state,
                                                    const List* target_list);
  // make a projection with state.root and appended windows from target_list.
  void ProjectTargetListWindows(State& state, const List* target_list);

  void ProcessGroupByList(State& state, const List* groupby,
                          const List* target_list,
                          CollectedAggregates collected);

  // something like a cache to avoid processing expressions twice
  TargetList ProcessTargetList(State& state, const List* target_list);

  void ProjectTargetList(State& state, TargetList target_list);

  void ProcessSortClause(State& state, const List* list,
                         const TargetList& target_list);
  std::vector<lp::SortingField> ProcessSortByList(State& state,
                                                  const List* list,
                                                  ExprKind expr_kind);
  void ProcessLimitNodes(State& state, const Node* limit_offset,
                         const Node* limit_count, LimitOption limit_option);

  void ProcessAlias(State& state, const Alias* alias);

  void ProcessAlias(State& state, const List* alias, const List* cte,
                    std::string_view table);

  std::vector<lp::ExprPtr> ProcessExprList(State& state, const List* list,
                                           ExprKind expr_kind);
  std::vector<lp::ExprPtr> ProcessExprListImpl(State& state, const List* list);

  lp::ExprPtr ProcessExprNode(State& state, const Node* expr,
                              ExprKind expr_kind);
  lp::ExprPtr ProcessExprNodeImpl(State& state, const Node* expr);

  lp::ExprPtr ProcessOp(std::string_view name, lp::ExprPtr lhs, lp::ExprPtr rhs,
                        int location);

  lp::ExprPtr ProcessLikeOp(std::string_view type, lp::ExprPtr input,
                            lp::ExprPtr pattern);

  lp::ExprPtr ProcessMatchOp(std::string_view type, lp::ExprPtr input,
                             lp::ExprPtr pattern);

  lp::ExprPtr MaybeTimeOp(std::string_view op, lp::ExprPtr& lhs,
                          lp::ExprPtr& rhs);

  lp::ExprPtr MaybeIntervalOp(std::string_view op, lp::ExprPtr& lhs,
                              lp::ExprPtr& rhs);

  lp::ExprPtr MakeComparator(std::string_view op, lp::ExprPtr lhs,
                             velox::TypePtr rhs_type, int location);

  lp::ExprPtr ProcessAExpr(State& state, const A_Expr& expr);

  lp::ExprPtr ProcessAConst(State& state, const A_Const& expr);

  lp::ExprPtr ProcessFuncCall(State& state, const FuncCall& expr);

  lp::ExprPtr ProcessWindowFunc(State& state, const FuncCall& func_call);

  lp::AggregateExprPtr MaybeAggregateFuncCall(
    State& state, const catalog::Function& logical_function,
    const FuncCall& func_call);

  lp::WindowExprPtr MaybeWindowFuncCall(
    State& state, const catalog::Function& logical_function,
    const FuncCall& func_call);

  lp::ExprPtr ProcessColumnRef(State& state, const ColumnRef& expr);

  lp::ExprPtr ProcessTypeCast(State& state, const TypeCast& expr);

  lp::ExprPtr ProcessAArrayExpr(State& state, const A_ArrayExpr& expr);

  lp::ExprPtr ProcessBoolExpr(State& state, const BoolExpr& expr);

  lp::ExprPtr ProcessNullTest(State& state, const NullTest& expr);

  lp::ExprPtr ProcessBooleanTest(State& state, const BooleanTest& expr);

  lp::ExprPtr ProcessCoalesceExpr(State& state, const CoalesceExpr& expr);

  lp::ExprPtr ProcessMinMaxExpr(State& state, const MinMaxExpr& expr);

  lp::ExprPtr ProcessCaseExpr(State& state, const CaseExpr& expr);

  lp::ExprPtr ProcessParamRef(State& state, const ParamRef& expr);

  lp::ExprPtr ProcessSubLink(State& state, const SubLink& expr);

  lp::ExprPtr ProcessSQLValueFunction(State& state,
                                      const SQLValueFunction& expr);
  lp::ExprPtr ProcessCollateClause(State& state, const CollateClause& expr);

  lp::ExprPtr ResolveExtract(std::vector<lp::ExprPtr> args);

  lp::ExprPtr ProcessAIndirection(State& state, const A_Indirection& expr);

  lp::ExprPtr ResolveVeloxFunctionAndInferArgsCommonType(
    std::string name, std::vector<lp::ExprPtr> args);

  // Return's a state of the function; We don't want it to
  // be a child because otherwise it'll be possible to lookup
  // parent state columns inside of the function body.
  // TODO(pashandor789): use project + to_array in a subquery (when
  // it will be able to use in axiom) to return multiple columns as a single
  // expression for ProcessFuncCall
  State ResolveSQLFunctionAndInferArgsCommonType(
    const catalog::Function& logical_function, std::vector<lp::ExprPtr> args,
    int location);

  void ProcessFunctionBody(State& state,
                           const State::FuncParamToExpr& func_params,
                           const Node& function_body,
                           const catalog::FunctionSignature& signature);

  lp::ExprPtr MakeCast(velox::TypePtr to, lp::ExprPtr from) {
    if (auto it = _param_to_idx.find(from.get()); it != _param_to_idx.end()) {
      auto param_idx = it->second;
      auto& param_type = _params.types[param_idx - 1];
      // in PG params has a type of the first cast.
      if (param_type == kUncastedParamPlaceholder) {
        param_type = to;
      }
    }

    return std::make_shared<lp::SpecialFormExpr>(
      std::move(to), lp::SpecialForm::kCast, std::move(from));
  }

  void ApplyCoercions(std::span<lp::ExprPtr> args,
                      std::span<const velox::TypePtr> coercions) {
    SDB_ASSERT(coercions.size() <= args.size());
    for (size_t i = 0; i < coercions.size(); ++i) {
      if (coercions[i] && coercions[i] != args[i]->type()) {
        args[i] = MakeCast(coercions[i], std::move(args[i]));
      }
    }
  }

  lp::ExprPtr MakeEquality(lp::ExprPtr left, lp::ExprPtr right) {
    return ResolveVeloxFunctionAndInferArgsCommonType(
      "presto_eq", {std::move(left), std::move(right)});
  }

  ColumnRefHook GetTargetListNamingResolver(
    absl::FunctionRef<const TargetList&()> target_list_getter);

  int ErrorPosition(int location) {
    // TODO(pasha): We should change _query_string when we going into a
    // function/procedure/view body
    if (location < 0 || _query_string.size() < location) {
      return 0;
    }
    // TODO(gnusi): We must honor DB encoding
    return irs::utf8_utils::Length(
      {reinterpret_cast<const irs::byte_type*>(_query_string.data()),
       static_cast<size_t>(location)});
  }

  int ListElementErrorPosition(int default_location, const List* args_list,
                               size_t index) {
    int err_pos = default_location;
    auto* list_node = list_nth(args_list, index);
    if (list_node) {
      err_pos = ErrorPosition(ExprLocation(list_node));
    }
    return err_pos;
  }

  void EnsureRoot(State& state) {
    if (state.root) {
      return;
    }
    auto dummy_row = std::make_shared<velox::RowVector>(
      &_memory_pool, velox::ROW({}), velox::BufferPtr{}, 1,
      std::vector<velox::VectorPtr>{});
    state.root = std::make_shared<lp::ValuesNode>(
      _id_generator.NextPlanId(), std::vector{std::move(dummy_row)});
  }

  void CrossProduct(State& state, lp::LogicalPlanNodePtr other) {
    if (!state.root) {
      state.root = std::move(other);
      return;
    }
    state.root = std::make_shared<lp::JoinNode>(
      _id_generator.NextPlanId(), std::move(state.root), std::move(other),
      lp::JoinType::kInner, nullptr);
  }

  void ErrorTooManyDottedNames(const List* fields, int location) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    CURSOR_POS(ErrorPosition(location)),
                    ERR_MSG("improper qualified name (too many dotted names): ",
                            NameToStr(fields)));
  }

  void ErrorAmbiguousColumn(std::string_view column_name, int location) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_AMBIGUOUS_COLUMN), CURSOR_POS(ErrorPosition(location)),
      ERR_MSG("column reference \"", column_name, "\" is ambiguous"));
  }

  void ErrorColumnDoesNotExist(std::string_view column_name, int location) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    CURSOR_POS(ErrorPosition(location)),
                    ERR_MSG("column \"", column_name, "\" does not exist"));
  }

  void ErrorTableDoesNotExist(std::string_view table_name, int location) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_TABLE), CURSOR_POS(ErrorPosition(location)),
      ERR_MSG("missing FROM-clause entry for table \"", table_name, "\""));
  }

  [[noreturn]] void ErrorIsProcedure(std::string_view name,
                                     const std::vector<lp::ExprPtr>& args,
                                     int location) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                    CURSOR_POS(ErrorPosition(location)),
                    ERR_MSG(ToPgFunctionString(name, args), " is a procedure"),
                    ERR_HINT("To call a procedure, use CALL."));
  }

  [[noreturn]] void ErrorUnsupportedLanguage(
    catalog::FunctionLanguage lang, std::string_view name,
    const std::vector<lp::ExprPtr>& args, int location) {
    THROW_SQL_ERROR(
      ERR_CODE(ERROR_NOT_IMPLEMENTED), CURSOR_POS(ErrorPosition(location)),
      ERR_MSG("unsupported function language for PG, function: ", name,
              ", language: ", magic_enum::enum_name(lang)));
  }

  [[maybe_unused]] const Objects& _objects;
  const QueryString& _query_string;
  UniqueIdGenerator& _id_generator;
  vc::QueryCtx& _query_ctx;
  velox::memory::MemoryPool& _memory_pool;

  pg::Params& _params;
  containers::FlatHashMap<const lp::Expr*, ParamIndex> _param_to_idx;
};

ColumnRefHook SqlAnalyzer::GetTargetListNamingResolver(
  absl::FunctionRef<const TargetList&()> target_list_getter) {
  return [this, target_list_getter](std::string_view name,
                                    const ColumnRef& ref) -> lp::ExprPtr {
    if (name.contains('.')) {
      // here we process only aliased columns from target list
      return nullptr;
    }

    auto resolved = target_list_getter().Resolve(name);
    if (resolved) {
      return std::move(*resolved);
    }

    switch (resolved.error()) {
      using enum TargetList::ResolveError;
      case ColumnNotFound:
        return nullptr;
      case Ambiguous:
        ErrorAmbiguousColumn(name, ExprLocation(&ref));
    }

    SDB_UNREACHABLE();
  };
}

void ValidateAggrInputRefsImpl(State& state, const lp::ExprPtr& expr,
                               AliasResolver& resolver) {
  SDB_ASSERT(state.has_aggregate || state.has_groupby);

  const auto& output = state.root->outputType();
  auto is_grouped_column = [&](std::string_view input_name) {
    auto res = resolver.Resolve(output, ToAlias(input_name));
    return !res.IsColumnNotFound();
  };

  auto visitor = [&](const lp::Expr& node) {
    if (node.isInputReference()) {
      const auto& input = node.as<lp::InputReferenceExpr>();
      if (!is_grouped_column(input->name())) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_GROUPING_ERROR),
                        ERR_MSG("column \"", ToAlias(input->name()),
                                "\" must appear in the GROUP BY "
                                "clause or be used in an aggregate "
                                "function"));
      }
    }
    return true;
  };

  auto ctx = lp::RecursiveExprVisitorContext{};
  ctx.preExprVisitor = std::move(visitor);
  lp::visitExprsRecursively(std::span<const lp::ExprPtr>{expr}, ctx);
}

template<typename T>
void ValidateAggrInputRefs(State& state, std::span<T> exprs) {
  if (!state.has_aggregate && !state.has_groupby) {
    return;
  }

  AliasResolver resolver;
  for (const auto& expr : exprs) {
    if constexpr (requires { expr.expression; }) {
      ValidateAggrInputRefsImpl(state, expr.expression, resolver);
    } else {
      ValidateAggrInputRefsImpl(state, expr, resolver);
    }
  }
}

void SqlAnalyzer::ProcessAlias(State& state, const Alias* alias) {
  SDB_ASSERT(state.root);
  if (alias) {
    state.resolver.ClearTables();
    ProcessAlias(state, alias->colnames, alias->colnames, alias->aliasname);
  }
}

void SqlAnalyzer::ProcessAlias(State& state, const List* new_aliases,
                               const List* old_aliases,
                               std::string_view table) {
  const auto& output = *state.root->outputType();
  const auto table_size = output.size();

  const uint32_t new_aliases_size = list_length(new_aliases);
  if (new_aliases_size > table_size) {
    THROW_SQL_ERROR(ERR_MSG("table \"", table, "\" has ", table_size,
                            " columns available but ", new_aliases_size,
                            " columns specified"));
  }

  std::vector<std::string> names;
  std::vector<lp::ExprPtr> exprs;
  names.reserve(table_size);
  exprs.reserve(table_size);

  uint32_t i = 0;
  auto add_expr = [&](std::string_view alias) {
    names.emplace_back(_id_generator.NextColumnName(alias));
    exprs.push_back(std::make_shared<lp::InputReferenceExpr>(output.childAt(i),
                                                             output.nameOf(i)));
  };
  for (; i < new_aliases_size; ++i) {
    const std::string_view alias = strVal(list_nth(new_aliases, i));
    if (alias.empty()) [[unlikely]] {
      THROW_SQL_ERROR(ERR_MSG("zero-length delimited identifier"));
    }
    add_expr(alias);
  }
  for (const uint32_t old_aliases_size = list_length(old_aliases);
       i < old_aliases_size; ++i) {
    const std::string_view alias =
      strVal(list_nth(old_aliases, i - new_aliases_size));
    add_expr(alias);
  }
  for (; i < table_size; ++i) {
    const auto alias = ToAlias(output.nameOf(i));
    add_expr(alias);
  }

  state.root = std::make_shared<lp::ProjectNode>(
    _id_generator.NextPlanId(), std::move(state.root), std::move(names),
    std::move(exprs));
  state.resolver.CreateTable(
    table, MakePtrView<velox::RowType>(state.root->outputType()));
}

std::pair<std::string_view, query::QueryContext::OptionValue> ConvertToOption(
  const DefElem* option) {
  std::string_view name = option->defname;
  query::QueryContext::OptionValue value;
  SDB_ASSERT(absl::c_none_of(name, absl::ascii_isupper));
  if (!option->arg) {
    return {std::string_view(name), true};
  }
  switch (option->arg->type) {
    case NodeTag::T_Integer:
      value = intVal(option->arg);
      break;
    case NodeTag::T_Boolean:
      value = boolVal(option->arg);
      break;
    case NodeTag::T_String:
      value = strVal(option->arg);
      break;
    case NodeTag::T_Float:
      value = floatVal(option->arg);
      break;
    default:
      SDB_ASSERT(false);
  }
  return {std::string_view(name), value};
}

containers::FlatHashMap<std::string_view, query::QueryContext::OptionValue>
ConvertOptions(const List* options) {
  const size_t options_size = list_length(options);
  containers::FlatHashMap<std::string_view, query::QueryContext::OptionValue>
    res;
  res.reserve(options_size);
  for (size_t i = 0; i < options_size; ++i) {
    res.insert(ConvertToOption(list_nth_node(DefElem, options, i)));
  }
  return res;
}

void SqlAnalyzer::ProcessSelectStmt(State& state, const SelectStmt& stmt) {
  if (stmt.lockingClause) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "LOCK clause is not implemented yet");
  }
  ProcessWithClause(state, stmt.withClause);

  if (stmt.valuesLists) {
    ProcessValuesList(state, stmt.valuesLists);
    ProcessSortClause(state, stmt.sortClause, {});
  } else if (stmt.op == SETOP_NONE) {
    if (stmt.intoClause) {
      SDB_THROW(ERROR_NOT_IMPLEMENTED, "INTO clause is not implemented yet");
    }
    ProcessPipeline(state, stmt);
  } else {
    ProcessPipelineSet(state, stmt);
    ProcessSortClause(state, stmt.sortClause, {});
  }

  ProcessLimitNodes(state, stmt.limitOffset, stmt.limitCount, stmt.limitOption);
  // TODO: ProcessFinalProject
}

// It's literally UNKNOWN
// but have different address to distinguish it from UNKNOWN().
const velox::UnknownType kDefaultValueTypePlaceHolder{};
const auto kDefaultValueTypePlaceHolderPtr =
  MakePtrView<velox::UnknownType>(&kDefaultValueTypePlaceHolder);
const lp::CallExpr kDefaultValuePlaceHolder{
  kDefaultValueTypePlaceHolderPtr, "presto_fail",
  MakeConst("DEFAULT is not allowed in this context")};
const auto kDefaultValuePlaceHolderPtr =
  MakePtrView<lp::CallExpr>(&kDefaultValuePlaceHolder);

void SqlAnalyzer::ProcessInsertStmt(State& state, const InsertStmt& stmt) {
  if (stmt.returningList) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "RETURNING clause is not implemented yet");
  }
  if (stmt.onConflictClause) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "ON CONFLICT clause is not implemented yet");
  }
  if (stmt.override != OVERRIDING_NOT_SET) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "OVERRIDING clause is not implemented yet");
  }
  ProcessWithClause(state, stmt.withClause);

  if (stmt.selectStmt) {
    // We need a child state here because both the INSERT statement and its
    // SELECT sub-statement can have their own CTEs
    auto child_state = state.MakeChild();
    child_state.expr_kind = ExprKind::InsertSelect;
    ProcessStmt(child_state, *stmt.selectStmt);
    state.root = std::move(child_state.root);
  } else {
    // INSERT ... DEFAULT VALUES
    EnsureRoot(state);
  }

  const auto& relation = *stmt.relation;
  const auto schema_name = absl::NullSafeStringView(relation.schemaname);
  const std::string_view table_name = relation.relname;
  auto object = _objects.getData(schema_name, table_name);
  SDB_ASSERT(object);
  SDB_ASSERT(object->object);
  const auto& logical_object = *object->object;
  if (logical_object.GetType() != catalog::ObjectType::Table) {
    // TODO: write more pg-like error message
    THROW_SQL_ERROR(
      ERR_CODE(ERROR_SERVER_OBJECT_TYPE_MISMATCH),
      CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
      ERR_MSG(
        "INSERT statement is only applicable for tables, but the object is: ",
        magic_enum::enum_name(logical_object.GetType())));
  }

  const auto& table = basics::downCast<catalog::Table>(logical_object);
  const auto& table_type = *table.RowType();
  std::vector<std::string> column_names;
  std::vector<lp::ExprPtr> column_exprs;
  column_names.reserve(table_type.size());
  column_exprs.reserve(table_type.size());

  const uint32_t explicit_columns = list_length(stmt.cols);
  const auto& input_type = *state.root->outputType();
  if (explicit_columns > input_type.size()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("INSERT has more target columns than expressions"),
                    CURSOR_POS(ErrorPosition(ExprLocation(&stmt))));
  }
  if ((stmt.cols ? explicit_columns : table_type.size()) < input_type.size()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("INSERT has more expressions than target columns"),
                    CURSOR_POS(ErrorPosition(ExprLocation(&stmt))));
  }
  containers::FlatHashSet<std::string_view> unique_aliases;
  unique_aliases.reserve(input_type.size());
  for (uint32_t i = 0; i < input_type.size(); ++i) {
    if (input_type.childAt(i) == kDefaultValueTypePlaceHolderPtr) {
      continue;  // will be handled in the loop below
    }
    if (stmt.cols) {
      const std::string_view name = strVal(list_nth(stmt.cols, i));
      if (!table_type.containsChild(name)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                        CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
                        ERR_MSG("column \"", name, "\" of relation \"",
                                table_name, "\" does not exist"));
      }
      auto [_, emplaced] = unique_aliases.emplace(name);
      if (!emplaced) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
          CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
          ERR_MSG("column \"", name, "\" specified more than once"));
      }
      column_names.emplace_back(name);
    } else {
      column_names.emplace_back(table_type.names()[i]);
    }
    lp::ExprPtr raw_expr = std::make_shared<lp::InputReferenceExpr>(
      input_type.childAt(i), input_type.nameOf(i));
    const auto& table_column_type = table_type.findChild(column_names.back());
    if (raw_expr->type() != table_column_type) {
      raw_expr = MakeCast(table_column_type, std::move(raw_expr));
    }
    column_exprs.push_back(std::move(raw_expr));
  }

  // set default value for not mentioned columns
  for (const auto& column : table.Columns()) {
    if (!absl::c_linear_search(column_names, column.name)) {
      lp::ExprPtr expr;
      if (const auto& default_value = column.default_value) {
        expr = ProcessExprNodeImpl(state, default_value->GetExpr());
      } else {
        expr = MakeConst(velox::TypeKind::UNKNOWN, column.type);
      }
      column_names.emplace_back(column.name);
      column_exprs.emplace_back(std::move(expr));
    }
  }

  object->EnsureTable();

  state.root = std::make_shared<lp::TableWriteNode>(
    _id_generator.NextPlanId(), std::move(state.root), object->table,
    axiom::connector::WriteKind::kInsert, std::move(column_names),
    std::move(column_exprs));
}

void SqlAnalyzer::ProcessUpdateStmt(State& state, const UpdateStmt& stmt) {
  if (stmt.returningList) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "RETURNING clause is not implemented yet");
  }

  if (stmt.fromClause) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "FROM clause is not implemented yet");
  }

  if (stmt.withClause) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "WITH clause is not implemented yet");
  }

  const auto& relation = *stmt.relation;
  const auto schema_name = absl::NullSafeStringView(relation.schemaname);
  auto object = _objects.getData(schema_name, relation.relname);
  SDB_ASSERT(object);
  SDB_ASSERT(object->object);
  const auto& logical_object = *object->object;
  if (logical_object.GetType() != catalog::ObjectType::Table) {
    // TODO: write more pg-like error message
    THROW_SQL_ERROR(
      ERR_CODE(ERROR_SERVER_OBJECT_TYPE_MISMATCH),
      CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
      ERR_MSG(
        "UPDATE statement is only applicable for tables, but the object is: ",
        magic_enum::enum_name(logical_object.GetType())));
  }

  const std::string_view table_name = relation.relname;

  auto table_state =
    ProcessTable(&state, schema_name, table_name, *object, &relation);
  state.root = std::move(table_state.root);

  ProcessFilterNode(state, stmt.whereClause, ExprKind::Where);

  const auto& table = basics::downCast<catalog::Table>(logical_object);
  const auto& pk_type = *table.PKType();
  std::vector<std::string> column_names;
  std::vector<lp::ExprPtr> column_exprs;
  column_names.reserve(pk_type.size() + list_length(stmt.targetList));
  column_exprs.reserve(pk_type.size() + list_length(stmt.targetList));

  const auto& output_type = state.root->outputType();

  for (const auto& [name, type] :
       std::ranges::views::zip(pk_type.names(), pk_type.children())) {
    auto column = state.resolver.Resolve(output_type, name);
    SDB_ASSERT(column.IsFound());
    std::string resolved{column.GetColumnName()};
    auto expr =
      std::make_shared<lp::InputReferenceExpr>(type, std::move(resolved));
    column_exprs.emplace_back(std::move(expr));
    column_names.emplace_back(name);
  }

  VisitNodes(stmt.targetList, [&](const ResTarget& target) {
    if (target.indirection) {
      SDB_THROW(ERROR_NOT_IMPLEMENTED,
                "Indirection in UPDATE target list is not implemented yet");
    }

    column_names.emplace_back(target.name);
    auto expr = ProcessExprNode(state, target.val, ExprKind::UpdateSource);
    const auto& table_column_type =
      table.RowType()->findChild(column_names.back());
    if (expr->type()->kind() != table_column_type->kind()) {
      expr = MakeCast(table_column_type, std::move(expr));
    }
    column_exprs.emplace_back(std::move(expr));
  });

  object->EnsureTable();

  state.root = std::make_shared<lp::TableWriteNode>(
    _id_generator.NextPlanId(), std::move(state.root), object->table,
    axiom::connector::WriteKind::kUpdate, std::move(column_names),
    std::move(column_exprs));
}

void SqlAnalyzer::ProcessDeleteStmt(State& state, const DeleteStmt& stmt) {
  if (stmt.returningList) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "RETURNING clause is not implemented yet");
  }

  if (stmt.usingClause) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "USING clause is not implemented yet");
  }

  if (stmt.withClause) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED, "WITH clause is not implemented yet");
  }

  const auto& relation = *stmt.relation;
  const auto schema_name = absl::NullSafeStringView(relation.schemaname);
  auto object = _objects.getData(schema_name, relation.relname);
  SDB_ASSERT(object);
  SDB_ASSERT(object->object);
  const auto& logical_object = *object->object;
  if (logical_object.GetType() != catalog::ObjectType::Table) {
    // TODO: write more pg-like error message
    THROW_SQL_ERROR(
      ERR_CODE(ERROR_SERVER_OBJECT_TYPE_MISMATCH),
      CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
      ERR_MSG(
        "DELETE statement is only applicable for tables , but the object is: ",
        magic_enum::enum_name(logical_object.GetType())));
  }

  const std::string_view table_name = relation.relname;

  auto table_state =
    ProcessTable(&state, schema_name, table_name, *object, &relation);
  state.root = std::move(table_state.root);

  ProcessFilterNode(state, stmt.whereClause, ExprKind::Where);

  const auto& table = basics::downCast<catalog::Table>(logical_object);
  const auto& pk_type = *table.PKType();
  std::vector<std::string> column_names;
  std::vector<lp::ExprPtr> column_exprs;
  column_names.reserve(pk_type.size());
  column_exprs.reserve(pk_type.size());

  const auto& output_type = state.root->outputType();
  for (const auto& [name, type] :
       std::ranges::views::zip(pk_type.names(), pk_type.children())) {
    auto column = state.resolver.Resolve(output_type, name);
    SDB_ASSERT(column.IsFound());
    std::string resolved{column.GetColumnName()};
    auto expr =
      std::make_shared<lp::InputReferenceExpr>(type, std::move(resolved));
    column_exprs.emplace_back(std::move(expr));
    column_names.emplace_back(name);
  }

  object->EnsureTable();

  state.root = std::make_shared<lp::TableWriteNode>(
    _id_generator.NextPlanId(), std::move(state.root), object->table,
    axiom::connector::WriteKind::kDelete, std::move(column_names),
    std::move(column_exprs));
}

void SqlAnalyzer::ProcessMergeStmt(State& state, const MergeStmt& stmt) {
  SDB_THROW(ERROR_NOT_IMPLEMENTED, "MERGE statement is not implemented yet");
}

void SqlAnalyzer::ProcessCallStmt(State& state, const CallStmt& stmt) {
  const auto& func_call = *stmt.funccall;
  const auto [_, schema, name] = GetDbSchemaRelation(func_call.funcname);
  auto* function = _objects.getData(schema, name);
  if (!function) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                    CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
                    ERR_MSG("Function '", name, "' is not resolved"));
  }

  auto args = ProcessExprListImpl(state, func_call.args);

  auto& logical_function =
    basics::downCast<catalog::Function>(*function->object);
  const auto language = logical_function.Options().language;
  if (language != catalog::FunctionLanguage::SQL) {
    ErrorUnsupportedLanguage(language, name, args, ExprLocation(&stmt));
  }

  const auto& signature = logical_function.Signature();
  if (!signature.IsProcedure()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
                    ERR_MSG(name, " is not a procedure"));
  }

  state = ResolveSQLFunctionAndInferArgsCommonType(
    logical_function, std::move(args), ExprLocation(&func_call));

  // body may be a statement which doesn't return any value (DDL for example)
  if (state.root) {
    // CALL returns void
    state.root = std::make_shared<lp::ProjectNode>(
      _id_generator.NextPlanId(), std::move(state.root),
      std::vector<std::string>{}, std::vector<lp::ExprPtr>{});
  }
}

// Just validate a body
void SqlAnalyzer::ProcessCreateFunctionStmt(State& state,
                                            const CreateFunctionStmt& stmt) {
  // TODO: validate also function body from AS in options (look at
  // create_function.cpp ParseOptions for details)
  if (stmt.sql_body) {
    if (IsA(stmt.sql_body, ReturnStmt)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
                      ERR_MSG("Return statement is not supported"));
    } else {
      SDB_ASSERT(IsA(stmt.sql_body, List));

      const auto* list = castNode(List, stmt.sql_body);
      if (list_length(list) != 1) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
          ERR_MSG("Function body is only implemented for a single statement"));
      }

      auto* inner_list_node = list_nth_node(Node, list, 0);
      SDB_ASSERT(IsA(inner_list_node, List));
      const auto* inner_list = castNode(List, inner_list_node);
      SDB_ASSERT(list_length(inner_list) == 1);

      State::FuncParamToExpr dummy_args;
      dummy_args.reserve(list_length(stmt.parameters));
      auto signature = pg::ToSignature(stmt.parameters, stmt.returnType);
      for (const auto& param : signature.parameters) {
        auto expr =
          std::make_shared<lp::InputReferenceExpr>(param.type, param.name);
        dummy_args.emplace(param.name, std::move(expr));
      }
      const auto& function_body = *list_nth_node(Node, inner_list, 0);
      ProcessFunctionBody(state, dummy_args, function_body, signature);
    }
  }
}

// Just validate a body
void SqlAnalyzer::ProcessCreateViewStmt(State& state, const ViewStmt& stmt) {
  if (stmt.options) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
                    ERR_MSG("VIEW options are not implemented yet"));
  }

  State dummy{};
  auto type = ProcessStmt(dummy, *stmt.query);
  SDB_ASSERT(type == SqlCommandType::Select);
  SDB_ASSERT(dummy.root);
  SDB_ASSERT(dummy.root->outputType());
  const auto& output_type = *dummy.root->outputType();
  containers::FlatHashSet<std::string_view> unique_aliases;
  for (const auto& name : output_type.names()) {
    auto alias = ToAlias(name);
    auto [_, emplaced] = unique_aliases.emplace(alias);
    if (!emplaced) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
        CURSOR_POS(ErrorPosition(ExprLocation(&stmt))),
        ERR_MSG("column \"", alias, "\" specified more than once"));
    }
  }
}

// Just validate some parts
void SqlAnalyzer::ProcessCreateStmt(State& state, const CreateStmt& stmt) {
  State dummy{};
  EnsureRoot(dummy);

  VisitNodes(stmt.tableElts, [&](const Node& node) {
    if (IsA(&node, ColumnDef)) {
      const auto& col_def = *castNode(ColumnDef, &node);
      VisitNodes(col_def.constraints, [&](const Constraint& constraint) {
        switch (constraint.contype) {
          case CONSTR_DEFAULT: {
            auto column_type = NameToType(*col_def.typeName);
            auto expr = ProcessExprNode(dummy, constraint.raw_expr,
                                        ExprKind::ColumnDefault);
            if (expr->type() != column_type) {
              THROW_SQL_ERROR(
                ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                CURSOR_POS(ErrorPosition(ExprLocation(&constraint))),
                ERR_MSG("column \"", col_def.colname, "\" is of type ",
                        ToPgTypeString(column_type),
                        " but default expression is of type ",
                        ToPgTypeString(expr->type())),
                ERR_HINT("You will need to rewrite or cast the expression."));
            }
          } break;
          default:
        }
      });
    }
  });
}

SqlCommandType SqlAnalyzer::ProcessStmt(State& state, const Node& node) {
  switch (node.type) {
    case T_SelectStmt: {
      const auto& stmt = *castNode(SelectStmt, &node);
      ProcessSelectStmt(state, stmt);
      return SqlCommandType::Select;
    }
    case T_InsertStmt: {
      const auto& stmt = *castNode(InsertStmt, &node);
      ProcessInsertStmt(state, stmt);
      return SqlCommandType::Insert;
    }
    case T_UpdateStmt: {
      const auto& stmt = *castNode(UpdateStmt, &node);
      ProcessUpdateStmt(state, stmt);
      return SqlCommandType::Update;
    }
    case T_DeleteStmt: {
      const auto& stmt = *castNode(DeleteStmt, &node);
      ProcessDeleteStmt(state, stmt);
      return SqlCommandType::Delete;
    }
    case T_MergeStmt: {
      const auto& stmt = *castNode(MergeStmt, &node);
      ProcessMergeStmt(state, stmt);
      return SqlCommandType::Merge;
    }
    case T_CallStmt: {
      const auto& stmt = *castNode(CallStmt, &node);
      ProcessCallStmt(state, stmt);
      return SqlCommandType::Call;
    }
    case T_ExplainStmt: {
      const auto& stmt = *castNode(ExplainStmt, &node);
      state.options = stmt.options;
      ProcessStmt(state, *stmt.query);
      return SqlCommandType::Explain;
    }
    case T_ViewStmt: {  // CREATE VIEW
      const auto& stmt = *castNode(ViewStmt, &node);
      ProcessCreateViewStmt(state, stmt);
      state.pgsql_node = &node;
      return SqlCommandType::DDL;
    }
    case T_CreateFunctionStmt: {
      const auto& stmt = *castNode(CreateFunctionStmt, &node);
      ProcessCreateFunctionStmt(state, stmt);
      state.pgsql_node = &node;
      return SqlCommandType::DDL;
    }
    case T_CreateStmt: {
      const auto& stmt = *castNode(CreateStmt, &node);
      ProcessCreateStmt(state, stmt);
      state.pgsql_node = &node;
      return SqlCommandType::DDL;
    }
    case T_CreateRoleStmt:
    case T_DropRoleStmt:
    case T_CreatedbStmt:
    case T_DropdbStmt:
    case T_CreateSchemaStmt:
    case T_DropStmt: {
      state.pgsql_node = &node;
      return SqlCommandType::DDL;
    }
    case T_VariableSetStmt: {
      state.pgsql_node = &node;
      return SqlCommandType::Set;
    }
    case T_TransactionStmt: {
      state.pgsql_node = &node;
      return SqlCommandType::Transaction;
    }
    case T_VariableShowStmt: {
      state.pgsql_node = &node;
      return SqlCommandType::Show;
    }
    default:
      SDB_ENSURE(false, ERROR_INTERNAL);
      return SqlCommandType::Unknown;
  }
}

void SqlAnalyzer::ProcessWithClause(State& state, const WithClause* clause) {
  if (!clause) {
    return;
  }
  if (clause->recursive) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("recursive WITH query is not supported"));
  }

  VisitNodes(clause->ctes, [&](const CommonTableExpr& cte) {
    std::string_view ctename = cte.ctename;

    const auto* ctequery = cte.ctequery;
    SDB_ASSERT(ctequery);

    auto child_state = state.MakeChild();
    ProcessStmt(child_state, *ctequery);

    if (cte.aliascolnames) {
      const uint32_t cte_aliases_size = list_length(cte.aliascolnames);
      const auto table_size = child_state.root->outputType()->size();
      if (cte_aliases_size > table_size) {
        THROW_SQL_ERROR(ERR_MSG("WITH query \"", ctename, "\" has ", table_size,
                                " columns available but ", cte_aliases_size,
                                " columns specified"));
      }
      for (uint32_t i = 0; i < cte_aliases_size; ++i) {
        const std::string_view alias = strVal(list_nth(cte.aliascolnames, i));
        if (alias.empty()) {
          THROW_SQL_ERROR(ERR_MSG("zero-length delimited identifier"));
        }
      }
    }

    auto [_, emplaced] = state.ctes.emplace(ctename, CTE{&cte});
    if (!emplaced) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_ALIAS),
        CURSOR_POS(ErrorPosition(ExprLocation(&cte))),
        ERR_MSG("WITH query name \"", ctename, "\" specified more than once"));
    }
  });
}

void SqlAnalyzer::ProjectTargetList(State& state, TargetList target_list) {
  auto entries = std::move(target_list).GetEntries();

  std::vector<std::string> names;
  std::vector<lp::ExprPtr> exprs;
  names.reserve(entries.size());
  exprs.reserve(entries.size());

  for (auto&& [expr, alias] : entries) {
    std::string name = _id_generator.NextColumnName(alias);
    names.emplace_back(std::move(name));
    exprs.emplace_back(std::move(expr));
  }

  ValidateAggrInputRefs(state, std::span{exprs});
  state.Project(_id_generator, std::move(names), std::move(exprs));
  // after the projection table scopes become invalid in PG
  state.resolver.ClearTables();
}

std::vector<lp::SortingField> SqlAnalyzer::ProcessSortByList(
  State& state, const List* list, ExprKind expr_kind) {
  const auto size = list_length(list);
  if (size == 0) {
    return {};
  }

  std::vector<lp::SortingField> fields;
  fields.reserve(size);

  VisitNodes(list, [&](const SortBy& sort_by) {
    auto expr = ProcessExprNode(state, sort_by.node, expr_kind);

    const auto dir = sort_by.sortby_dir;
    SDB_ASSERT(dir != SORTBY_USING);
    const bool ascending = dir == SORTBY_DEFAULT || dir == SORTBY_ASC;
    const auto nulls = sort_by.sortby_nulls;
    const bool nulls_first = nulls == SORTBY_NULLS_FIRST ||
                             (!ascending && nulls == SORTBY_NULLS_DEFAULT);

    fields.emplace_back(std::move(expr), lp::SortOrder{ascending, nulls_first});
  });

  return fields;
}

void SqlAnalyzer::ProcessSortClause(State& state, const List* list,
                                    const TargetList& target_list) {
  auto getter = [&] -> const TargetList& { return target_list; };
  // for order by first we try to resolve columns in target list
  state.pre_columnref_hook = GetTargetListNamingResolver(getter);
  auto fields = ProcessSortByList(state, list, ExprKind::OrderBy);
  state.pre_columnref_hook = nullptr;
  if (fields.empty()) {
    return;
  }
  ValidateAggrInputRefs(state, std::span{fields});
  state.root = std::make_shared<lp::SortNode>(
    _id_generator.NextPlanId(), std::move(state.root), std::move(fields));
}

template<typename T>
T GetInt(const velox::Variant& v) {
  switch (v.kind()) {
    case velox::TypeKind::TINYINT:
      return static_cast<T>(v.value<velox::TypeKind::TINYINT>());
    case velox::TypeKind::SMALLINT:
      return static_cast<T>(v.value<velox::TypeKind::SMALLINT>());
    case velox::TypeKind::INTEGER:
      return static_cast<T>(v.value<velox::TypeKind::INTEGER>());
    case velox::TypeKind::BIGINT:
      return static_cast<T>(v.value<velox::TypeKind::BIGINT>());
    default:
      VELOX_UNREACHABLE();
  }
}

void SqlAnalyzer::ProcessLimitNodes(State& state, const Node* limit_offset,
                                    const Node* limit_count,
                                    LimitOption limit_option) {
  if (!limit_offset && !limit_count) {
    return;
  }

  if (limit_option == LIMIT_OPTION_WITH_TIES) {
    SDB_THROW(ERROR_BAD_PARAMETER, "Unsupported LIMIT with ties");
  }

  int64_t limit_offset_value = 0;
  if (limit_offset) {
    try {
      auto expr = ProcessExprNode(state, limit_offset, ExprKind::Offset);
      auto variant =
        axiom::optimizer::ConstantExprEvaluator::evaluateConstantExpr(*expr);
      limit_offset_value = GetInt<int64_t>(variant);
    } catch (...) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      CURSOR_POS(ErrorPosition(ExprLocation(limit_offset))),
                      ERR_MSG("OFFSET value must be a constant bigint"));
    }
  }

  int64_t limit_count_value = std::numeric_limits<int64_t>::max();
  if (limit_count) {
    try {
      auto expr = ProcessExprNode(state, limit_count, ExprKind::Limit);
      auto variant =
        axiom::optimizer::ConstantExprEvaluator::evaluateConstantExpr(*expr);
      limit_count_value = GetInt<int64_t>(variant);
    } catch (...) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      CURSOR_POS(ErrorPosition(ExprLocation(limit_count))),
                      ERR_MSG("LIMIT value must be a constant bigint"));
    }
  }

  state.root = std::make_shared<lp::LimitNode>(
    _id_generator.NextPlanId(), std::move(state.root), limit_offset_value,
    limit_count_value);
}

void SqlAnalyzer::ProcessValuesList(State& state, const List* list) {
  int row_size = -1;
  bool const_values = true;
  std::vector<velox::Variant> columns_values;
  std::vector<velox::Variant> row_values;
  std::vector<std::vector<lp::ExprPtr>> values;
  row_values.reserve(list_length(list));
  values.reserve(list_length(list));
  VisitNodes(list, [&](const List& row) {
    if (row_size < 0) {
      row_size = list_length(&row);
    } else if (row_size != list_length(&row)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      CURSOR_POS(ErrorPosition(ExprLocation(&row))),
                      ERR_MSG("VALUES lists must all be the same length"));
    }
    auto& value = values.emplace_back();
    value.reserve(row_size);
    if (const_values) {
      columns_values.clear();
      columns_values.reserve(row_size);
    }
    VisitNodes(&row, [&](const Node& node) {
      auto expr = ProcessExprNode(state, &node, ExprKind::Values);
      if (const_values) {
        // TODO: replace try catch with condition
        try {
          auto column_value =
            axiom::optimizer::ConstantExprEvaluator::evaluateConstantExpr(
              *expr);
          columns_values.push_back(std::move(column_value));
        } catch (...) {
          const_values = false;
        }
      }
      value.emplace_back(std::move(expr));
    });
    if (const_values) {
      row_values.emplace_back(velox::Variant::row(std::move(columns_values)));
    }
  });
  if (row_size < 0) {
    return;
  }
  std::vector<std::string> names;
  static constexpr std::string_view kPrefix = "column";
  // TODO Maybe we don't want to reuse unique column id
  const auto id = _id_generator.NextColumnId();
  for (int i = 0; i < row_size; ++i) {
    std::string alias = absl::StrCat(kPrefix, i + 1);
    std::string name = absl::StrCat(alias, query::kColumnSeparator, id);
    names.emplace_back(std::move(name));
  }

  if (const_values) {
    std::vector<velox::TypePtr> types;
    types.reserve(row_size);
    for (const auto& value : values.back()) {
      types.push_back(value->type());
    }
    state.root = std::make_shared<lp::ValuesNode>(
      _id_generator.NextPlanId(),
      velox::ROW(std::move(names), std::move(types)), std::move(row_values));
    return;
  }

  std::vector<lp::LogicalPlanNodePtr> values_nodes;
  values_nodes.reserve(values.size());
  for (auto& value : values) {
    State value_state;
    value_state.parent = &state;
    value_state.expr_kind = ExprKind::Values;
    EnsureRoot(value_state);
    value_state.root = std::make_shared<lp::ProjectNode>(
      _id_generator.NextPlanId(), std::move(value_state.root), names,
      std::move(value));
    values_nodes.emplace_back(std::move(value_state.root));
  }

  // TODO: Create an issue about ExpandNode can be used when it's just access +
  // const to optimize large VALUES clause. Also we can extend ExpandNode to
  // support more expressions. Also we need to support ExpandNode in axiom.

  SDB_ASSERT(!state.root);
  SDB_ASSERT(!values_nodes.empty());
  if (values_nodes.size() == 1) {
    state.root = std::move(values_nodes.front());
  } else {
    state.root = std::make_shared<lp::SetNode>(_id_generator.NextPlanId(),
                                               std::move(values_nodes),
                                               lp::SetOperation::kUnionAll);
  }
}

TargetList SqlAnalyzer::ProcessTargetList(State& state, const List* tlist) {
  TargetList target_list;

  const auto& output_type =
    state.lookup_columns ? *state.lookup_columns : *state.root->outputType();
  auto process_star = [&](const ResTarget* n, const ColumnRef* cref) {
    VisitName(
      cref->fields,
      absl::Overload{
        [&](auto /* star */) {
          if (output_type.children().empty()) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_UNDEFINED_TABLE),
              CURSOR_POS(ErrorPosition(ExprLocation(cref))),
              ERR_MSG("SELECT * with no tables specified is not valid"));
          }
          target_list.ExpandStar(output_type);
        },
        [&](auto table, auto /* star */) {
          const auto* output = state.resolver.GetTableOutput(table);
          if (!output) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_UNDEFINED_TABLE),
              CURSOR_POS(ErrorPosition(ExprLocation(cref))),
              ERR_MSG("missing FROM-clause entry for table \"", table, "\""));
          }
          target_list.ExpandStar(*output);
        },
        [&](auto schema, auto table, auto /* star */) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                          CURSOR_POS(ErrorPosition(ExprLocation(cref))),
                          ERR_MSG("schema-qualified * is not supported"));
        },
        [&](auto db, auto schema, auto table, auto /* star */) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                          CURSOR_POS(ErrorPosition(ExprLocation(cref))),
                          ERR_MSG("database-qualified * is not supported"));
        },
        [&](...) { ErrorTooManyDottedNames(cref->fields, ExprLocation(cref)); },
      });
  };

  const auto figure_colname = [](const ResTarget& target,
                                 const lp::Expr& expr) -> std::string_view {
    if (target.name) {
      return target.name;
    }

    // FigureColname is supposed to do this; but it expects
    // a node from the next PG stage, so we handle SubLink here.
    if (target.val->type == T_SubLink) {
      const auto& sublink = *castNode(SubLink, target.val);
      if (sublink.subLinkType == EXPR_SUBLINK) {
        SDB_ASSERT(expr.isSubquery());
        const auto& subquery = expr.as<lp::SubqueryExpr>()->subquery();
        const auto& names = subquery->outputType()->names();
        SDB_ASSERT(names.size() == 1);
        return ToAlias(names.front());
      }
    }

    return FigureColname(target.val);
  };

  VisitNodes(tlist, [&](const ResTarget& n) {
    switch (const auto* expr = n.val; expr->type) {
      case T_ColumnRef:
        if (const auto* cref = castNode(ColumnRef, expr);
            IsA(llast(cref->fields), A_Star)) {
          process_star(&n, cref);
          return;
        }
        [[fallthrough]];
      default: {
        auto expr = ProcessExprNode(state, n.val, ExprKind::SelectTarget);
        auto name = figure_colname(n, *expr);
        target_list.PushBack(name, std::move(expr));
      }
    }
  });

  return target_list;
}

lp::AggregateExprPtr SqlAnalyzer::MaybeAggregateFuncCall(
  State& state, const catalog::Function& logical_function,
  const FuncCall& func_call) {
  if (!logical_function.Options().IsAggregate() || func_call.over) {
    return nullptr;
  }

  auto func_args =
    ProcessExprList(state, func_call.args, ExprKind::AggregateArgument);

  switch (state.expr_kind) {
    case ExprKind::AggregateOrder:
      [[fallthrough]];
    case ExprKind::AggregateArgument:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_GROUPING_ERROR),
                      CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
                      ERR_MSG("aggregate function calls cannot be nested"));
    case ExprKind::FromFunction:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_GROUPING_ERROR),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
        ERR_MSG("aggregate functions are not allowed in functions in FROM"));
    case ExprKind::Where:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_GROUPING_ERROR),
                      CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
                      ERR_MSG("aggregate functions are not allowed in WHERE"));
    case ExprKind::JoinOn:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_GROUPING_ERROR),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
        ERR_MSG("aggregate functions are not allowed in JOIN conditions"));
    case ExprKind::GroupBy:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_GROUPING_ERROR),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
        ERR_MSG("aggregate functions are not allowed in GROUP BY"));
    case ExprKind::Filter:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_GROUPING_ERROR),
                      CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
                      ERR_MSG("aggregate functions are not allowed in FILTER"));
    case ExprKind::Limit:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_GROUPING_ERROR),
                      CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
                      ERR_MSG("aggregate functions are not allowed in LIMIT"));
    case ExprKind::Offset:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_GROUPING_ERROR),
                      CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
                      ERR_MSG("aggregate functions are not allowed in OFFSET"));
    case ExprKind::ColumnDefault:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_GROUPING_ERROR),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
        ERR_MSG("aggregate functions are not allowed in DEFAULT expressions"));
    default:
      break;
  }

  std::string aggr_func_name{logical_function.GetName()};
  auto type = FixupReturnType(
    ve::resolveResultType(aggr_func_name, GetExprsTypes(func_args)));

  auto filter_expr =
    ProcessExprNode(state, func_call.agg_filter, ExprKind::Filter);
  auto order_by =
    ProcessSortByList(state, func_call.agg_order, ExprKind::AggregateOrder);

  return std::make_shared<lp::AggregateExpr>(
    std::move(type), std::move(aggr_func_name), std::move(func_args),
    std::move(filter_expr), std::move(order_by), func_call.agg_distinct);
}

std::string GroupByExprToName(const lp::Expr& aggr) {
  auto name = lp::ExprPrinter::toText(aggr);
  static_assert(!query::kColumnSeparator.contains('_'));
  absl::StrReplaceAll({{query::kColumnSeparator, "_"}}, &name);
  return name;
}

SqlAnalyzer::CollectedAggregates SqlAnalyzer::CollectAggregateFunctions(
  State& state, const List* target_list, const List* orderby_list,
  const Node* having) {
  const auto saved_expr_kind = std::exchange(state.expr_kind, ExprKind::None);
  irs::Finally restore_state = [&] noexcept {
    state.expr_kind = saved_expr_kind;
  };

  CollectedAggregates result;
  auto& func_to_aggr = state.aggregate_or_window;
  auto visit_func = [&](const FuncCall& func_call) {
    const auto [_, schema, func_name] = GetDbSchemaRelation(func_call.funcname);
    const auto* func = _objects.getData(schema, func_name);
    if (!func) {
      return;
    }
    const auto& logical_function =
      basics::downCast<catalog::Function>(*func->object);
    const auto& aggr_expr =
      MaybeAggregateFuncCall(state, logical_function, func_call);
    if (!aggr_expr) {
      return;
    }

    auto name = _id_generator.NextColumnName(func_name);
    auto [_, emplaced] =
      func_to_aggr.try_emplace(&func_call, aggr_expr->type(), name);
    SDB_ASSERT(emplaced);
    result.names.emplace_back(std::move(name));
    result.aggregates.emplace_back(std::move(aggr_expr));
  };

  AstVisitor<FuncCall> visitor;
  visitor.VisitList(target_list, visit_func);
  visitor.VisitList(orderby_list, visit_func);
  visitor.Visit(having, visit_func);

  SDB_ASSERT(result.aggregates.size() == result.names.size());
  return result;
}

SqlAnalyzer::CollectedWindows SqlAnalyzer::CollectTargetListWindowFunctions(
  State& state, const List* target_list) {
  const auto saved_expr_kind = std::exchange(state.expr_kind, ExprKind::None);
  irs::Finally restore_state = [&] noexcept {
    state.expr_kind = saved_expr_kind;
  };

  CollectedWindows result;

  auto& func_to_window = state.aggregate_or_window;
  auto visit_func = [&](const FuncCall& func_call) {
    auto [_, schema, func_name] = GetDbSchemaRelation(func_call.funcname);
    const auto* func = _objects.getData(schema, func_name);
    if (!func) {
      return;
    }
    const auto& logical_function =
      basics::downCast<catalog::Function>(*func->object);
    const auto& window_expr =
      MaybeWindowFuncCall(state, logical_function, func_call);
    if (!window_expr) {
      return;
    }

    auto name = _id_generator.NextColumnName(func_name);
    auto [_, emplaced] =
      func_to_window.try_emplace(&func_call, window_expr->type(), name);
    SDB_ASSERT(emplaced);
    result.windows.emplace_back(std::move(window_expr));
    result.names.emplace_back(std::move(name));
  };

  AstVisitor<FuncCall> visitor;
  visitor.VisitList(target_list, visit_func);

  SDB_ASSERT(result.windows.size() == result.names.size());
  return result;
}

void SqlAnalyzer::ProjectTargetListWindows(State& state,
                                           const List* target_list) {
  auto collected = CollectTargetListWindowFunctions(state, target_list);
  SDB_ASSERT(collected.windows.size() == collected.names.size());
  if (collected.windows.empty()) {
    return;
  }

  ValidateAggrInputRefs(state, std::span{collected.windows});
  const auto& output_type = *state.root->outputType();
  size_t size = collected.windows.size() + output_type.size();
  std::vector<std::string> names;
  std::vector<lp::ExprPtr> exprs;
  names.reserve(size);
  exprs.reserve(size);

  for (const auto& [name, type] :
       std::views::zip(output_type.names(), output_type.children())) {
    names.emplace_back(name);
    exprs.push_back(std::make_shared<lp::InputReferenceExpr>(type, name));
  }

  for (auto&& [window, name] :
       std::views::zip(collected.windows, collected.names)) {
    names.emplace_back(std::move(name));
    exprs.push_back(std::move(window));
  }

  state.root = std::make_shared<lp::ProjectNode>(
    _id_generator.NextPlanId(), std::move(state.root), std::move(names),
    std::move(exprs));
}

void SqlAnalyzer::ProcessGroupByList(State& state, const List* groupby,
                                     const List* tlist,
                                     CollectedAggregates collected) {
  if (!state.has_aggregate && !state.has_groupby) {
    return;
  }

  std::optional<TargetList> target_list;
  auto getter = [&]() -> const TargetList& {
    if (!target_list) {
      target_list = ProcessTargetList(state, tlist);
    }
    return *target_list;
  };
  // we try to resolve columns in target list if they weren't found
  // in root
  state.post_columnref_hook = GetTargetListNamingResolver(getter);
  auto grouping_keys = ProcessExprList(state, groupby, ExprKind::GroupBy);
  state.post_columnref_hook = nullptr;

  std::vector<std::string> output_names;
  output_names.reserve(grouping_keys.size() + collected.aggregates.size());
  for (size_t i = 0; i < grouping_keys.size(); ++i) {
    std::string name;
    if (grouping_keys[i]->isInputReference()) {
      // we don't want to keep an old name otherwise table scope will be invalid
      name = grouping_keys[i]->as<lp::InputReferenceExpr>()->name();
    } else {
      name = _id_generator.NextColumnName(
        absl::StrCat("groupby_expr_", _id_generator.NextColumnId()));
    }
    auto [_, emplaced] = state.groupped_columns.try_emplace(
      GroupByExprToName(*grouping_keys[i]), grouping_keys[i]->type(), name);
    SDB_ASSERT(emplaced);
    output_names.emplace_back(std::move(name));
  }
  output_names.insert(output_names.end(),
                      std::make_move_iterator(collected.names.begin()),
                      std::make_move_iterator(collected.names.end()));

  // this will help us to resolve columns after
  // aggregation removed all the columns in the output_type
  state.lookup_columns = MakePtrView<velox::RowType>(state.root->outputType());

  state.root = std::make_shared<lp::AggregateNode>(
    _id_generator.NextPlanId(), std::move(state.root), std::move(grouping_keys),
    std::vector<lp::AggregateNode::GroupingSet>{},
    std::move(collected.aggregates), std::move(output_names));
}

void SqlAnalyzer::ProcessPipeline(State& state, const SelectStmt& stmt) {
  ProcessFromList(state, stmt.fromClause);
  EnsureRoot(state);
  ProcessFilterNode(state, stmt.whereClause, ExprKind::Where);

  auto collected = CollectAggregateFunctions(
    state, stmt.targetList, stmt.sortClause, stmt.havingClause);
  state.has_groupby = list_length(stmt.groupClause) > 0;
  state.has_aggregate = !collected.aggregates.empty();
  ProcessGroupByList(state, stmt.groupClause, stmt.targetList,
                     std::move(collected));
  ProcessFilterNode(state, stmt.havingClause, ExprKind::Having);

  // PG OrderBy has access to items before target list
  // That's why we put it before the target list projection.
  // Also it's the reason why we need to project windows
  // before order by - because sort and project nodes are mixed up.
  ProjectTargetListWindows(state, stmt.targetList);
  auto target_list = ProcessTargetList(state, stmt.targetList);
  ProcessSortClause(state, stmt.sortClause, target_list);
  ProjectTargetList(state, std::move(target_list));
}

void SqlAnalyzer::ProcessPipelineSet(State& state, const SelectStmt& stmt) {
  SDB_ASSERT(stmt.op != SETOP_NONE);
  SDB_ASSERT(stmt.larg);
  SDB_ASSERT(stmt.rarg);

  auto l_state = state.MakeChild();
  auto l_query_type = ProcessStmt(l_state, *castNode(Node, stmt.larg));
  SDB_ASSERT(l_query_type == SqlCommandType::Select);
  auto r_state = state.MakeChild();
  auto r_query_type = ProcessStmt(r_state, *castNode(Node, stmt.rarg));
  SDB_ASSERT(r_query_type == SqlCommandType::Select);

  const auto set_operation_type = [&] {
    switch (stmt.op) {
      case SETOP_UNION:
        if (stmt.all) {
          return lp::SetOperation::kUnionAll;
        } else {
          return lp::SetOperation::kUnion;
        }
      case SETOP_INTERSECT:
        if (stmt.all) {
          // TODO: implement, I'm also not sure that current intersect isn't all
          SDB_THROW(ERROR_NOT_IMPLEMENTED,
                    "INTERSECT ALL is not implemented yet");
        } else {
          return lp::SetOperation::kIntersect;
        }
      case SETOP_EXCEPT:
        if (stmt.all) {
          // TODO: implement, I'm also not sure that current except isn't all
          SDB_THROW(ERROR_NOT_IMPLEMENTED, "EXCEPT ALL is not implemented yet");
        } else {
          return lp::SetOperation::kExcept;
        }
      default:
        SDB_THROW(ERROR_NOT_IMPLEMENTED);
    }
  }();

  auto& l_output = l_state.root->outputType();
  auto& r_output = r_state.root->outputType();
  if (l_output->children() != r_output->children()) {
    SDB_THROW(ERROR_BAD_PARAMETER,
              "UNION ALL requires both sides to have the same number of "
              "columns and same types, but got: ",
              l_output->size(), " and ", r_output->size());
  }
  state.resolver = std::move(l_state.resolver);
  state.resolver.ClearTables();
  SDB_ASSERT(!state.root);
  state.root = std::make_shared<lp::SetNode>(_id_generator.NextPlanId(),
                                             std::vector{
                                               std::move(l_state.root),
                                               std::move(r_state.root),
                                             },
                                             set_operation_type);
}

void SqlAnalyzer::ProcessFromList(State& state, const List* list) {
  AliasResolver resolver;
  VisitNodes(list, [&](const Node& node) {
    auto child_state = ProcessFromNode(&state, &node);
    CrossProduct(state, child_state.root);
    resolver.AddTables(std::move(child_state.resolver));
  });
  state.resolver = std::move(resolver);
}

State SqlAnalyzer::ProcessFromNode(State* parent, const Node* node) {
  SDB_ASSERT(node);
  switch (node->type) {
    case T_RangeVar:
      return ProcessRangeVar(parent, castNode(RangeVar, node));
    case T_JoinExpr:
      return ProcessJoinExpr(parent, castNode(JoinExpr, node));
    case T_RangeSubselect:
      return ProcessRangeSubselect(parent, castNode(RangeSubselect, node));
    case T_RangeFunction:
      return ProcessRangeFunction(parent, castNode(RangeFunction, node));
    default:
      SDB_UNREACHABLE();
  }
}

std::optional<State> SqlAnalyzer::MaybeCTE(State* parent, std::string_view name,
                                           const RangeVar* node) {
  const auto* cte = GetCTE(parent, name);
  if (!cte) {
    return std::nullopt;
  }

  SDB_ASSERT(cte->node);
  auto state = parent->MakeChild();
  ProcessStmt(state, *cte->node->ctequery);
  if (node->alias) {
    ProcessAlias(state, node->alias->colnames, cte->node->aliascolnames,
                 node->alias->aliasname);
  } else {
    // rewrite column's ids to avoid velox names conflicts
    // (for ex: select * from cte1, cte1)
    ProcessAlias(state, cte->node->aliascolnames, cte->node->aliascolnames,
                 node->relname);
  }
  return state;
}

State SqlAnalyzer::ProcessView(State* parent, std::string_view view_name,
                               const SqlQueryView& view, const RangeVar* node) {
  auto view_state = view.GetState();
  SDB_ASSERT(view_state->stmt);
  SDB_ASSERT(view_state->stmt->stmt);

  auto state = parent->MakeChild();
  auto subquery_type = ProcessStmt(state, *view_state->stmt->stmt);
  SDB_ASSERT(subquery_type == SqlCommandType::Select);
  SDB_ASSERT(!state.resolver.HasTables());
  // ^ is supposed to be cleared in the project target list

  if (node->alias) {
    ProcessAlias(state, node->alias);
  } else {
    state.resolver.CreateTable(
      view_name, MakePtrView<velox::RowType>(state.root->outputType()));
  }

  return state;
}

std::string_view SqlAnalyzer::ProcessTableColumns(
  State* parent, const RangeVar* node, const velox::RowTypePtr& row_type,
  std::vector<std::string>& column_names) {
  const auto& type = *row_type;
  std::string_view table_alias = node->relname;
  if (node->alias) {
    const auto* aliases = node->alias->colnames;
    const uint32_t aliases_size = list_length(aliases);
    if (aliases_size > type.size()) {
      THROW_SQL_ERROR(ERR_MSG("table \"", table_alias, "\" has ", type.size(),
                              " columns available but ", aliases_size,
                              " columns specified"));
    }
    table_alias = node->alias->aliasname;
    for (uint32_t i = 0; i < aliases_size; ++i) {
      const std::string_view alias = strVal(list_nth(aliases, i));
      if (alias.empty()) [[unlikely]] {
        THROW_SQL_ERROR(ERR_MSG("zero-length delimited identifier"));
      }
      column_names.emplace_back(_id_generator.NextColumnName(alias));
    }
  }
  for (size_t i = column_names.size(); i < type.size(); ++i) {
    column_names.emplace_back(_id_generator.NextColumnName(type.nameOf(i)));
  }
  return table_alias;
}

State SqlAnalyzer::ProcessTable(State* parent, std::string_view schema_name,
                                std::string_view table_name,
                                const Objects::ObjectData& object,
                                const RangeVar* node) {
  const auto& table = basics::downCast<catalog::Table>(*object.object);
  const auto& type = *table.RowType();
  std::vector<std::string> column_names;
  column_names.reserve(type.size());
  std::string_view table_alias =
    ProcessTableColumns(parent, node, table.RowType(), column_names);

  object.EnsureTable();

  auto state = parent->MakeChild();
  state.root = std::make_shared<lp::TableScanNode>(
    _id_generator.NextPlanId(),
    velox::ROW(std::move(column_names), type.children()), object.table,
    type.names());
  state.resolver.CreateTable(
    table_alias, MakePtrView<velox::RowType>(state.root->outputType()));
  return state;
}

State SqlAnalyzer::ProcessSystemTable(State* parent, std::string_view name,
                                      catalog::VirtualTableSnapshot& snapshot,
                                      const RangeVar* node) {
  const auto& row_type = snapshot.RowType();
  SDB_ASSERT(row_type);
  std::vector<std::string> column_names;
  column_names.reserve(row_type->size());
  std::string_view table_alias =
    ProcessTableColumns(parent, node, row_type, column_names);
  auto state = parent->MakeChild();
  auto data = snapshot.GetData(std::move(column_names), _memory_pool);
  state.root = std::make_shared<lp::ValuesNode>(
    _id_generator.NextPlanId(), std::vector<velox::RowVectorPtr>{data});
  state.resolver.CreateTable(table_alias, state.root->outputType());
  return state;
}

State SqlAnalyzer::ProcessRangeVar(State* parent, const RangeVar* node) {
  const std::string_view name = node->relname;
  if (auto cte_state = MaybeCTE(parent, name, node)) {
    return *cte_state;
  }

  const auto schema_name = absl::NullSafeStringView(node->schemaname);
  auto object = _objects.getData(schema_name, name);
  SDB_ASSERT(object);
  SDB_ASSERT(object->object);
  auto& logical_object = *object->object;

  if (logical_object.GetType() == catalog::ObjectType::View) {
    const auto& view = basics::downCast<SqlQueryView>(*object->object);
    return ProcessView(parent, name, view, node);
  } else if (logical_object.GetType() == catalog::ObjectType::Table) {
    return ProcessTable(parent, schema_name, name, *object, node);
  } else if (logical_object.GetType() == catalog::ObjectType::Virtual) {
    // Only for system tables now
    auto& snapshot =
      basics::downCast<catalog::VirtualTableSnapshot>(logical_object);
    return ProcessSystemTable(parent, snapshot.GetTable().Name(), snapshot,
                              node);
  }

  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  CURSOR_POS(ErrorPosition(ExprLocation(node))),
                  ERR_MSG("object '", name, "' of type '",
                          magic_enum::enum_name(logical_object.GetType()),
                          "' cannot be used in FROM clause"));
}

template<typename UsingList>
SqlAnalyzer::JoinUsingReturn SqlAnalyzer::ProcessJoinUsingClause(
  State& l_state, State& r_state, const UsingList& using_list) {
  if (using_list.empty()) {
    return JoinUsingReturn(nullptr, std::vector<std::string>{},
                           std::vector<lp::ExprPtr>{});
  }
  auto& l_resolver = l_state.resolver;
  auto& r_resolver = r_state.resolver;

  const auto& l_output = l_state.root->outputType();
  const auto& r_output = r_state.root->outputType();

  static constexpr std::string_view kLeft = "left";
  static constexpr std::string_view kRight = "right";
  const auto resolve_using =
    [](AliasResolver& resolver, const velox::RowTypePtr& output,
       std::string_view column, std::string_view side) -> size_t {
    auto res = resolver.Resolve(output, column);
    if (res.IsAmbiguous()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_AMBIGUOUS_COLUMN),
                      ERR_MSG("common column name \"", column,
                              "\" appears more than once in ", side, " table"));
    }
    if (res.IsColumnNotFound()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                      ERR_MSG("column \"", column,
                              "\" specified in USING clause does "
                              "not exist in ",
                              side, " table"));
    }

    SDB_ASSERT(res.IsFound());
    return output->getChildIdx(res.GetColumnName());
  };

  const auto make_input_ref =
    [](const velox::RowTypePtr& output,
       size_t column_idx) -> lp::InputReferenceExprPtr {
    auto name = output->nameOf(column_idx);
    auto type = output->childAt(column_idx);
    return std::make_shared<lp::InputReferenceExpr>(std::move(type),
                                                    std::move(name));
  };

  // using projects columns as [common, other_left, other_right]
  // projection invalidates table scopes, so we take common columns names
  // from the left side (left side will be valid) and to make the right side
  // valid we rename its common columns to the left side names
  containers::FlatHashMap<std::string_view, std::string> r_renames;

  std::vector<std::string> project_names;
  std::vector<lp::ExprPtr> project_exprs;
  project_names.reserve(l_output->size() + r_output->size());
  project_exprs.reserve(l_output->size() + r_output->size());
  lp::ExprPtr join_condition;
  for (const auto using_column : using_list) {
    auto l_column_idx =
      resolve_using(l_resolver, l_output, using_column, kLeft);
    auto r_column_idx =
      resolve_using(r_resolver, r_output, using_column, kRight);
    auto l_column_ref = make_input_ref(l_output, l_column_idx);
    auto r_column_ref = make_input_ref(r_output, r_column_idx);

    std::string_view common_column = l_column_ref->name();
    project_names.emplace_back(common_column);
    project_exprs.emplace_back(l_column_ref);
    auto [_, emplaced] = r_renames.emplace(r_column_ref->name(), common_column);
    SDB_ASSERT(emplaced);

    auto eq = MakeEquality(std::move(l_column_ref), std::move(r_column_ref));
    AndToLeft(join_condition, std::move(eq));
  }

  // projects as pg [common (already in vector), other_left, other_right]
  auto using_project = [&](const velox::RowTypePtr& output) {
    for (const auto& [name, type] :
         std::ranges::views::zip(output->names(), output->children())) {
      auto alias = ToAlias(name);
      if (absl::c_linear_search(using_list, alias)) {
        continue;
      }
      auto column_ref = std::make_shared<lp::InputReferenceExpr>(type, name);
      project_names.emplace_back(column_ref->name());
      project_exprs.emplace_back(std::move(column_ref));
    }
  };
  using_project(l_output);
  using_project(r_output);

  const auto maybe_rename =
    [&](const velox::RowType& output) -> velox::RowTypePtr {
    const auto& output_names = output.names();
    if (std::ranges::find_if(output_names, [&](const auto& name) {
          return r_renames.find(name) != r_renames.end();
        }) == output_names.end()) {
      return nullptr;
    }

    auto renamed_names =
      output_names |
      std::views::transform([&](const auto& name) -> std::string {
        auto it = r_renames.find(name);
        return it == r_renames.end() ? name : it->second;
      }) |
      std::ranges::to<std::vector>();
    return velox::ROW(std::move(renamed_names), output.children());
  };

  for (auto [r_table, r_output] : r_resolver.TableWithOutputViews()) {
    if (auto r_renamed_output = maybe_rename(*r_output)) {
      r_output = r_renamed_output;
    }
  }

  return JoinUsingReturn(std::move(join_condition), std::move(project_names),
                         std::move(project_exprs));
}

State SqlAnalyzer::ProcessJoinExpr(State* parent, const JoinExpr* node) {
  SDB_ASSERT(node);
  SDB_ASSERT(node->larg);
  SDB_ASSERT(node->rarg);

  const auto join_type = [&] {
    switch (node->jointype) {
      case JOIN_INNER:
        return lp::JoinType::kInner;
      case JOIN_LEFT:
        return lp::JoinType::kLeft;
      case JOIN_RIGHT:
        return lp::JoinType::kRight;
      case JOIN_FULL:
        return lp::JoinType::kFull;
      default:
        // other postgres join types doesn't appear in parser
        SDB_ENSURE(false, ERROR_INTERNAL, "Unsupported join type: ",
                   magic_enum::enum_name(node->jointype));
    }
  }();

  auto l_state = ProcessFromNode(parent, node->larg);
  auto r_state = ProcessFromNode(parent, node->rarg);

  lp::ExprPtr join_condition;
  std::vector<std::string> project_names;
  std::vector<lp::ExprPtr> project_exprs;
  if (list_length(node->usingClause) > 0) {
    SDB_ASSERT(!node->isNatural);
    SDB_ASSERT(!node->quals);
    const auto using_columns = PgStrListWrapper(node->usingClause);
    std::tie(join_condition, project_names, project_exprs) =
      ProcessJoinUsingClause(l_state, r_state, using_columns);
  } else if (node->isNatural) {
    SDB_ASSERT(!node->usingClause);
    SDB_ASSERT(!node->quals);
    const auto& l_output = l_state.root->outputType();
    const auto& r_output = r_state.root->outputType();
    auto& r_resolver = r_state.resolver;
    std::vector<std::string_view> common_columns;
    for (const auto& name : l_output->names()) {
      std::string_view alias = ToAlias(name);
      auto res = r_resolver.Resolve(r_output, alias);
      if (!res.IsColumnNotFound()) {
        SDB_ASSERT(!res.IsTableNotFound());
        common_columns.emplace_back(alias);
      }
    }

    std::tie(join_condition, project_names, project_exprs) =
      ProcessJoinUsingClause(l_state, r_state, common_columns);
  }

  auto join_node = std::make_shared<lp::JoinNode>(
    _id_generator.NextPlanId(), std::move(l_state.root),
    std::move(r_state.root), join_type, nullptr);
  auto* join_node_ptr = join_node.get();
  l_state.root = std::move(join_node);
  l_state.resolver.AddTables(std::move(r_state.resolver));
  if (node->quals) {
    SDB_ASSERT(!node->isNatural);
    SDB_ASSERT(!node->usingClause);
    SDB_ASSERT(!join_condition);
    join_condition = ProcessExprNode(l_state, node->quals, ExprKind::JoinOn);
  }
  join_node_ptr->setJoinCondition(std::move(join_condition));

  SDB_ASSERT(project_names.size() == project_exprs.size());
  if (!project_exprs.empty()) {
    l_state.root = std::make_shared<lp::ProjectNode>(
      _id_generator.NextPlanId(), std::move(l_state.root),
      std::move(project_names), std::move(project_exprs));
  }

  ProcessAlias(l_state, node->alias);

  return l_state;
}

State SqlAnalyzer::ProcessRangeSubselect(State* parent,
                                         const RangeSubselect* node) {
  SDB_ASSERT(node);
  SDB_ASSERT(node->subquery);
  auto subquery_state = parent->MakeChild();
  auto subquery_type = ProcessStmt(subquery_state, *node->subquery);
  SDB_ASSERT(subquery_type == SqlCommandType::Select);
  SDB_ASSERT(!subquery_state.resolver.HasTables());
  // ^ is supposed to be cleared in the project target list
  ProcessAlias(subquery_state, node->alias);
  return subquery_state;
}

State SqlAnalyzer::ProcessRangeFunction(State* parent,
                                        const RangeFunction* node) {
  SDB_ASSERT(node);
  SDB_ASSERT(node->functions);

  std::vector<lp::ExprPtr> unnest_exprs;
  std::vector<std::vector<std::string>> unnested_names;
  std::vector<std::string> project_names;
  std::vector<lp::ExprPtr> project_exprs;

  auto functions_state = parent->MakeChild();

  bool sql_func = false;
  VisitNodes(node->functions, [&](const List& func_columns) {
    SDB_ASSERT(list_length(&func_columns) == 2);
    auto* func = linitial(&func_columns);
    switch (nodeTag(func)) {
      case T_FuncCall: {
        auto* func_call = castNode(FuncCall, func);
        auto args = ProcessExprList(functions_state, func_call->args,
                                    ExprKind::FromFunction);

        // TODO fix function resolve
        auto [_, schema, func_name] = GetDbSchemaRelation(func_call->funcname);
        auto* function = _objects.getData(schema, func_name);
        if (!function) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
            CURSOR_POS(ErrorPosition(ExprLocation(func_call))),
            ERR_MSG("Function '", func_name, "' is not resolved"));
        }

        auto& logical_function =
          basics::downCast<catalog::Function>(*function->object);
        if (logical_function.Options().IsAggregate()) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_GROUPING_ERROR),
            CURSOR_POS(ErrorPosition(ExprLocation(func_call))),
            ERR_MSG(
              "aggregate functions are not allowed in functions in FROM"));
        }
        auto name = logical_function.GetName();

        if (logical_function.Signature().IsProcedure()) {
          ErrorIsProcedure(name, args, ExprLocation(func_call));
        }

        const auto lang = logical_function.Options().language;
        if (lang == catalog::FunctionLanguage::VeloxNative) {
          auto func_expr = ResolveVeloxFunctionAndInferArgsCommonType(
            std::string{name}, std::move(args));
          args.push_back(std::move(func_expr));
        } else if (lang == catalog::FunctionLanguage::SQL) {
          if (list_length(node->functions) != 1) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
              CURSOR_POS(ErrorPosition(ExprLocation(func_call))),
              ERR_MSG(
                "User-defined SQL functions are not supported in ROWS FROM"));
          }
          functions_state = ResolveSQLFunctionAndInferArgsCommonType(
            logical_function, std::move(args), ExprLocation(func_call));
          if (!logical_function.Signature().ReturnsTable()) {
            SDB_ASSERT(functions_state.root &&
                       functions_state.root->outputType());
            const auto& output_type = *functions_state.root->outputType();

            SDB_ASSERT(output_type.size() == 1);
            args.emplace_back(std::make_shared<lp::InputReferenceExpr>(
              output_type.childAt(0), output_type.nameOf(0)));
          }
          sql_func = true;
        } else if (lang != catalog::FunctionLanguage::Decorator) {
          ErrorUnsupportedLanguage(lang, name, args, ExprLocation(func_call));
        }

        const auto* column_definitions = castNode(List, lsecond(&func_columns));
        const auto column_definitions_size = list_length(column_definitions);
        for (size_t i = 0; i < args.size(); ++i) {
          const int column = static_cast<int>(i);
          auto alias = func_name;
          if (column < column_definitions_size) {
            const auto* column_definition =
              list_nth_node(ColumnDef, column_definitions, column);
            SDB_ASSERT(column_definition);
            SDB_ASSERT(column_definition->colname);
            alias = column_definition->colname;
          }
          const bool table = logical_function.Options().table;
          const auto& type = *args[i]->type();
          if (table && type.size() != 0) {
            unnest_exprs.emplace_back(std::move(args[i]));
            auto& names = unnested_names.emplace_back();
            names.reserve(type.size());
            for (size_t j = 0; j != type.size(); ++j) {
              names.emplace_back(_id_generator.NextColumnName(alias));
              project_exprs.emplace_back(
                std::make_shared<lp::InputReferenceExpr>(type.childAt(j),
                                                         names.back()));
              project_names.emplace_back(names.back());
            }
          } else {
            project_exprs.emplace_back(std::move(args[i]));
            project_names.emplace_back(_id_generator.NextColumnName(alias));
          }
        }
      } break;
      default:
        SDB_ASSERT(false);
        break;
    }
  });

  EnsureRoot(functions_state);

  if (!unnest_exprs.empty()) {
    functions_state.root = std::make_shared<lp::UnnestNode>(
      _id_generator.NextPlanId(), std::move(functions_state.root),
      std::move(unnest_exprs), std::move(unnested_names), std::nullopt);
  }

  if (!project_exprs.empty()) {
    functions_state.root = std::make_shared<lp::ProjectNode>(
      _id_generator.NextPlanId(), std::move(functions_state.root),
      std::move(project_names), std::move(project_exprs));
  } else {
    SDB_ASSERT(sql_func);
  }
  ProcessAlias(functions_state, node->alias);
  return functions_state;
}

void SqlAnalyzer::ProcessFilterNode(State& state, const Node* node,
                                    ExprKind expr_kind) {
  if (node) {
    auto condition = ProcessExprNode(state, node, expr_kind);
    ValidateAggrInputRefs(state, std::span{&condition, 1});
    state.root = std::make_shared<lp::FilterNode>(
      _id_generator.NextPlanId(), std::move(state.root), std::move(condition));
  }
}

std::vector<lp::ExprPtr> SqlAnalyzer::ProcessExprList(State& state,
                                                      const List* list,
                                                      ExprKind expr_kind) {
  const auto saved_expr_kind = std::exchange(state.expr_kind, expr_kind);
  irs::Finally restore_state = [&] noexcept {
    state.expr_kind = saved_expr_kind;
  };
  return ProcessExprListImpl(state, list);
}

std::vector<lp::ExprPtr> SqlAnalyzer::ProcessExprListImpl(State& state,
                                                          const List* list) {
  std::vector<lp::ExprPtr> result;
  result.reserve(list_length(list));

  VisitNodes(list, [&](const Node& n) {
    auto expr = ProcessExprNodeImpl(state, &n);
    SDB_ASSERT(expr);
    result.emplace_back(std::move(expr));
  });

  return result;
}

lp::ExprPtr SqlAnalyzer::ProcessExprNode(State& state, const Node* node,
                                         ExprKind expr_kind) {
  const auto saved_expr_kind = std::exchange(state.expr_kind, expr_kind);
  irs::Finally restore_state = [&] noexcept {
    state.expr_kind = saved_expr_kind;
  };
  return ProcessExprNodeImpl(state, node);
}

lp::ExprPtr SqlAnalyzer::ProcessExprNodeImpl(State& state, const Node* expr) {
  if (!expr) {
    return {};
  }
  CheckStackOverflow();

  lp::ExprPtr res;
  switch (expr->type) {
    case T_ColumnRef:
      res = ProcessColumnRef(state, *castNode(ColumnRef, expr));
      break;
    case T_A_Expr:
      res = ProcessAExpr(state, *castNode(A_Expr, expr));
      break;
    case T_A_Const:
      res = ProcessAConst(state, *castNode(A_Const, expr));
      break;
    case T_TypeCast:
      res = ProcessTypeCast(state, *castNode(TypeCast, expr));
      break;
    case T_FuncCall: {
      auto* funccall = castNode(FuncCall, expr);
      if (auto it = state.aggregate_or_window.find(funccall);
          it != state.aggregate_or_window.end()) {
        const auto& column = it->second;
        return std::make_shared<lp::InputReferenceExpr>(column.type,
                                                        column.name);
      }
      res = ProcessFuncCall(state, *castNode(FuncCall, expr));
    } break;
    case T_A_ArrayExpr:
      res = ProcessAArrayExpr(state, *castNode(A_ArrayExpr, expr));
      break;
    case T_BoolExpr:
      res = ProcessBoolExpr(state, *castNode(BoolExpr, expr));
      break;
    case T_NullTest:
      res = ProcessNullTest(state, *castNode(NullTest, expr));
      break;
    case T_BooleanTest:
      res = ProcessBooleanTest(state, *castNode(BooleanTest, expr));
      break;
    case T_CoalesceExpr:
      res = ProcessCoalesceExpr(state, *castNode(CoalesceExpr, expr));
      break;
    case T_MinMaxExpr:
      res = ProcessMinMaxExpr(state, *castNode(MinMaxExpr, expr));
      break;
    case T_CaseExpr:
      res = ProcessCaseExpr(state, *castNode(CaseExpr, expr));
      break;
    case T_ParamRef:
      res = ProcessParamRef(state, *castNode(ParamRef, expr));
      break;
    case T_SubLink:
      res = ProcessSubLink(state, *castNode(SubLink, expr));
      break;
    case T_SQLValueFunction:
      res = ProcessSQLValueFunction(state, *castNode(SQLValueFunction, expr));
      break;
    case T_A_Indirection:
      res = ProcessAIndirection(state, *castNode(A_Indirection, expr));
      break;
    case T_CollateClause:
      res = ProcessCollateClause(state, *castNode(CollateClause, expr));
      break;
    case T_SetToDefault:
      if (state.expr_kind != ExprKind::Values) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        CURSOR_POS(ErrorPosition(ExprLocation(expr))),
                        ERR_MSG("DEFAULT is not allowed in this context"));
      }
      return kDefaultValuePlaceHolderPtr;
    case T_MultiAssignRef:
    case T_GroupingFunc:
    case T_NamedArgExpr:
    case T_RowExpr:
    case T_CurrentOfExpr:
    case T_JsonIsPredicate:
    case T_JsonObjectAgg:
    case T_JsonArrayAgg:
    case T_JsonObjectConstructor:
    case T_JsonArrayConstructor:
    case T_JsonArrayQueryConstructor:
    case T_XmlExpr:
    case T_XmlSerialize:
    default:
      // should not reach here
      THROW_SQL_ERROR(CURSOR_POS(ErrorPosition(ExprLocation(expr))),
                      ERR_MSG("unrecognized node type: ", expr->type));
  }

  if (!state.has_groupby) {
    return res;
  }

  auto maybe_groupby = GroupByExprToName(*res);
  if (auto it = state.groupped_columns.find(maybe_groupby);
      it != state.groupped_columns.end()) {
    const auto& aggr = it->second;
    return std::make_shared<lp::InputReferenceExpr>(aggr.type, aggr.name);
  }

  return res;
}

using OpToFuncMap = containers::FlatHashMap<std::string_view, std::string_view>;

const OpToFuncMap kOpToFunc{
  {"+", "presto_plus"},
  {"-", "presto_minus"},
  {"*", "presto_multiply"},
  {"/", "presto_divide"},
  {"%", "presto_mod"},
  {"<", "presto_lt"},
  {"<=", "presto_lte"},
  {">", "presto_gt"},
  {">=", "presto_gte"},
  {"=", "presto_eq"},
  {"!", "presto_not"},
  {"!=", "presto_neq"},
  {"<>", "presto_neq"},
  {"and", "and"},
  {"or", "or"},
  {"is", "presto_is"},
  {"IS DISTINCT FROM", "presto_distinct_from"},
  {"count_star", "presto_count"},
};

const OpToFuncMap kDateIntervalOp{
  {"+", "pg_time_plus"},
  {"-", "pg_time_minus"},
};

const OpToFuncMap kDateOp{
  {"+", "presto_date_add"},
  {"-", "presto_date_diff"},
};

static constexpr std::string_view kMatch = "~";
static constexpr std::string_view kMatchNot = "!~";
static constexpr std::string_view kIMatch = "~*";
static constexpr std::string_view kIMatchNot = "!~*";
static constexpr std::string_view kLike = "~~";
static constexpr std::string_view kLikeNot = "!~~";
static constexpr std::string_view kILike = "~~*";
static constexpr std::string_view kILikeNot = "!~~*";

static constexpr bool IsLikeOperator(std::string_view type) {
  return type == kLike || type == kLikeNot || type == kILike ||
         type == kILikeNot;
}

static constexpr bool IsMatchOperator(std::string_view type) {
  return type == kMatch || type == kMatchNot || type == kIMatch ||
         type == kIMatchNot;
}

lp::ExprPtr SqlAnalyzer::ProcessMatchOp(std::string_view type,
                                        lp::ExprPtr input,
                                        lp::ExprPtr pattern) {
  SDB_ASSERT(IsMatchOperator(type));

  if (type.back() == '*') {  // IMatch
    input = ResolveVeloxFunctionAndInferArgsCommonType("presto_lower",
                                                       {std::move(input)});
    pattern = ResolveVeloxFunctionAndInferArgsCommonType("presto_lower",
                                                         {std::move(pattern)});
  }

  auto res = ResolveVeloxFunctionAndInferArgsCommonType(
    "presto_regexp_like", {std::move(input), std::move(pattern)});
  if (type.front() == '!') {
    return ResolveVeloxFunctionAndInferArgsCommonType("presto_not",
                                                      {std::move(res)});
  }
  return res;
}

lp::ExprPtr SqlAnalyzer::ProcessLikeOp(std::string_view type, lp::ExprPtr input,
                                       lp::ExprPtr pattern) {
  SDB_ASSERT(IsLikeOperator(type));
  lp::ExprPtr escape = MakeConst(std::string_view("\\"), velox::VARCHAR());
  pattern = std::make_shared<lp::CallExpr>(
    velox::VARCHAR(), "pg_process_escape_pattern",
    std::vector<lp::ExprPtr>{std::move(pattern)});

  if (type.back() == '*') {  // ILike
    input = ResolveVeloxFunctionAndInferArgsCommonType("presto_lower",
                                                       {std::move(input)});
    pattern = ResolveVeloxFunctionAndInferArgsCommonType("presto_lower",
                                                         {std::move(pattern)});
  }
  std::vector<lp::ExprPtr> args = {std::move(input), std::move(pattern)};
  if (escape) {
    args.push_back(std::move(escape));
  }

  auto res =
    ResolveVeloxFunctionAndInferArgsCommonType("presto_like", std::move(args));
  if (type.front() == '!') {
    return ResolveVeloxFunctionAndInferArgsCommonType("presto_not",
                                                      {std::move(res)});
  }
  return res;
}

lp::ExprPtr SqlAnalyzer::MaybeIntervalOp(std::string_view op, lp::ExprPtr& lhs,
                                         lp::ExprPtr& rhs) {
  auto it = kDateIntervalOp.find(op);
  if (it == kDateIntervalOp.end()) {
    return nullptr;
  }

  const auto is_interval_op = [](lp::ExprPtr& lhs, lp::ExprPtr& rhs) -> bool {
    const auto& l_type = lhs->type();
    const auto& r_type = rhs->type();
    return pg::IsInterval(r_type) &&
           (l_type->isDate() || l_type->isTimestamp() ||
            pg::IsInterval(l_type));
  };

  if (!is_interval_op(lhs, rhs) && !is_interval_op(rhs, lhs)) {
    return nullptr;
  }

  return ResolveVeloxFunctionAndInferArgsCommonType(
    std::string{it->second}, {std::move(lhs), std::move(rhs)});
}

lp::ExprPtr SqlAnalyzer::MaybeTimeOp(std::string_view op, lp::ExprPtr& lhs,
                                     lp::ExprPtr& rhs) {
  if (auto interval_op = MaybeIntervalOp(op, lhs, rhs)) {
    return interval_op;
  }

  const auto& l_type = lhs->type();
  const auto& r_type = rhs->type();

  if (l_type->isDate() && r_type->isDate()) {
    if (auto it = kDateOp.find(op); it != kDateOp.end()) {
      std::swap(lhs, rhs);  // date_diff(x1, x2) does x2 - x1
      return std::make_shared<lp::CallExpr>(
        velox::BIGINT(), std::string{it->second},
        std::vector<lp::ExprPtr>{MakeConst("day"), std::move(lhs),
                                 std::move(rhs)});
    }
  }

  return nullptr;
}

lp::ExprPtr SqlAnalyzer::ProcessOp(std::string_view name, lp::ExprPtr lhs,
                                   lp::ExprPtr rhs, int location) {
  if (auto time_op = MaybeTimeOp(name, lhs, rhs)) {
    return time_op;
  }

  auto it = kOpToFunc.find(name);
  if (it != kOpToFunc.end()) {
    return ResolveVeloxFunctionAndInferArgsCommonType(
      std::string{it->second}, {std::move(lhs), std::move(rhs)});
  }

  if (IsMatchOperator(name)) {
    return ProcessMatchOp(name, std::move(lhs), std::move(rhs));
  }

  if (IsLikeOperator(name)) {
    return ProcessLikeOp(name, std::move(lhs), std::move(rhs));
  }

  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_FUNCTION), CURSOR_POS(ErrorPosition(location)),
    ERR_MSG("operator does not exist:",
            ToPgOperatorString(name, {std::move(lhs), std::move(rhs)})));
}

// x -> lhs op x
lp::ExprPtr SqlAnalyzer::MakeComparator(std::string_view op, lp::ExprPtr lhs,
                                        velox::TypePtr rhs_type, int location) {
  auto lambda_param = std::make_shared<lp::InputReferenceExpr>(rhs_type, "x");
  auto compare_body =
    ProcessOp(op, std::move(lhs), std::move(lambda_param), location);
  auto signature = velox::ROW({"x"}, {std::move(rhs_type)});
  return std::make_shared<lp::LambdaExpr>(std::move(signature),
                                          std::move(compare_body));
}

lp::ExprPtr SqlAnalyzer::ProcessAExpr(State& state, const A_Expr& expr) {
  std::string_view name = strVal(llast(expr.name));
  switch (expr.kind) {
    case AEXPR_OP: {
      auto lhs = ProcessExprNodeImpl(state, expr.lexpr);
      auto rhs = ProcessExprNodeImpl(state, expr.rexpr);
      if (!lhs || !rhs) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                        ERR_MSG("expression must have both left and right "
                                "arguments"));
      }
      return ProcessOp(name, std::move(lhs), std::move(rhs),
                       ExprLocation(&expr));
    }
    case AEXPR_NULLIF: {
      auto lhs = ProcessExprNodeImpl(state, expr.lexpr);
      auto rhs = ProcessExprNodeImpl(state, expr.rexpr);
      // TODO: try to find common type
      if (lhs->type() != rhs->type()) {
        if (rhs->type()->kind() == velox::TypeKind::UNKNOWN) {
          return lhs;
        }
        return MakeConst(velox::TypeKind::UNKNOWN);
      }
      auto value = lhs;
      auto value_type = value->type();
      auto cmp = ResolveVeloxFunctionAndInferArgsCommonType(
        "presto_eq", {std::move(lhs), std::move(rhs)});
      return ResolveVeloxFunctionAndInferArgsCommonType(
        "if",
        std::vector{std::move(cmp),
                    MakeConst(velox::TypeKind::UNKNOWN, std::move(value_type)),
                    std::move(value)});
    }
    case AEXPR_IN: {
      auto lhs = ProcessExprNodeImpl(state, expr.lexpr);
      auto rhs = ProcessExprListImpl(state, castNode(List, expr.rexpr));
      rhs.insert(rhs.begin(), std::move(lhs));
      auto res =
        ResolveVeloxFunctionAndInferArgsCommonType("in", std::move(rhs));
      if (name == "<>") {
        return ResolveVeloxFunctionAndInferArgsCommonType("presto_not",
                                                          {std::move(res)});
      }
      return res;
    }
    case AEXPR_LIKE:
    case AEXPR_ILIKE: {
      auto lhs = ProcessExprNodeImpl(state, expr.lexpr);
      auto rhs = ProcessExprNodeImpl(state, expr.rexpr);
      return ProcessLikeOp(name, std::move(lhs), std::move(rhs));
    }
    case AEXPR_BETWEEN:
    case AEXPR_NOT_BETWEEN: {
      auto lhs = ProcessExprNodeImpl(state, expr.lexpr);
      auto rhs = ProcessExprListImpl(state, castNode(List, expr.rexpr));
      SDB_ASSERT(rhs.size() == 2);
      auto res = ResolveVeloxFunctionAndInferArgsCommonType(
        "presto_between",
        {std::move(lhs), std::move(rhs[0]), std::move(rhs[1])});
      if (expr.kind == AEXPR_NOT_BETWEEN) {
        return ResolveVeloxFunctionAndInferArgsCommonType("presto_not",
                                                          {std::move(res)});
      }
      return res;
    }
    case AEXPR_BETWEEN_SYM:
    case AEXPR_NOT_BETWEEN_SYM: {
      auto lhs = ProcessExprNodeImpl(state, expr.lexpr);
      auto rhs = ProcessExprListImpl(state, castNode(List, expr.rexpr));
      SDB_ASSERT(rhs.size() == 2);
      auto& low = rhs[0];
      auto& high = rhs[1];
      auto least_val =
        ResolveVeloxFunctionAndInferArgsCommonType("presto_least", {low, high});
      auto greatest_val = ResolveVeloxFunctionAndInferArgsCommonType(
        "presto_greatest", {std::move(low), std::move(high)});
      auto res = ResolveVeloxFunctionAndInferArgsCommonType(
        "presto_between",
        {std::move(lhs), std::move(least_val), std::move(greatest_val)});
      if (expr.kind == AEXPR_NOT_BETWEEN_SYM) {
        return ResolveVeloxFunctionAndInferArgsCommonType("presto_not",
                                                          {std::move(res)});
      }
      return res;
    }
    case AEXPR_DISTINCT:
    case AEXPR_NOT_DISTINCT: {
      auto lhs = ProcessExprNodeImpl(state, expr.lexpr);
      auto rhs = ProcessExprNodeImpl(state, expr.rexpr);
      if (!lhs || !rhs) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_SYNTAX_ERROR),
          CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
          ERR_MSG("DISTINCT expression must have both left and right "
                  "arguments"));
      }
      auto res = ResolveVeloxFunctionAndInferArgsCommonType(
        "presto_distinct_from", {std::move(lhs), std::move(rhs)});
      if (expr.kind == AEXPR_NOT_DISTINCT) {
        return ResolveVeloxFunctionAndInferArgsCommonType("presto_not",
                                                          {std::move(res)});
      }
      return res;
    }
    case AEXPR_OP_ANY:
    case AEXPR_OP_ALL: {
      auto lhs = ProcessExprNodeImpl(state, expr.lexpr);
      if (!lhs) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                        ERR_MSG("ANY/ALL expression must have left argument"));
      }
      auto rhs = ProcessExprNodeImpl(state, expr.rexpr);
      if (!rhs) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                        ERR_MSG("ANY/ALL expression must have right argument"));
      }

      const auto& rhs_type = rhs->type();
      if (!rhs_type->isArray()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
          ERR_MSG("op ANY/ALL (array) requires array on right side"));
      }
      auto predicate =
        MakeComparator(name, std::move(lhs), rhs_type->asArray().elementType(),
                       ExprLocation(&expr));
      std::string func_name =
        expr.kind == AEXPR_OP_ANY ? "presto_any_match" : "presto_all_match";
      return std::make_shared<lp::CallExpr>(
        velox::BOOLEAN(), std::move(func_name),
        std::vector<lp::ExprPtr>{std::move(rhs), std::move(predicate)});
    }
    case AEXPR_SIMILAR: {
      auto lhs = ProcessExprNodeImpl(state, expr.lexpr);
      auto rhs = ProcessExprNodeImpl(state, expr.rexpr);
      return ProcessMatchOp(name, std::move(lhs), std::move(rhs));
    }
  }
}

lp::ExprPtr SqlAnalyzer::ProcessAConst(State& state, const A_Const& expr) {
  if (expr.isnull) {
    return MakeConst(velox::TypeKind::UNKNOWN);
  }
  switch (nodeTag(&expr.val)) {
    case T_Integer: {
      const auto v = intVal(&expr.val);
      return MakeConst(v);
    }
    case T_Float: {
      const auto v = floatVal(&expr.val);
      return MakeConst(v);
    }
    case T_Boolean: {
      const auto v = boolVal(&expr.val);
      return MakeConst(v);
    }
    case T_String: {
      std::string_view v = strVal(&expr.val);
      return MakeConst(v);
    }
    case T_BitString:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                      ERR_MSG("bit string is not supported"));
    default:
      SDB_ASSERT(false);
  }
  return {};
}

lp::ExprPtr SqlAnalyzer::ProcessFuncCall(State& state, const FuncCall& expr) {
  auto [_, schema, name] = GetDbSchemaRelation(expr.funcname);
  auto* function = _objects.getData(schema, name);
  if (!function) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                    CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                    ERR_MSG("Function '", name, "' is not resolved"));
  }
  auto& logical_function =
    basics::downCast<catalog::Function>(*function->object);

  if (logical_function.Options().table &&
      state.expr_kind == ExprKind::AggregateArgument) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                    ERR_MSG("aggregate function calls cannot contain "
                            "set-returning function calls"));
  }

  if (auto window = MaybeWindowFuncCall(state, logical_function, expr)) {
    return window;
  }

  if (auto aggr = MaybeAggregateFuncCall(state, logical_function, expr)) {
    return aggr;
  }

  if (expr.agg_distinct) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                    ERR_MSG("DISTINCT specified, but ", name,
                            " is not an aggregate function"));
  }
  if (expr.agg_filter) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
      ERR_MSG("FILTER specified, but ", name, " is not an aggregate function"));
  }
  if (expr.agg_order) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                    ERR_MSG("ORDER BY specified, but ", name,
                            " is not an aggregate function"));
  }

  const auto lang = logical_function.Options().language;
  name = logical_function.GetName();
  auto args = ProcessExprListImpl(state, expr.args);
  if (lang == catalog::FunctionLanguage::Decorator) {
    if (args.size() != 1) {
      // TODO(mbkkt) it's possible to do concat of rows here
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                      ERR_MSG("Decorator function '", name,
                              "' must have exactly one argument"));
    }
    return std::move(args[0]);
  }

  if (lang == catalog::FunctionLanguage::VeloxNative) {
    return ResolveVeloxFunctionAndInferArgsCommonType(std::string{name},
                                                      std::move(args));
  }

  if (logical_function.Signature().IsProcedure()) {
    ErrorIsProcedure(name, args, ExprLocation(&expr));
  }

  if (lang == catalog::FunctionLanguage::SQL) {
    // look at ResolveSQLFunctionAndInferArgsCommonType for TODO details.
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG(
        "User-defined SQL functions are only supported in a FROM clause"));
  }

  ErrorUnsupportedLanguage(lang, name, args, ExprLocation(&expr));
}

velox::TypePtr ResolveWindowFunction(
  const std::string& function_name,
  const std::vector<velox::TypePtr>& arg_types) {
  if (auto signatures = ve::getWindowFunctionSignatures(function_name)) {
    for (const auto& signature : signatures.value()) {
      ve::SignatureBinder binder(*signature, arg_types);
      if (binder.tryBind()) {
        return binder.tryResolveType(signature->returnType());
      }
    }
  }
  return nullptr;
}

lp::WindowExprPtr SqlAnalyzer::MaybeWindowFuncCall(
  State& state, const catalog::Function& logical_function,
  const FuncCall& func_call) {
  if (!func_call.over) {
    return nullptr;
  }

  switch (state.expr_kind) {
    case ExprKind::GroupBy:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_WINDOWING_ERROR),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
        ERR_MSG("window function calls are not allowed in GROUP BY"));
    case ExprKind::Where:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_WINDOWING_ERROR),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
        ERR_MSG("window function calls are not allowed in WHERE"));
    case ExprKind::Having:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_WINDOWING_ERROR),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
        ERR_MSG("window function calls are not allowed in HAVING"));
    case ExprKind::WindowPartition:
    case ExprKind::WindowOrder:
    case ExprKind::WindowFrameRange:
    case ExprKind::WindowFrameRows:
    case ExprKind::WindowFrameGroups:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_WINDOWING_ERROR),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
        ERR_MSG("window function calls are not allowed in window definitions"));
    case ExprKind::ColumnDefault:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_WINDOWING_ERROR),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
        ERR_MSG("window functions are not allowed in DEFAULT expressions"));
    default:
      break;
  }

  std::string name{logical_function.GetName()};
  auto args = ProcessExprListImpl(state, func_call.args);

  const WindowDef* over = func_call.over;
  auto partition_keys =
    ProcessExprList(state, over->partitionClause, ExprKind::WindowPartition);
  auto ordering =
    ProcessSortByList(state, over->orderClause, ExprKind::WindowOrder);

  lp::WindowExpr::Frame frame;
  const int fo = over->frameOptions;

  ExprKind window_kind{};
  auto frame_from_flags = [&] {
    if (fo & FRAMEOPTION_ROWS) {
      window_kind = ExprKind::WindowFrameRows;
      return lp::WindowExpr::WindowType::kRows;
    }
    if (fo & FRAMEOPTION_RANGE) {
      window_kind = ExprKind::WindowFrameRange;
      return lp::WindowExpr::WindowType::kRange;
    }
    if (fo & FRAMEOPTION_GROUPS) {
      window_kind = ExprKind::WindowFrameGroups;
      return lp::WindowExpr::WindowType::kGroups;
    }
    SDB_UNREACHABLE();
  };
  frame.type = frame_from_flags();

  auto start_bound_from_flags = [&] {
    if (fo & FRAMEOPTION_START_UNBOUNDED_PRECEDING) {
      return lp::WindowExpr::BoundType::kUnboundedPreceding;
    }
    if (fo & FRAMEOPTION_START_CURRENT_ROW) {
      return lp::WindowExpr::BoundType::kCurrentRow;
    }
    if (fo & FRAMEOPTION_START_OFFSET_PRECEDING) {
      return lp::WindowExpr::BoundType::kPreceding;
    }
    if (fo & FRAMEOPTION_START_OFFSET_FOLLOWING) {
      return lp::WindowExpr::BoundType::kFollowing;
    }
    SDB_UNREACHABLE();
  };

  frame.startType = start_bound_from_flags();
  if (over->startOffset) {
    frame.startValue = ProcessExprNode(state, over->startOffset, window_kind);
  }

  auto end_bound_from_flags = [&] {
    if (fo & FRAMEOPTION_END_UNBOUNDED_FOLLOWING) {
      return lp::WindowExpr::BoundType::kUnboundedFollowing;
    }
    if (fo & FRAMEOPTION_END_CURRENT_ROW) {
      return lp::WindowExpr::BoundType::kCurrentRow;
    }
    if (fo & FRAMEOPTION_END_OFFSET_PRECEDING) {
      return lp::WindowExpr::BoundType::kPreceding;
    }
    if (fo & FRAMEOPTION_END_OFFSET_FOLLOWING) {
      return lp::WindowExpr::BoundType::kFollowing;
    }
    SDB_UNREACHABLE();
  };

  frame.endType = end_bound_from_flags();
  if (over->endOffset) {
    frame.endValue = ProcessExprNode(state, over->endOffset, window_kind);
  }

  auto resolve_window_function = [&] -> velox::TypePtr {
    auto arg_types = GetExprsTypes(args);
    if (logical_function.Options().IsWindow()) {
      if (auto type = ResolveWindowFunction(name, arg_types)) {
        return type;
      }
    }
    if (logical_function.Options().IsAggregate()) {
      if (auto type = ve::resolveResultType(name, arg_types)) {
        return type;
      }
    }

    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
      CURSOR_POS(ErrorPosition(ExprLocation(&func_call))),
      ERR_MSG("function ", ToPgFunctionString(name, args), " does not exist"));
  };

  auto type = resolve_window_function();

  // This is not implemented in PostgreSQL:
  // the behavior is always the same as the standard's default,
  // namely RESPECT NULLS (c) postgresql.org
  constexpr bool kIgnoreNulls = false;
  return std::make_shared<lp::WindowExpr>(
    std::move(type), std::move(name), std::move(args),
    std::move(partition_keys), std::move(ordering), std::move(frame),
    kIgnoreNulls);
}

lp::ExprPtr SqlAnalyzer::ProcessColumnRef(State& state, const ColumnRef& expr) {
  std::string name = NameToStr(expr.fields);

  SDB_ASSERT(state.root);

  if (state.expr_kind == ExprKind::ColumnDefault) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
      ERR_MSG("cannot use column reference in DEFAULT expression"));
  }

  if (state.pre_columnref_hook) {
    if (auto res = state.pre_columnref_hook(name, expr)) {
      return res;
    }
  }

  const auto& lookup_columns =
    state.lookup_columns ? state.lookup_columns : state.root->outputType();
  SDB_ASSERT(lookup_columns);
  auto result = state.resolver.Resolve(lookup_columns, name);

  if (result.IsAmbiguous()) {
    ErrorAmbiguousColumn(name, ExprLocation(&expr));
  } else if (result.IsColumnNotFound()) {
    if (state.post_columnref_hook) {
      if (auto res = state.post_columnref_hook(name, expr)) {
        return res;
      }
    }

    if (auto res = state.MaybeFuncParam(name)) {
      return res;
    }

    if (state.is_sublink && state.parent) {
      return ProcessColumnRef(*state.parent, expr);
    }

    ErrorColumnDoesNotExist(name, ExprLocation(&expr));
  } else if (result.IsTableNotFound()) {
    if (state.is_sublink && state.parent) {
      return ProcessColumnRef(*state.parent, expr);
    }

    ErrorTableDoesNotExist(result.GetTableName(), ExprLocation(&expr));
  } else if (result.IsFound()) {
    name = result.GetColumnName();
  }

  const auto& type = lookup_columns->findChild(name);
  return std::make_shared<lp::InputReferenceExpr>(type, name);
}

// You should be catious with alias names, because some of them transformed in
// the parser, so don't add them here!
const containers::FlatHashMap<std::string_view, velox::TypePtr> kTypeCasts{
  {"unknown", velox::UNKNOWN()},
  {"bool", velox::BOOLEAN()},
  {"bpchar", velox::TINYINT()},
  {"int2", velox::SMALLINT()},
  {"int4", velox::INTEGER()},
  {"int8", velox::BIGINT()},
  {"int16", velox::HUGEINT()},
  {"float4", velox::REAL()},
  {"float8", velox::DOUBLE()},
  {"varchar", velox::VARCHAR()},
  {"text", velox::VARCHAR()},
  {"bytea", velox::VARBINARY()},
  {"json", velox::JSON()},
  {"numeric", velox::DECIMAL(velox::LongDecimalType::kMaxPrecision, 0)},
  {"timestamp", velox::TIMESTAMP()},
  {"timestamptz", velox::TIMESTAMP_WITH_TIME_ZONE()},
  {"date", velox::DATE()},
  {"uuid", velox::UUID()},
  {"cidr", velox::IPPREFIX()},
  {"void", pg::VOID()},
};

lp::ExprPtr SqlAnalyzer::ProcessAArrayExpr(State& state,
                                           const A_ArrayExpr& expr) {
  auto elements = ProcessExprListImpl(state, expr.elements);
  return ResolveVeloxFunctionAndInferArgsCommonType("presto_array_constructor",
                                                    std::move(elements));
}

std::string ToString(BoolExprType type) {
  switch (type) {
    case AND_EXPR:
      return "AND";
    case OR_EXPR:
      return "OR";
    case NOT_EXPR:
      return "NOT";
    default:
      return "UNKNOWN";
  }
}

void SqlAnalyzer::ProcessFunctionBody(
  State& state, const State::FuncParamToExpr& func_params,
  const Node& func_body, const catalog::FunctionSignature& signature) {
  state.func_params = &func_params;
  auto cmd_type = ProcessStmt(state, func_body);
  state.func_params = nullptr;

  // maybe ddl statement
  EnsureRoot(state);

  if (signature.IsProcedure()) {
    return;
  }

  if (signature.ReturnsVoid()) {
    state.root = std::make_shared<lp::LimitNode>(_id_generator.NextPlanId(),
                                                 std::move(state.root), 0, 1);
    state.root = std::make_shared<lp::ProjectNode>(
      _id_generator.NextPlanId(), std::move(state.root),
      std::vector<std::string>{_id_generator.NextColumnName("column")},
      std::vector<lp::ExprPtr>{MakeConst(velox::TypeKind::UNKNOWN)});
    return;
  }

  SDB_ASSERT(cmd_type == SqlCommandType::Select);

  SDB_ASSERT(state.root);
  SDB_ASSERT(state.root->outputType());
  const auto& actual_type = *state.root->outputType();
  SDB_ASSERT(signature.return_type);
  const auto& expected_type = *signature.return_type;
  if (expected_type.kind() != velox::TypeKind::ROW) {
    if (actual_type.size() != 1) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_FUNCTION_DEFINITION),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_body))),
        ERR_MSG("return type mismatch in function declared to return ",
                ToPgTypeString(signature.return_type)),
        ERR_DETAIL("Final statement must return exactly one column."));
    }

    if (expected_type != *actual_type.childAt(0)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_FUNCTION_DEFINITION),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_body))),
        ERR_MSG("return type mismatch in function declared to return ",
                ToPgTypeString(signature.return_type)),
        ERR_DETAIL("Actual return type is", " ",
                   ToPgTypeString(actual_type.childAt(0)), "."));
    }

    state.root = std::make_shared<lp::LimitNode>(_id_generator.NextPlanId(),
                                                 std::move(state.root), 0, 1);
    return;
  }

  const auto& expected_row = expected_type.asRow();

  std::vector<std::string> names;
  std::vector<lp::ExprPtr> exprs;
  names.reserve(expected_row.size());
  exprs.reserve(expected_row.size());

  size_t size = std::min(expected_row.size(), actual_type.size());
  // we do min size just to make it act like postgres (first check all the types
  // then detailed message 'final statement too ...')
  for (size_t i = 0; i < size; ++i) {
    const auto& expected_col_type = expected_row.childAt(i);
    const auto& actual_col_type = actual_type.childAt(i);

    if (*expected_col_type != *actual_col_type) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_FUNCTION_DEFINITION),
        CURSOR_POS(ErrorPosition(ExprLocation(&func_body))),
        ERR_MSG("return type mismatch in function declared to return record"),
        ERR_DETAIL("Final statement returns ", ToPgTypeString(actual_col_type),
                   " instead of ", ToPgTypeString(expected_col_type),
                   " at column ", i + 1, "."));
    }

    names.emplace_back(expected_row.nameOf(i));
    exprs.emplace_back(std::make_shared<lp::InputReferenceExpr>(
      actual_col_type, actual_type.nameOf(i)));
  }

  if (actual_type.size() != expected_row.size()) {
    std::string_view detail =
      actual_type.size() < expected_row.size() ? "few" : "many";
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_FUNCTION_DEFINITION),
      CURSOR_POS(ErrorPosition(ExprLocation(&func_body))),
      ERR_MSG("return type mismatch in function declared to return record"),
      ERR_DETAIL("Final statement returns too ", detail, " columns."));
  }

  state.root = std::make_shared<lp::ProjectNode>(
    _id_generator.NextPlanId(), std::move(state.root), std::move(names),
    std::move(exprs));
}

lp::ExprPtr SqlAnalyzer::ProcessBoolExpr(State& state, const BoolExpr& expr) {
  auto args = ProcessExprListImpl(state, expr.args);

  std::string function_name;
  switch (expr.boolop) {
    case AND_EXPR: {
      function_name = "and";
    } break;
    case OR_EXPR: {
      function_name = "or";
    } break;
    case NOT_EXPR: {
      SDB_ASSERT(args.size() == 1);
      function_name = "presto_not";
    } break;
    default: {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      CURSOR_POS(ErrorPosition(ExprLocation(&expr.boolop))),
                      ERR_MSG(expr.boolop, " is not supported"));
    }
  }

  for (size_t i = 0; i < args.size(); ++i) {
    if (!args[i]->type()->isBoolean()) {
      int err_pos = ListElementErrorPosition(expr.location, expr.args, i);
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR), CURSOR_POS(err_pos),
                      ERR_MSG("argument of ", ToString(expr.boolop),
                              " must be type boolean, not type ",
                              ToPgTypeString(args[i]->type())));
    }
  }

  return ResolveVeloxFunctionAndInferArgsCommonType(std::move(function_name),
                                                    std::move(args));
}

lp::ExprPtr SqlAnalyzer::ProcessNullTest(State& state, const NullTest& expr) {
  auto arg = ProcessExprNodeImpl(state, castNode(Node, expr.arg));
  std::string function_name;
  switch (expr.nulltesttype) {
    case IS_NULL: {
      function_name = "spark_isnull";
    } break;
    case IS_NOT_NULL: {
      function_name = "spark_isnotnull";
    } break;
    default: {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        CURSOR_POS(ErrorPosition(ExprLocation(&expr.nulltesttype))),
        ERR_MSG(expr.nulltesttype, " is not supported"));
    }
  }

  return ResolveVeloxFunctionAndInferArgsCommonType(
    std::move(function_name), std::vector{std::move(arg)});
}

lp::ExprPtr SqlAnalyzer::ProcessBooleanTest(State& state,
                                            const BooleanTest& expr) {
  auto arg = ProcessExprNodeImpl(state, castNode(Node, expr.arg));

  bool val;
  bool is_not = false;
  switch (expr.booltesttype) {
    case IS_NOT_FALSE:
      is_not = true;
      [[fallthrough]];
    case IS_TRUE: {
      val = true;
    } break;
    case IS_NOT_TRUE:
      is_not = true;
      [[fallthrough]];
    case IS_FALSE: {
      val = false;
    } break;
    case IS_UNKNOWN:
      return ResolveVeloxFunctionAndInferArgsCommonType(
        "spark_isnull", std::vector{std::move(arg)});
    case IS_NOT_UNKNOWN:
      return ResolveVeloxFunctionAndInferArgsCommonType(
        "spark_isnotnull", std::vector{std::move(arg)});
    default:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        CURSOR_POS(ErrorPosition(ExprLocation(&expr.booltesttype))),
        ERR_MSG(expr.booltesttype, " is not supported"));
  }

  arg = MakeCast(velox::BOOLEAN(), std::move(arg));
  std::vector cmp_args{std::move(arg), MakeConst(val)};

  lp::ExprPtr cmp = std::make_shared<lp::CallExpr>(
    velox::BOOLEAN(), "presto_eq", std::move(cmp_args));
  return std::make_shared<lp::SpecialFormExpr>(
    velox::BOOLEAN(), lp::SpecialForm::kCoalesce,
    std::vector{std::move(cmp), MakeConst(is_not)});
}

lp::ExprPtr SqlAnalyzer::ProcessCoalesceExpr(State& state,
                                             const CoalesceExpr& expr) {
  auto args = ProcessExprListImpl(state, expr.args);
  return ResolveVeloxFunctionAndInferArgsCommonType("coalesce",
                                                    std::move(args));
}

// TODO: use location here
// Kind of optimization for EXTRACT(field FROM expr); we don't want to compare
// field at runtime - so put the function which extracts the field according to
// the first argument
lp::ExprPtr SqlAnalyzer::ResolveExtract(std::vector<lp::ExprPtr> args) {
  if (args.size() != 2) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
                    ERR_MSG("function ", ToPgFunctionString("extract", args),
                            " does not exist"));
  }

  auto& field_expr = args[0];
  if (!field_expr->isConstant()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("first argument of extract must be a constant"));
  }

  auto& from = args[1];
  const auto& from_type = from->type();
  std::string field = field_expr->as<lp::ConstantExpr>()
                        ->value()
                        ->value<velox::TypeKind::VARCHAR>();
  int val = 0;
  static constexpr int kDummyFieldArg = 0;
  int unit_type = DecodeUnits(kDummyFieldArg, field.c_str(), &val);
  if (unit_type == UNKNOWN_FIELD) {
    unit_type = DecodeSpecial(kDummyFieldArg, field.c_str(), &val);
  }
  auto func_name = [&] -> std::string {
    if (unit_type == RESERV) {
      switch (val) {
        case DTK_EPOCH:
          return "pg_extract_epoch";
        default:
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("unit \"", field, "\" not supported for type ",
                    ToPgTypeString(from_type)));
      }
    }

    if (unit_type != UNITS) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unit \"", field, "\" not recognized for type ",
                              ToPgTypeString(from_type)));
    }

    switch (val) {
      case DTK_MILLENNIUM:
        return "pg_extract_millennium";
      case DTK_CENTURY:
        return "pg_extract_century";
      case DTK_DECADE:
        return "pg_extract_decade";
      case DTK_YEAR:
        return "pg_extract_year";
      case DTK_QUARTER:
        return "pg_extract_quarter";
      case DTK_MONTH:
        return "pg_extract_month";
      case DTK_WEEK:
        return "pg_extract_week";
      case DTK_DAY:
        return "pg_extract_day";
      case DTK_HOUR:
        return "pg_extract_hour";
      case DTK_MINUTE:
        return "pg_extract_minute";
      case DTK_SECOND:
        return "pg_extract_second";
      case DTK_MILLISEC:
        return "pg_extract_millisecond";
      case DTK_MICROSEC:
        return "pg_extract_microsecond";
      case DTK_DOW:
        return "pg_extract_dow";
      case DTK_ISODOW:
        return "pg_extract_isodow";
      case DTK_DOY:
        return "pg_extract_doy";
      case DTK_ISOYEAR:
        return "pg_extract_isoyear";
      default:
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("unit \"", field, "\" not supported for type ",
                                ToPgTypeString(from_type)));
    }
  }();

  std::vector<velox::TypePtr> coercions;
  auto type = ResolveFunction(func_name, {from}, &coercions);
  if (!type) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("unit \"", field, "\" not supported for type ",
                            ToPgTypeString(from_type)));
  }
  ApplyCoercions(args, coercions);

  return std::make_shared<lp::CallExpr>(std::move(type), std::move(func_name),
                                        std::move(from));
}

lp::ExprPtr SqlAnalyzer::ResolveVeloxFunctionAndInferArgsCommonType(
  std::string name, std::vector<lp::ExprPtr> args) {
  if (name == "pg_extract") {
    return ResolveExtract(std::move(args));
  }

  std::vector<velox::TypePtr> coercions;
  auto type = ResolveFunction(name, args, &coercions);
  if (!type) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
      ERR_MSG("function ", ToPgFunctionString(name, args), " does not exist"));
  }

  ApplyCoercions(args, coercions);
  auto it = kSpecialForms.find(name);
  if (it == kSpecialForms.end()) {
    return std::make_shared<lp::CallExpr>(std::move(type), std::move(name),
                                          std::move(args));
  }
  return std::make_shared<lp::SpecialFormExpr>(std::move(type), it->second,
                                               std::move(args));
}

State SqlAnalyzer::ResolveSQLFunctionAndInferArgsCommonType(
  const catalog::Function& logical_function, std::vector<lp::ExprPtr> args,
  int location) {
  SDB_ASSERT(logical_function.Options().language ==
             catalog::FunctionLanguage::SQL);
  const auto& sql_function = logical_function.SqlFunction();
  const auto& signature = logical_function.Signature();

  std::string_view name = logical_function.GetName();
  if (!signature.Matches(GetExprsTypes(args))) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_FUNCTION), CURSOR_POS(ErrorPosition(location)),
      ERR_HINT("maybe you intended to call ", ToPgFunctionString(name, args),
               "?"),
      ERR_MSG("function ", ToPgFunctionString(name, args), " does not exist"));
  }

  const auto& params = signature.parameters;
  SDB_ASSERT(params.size() == args.size());

  State::FuncParamToExpr param2expr;
  param2expr.reserve(params.size());
  for (auto&& [param, expr] : std::views::zip(params, args)) {
    if (param.mode != catalog::FunctionParameter::Mode::In) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        CURSOR_POS(ErrorPosition(location)),
        ERR_MSG("only IN parameters are supported in function '", name, "'"));
    }

    param2expr.try_emplace(param.name, std::move(expr));
  }

  State state;
  const auto& function_body = *sql_function.GetStatement()->stmt;
  ProcessFunctionBody(state, param2expr, function_body, signature);
  return state;
}

lp::ExprPtr SqlAnalyzer::ProcessMinMaxExpr(State& state,
                                           const MinMaxExpr& expr) {
  auto args = ProcessExprListImpl(state, expr.args);
  std::string function_name;
  switch (expr.op) {
    case IS_GREATEST: {
      function_name = "presto_greatest";
    } break;
    case IS_LEAST: {
      function_name = "presto_least";
    } break;
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      CURSOR_POS(ErrorPosition(expr.location)),
                      ERR_MSG(expr.op, " is not supported"));
  }

  return ResolveVeloxFunctionAndInferArgsCommonType(std::move(function_name),
                                                    std::move(args));
}

lp::ExprPtr SqlAnalyzer::ProcessCaseExpr(State& state, const CaseExpr& expr) {
  std::vector<lp::ExprPtr> args;
  args.reserve(list_length(expr.args) * 2 + 1);
  lp::ExprPtr arg_expr;
  if (expr.arg) {
    arg_expr = ProcessExprNodeImpl(state, castNode(Node, expr.arg));
  }
  velox::TypePtr output_type;
  VisitNodes(expr.args, [&](const CaseWhen& node) {
    auto when_expr = ProcessExprNodeImpl(state, castNode(Node, node.expr));
    auto then_expr = ProcessExprNodeImpl(state, castNode(Node, node.result));
    if (arg_expr) {
      args.emplace_back(MakeEquality(arg_expr, when_expr));
    } else {
      args.emplace_back(std::move(when_expr));
    }
    if (!output_type) {
      output_type = then_expr->type();
    }
    args.emplace_back(std::move(then_expr));
  });
  lp::ExprPtr else_expr;
  if (expr.defresult) {
    else_expr = ProcessExprNodeImpl(state, castNode(Node, expr.defresult));
  } else {
    auto null_val = std::make_shared<velox::Variant>(output_type->kind());
    else_expr = std::make_shared<lp::ConstantExpr>(std::move(output_type),
                                                   std::move(null_val));
  }
  args.emplace_back(std::move(else_expr));
  return ResolveVeloxFunctionAndInferArgsCommonType("switch", std::move(args));
}

lp::ExprPtr SqlAnalyzer::ProcessParamRef(State& state, const ParamRef& expr) {
  SDB_ASSERT(expr.number > 0);

  if (state.func_params) {
    // param refs inside a function body
    // can be used only as an unnamed arguments

    std::string name = GetUnnamedFunctionArgumentName(expr.number);
    if (auto it = state.func_params->find(name);
        it != state.func_params->end()) {
      return it->second;
    }

    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_PARAMETER),
                    CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                    ERR_MSG("there is no parameter $", expr.number));
  }

  SDB_ASSERT(expr.number < std::numeric_limits<ParamIndex>::max());
  ParamIndex param_idx = expr.number;
  SDB_ASSERT(param_idx - 1 < _params.types.size());

  if (_params.values.empty()) {
    // parse stage
    auto param = MakeConst(velox::TypeKind::UNKNOWN);
    _params.types[param_idx - 1] = kUncastedParamPlaceholder;
    _param_to_idx.try_emplace(param.get(), param_idx);
    return param;
  }

  // Bind stage
  SDB_ASSERT(_params.values.size() == _params.types.size());
  SDB_ASSERT(param_idx - 1 < _params.values.size());

  const auto& type = _params.types[param_idx - 1];
  const auto& value = _params.values[param_idx - 1];
  SDB_ASSERT(type);
  SDB_ASSERT(value);

  return std::make_shared<lp::ConstantExpr>(type, value);
}

lp::ExprPtr SqlAnalyzer::ProcessSubLink(State& state, const SubLink& expr) {
  SDB_ASSERT(expr.subselect);

  if (state.expr_kind == ExprKind::ColumnDefault) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
      ERR_MSG("subqueries are not allowed in DEFAULT expressions"));
  }

  auto child_state = state.MakeChild();
  child_state.is_sublink = true;
  auto type = ProcessStmt(child_state, *expr.subselect);
  SDB_ASSERT(type == SqlCommandType::Select);
  SDB_ASSERT(child_state.root);

  const auto& subquery_output = *child_state.root->outputType();
  switch (expr.subLinkType) {
    case EXISTS_SUBLINK: {
      auto subquery_expr =
        std::make_shared<lp::SubqueryExpr>(std::move(child_state.root));
      return std::make_shared<lp::SpecialFormExpr>(
        velox::BOOLEAN(), lp::SpecialForm::kExists, std::move(subquery_expr));
    }
    case EXPR_SUBLINK: {
      if (subquery_output.size() != 1) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("subquery must return only one column"),
                        CURSOR_POS(ErrorPosition(expr.location)));
      }
      return std::make_shared<lp::SubqueryExpr>(std::move(child_state.root));
    }
    case ANY_SUBLINK: {
      if (subquery_output.size() != 1) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("subquery has too many columns"),
                        CURSOR_POS(ErrorPosition(expr.location)));
      }

      auto test_expr = ProcessExprNodeImpl(state, expr.testexpr);
      auto subquery_expr =
        std::make_shared<lp::SubqueryExpr>(std::move(child_state.root));

      if (expr.operName) {
        std::string_view oper_name = strVal(llast(expr.operName));
        if (oper_name != "=") {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("only '=' operator is supported in ANY sublink, got: ",
                    oper_name),
            CURSOR_POS(ErrorPosition(expr.location)));
        }
      }

      return std::make_shared<lp::SpecialFormExpr>(
        velox::BOOLEAN(), lp::SpecialForm::kIn, std::move(test_expr),
        std::move(subquery_expr));
    }
    case ARRAY_SUBLINK:
    case ALL_SUBLINK:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("subquery type is not implemented: ", expr.subLinkType),
        CURSOR_POS(ErrorPosition(expr.location)));
    case MULTIEXPR_SUBLINK:
    case ROWCOMPARE_SUBLINK:
    case CTE_SUBLINK:
      SDB_UNREACHABLE();
  }
}

lp::ExprPtr SqlAnalyzer::ProcessTypeCast(State& state, const TypeCast& expr) {
  const auto& type_name = *expr.typeName;
  auto type = NameToType(type_name);
  if (!type) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR), CURSOR_POS(ErrorPosition(expr.location)),
      ERR_MSG("cannot cast value to ", NameToStr(expr.typeName->names)));
  }

  auto arg = ProcessExprNodeImpl(state, expr.arg);

  // TODO(mkornaukhov): use velox::exec::CastHook for custom cast functions
  // instead of such hacks
  if (arg->type() == velox::VARCHAR() && type == velox::VARBINARY()) {
    return std::make_shared<lp::CallExpr>(std::move(type), "pg_byteain",
                                          std::move(arg));
  }

  if (arg->type() == velox::VARBINARY() && type == velox::VARCHAR()) {
    return std::make_shared<lp::CallExpr>(std::move(type), "pg_byteaout",
                                          std::move(arg));
  }

  if (arg->type() == velox::VARCHAR() && pg::IsInterval(type)) {
    const auto* typemod = type_name.typmods;
    auto range = TryGet<int>(typemod, 0).value_or(INTERVAL_FULL_RANGE);
    auto precision = TryGet<int>(typemod, 1).value_or(INTERVAL_FULL_PRECISION);
    return std::make_shared<lp::CallExpr>(std::move(type), "pg_intervalin",
                                          std::move(arg), MakeConst(range),
                                          MakeConst(precision));
  }

  if (pg::IsInterval(arg->type()) && type == velox::VARCHAR()) {
    return std::make_shared<lp::CallExpr>(std::move(type), "pg_intervalout",
                                          std::move(arg));
  }

  return MakeCast(std::move(type), std::move(arg));
}

lp::ExprPtr SqlAnalyzer::ProcessSQLValueFunction(State& state,
                                                 const SQLValueFunction& expr) {
  switch (expr.op) {
    case SVFOP_CURRENT_SCHEMA:
      return std::make_shared<lp::CallExpr>(velox::VARCHAR(),
                                            "pg_current_schema");
    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      CURSOR_POS(ErrorPosition(ExprLocation(&expr))),
                      ERR_MSG(expr.op, " is not supported"));
  }
}

lp::ExprPtr SqlAnalyzer::ProcessCollateClause(State& state,
                                              const CollateClause& expr) {
  auto arg = ProcessExprNodeImpl(state, castNode(Node, expr.arg));
  // TODO(codeworse): implement collation clause
  return arg;
}

lp::ExprPtr SqlAnalyzer::ProcessAIndirection(State& state,
                                             const A_Indirection& expr) {
  auto arg = ProcessExprNodeImpl(state, castNode(Node, expr.arg));
  VisitNodes(expr.indirection, [&](const Node& node) {
    if (IsA(&node, A_Indices)) {
      const auto& indices = *castNode(A_Indices, &node);
      if (indices.is_slice || indices.lidx) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          CURSOR_POS(ErrorPosition(ExprLocation(&node))),
          ERR_MSG("only single index subscripts are supported in indirection"));
      }
      auto index = ProcessExprNode(state, indices.uidx, ExprKind::Other);
      arg = ResolveVeloxFunctionAndInferArgsCommonType(
        "presto_subscript", std::vector{std::move(arg), std::move(index)});
    } else if (IsA(&node, String)) {
      std::string name = strVal(castNode(String, &node));
      const auto& arg_type = *arg->type();
      if (!arg_type.isRow()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        CURSOR_POS(ErrorPosition(ExprLocation(&node))),
                        ERR_MSG("only row type supports field access by name"));
      }
      const auto& arg_row_type = arg_type.asRow();
      const auto arg_idx = arg_row_type.getChildIdx(name);
      auto result_type = arg_row_type.childAt(arg_idx);
      arg = std::make_shared<lp::SpecialFormExpr>(
        std::move(result_type), lp::SpecialForm::kDereference,
        std::vector{std::move(arg), MakeConst<int64_t>(arg_idx)});
    } else {
      SDB_ASSERT(IsA(&node, A_Star));
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      CURSOR_POS(ErrorPosition(ExprLocation(&node))),
                      ERR_MSG("'*' is not supported in indirection"));
    }
  });
  return arg;
}

VeloxQuery SqlAnalyzer::ProcessRoot(State& state, const Node& node) {
  auto command_type = ProcessStmt(state, node);
  return {
    .root = std::move(state.root),
    .options = ConvertOptions(state.options),
    .pgsql_node = state.pgsql_node,
    .type = command_type,
  };
}

}  // namespace

VeloxQuery AnalyzeVelox(const RawStmt& node, const QueryString& query_string,
                        const Objects& objects, UniqueIdGenerator& id_generator,
                        query::QueryContext& query_ctx, pg::Params& params) {
  SqlAnalyzer analyzer{query_string, objects, id_generator, query_ctx, params};
  State state;
  auto query = analyzer.ProcessRoot(state, *node.stmt);

  // PG set default param types to text if they are not casted
  for (ParamIndex i = 0; i < params.types.size(); ++i) {
    auto& type = params.types[i];
    if (!type) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INDETERMINATE_DATATYPE),
        ERR_MSG("could not determine data type of parameter $", i + 1));
    }

    if (type == kUncastedParamPlaceholder) {
      type = velox::VARCHAR();
    }
  }

  return query;
}

velox::TypePtr NameToType(const TypeName& type_name) {
  auto* names = type_name.names;
  std::string_view name = strVal(llast(names));
  const auto mods_size = list_length(type_name.typmods);

  auto wrap_in_array = [&](velox::TypePtr type) {
    const auto array_bounds_size = list_length(type_name.arrayBounds);
    for (int i = 0; i < array_bounds_size; ++i) {
      type = velox::ARRAY(std::move(type));
    }
    return type;
  };

  if (mods_size == 0) {
    const auto it = kTypeCasts.find(name);
    if (it != kTypeCasts.end()) {
      return wrap_in_array(it->second);
    }
  }
  // TODO(mbkkt) more types and validation
  if (name == "numeric") {
    SDB_ASSERT(mods_size >= 1);
    const auto precision = TryGet<int>(type_name.typmods, 0);
    const auto scale =
      mods_size > 1 ? TryGet<int>(type_name.typmods, 1) : std::nullopt;
    auto decimal =
      velox::DECIMAL(precision.value_or(velox::LongDecimalType::kMaxPrecision),
                     scale.value_or(0));
    return wrap_in_array(std::move(decimal));
  }

  // a particular case because mods_size can be != 0
  if (name == "interval") {
    return wrap_in_array(pg::INTERVAL());
  }

  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  CURSOR_POS(ExprLocation(&type_name)),
                  ERR_MSG("type \"", name, "\" does not exist"));
}

}  // namespace sdb::pg
