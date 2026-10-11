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

#include "pg/catalog/engine/system_table.h"

#include <absl/algorithm/container.h>
#include <absl/container/inlined_vector.h>
#include <absl/strings/numbers.h>

#include <algorithm>
#include <compare>
#include <cstring>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/function/scalar/regexp.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/expression_iterator.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <numeric>
#include <ranges>
#include <utf8proc_wrapper.hpp>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/system_table.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/scan_function.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/oids.h"
#include "pg/connection_context.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

duckdb::LogicalType SystemColumnType(int32_t oid) {
  const auto& type = *FindBuiltinType(oid);
  if (type.relid != 0) {
    return SystemRowType(FindSystemTable(type.relid)->Sql())
      .WithAlias(std::string{type.name});
  }
  auto logical = BuiltinLogicalType(type);
  if (logical.id() == duckdb::LogicalTypeId::INVALID) {
    return duckdb::LogicalType::LIST(SystemColumnType(type.elem));
  }
  return logical;
}

SystemDefault PrepareDefault(duckdb::Value value) {
  SystemDefault prepared{.value = std::move(value), .bytes = {}};
  const auto physical = prepared.value.type().InternalType();
  if (prepared.value.IsNull() || (!duckdb::TypeIsConstantSize(physical) &&
                                  physical != duckdb::PhysicalType::VARCHAR)) {
    return prepared;
  }
  const duckdb::Vector constant{prepared.value, duckdb::count_t{1}};
  if (physical == duckdb::PhysicalType::VARCHAR &&
      !duckdb::ConstantVector::GetData<duckdb::string_t>(constant)
         ->IsInlined()) {
    return prepared;
  }
  std::memcpy(prepared.bytes.data(), duckdb::ConstantVector::GetData(constant),
              duckdb::GetTypeIdSize(physical));
  return prepared;
}

constexpr auto kMemberTypes = std::views::filter([](duckdb::CatalogType type) {
  return type != duckdb::CatalogType::SCHEMA_ENTRY;
});

bool IsNumber(const duckdb::LogicalType& type) {
  return type.id() == duckdb::LogicalTypeId::BOOLEAN ||
         (type.IsIntegral() &&
          type.InternalType() != duckdb::PhysicalType::UINT64 &&
          duckdb::GetTypeIdSize(type.InternalType()) <= sizeof(int64_t));
}

template<typename T>
std::optional<T> Constant(const duckdb::Value& value) {
  if constexpr (std::is_same_v<T, std::string>) {
    if (value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
      return std::nullopt;
    }
    return duckdb::StringValue::Get(value);
  } else {
    if (!IsNumber(value.type())) {
      return std::nullopt;
    }
    return value.GetValue<int64_t>();
  }
}

template<typename T>
std::optional<T> Constant(const duckdb::Expression& expr) {
  if (expr.GetExpressionType() != duckdb::ExpressionType::VALUE_CONSTANT) {
    return std::nullopt;
  }
  const auto& value = expr.Cast<duckdb::BoundConstantExpression>().GetValue();
  if (value.IsNull()) {
    return std::nullopt;
  }
  return Constant<T>(value);
}

std::string_view ScanPrefix(const SystemCondition<std::string>& condition) {
  if (!condition.lower || !condition.upper) {
    return {};
  }
  const std::string_view lower = condition.lower->value;
  const std::string_view upper = condition.upper->value;
  const auto common =
    static_cast<size_t>(absl::c_mismatch(lower, upper).first - lower.begin());
  if (!condition.upper->inclusive && common + 1 == lower.size() &&
      upper.size() == lower.size() &&
      static_cast<unsigned char>(upper[common]) ==
        static_cast<unsigned char>(lower[common]) + 1) {
    return lower;
  }
  return lower.substr(0, common);
}

template<typename T>
void Tighten(std::optional<SystemBound<T>>& bound, SystemBound<T> candidate,
             bool lower) {
  const bool tighter = !bound || (candidate.value == bound->value
                                    ? bound->inclusive && !candidate.inclusive
                                    : (lower ? bound->value < candidate.value
                                             : candidate.value < bound->value));
  if (tighter) {
    bound = std::move(candidate);
  }
}

template<typename T>
void Intersect(std::optional<std::vector<T>>& keys, std::vector<T> set) {
  absl::c_sort(set);
  set.erase(std::unique(set.begin(), set.end()), set.end());
  if (keys) {
    std::erase_if(set, [&](const T& value) {
      return !absl::c_binary_search(*keys, value);
    });
  }
  keys = std::move(set);
}

template<typename T>
bool Restrict(SystemCondition<T>& condition, duckdb::ExpressionType type,
              T value) {
  using enum duckdb::ExpressionType;
  if (type == COMPARE_EQUAL) {
    Intersect(condition.keys, std::vector<T>{std::move(value)});
  } else if (type == COMPARE_NOTEQUAL) {
    condition.excluded.emplace_back(std::move(value));
  } else if (type == COMPARE_GREATERTHAN ||
             type == COMPARE_GREATERTHANOREQUALTO) {
    Tighten(
      condition.lower,
      SystemBound<T>{std::move(value), type == COMPARE_GREATERTHANOREQUALTO},
      true);
  } else if (type == COMPARE_LESSTHAN || type == COMPARE_LESSTHANOREQUALTO) {
    Tighten(condition.upper,
            SystemBound<T>{std::move(value), type == COMPARE_LESSTHANOREQUALTO},
            false);
  } else {
    return false;
  }
  return true;
}

template<typename T>
bool Capture(const duckdb::Expression& expr, SystemCondition<T>& condition) {
  using enum duckdb::ExpressionClass;
  if (expr.GetExpressionClass() == BOUND_OPERATOR &&
      (expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_IN ||
       expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_NOT_IN)) {
    const auto& children =
      expr.Cast<duckdb::BoundOperatorExpression>().GetChildren();
    if (children.empty() ||
        !duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) {
      return false;
    }
    const bool excluded =
      expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_NOT_IN;
    std::vector<T> set;
    for (const auto& child : children | std::views::drop(1)) {
      if (child->GetExpressionType() !=
          duckdb::ExpressionType::VALUE_CONSTANT) {
        return false;
      }
      const auto& value =
        child->Cast<duckdb::BoundConstantExpression>().GetValue();
      if (value.IsNull()) {
        if (excluded) {
          return false;
        }
        continue;
      }
      auto constant = Constant<T>(value);
      if (!constant) {
        return false;
      }
      set.emplace_back(std::move(*constant));
    }
    if (excluded) {
      condition.excluded.insert(condition.excluded.end(),
                                std::make_move_iterator(set.begin()),
                                std::make_move_iterator(set.end()));
    } else {
      Intersect(condition.keys, std::move(set));
    }
    return true;
  }
  if (duckdb::BoundComparisonExpression::IsComparison(expr)) {
    auto type = duckdb::ExpressionType::INVALID;
    const auto constant =
      duckdb::ExpressionFilter::TryGetColumnConstantComparison(
        expr.Cast<duckdb::BoundFunctionExpression>(), type);
    if (!constant) {
      return false;
    }
    auto value = Constant<T>(*constant);
    return value && Restrict(condition, type, std::move(*value));
  }
  if (expr.GetExpressionClass() == BOUND_CONJUNCTION &&
      expr.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
    bool exact = true;
    for (const auto& child :
         expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
      exact = Capture(*child, condition) && exact;
    }
    return exact;
  }
  if (expr.GetExpressionClass() == BOUND_CONJUNCTION &&
      expr.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_OR) {
    std::vector<T> set;
    for (const auto& child :
         expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
      SystemCondition<T> branch;
      if (!Capture(*child, branch) || !branch.keys ||
          !branch.excluded.empty() || branch.lower || branch.upper ||
          branch.null) {
        return false;
      }
      set.insert(set.end(), branch.keys->begin(), branch.keys->end());
    }
    Intersect(condition.keys, std::move(set));
    return true;
  }
  if (duckdb::ExpressionFilter::IsRootOptionalExpression(expr)) {
    if (const auto child =
          duckdb::ExpressionFilter::GetOptionalFilterChild(expr)) {
      Capture(*child, condition);
    }
    return true;
  }
  if constexpr (std::is_same_v<T, int64_t>) {
    if (const auto value = BooleanOf(expr)) {
      Intersect(condition.keys, std::vector<T>{*value ? 1 : 0});
      return true;
    }
  }
  if constexpr (std::is_same_v<T, std::string>) {
    if (auto range = RangeOf(expr)) {
      Tighten(condition.lower, SystemBound<T>{std::move(range->lower), true},
              true);
      if (range->upper) {
        Tighten(condition.upper, std::move(*range->upper), false);
      }
      return range->exact;
    }
  }
  if (const auto null = NullCheckOf(expr)) {
    (*null ? condition.null : condition.not_null) = true;
    return true;
  }
  if (expr.GetExpressionType() == duckdb::ExpressionType::OPERATOR_NOT) {
    const auto& child =
      *expr.Cast<duckdb::BoundOperatorExpression>().GetChildren()[0];
    bool nulls = false;
    duckdb::ExpressionIterator::VisitExpression<
      duckdb::BoundConstantExpression>(
      child, [&](const duckdb::BoundConstantExpression& constant) {
        nulls = nulls || constant.GetValue().IsNull();
      });
    SystemCondition<T> negated;
    if (nulls || !Capture(child, negated) || !negated.keys ||
        !negated.excluded.empty() || negated.lower || negated.upper ||
        negated.null || negated.not_null) {
      return false;
    }
    condition.excluded.insert(condition.excluded.end(),
                              std::make_move_iterator(negated.keys->begin()),
                              std::make_move_iterator(negated.keys->end()));
    return true;
  }
  return false;
}

template<typename T>
void Seal(SystemCondition<T>& condition) {
  std::optional<std::vector<T>> keys;
  std::swap(keys, condition.keys);
  if (!keys && condition.lower && condition.upper &&
      condition.lower->inclusive && condition.upper->inclusive &&
      condition.lower->value == condition.upper->value) {
    keys.emplace().emplace_back(condition.lower->value);
  }
  if (!keys) {
    return;
  }
  std::erase_if(*keys, [&](const T& key) { return !condition.Passes(key); });
  condition = {.keys = std::move(keys),
               .excluded = {},
               .lower = {},
               .upper = {},
               .null = condition.null,
               .not_null = condition.not_null};
}

using SystemDataChunk = std::unique_ptr<duckdb::DataChunk>;

SystemRows<SystemDataChunk> LoadDataChunks(SystemScan& scan) {
  return {scan.Table().Chunks()};
}

class SystemDataRows final : public SystemScan {
 public:
  using SystemScan::SystemScan;

  static constexpr std::tuple kSources{
    ArraySource<SystemDataChunk>{&LoadDataChunks, {}, {}}};

  void Row(const SystemDataChunk& chunk) { EmitChunk(*chunk); }
};

void PutCell(duckdb::Vector& vector, duckdb::idx_t row,
             const duckdb::Value& fallback) {
  if (fallback.IsNull()) {
    duckdb::FlatVector::ValidityMutable(vector).SetInvalid(row);
    return;
  }
  if (vector.GetType().InternalType() == duckdb::PhysicalType::VARCHAR) {
    const auto& text = duckdb::StringValue::Get(fallback);
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vector)[row] =
      duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())};
    return;
  }
  SDB_ASSERT(vector.GetType().InternalType() == duckdb::PhysicalType::INT32);
  duckdb::FlatVector::GetDataMutable<int32_t>(vector)[row] =
    fallback.GetValue<int32_t>();
}

void PutCell(duckdb::Vector& vector, duckdb::idx_t row, std::string_view cell) {
  if (vector.GetType().InternalType() == duckdb::PhysicalType::VARCHAR) {
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vector)[row] =
      duckdb::string_t{cell.data(), static_cast<uint32_t>(cell.size())};
    return;
  }
  SDB_ASSERT(vector.GetType().InternalType() == duckdb::PhysicalType::INT32);
  int32_t number = 0;
  const bool parsed = absl::SimpleAtoi(cell, &number);
  SDB_ASSERT(parsed);
  duckdb::FlatVector::GetDataMutable<int32_t>(vector)[row] = number;
}

}  // namespace

SystemTable::SystemTable(const SystemSql& sql, SystemScanFunctions functions,
                         std::span<const SystemFact> facts,
                         std::optional<SystemVisibility> visibility,
                         std::span<const SystemTypeShape> type_shapes)
  : _sql{sql},
    _functions{functions},
    _facts{facts},
    _visibility{visibility},
    _type_shapes{type_shapes} {}

SystemTable::SystemTable(const SystemSql& sql,
                         std::span<const SystemCell> cells)
  : _sql{sql},
    _functions{&SystemScanInit<SystemDataRows>, &SystemScanRun<SystemDataRows>},
    _cells{cells} {
  SDB_ASSERT(cells.size() % sql.columns.size() == 0);
}

void SystemTable::Init() {
  for (const auto& column : _sql.columns) {
    auto type = SystemColumnType(column.type);
    SDB_ASSERT(
      type.InternalType() == column.physical &&
      (column.physical != duckdb::PhysicalType::LIST ||
       duckdb::ListType::GetChildType(type).InternalType() == column.element));
    _defaults.emplace_back(PrepareDefault(
      column.default_value
        ? duckdb::Value{std::string{*column.default_value}}.DefaultCastAs(type)
        : duckdb::Value{type}));
    _columns.AddColumn(duckdb::ColumnDefinition{duckdb::Identifier{column.name},
                                                std::move(type)});
  }
  const auto width = _sql.columns.size();
  const auto types = _columns.GetColumnTypes();
  for (size_t first = 0; first < _cells.size();
       first += width * STANDARD_VECTOR_SIZE) {
    const auto rows = std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE,
                                              (_cells.size() - first) / width);
    auto& chunk = *_chunks.emplace_back(std::make_unique<duckdb::DataChunk>());
    chunk.InitializeEmpty(types);
    for (size_t column = 0; column < width; ++column) {
      auto& storage = _storage.emplace_back(std::make_unique<std::byte[]>(
        rows * duckdb::GetTypeIdSize(types[column].InternalType())));
      chunk.data[column].Reference(duckdb::Vector{
        types[column], reinterpret_cast<duckdb::data_ptr_t>(storage.get()),
        rows});
      for (duckdb::idx_t row = 0; row < rows; ++row) {
        if (const auto& cell = _cells[first + row * width + column]) {
          PutCell(chunk.data[column], row, *cell);
        } else {
          PutCell(chunk.data[column], row, _defaults[column].value);
        }
      }
    }
    chunk.SetCardinality(rows);
  }
}

bool SystemTable::DefaultsHold(uint64_t provided) const {
  return absl::c_all_of(_facts, [&](const SystemFact& fact) {
    const auto& fallback = _defaults[fact.column].value;
    return ((provided >> fact.column) & 1) != 0 || fallback.IsNull() ||
           Holds(fact.column, fallback.GetValue<int64_t>());
  });
}

std::optional<SystemRange> RangeOf(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return std::nullopt;
  }
  const auto& function = expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& name = function.Function().GetName();
  const auto& children = function.GetChildren();
  const bool regex = name == "regexp_matches";
  if ((!regex && name != "prefix" && name != "starts_with" && name != "^@") ||
      children.size() != 2) {
    return std::nullopt;
  }
  const auto* column = children[0].get();
  if (duckdb::BoundCastExpression::IsCast(*column)) {
    const auto& cast = column->Cast<duckdb::BoundFunctionExpression>();
    if (duckdb::BoundCastExpression::SourceType(cast).id() !=
          duckdb::LogicalTypeId::VARCHAR ||
        cast.GetReturnType().id() != duckdb::LogicalTypeId::VARCHAR) {
      return std::nullopt;
    }
    column = &duckdb::BoundCastExpression::Child(cast);
  }
  auto text = Constant<std::string>(*children[1]);
  if (!duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*column) || !text) {
    return std::nullopt;
  }
  if (regex) {
    const auto& info =
      function.BindInfo()->Cast<duckdb::RegexpMatchesBindData>();
    if (!info.range_success || !text->starts_with('^') || text->contains('|')) {
      return std::nullopt;
    }
    return SystemRange{.lower = info.range_min,
                       .upper = SystemBound<std::string>{info.range_max, true},
                       .exact = false};
  }
  if (text->empty()) {
    return std::nullopt;
  }
  SystemRange range{.lower = *text, .upper = std::nullopt, .exact = false};
  if (duckdb::Utf8Proc::FindNextLegalUTF8(*text)) {
    range.upper = SystemBound<std::string>{std::move(*text), false};
    range.exact = true;
  }
  return range;
}

std::optional<bool> BooleanOf(const duckdb::Expression& expr) {
  const bool negated =
    expr.GetExpressionType() == duckdb::ExpressionType::OPERATOR_NOT;
  const auto& operand =
    negated ? *expr.Cast<duckdb::BoundOperatorExpression>().GetChildren()[0]
            : expr;
  if (!duckdb::ExpressionFilter::IsSimpleFilterColumnRef(operand) ||
      operand.GetReturnType().id() != duckdb::LogicalTypeId::BOOLEAN) {
    return std::nullopt;
  }
  return !negated;
}

bool IsVisibility(const duckdb::Expression& expr,
                  const SystemVisibility& visibility) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_FUNCTION) {
    return false;
  }
  const auto& function = expr.Cast<duckdb::BoundFunctionExpression>();
  const auto& children = function.GetChildren();
  return function.Function().GetName() == visibility.function &&
         children.size() == 1 &&
         duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0]);
}

std::optional<bool> NullCheckOf(const duckdb::Expression& expr) {
  const auto type = expr.GetExpressionType();
  if ((type != duckdb::ExpressionType::OPERATOR_IS_NULL &&
       type != duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) ||
      !duckdb::ExpressionFilter::IsSimpleFilterColumnRef(
        *expr.Cast<duckdb::BoundOperatorExpression>().GetChildren()[0])) {
    return std::nullopt;
  }
  return type == duckdb::ExpressionType::OPERATOR_IS_NULL;
}

bool IsExclusion(const duckdb::Expression& expr) {
  if (expr.GetExpressionClass() != duckdb::ExpressionClass::BOUND_OPERATOR) {
    return false;
  }
  const auto type = expr.GetExpressionType();
  const auto& children =
    expr.Cast<duckdb::BoundOperatorExpression>().GetChildren();
  if (type == duckdb::ExpressionType::COMPARE_NOT_IN) {
    return !children.empty() &&
           duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0]) &&
           absl::c_all_of(
             children | std::views::drop(1),
             [](const duckdb::unique_ptr<duckdb::Expression>& child) {
               return child->GetExpressionType() ==
                        duckdb::ExpressionType::VALUE_CONSTANT &&
                      !child->Cast<duckdb::BoundConstantExpression>()
                         .GetValue()
                         .IsNull();
             });
  }
  if (type != duckdb::ExpressionType::OPERATOR_NOT) {
    return false;
  }
  const auto child = children[0]->GetExpressionType();
  return child == duckdb::ExpressionType::COMPARE_IN ||
         child == duckdb::ExpressionType::CONJUNCTION_OR;
}

std::strong_ordering operator<=>(const SystemConcatText& text,
                                 std::string_view other) {
  for (const auto part : text.parts) {
    const auto common = std::min(part.size(), other.size());
    if (const auto order = part.substr(0, common) <=> other.substr(0, common);
        order != 0) {
      return order;
    }
    if (part.size() > other.size()) {
      return std::strong_ordering::greater;
    }
    other.remove_prefix(common);
  }
  return other.empty() ? std::strong_ordering::equal
                       : std::strong_ordering::less;
}

duckdb::LogicalType SystemRowType(const SystemSql& sql) {
  duckdb::child_list_t<duckdb::LogicalType> children;
  for (const auto& column : sql.columns) {
    children.emplace_back(duckdb::Identifier{column.name},
                          SystemColumnType(column.type));
  }
  return duckdb::LogicalType::STRUCT(std::move(children));
}

SystemScan::SystemScan(duckdb::ClientContext& context,
                       duckdb::TableFunctionInitInput& input)
  : _context{context},
    _database{
      connector::BoundSystemTable(input).catalog.Cast<duckdb::DuckCatalog>()},
    _transaction{_database.GetCatalogTransaction(context)},
    _table{connector::BoundSystemTable(input).Table()},
    _column_ids{input.column_ids} {
  SDB_ASSERT(_column_ids.size() <= _places.size());
  _places.fill(kNowhere);
  if (input.CanRemoveFilterColumns()) {
    for (size_t i = 0; i < input.projection_ids.size(); ++i) {
      _places[input.projection_ids[i]] = static_cast<uint32_t>(i);
    }
  } else {
    std::iota(_places.begin(), _places.begin() + _column_ids.size(),
              uint32_t{0});
  }
  const auto width = static_cast<uint32_t>(input.CanRemoveFilterColumns()
                                             ? input.projection_ids.size()
                                             : _column_ids.size());
  duckdb::vector<duckdb::LogicalType> side;
  if (input.filters) {
    for (const auto& entry : *input.filters) {
      const auto slot = entry.GetIndex();
      const auto column = _column_ids[slot];
      if (!_table.Chunks().empty() || !Compile(column, entry.Filter())) {
        _residuals.emplace_back(SystemResidual{
          .slot = static_cast<uint32_t>(slot),
          .state =
            duckdb::TableFilterState::Initialize(context, entry.Filter())});
        if (_places[slot] == kNowhere) {
          _places[slot] = width + static_cast<uint32_t>(side.size());
          side.emplace_back(
            column < _table.Columns().LogicalColumnCount()
              ? _table.Columns().GetColumn(duckdb::LogicalIndex{column}).Type()
              : duckdb::LogicalType::BIGINT);
        }
      } else if (const auto& type = _table.Columns()
                                      .GetColumn(duckdb::LogicalIndex{column})
                                      .Type();
                 type.IsIntegral() &&
                 duckdb::ExpressionFilter::IsRootOptionalFilter(
                   entry.Filter()) &&
                 !(Filtered(static_cast<uint32_t>(column)) &&
                   _filters[_filter_index[column]].numbers.keys)) {
        _pruned |= uint64_t{1} << column;
        _prunes.emplace_back(SystemPrune{
          .column = static_cast<uint32_t>(column),
          .filter = &entry.Filter(),
          .state =
            duckdb::TableFilterState::Initialize(context, entry.Filter()),
          .stats = duckdb::BaseStatistics::FromConstant(
            duckdb::Value::BIGINT(0).DefaultCastAs(type))});
      }
    }
  }
  if (!side.empty()) {
    _side.Initialize(context, side);
  }
  const auto column_count = _table.Columns().LogicalColumnCount();
  for (size_t slot = 0; slot < _column_ids.size(); ++slot) {
    const auto column = _column_ids[slot];
    if (column >= column_count || _places[slot] == kNowhere) {
      continue;
    }
    _slots[column] = _places[slot];
    _needed |= uint64_t{1} << column;
    if (!_roles && _table.Sql().columns[column].type == kAclitemArray) {
      _roles = auth::RolesOf(&context);
    }
  }
}

bool SystemScan::AllowsAbove(uint32_t column, int64_t value) const {
  if (!Filtered(column)) {
    return true;
  }
  const auto& numbers = _filters[_filter_index[column]].numbers;
  if (numbers.keys) {
    return !numbers.keys->empty() && value < numbers.keys->back();
  }
  if (!numbers.upper) {
    return true;
  }
  const auto& upper = *numbers.upper;
  return value < upper.value && (upper.inclusive || value + 1 < upper.value);
}

bool SystemScan::Survives(uint32_t column, int64_t min, int64_t max) const {
  return absl::c_none_of(_prunes, [&](const SystemPrune& prune) {
    if (prune.column != column) {
      return false;
    }
    duckdb::NumericStats::SetMin<int64_t>(prune.stats, min);
    duckdb::NumericStats::SetMax<int64_t>(prune.stats, max);
    const auto result =
      duckdb::ExpressionFilter::GetExpressionFilter(*prune.filter, "SystemScan")
        .CheckStatistics(prune.stats, *prune.state);
    return result == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE ||
           result == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL;
  });
}

bool SystemScan::Possible(std::span<const SystemFact> facts) const {
  uint64_t constrained = 0;
  uint64_t possible = 0;
  for (const auto& fact : facts) {
    const auto bit = uint64_t{1} << fact.column;
    constrained |= bit;
    if (Overlaps(fact)) {
      possible |= bit;
    }
  }
  return possible == constrained;
}

bool SystemScan::Overlaps(const SystemFact& fact) const {
  if (const auto* filter = FilterOf(fact.column)) {
    const auto& numbers = filter->numbers;
    if (numbers.null) {
      return false;
    }
    if (numbers.keys) {
      const auto key = absl::c_lower_bound(*numbers.keys, fact.min);
      if (key == numbers.keys->end() || fact.max < *key) {
        return false;
      }
    }
    if (numbers.lower &&
        (numbers.lower->inclusive ? fact.max < numbers.lower->value
                                  : fact.max <= numbers.lower->value)) {
      return false;
    }
    if (numbers.upper &&
        (numbers.upper->inclusive ? numbers.upper->value < fact.min
                                  : numbers.upper->value <= fact.min)) {
      return false;
    }
    if (fact.min == fact.max &&
        absl::c_linear_search(numbers.excluded, fact.min)) {
      return false;
    }
  }
  return ((_pruned >> fact.column) & 1) == 0 ||
         Survives(fact.column, fact.min, fact.max);
}

bool SystemScan::AllowsSchema(std::optional<uint32_t> column,
                              const duckdb::SchemaCatalogEntry& schema) const {
  if (!column) {
    return true;
  }
  if (_table.Sql().columns[*column].physical == duckdb::PhysicalType::VARCHAR) {
    return Allows(*column, std::string_view{schema.name.GetIdentifierName()});
  }
  return Allows(*column, static_cast<int64_t>(schema.oid));
}

#ifdef SDB_DEV
bool SystemScan::Declared(uint64_t provided) const {
  const auto shapes = _table.TypeShapes();
  return shapes.empty() || _visiting == duckdb::CatalogType::INVALID ||
         absl::c_any_of(shapes, [&](const SystemTypeShape& shape) {
           return shape.type == _visiting && shape.provided == provided;
         });
}
#endif

bool SystemScan::Compile(duckdb::column_t column,
                         const duckdb::TableFilter& filter) {
  if (column >= _table.Columns().LogicalColumnCount()) {
    return false;
  }
  const auto& type =
    _table.Columns().GetColumn(duckdb::LogicalIndex{column}).Type();
  const auto& expr =
    *duckdb::ExpressionFilter::GetExpressionFilter(filter, "SystemScan").expr;
  if (const auto& visibility = _table.Visibility();
      visibility && visibility->column == column &&
      IsVisibility(expr, *visibility)) {
    _visible_only = true;
    return true;
  }
  const auto& fallback = _table.Default(column).value;
  SystemFilter compiled;
  bool exact = false;
  bool passes = false;
  const bool not_null = _table.Sql().columns[column].not_null;
  if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
    exact = Capture(expr, compiled.texts);
    compiled.texts.not_null = compiled.texts.not_null && !not_null;
    Seal(compiled.texts);
    if (_table.Sql().columns[column].type == kChar) {
      compiled.chars[0] = compiled.texts.Passes(std::string_view{});
      for (uint32_t c = 1; c < 256; ++c) {
        const auto ch = static_cast<char>(c);
        compiled.chars[c] = compiled.texts.Passes(std::string_view{&ch, 1});
      }
    }
    passes = fallback.IsNull() ? compiled.texts.PassesNull()
                               : compiled.texts.Passes(std::string_view{
                                   duckdb::StringValue::Get(fallback)});
  } else if (IsNumber(type)) {
    exact = Capture(expr, compiled.numbers);
    compiled.numbers.not_null = compiled.numbers.not_null && !not_null;
    Seal(compiled.numbers);
    passes = fallback.IsNull()
               ? compiled.numbers.PassesNull()
               : compiled.numbers.Passes(fallback.GetValue<int64_t>());
  } else {
    return false;
  }
  if (!compiled.texts.Empty() || !compiled.numbers.Empty()) {
    _filtered |= uint64_t{1} << column;
    if (!passes) {
      _rejecting |= uint64_t{1} << column;
    }
    _filter_index[column] = static_cast<uint8_t>(_filters.size());
    _filters.emplace_back(std::move(compiled));
  }
  return exact;
}

SystemScan::~SystemScan() = default;

std::shared_ptr<const auth::RoleClosure> SystemScan::SessionClosure() const {
  auto* connection = connector::GetSereneDBContextPtr(_context);
  return connection ? auth::ClosureFor(&_context, connection->GetRoleId())
                    : nullptr;
}

void SystemScan::Begin(duckdb::DataChunk& output) {
  output.Reset();
  _row = 0;
  _full = false;
  _vectors.clear();
  const auto add = [&](duckdb::Vector& vector) {
    _vectors.emplace_back(
      SystemVector{.vector = &vector,
                   .data = duckdb::FlatVector::GetDataMutable(vector),
                   .validity = &duckdb::FlatVector::ValidityMutable(vector)});
  };
  output.SetChildCardinality(STANDARD_VECTOR_SIZE);
  for (auto& vector : output.data) {
    add(vector);
  }
  _side.Reset();
  _side.SetChildCardinality(STANDARD_VECTOR_SIZE);
  for (auto& vector : _side.data) {
    add(vector);
  }
}

void SystemScan::End(duckdb::DataChunk& output) {
  output.SetChildCardinality(_row);
  _side.SetChildCardinality(_row);
  for (size_t slot = 0; slot < _column_ids.size(); ++slot) {
    if (_places[slot] == kNowhere) {
      continue;
    }
    if (_column_ids[slot] == connector::kColumnIdentifierTableOid) {
      _vectors[_places[slot]].vector->Reference(
        duckdb::Value::BIGINT(static_cast<int64_t>(_table.Sql().oid)),
        duckdb::count_t{_row});
    } else if (_column_ids[slot] == duckdb::COLUMN_IDENTIFIER_EMPTY) {
      _vectors[_places[slot]].vector->Reference(
        duckdb::Value{duckdb::LogicalType::BOOLEAN}, duckdb::count_t{_row});
    }
  }
  if (_residuals.empty() || _row == 0) {
    return;
  }
  duckdb::SelectionVector sel;
  duckdb::idx_t approved = _row;
  for (const auto& residual : _residuals) {
    if (approved == 0) {
      break;
    }
    duckdb::ColumnSegment::FilterSelection(
      sel, *_vectors[_places[residual.slot]].vector, *residual.state, _row,
      approved);
  }
  if (approved == 0) {
    output.SetCardinality(0);
    return;
  }
  if (approved == _row) {
    return;
  }
  duckdb::SelectionVector owned{approved};
  std::copy_n(sel.data(), approved, owned.data());
  output.Slice(owned, approved);
}

void SystemScan::EmitChunk(const duckdb::DataChunk& source) {
  if (_row != 0) {
    _full = true;
    return;
  }
  const auto column_count = _table.Columns().LogicalColumnCount();
  for (size_t slot = 0; slot < _column_ids.size(); ++slot) {
    if (const auto column = _column_ids[slot];
        column < column_count && _places[slot] != kNowhere) {
      _vectors[_places[slot]].vector->Reference(source.data[column]);
    }
  }
  _row = source.size();
}

void SystemScan::PutText(const SystemVector& target, duckdb::idx_t row,
                         std::string_view value) {
  auto* data = Cells<duckdb::string_t>(target);
  if (value.size() <= duckdb::string_t::INLINE_LENGTH) {
    data[row] =
      duckdb::string_t{value.data(), static_cast<uint32_t>(value.size())};
    return;
  }
  data[row] = duckdb::StringVector::AddStringOrBlob(*target.vector,
                                                    value.data(), value.size());
}

void SystemScan::PutConcat(const SystemVector& target, duckdb::idx_t row,
                           const SystemConcatText& value) {
  const auto size = absl::c_accumulate(
    value.parts, size_t{0},
    [](size_t sum, std::string_view part) { return sum + part.size(); });
  auto text = duckdb::StringVector::EmptyString(*target.vector, size);
  auto* out = text.GetDataWriteable();
  for (const auto part : value.parts) {
    out = std::ranges::copy(part, out).out;
  }
  text.Finalize();
  Cells<duckdb::string_t>(target)[row] = text;
}

void SystemScan::PutAcl(const SystemVector& target, duckdb::idx_t row,
                        const duckdb::AclItem& item) {
  _text.clear();
  AppendAcl(_text, item, *_roles);
  PutText(target, row, _text);
}

const SystemIndex* SystemScan::Choose(std::span<const SystemIndex> indexes,
                                      size_t name_probes) const {
  const SystemIndex* chosen = nullptr;
  auto fewest = std::numeric_limits<size_t>::max();
  for (const auto& index : indexes) {
    const auto* filter = FilterOf(index.column);
    if (!filter || (index.lookup != SystemLookup::Object &&
                    index.lookup != SystemLookup::Dependents)) {
      continue;
    }
    const auto probes = filter->numbers.keys ? filter->numbers.keys->size()
                        : filter->texts.keys
                          ? filter->texts.keys->size() * name_probes
                          : fewest;
    if (probes < fewest) {
      chosen = &index;
      fewest = probes;
    }
  }
  return chosen;
}

std::string_view SystemScan::NamePrefix(
  std::span<const SystemIndex> indexes) const {
  for (const auto& index : indexes) {
    if (index.lookup != SystemLookup::Object ||
        _table.Sql().columns[index.column].physical !=
          duckdb::PhysicalType::VARCHAR) {
      continue;
    }
    if (const auto* filter = FilterOf(index.column)) {
      return ScanPrefix(filter->texts);
    }
  }
  return {};
}

SystemMembers SystemScan::Resolve(const CatalogSource& source) {
  using enum duckdb::CatalogType;
  SystemMembers members;
  auto& wanted = members.wanted;
  auto& entries = members.entries;
  for (const auto type : source.types) {
    wanted.set(std::to_underlying(type));
  }
  bool numbers = false;
  bool names = false;
  for (const auto& index : source.indexes) {
    if (index.lookup == SystemLookup::Namespace) {
      members.nsp_column = index.column;
    }
    const auto* filter = FilterOf(index.column);
    if (!filter) {
      continue;
    }
    switch (index.lookup) {
      case SystemLookup::Kind: {
        const bool text = _table.Sql().columns[index.column].physical ==
                          duckdb::PhysicalType::VARCHAR;
        for (const auto type : source.types) {
          if (absl::c_none_of(index.kinds, [&](const SystemKindTypes& kind) {
                return absl::c_linear_search(kind.types, type) &&
                       (text
                          ? Passes(index.column, static_cast<char>(kind.kind))
                          : Passes(index.column, kind.kind));
              })) {
            wanted.reset(std::to_underlying(type));
          }
        }
        break;
      }
      case SystemLookup::Namespace:
        break;
      case SystemLookup::Object:
      case SystemLookup::Dependents:
        numbers = numbers || filter->numbers.keys.has_value();
        names = names || filter->texts.keys.has_value();
        break;
    }
  }
  if (_rejecting != 0) {
    for (const auto type : source.types) {
      bool declared = false;
      bool possible = false;
      for (const auto& shape : _table.TypeShapes()) {
        if (shape.type == type) {
          declared = true;
          possible = possible || (_rejecting & ~shape.provided) == 0;
        }
      }
      if (declared && !possible) {
        wanted.reset(std::to_underlying(type));
      }
    }
  }
  if (wanted.none()) {
    return members;
  }
  if (names || !numbers) {
    members.schemas = Schemas(source.schemas);
  }
  auto member_types = source.types | kMemberTypes |
                      std::views::filter([&](duckdb::CatalogType type) {
                        return wanted.test(std::to_underlying(type));
                      });
  const auto scoped = names
                        ? static_cast<size_t>(absl::c_count_if(
                            members.schemas,
                            [&](const duckdb::SchemaCatalogEntry* schema) {
                              return AllowsSchema(members.nsp_column, *schema);
                            }))
                        : size_t{0};
  const auto* index = Choose(
    source.indexes,
    std::max<size_t>(
      scoped * static_cast<size_t>(std::ranges::distance(member_types)), 1));
  if (!index) {
    if (!members.schemas.empty()) {
      auto& sets = members.schemas.front()->Cast<duckdb::DuckSchemaEntry>();
      absl::InlinedVector<const duckdb::CatalogSet*, 8> walked;
      for (const auto type : member_types) {
        if (const auto* set = &sets.GetCatalogSet(type);
            !absl::c_linear_search(walked, set)) {
          walked.emplace_back(set);
          members.sets.emplace_back(type);
        }
      }
    }
    if (const auto text = NamePrefix(source.indexes);
        !text.empty() && !(text.starts_with('_') &&
                           wanted.test(std::to_underlying(TYPE_ENTRY)))) {
      members.prefix.emplace(SystemPrefix{
        .lower = duckdb::Identifier{text},
        .upper =
          duckdb::Identifier{absl::StrCat(text, std::string_view{"\xff", 1})}});
    }
    return members;
  }
  const auto& filter = *FilterOf(index->column);
  if (const auto& keys = filter.texts.keys) {
    for (const auto& name : *keys) {
      AppendNamed(name, source.types, source.schemas, members);
    }
  }
  if (const auto& oids = filter.numbers.keys) {
    for (const auto oid : *oids) {
      AppendOwned(static_cast<duckdb::idx_t>(oid), source.schemas, entries);
    }
  }
  if (index->lookup == SystemLookup::Dependents) {
    auto& dependencies = Dependencies();
    for (size_t i = 0, owners = entries.size(); i < owners; ++i) {
      dependencies.ScanEdges(
        Transaction(), *entries[i], false,
        [&](duckdb::CatalogEntry& dependent,
            const duckdb::DependencyDependentFlags&) {
          if (!dependent.internal &&
              wanted.test(std::to_underlying(dependent.type))) {
            entries.emplace_back(&dependent);
          }
        });
    }
  }
  absl::c_sort(entries, [](const duckdb::CatalogEntry* lhs,
                           const duckdb::CatalogEntry* rhs) {
    return std::tuple{lhs->type, lhs->oid, lhs} <
           std::tuple{rhs->type, rhs->oid, rhs};
  });
  entries.erase(std::unique(entries.begin(), entries.end()), entries.end());
  members.schemas.clear();
  return members;
}

SystemSet SystemScan::MemberSet(duckdb::SchemaCatalogEntry& schema,
                                duckdb::CatalogType type,
                                const SystemPrefix* prefix) const {
  auto& set = schema.Cast<duckdb::DuckSchemaEntry>().GetCatalogSet(type);
  auto snapshot =
    _database.Cast<catalog::SereneDBCatalog>().Snapshot(_context, set);
  if (!snapshot) {
    auto scanned = std::make_shared<catalog::CatalogSnapshot>();
    set.Scan(Transaction(), [&](duckdb::CatalogEntry& entry) {
      scanned->entries.emplace_back(&entry);
    });
    snapshot = std::move(scanned);
  }
  std::span<duckdb::CatalogEntry* const> entries{snapshot->entries};
  if (prefix) {
    const duckdb::IdentifierCompare compare{
      schema.ParentCatalog().IsCaseSensitive()};
    const auto name =
      [](const duckdb::CatalogEntry* entry) -> const duckdb::Identifier& {
      return entry->name;
    };
    const auto first =
      std::ranges::lower_bound(entries, prefix->lower, compare, name);
    entries = {first, std::ranges::upper_bound(first, entries.end(),
                                               prefix->upper, compare, name)};
  }
  return {.snapshot = std::move(snapshot), .found = {}, .entries = entries};
}

std::span<
  const std::pair<duckdb::CatalogEntry*, duckdb::DependencyDependentFlags>>
SystemScan::WalkedSubjects(duckdb::CatalogEntry& entry) const {
  const auto collect = [&](duckdb::CatalogEntry& member) {
    std::vector<
      std::pair<duckdb::CatalogEntry*, duckdb::DependencyDependentFlags>>
      edges;
    Dependencies().ScanEdges(
      Transaction(), member, true,
      [&](duckdb::CatalogEntry& subject,
          const duckdb::DependencyDependentFlags& flags) {
        edges.emplace_back(&subject, flags);
      });
    return edges;
  };
  const auto& subjects = _walked->subjects.Get([&](auto& subjects) {
    for (auto* member : _walked->entries) {
      if (auto edges = collect(*member); !edges.empty()) {
        subjects.emplace(member, std::move(edges));
      }
    }
  });
  const auto it = subjects.find(&entry);
  const std::span<
    const std::pair<duckdb::CatalogEntry*, duckdb::DependencyDependentFlags>>
    edges = it == subjects.end()
              ? std::span<const std::pair<duckdb::CatalogEntry*,
                                          duckdb::DependencyDependentFlags>>{}
              : std::span{it->second};
  SDB_ASSERT(absl::c_equal(edges, collect(entry)));
  return edges;
}

bool SystemScan::Indexed(duckdb::CatalogEntry& relation) const {
  if (!_walked) {
    bool indexed = false;
    Dependencies().ScanEdges(
      Transaction(), relation, false,
      [&](duckdb::CatalogEntry& dependent,
          const duckdb::DependencyDependentFlags&) {
        indexed =
          indexed || dependent.type == duckdb::CatalogType::INDEX_ENTRY;
      });
    return indexed;
  }
  const auto& indexed = _walked->indexed.Get([&](auto& indexed) {
    const auto indexes = MemberSet(relation.ParentSchema(Transaction()),
                                   duckdb::CatalogType::INDEX_ENTRY, nullptr);
    for (auto* entry : indexes.entries) {
      indexed.insert(entry->Cast<duckdb::IndexCatalogEntry>().table_oid);
    }
  });
  return !indexed.empty() && indexed.contains(relation.oid);
}

bool SystemScan::Triggered(const duckdb::TableCatalogEntry& table) const {
  const auto any = [&](const duckdb::TableCatalogEntry& member) {
    bool triggered = false;
    member.ScanTriggers(Transaction(),
                        [&](duckdb::CatalogEntry&) { triggered = true; });
    return triggered;
  };
  if (!_walked) {
    return any(table);
  }
  const auto& triggered = _walked->triggered.Get([&](auto& triggered) {
    for (auto* entry : _walked->entries) {
      if (entry->type == duckdb::CatalogType::TABLE_ENTRY &&
          any(entry->Cast<duckdb::TableCatalogEntry>())) {
        triggered.insert(entry->oid);
      }
    }
  });
  return !triggered.empty() && triggered.contains(table.oid);
}

SystemSet SystemScan::Resolve(const CatalogSetSource& source) const {
  catalog::SnapshotCatalog& catalog =
    source.catalog == SystemCatalog::Cluster
      ? catalog::ClusterOf(_context)
      : _database.Cast<catalog::SnapshotCatalog>();
  auto& set = catalog.GetCatalogSet(source.type);
  const auto transaction = catalog.GetCatalogTransaction(_context);
  SystemSet result;
  auto& entries = result.found;
  const auto* index = Choose(source.indexes, 1);
  if (!index) {
    result.snapshot = catalog.Snapshot(_context, set);
    if (result.snapshot) {
      result.entries = result.snapshot->entries;
      return result;
    }
    set.Scan(transaction, [&](duckdb::CatalogEntry& entry) {
      entries.emplace_back(&entry);
    });
    result.entries = entries;
    return result;
  }
  const auto add = [&](duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
    if (entry && entry->type == source.type) {
      entries.emplace_back(entry.get());
    }
  };
  const auto& filter = *FilterOf(index->column);
  if (const auto& names = filter.texts.keys) {
    for (const auto& name : *names) {
      add(set.GetEntry(transaction, duckdb::Identifier{name}));
    }
  }
  if (const auto& oids = filter.numbers.keys) {
    for (const auto oid : *oids) {
      add(catalog.GetOidIndex().GetVisible(static_cast<duckdb::idx_t>(oid),
                                           transaction.view));
    }
  }
  result.entries = entries;
  return result;
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SystemScan::FindSchema(
  std::string_view name, SystemSchemas system) const {
  auto schema =
    _database.GetSchema(Transaction(), duckdb::Identifier{std::string{name}},
                        duckdb::OnEntryNotFound::RETURN_NULL);
  if (!schema || (schema->internal && (system == SystemSchemas::Skip ||
                                       !FindSystemNamespace(name)))) {
    return nullptr;
  }
  return schema;
}

std::vector<duckdb::SchemaCatalogEntry*> SystemScan::Schemas(
  SystemSchemas system) const {
  std::vector<duckdb::SchemaCatalogEntry*> schemas;
  if (system == SystemSchemas::Visit) {
    for (const auto& schema : kSystemNamespaces) {
      if (auto entry = FindSchema(schema.name, system)) {
        schemas.emplace_back(entry.get());
      }
    }
  }
  for (auto& schema : _database.GetSchemas(_context) |
                        std::views::filter([](const auto& schema) {
                          return !schema.get().internal;
                        })) {
    schemas.emplace_back(&schema.get());
  }
  return schemas;
}

void SystemScan::AppendOwned(
  duckdb::idx_t oid, SystemSchemas system,
  std::vector<duckdb::CatalogEntry*>& entries) const {
  if (system == SystemSchemas::Visit) {
    if (const auto* schema = FindSystemNamespace(oid)) {
      if (auto entry = FindSchema(schema->name, system)) {
        entries.emplace_back(entry.get());
      }
      return;
    }
    std::pair<std::string_view, std::string_view> relation;
    if (const auto* table = FindSystemTable(oid)) {
      relation = {table->Sql().schema, table->Sql().name};
    } else if (const auto* view = FindSystemView(oid)) {
      relation = {view->schema, view->name};
    }
    if (!relation.second.empty()) {
      if (auto schema = FindSchema(relation.first, system)) {
        if (auto entry = FindMember(Transaction(), *schema,
                                    duckdb::CatalogType::TABLE_ENTRY,
                                    duckdb::Identifier{relation.second})) {
          entries.emplace_back(entry.get());
        }
      }
      return;
    }
  }
  if (oid & kRowTypeOidBit) {
    oid = RowTypeRelation(oid);
  }
  auto owner = FindOwner(oid);
  if (!owner) {
    owner = FindOwner(oid + 1);
    if (owner && owner->type != duckdb::CatalogType::TYPE_ENTRY) {
      return;
    }
  }
  if (owner && !owner->internal) {
    entries.emplace_back(owner.get());
  }
}

void SystemScan::AppendNamed(std::string_view name,
                             std::span<const duckdb::CatalogType> types,
                             SystemSchemas system,
                             SystemMembers& members) const {
  using enum duckdb::CatalogType;
  const auto& wanted = members.wanted;
  auto& entries = members.entries;
  const duckdb::Identifier identifier{name};
  for (auto* schema : members.schemas) {
    if (!AllowsSchema(members.nsp_column, *schema)) {
      continue;
    }
    const auto found = entries.size();
    for (const auto type : types | kMemberTypes) {
      if (!wanted.test(std::to_underlying(type))) {
        continue;
      }
      if (auto entry = FindMember(Transaction(), *schema, type, identifier);
          entry && entry->type == type &&
          (!entry->internal || schema->internal)) {
        entries.emplace_back(entry.get());
      }
    }
    if (entries.size() == found &&
        wanted.test(std::to_underlying(INDEX_ENTRY))) {
      if (const auto key = FindKeyIndex(_context, *schema, name);
          key.key_index) {
        entries.emplace_back(key.table);
      }
    }
  }
  if (wanted.test(std::to_underlying(SCHEMA_ENTRY))) {
    if (auto schema = FindSchema(name, system)) {
      entries.emplace_back(schema.get());
    }
  }
  if (wanted.test(std::to_underlying(TYPE_ENTRY)) && name.starts_with('_')) {
    AppendNamed(name.substr(1), types, system, members);
  }
}

}  // namespace sdb::pg
