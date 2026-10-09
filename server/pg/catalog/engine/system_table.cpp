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
#include <cstring>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/function/scalar/regexp.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_conjunction_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/system_compiler.hpp>
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
  if (condition.keys || !condition.lower || !condition.upper) {
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
      expr.GetExpressionType() == duckdb::ExpressionType::COMPARE_IN) {
    const auto& children =
      expr.Cast<duckdb::BoundOperatorExpression>().GetChildren();
    if (children.empty() ||
        !duckdb::ExpressionFilter::IsSimpleFilterColumnRef(*children[0])) {
      return false;
    }
    std::vector<T> set;
    for (const auto& child : children | std::views::drop(1)) {
      if (child->GetExpressionType() !=
          duckdb::ExpressionType::VALUE_CONSTANT) {
        return false;
      }
      const auto& value =
        child->Cast<duckdb::BoundConstantExpression>().GetValue();
      if (value.IsNull()) {
        continue;
      }
      auto constant = Constant<T>(value);
      if (!constant) {
        return false;
      }
      set.emplace_back(std::move(*constant));
    }
    absl::c_sort(set);
    set.erase(std::unique(set.begin(), set.end()), set.end());
    Intersect(condition.keys, std::move(set));
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
  condition = {
    .keys = std::move(keys), .excluded = {}, .lower = {}, .upper = {}};
}

using SystemDataChunk = std::unique_ptr<duckdb::DataChunk>;

SystemRows<SystemDataChunk> LoadDataChunks(SystemScan& scan) {
  return {scan.Table().Chunks()};
}

class SystemDataRows final : public SystemScan {
 public:
  using SystemScan::SystemScan;

  static constexpr std::tuple kSources{
    ArraySource<SystemDataChunk>{&LoadDataChunks, {}}};

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
                         std::span<const SystemFact> facts)
  : _sql{sql}, _functions{functions}, _facts{facts} {}

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
    _column_ids{input.column_ids},
    _places(_column_ids.size(), kNowhere) {
  if (input.CanRemoveFilterColumns()) {
    for (size_t i = 0; i < input.projection_ids.size(); ++i) {
      _places[input.projection_ids[i]] = static_cast<uint32_t>(i);
    }
  } else {
    absl::c_iota(_places, uint32_t{0});
  }
  const auto width = static_cast<uint32_t>(absl::c_count_if(
    _places, [](uint32_t place) { return place != kNowhere; }));
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
                   entry.Filter())) {
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

bool SystemScan::Survives(uint32_t column, int64_t value) const {
  return absl::c_none_of(_prunes, [&](const SystemPrune& prune) {
    if (prune.column != column) {
      return false;
    }
    duckdb::NumericStats::SetMin<int64_t>(prune.stats, value);
    duckdb::NumericStats::SetMax<int64_t>(prune.stats, value);
    const auto result =
      duckdb::ExpressionFilter::GetExpressionFilter(*prune.filter, "SystemScan")
        .CheckStatistics(prune.stats, *prune.state);
    return result == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE ||
           result == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL;
  });
}

bool SystemScan::Compile(duckdb::column_t column,
                         const duckdb::TableFilter& filter) {
  if (column >= _table.Columns().LogicalColumnCount()) {
    return false;
  }
  const auto& type =
    _table.Columns().GetColumn(duckdb::LogicalIndex{column}).Type();
  const auto& expr =
    *duckdb::ExpressionFilter::GetExpressionFilter(filter, "SystemScan").expr;
  const auto& fallback = _table.Default(column).value;
  SystemFilter compiled{
    .column = static_cast<uint32_t>(column), .numbers = {}, .texts = {}};
  bool exact = false;
  bool passes = false;
  if (type.id() == duckdb::LogicalTypeId::VARCHAR) {
    exact = Capture(expr, compiled.texts);
    Seal(compiled.texts);
    if (_table.Sql().columns[column].type == kChar) {
      for (uint32_t c = 0; c < 256; ++c) {
        const auto ch = static_cast<char>(c);
        compiled.chars[c] = compiled.texts.Passes(std::string_view{&ch, 1});
      }
      compiled.chars[256] = compiled.texts.Passes(std::string_view{});
    }
    passes = !fallback.IsNull() && compiled.texts.Passes(std::string_view{
                                     duckdb::StringValue::Get(fallback)});
  } else if (IsNumber(type)) {
    exact = Capture(expr, compiled.numbers);
    Seal(compiled.numbers);
    passes = !fallback.IsNull() &&
             compiled.numbers.Passes(fallback.GetValue<int64_t>());
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
    if (_column_ids[slot] == connector::kColumnIdentifierTableOid &&
        _places[slot] != kNowhere) {
      _vectors[_places[slot]].vector->Reference(
        duckdb::Value::BIGINT(static_cast<int64_t>(_table.Sql().oid)),
        duckdb::count_t{_row});
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

void SystemScan::PutAcl(const SystemVector& target, duckdb::idx_t row,
                        const duckdb::AclItem& item) {
  _text.clear();
  AppendAcl(_text, item, *_roles);
  PutText(target, row, _text);
}

const SystemIndex* SystemScan::Choose(
  std::span<const SystemIndex> indexes) const {
  const auto it = absl::c_find_if(indexes, [&](const SystemIndex& index) {
    const auto* filter = FilterOf(index.column);
    return (index.lookup == SystemLookup::Object ||
            index.lookup == SystemLookup::Dependents) &&
           filter && (filter->numbers.keys || filter->texts.keys);
  });
  return it == indexes.end() ? nullptr : &*it;
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

std::vector<duckdb::CatalogEntry*> SystemScan::Resolve(
  const CatalogSource& source) const {
  using enum duckdb::CatalogType;
  std::vector<duckdb::CatalogType> types{source.types.begin(),
                                         source.types.end()};
  std::vector<duckdb::SchemaCatalogEntry*> schemas;
  bool scoped = false;
  const auto scope =
    [&](duckdb::optional_ptr<duckdb::SchemaCatalogEntry> schema) {
      if (schema && !absl::c_linear_search(schemas, schema.get())) {
        schemas.emplace_back(schema.get());
      }
    };
  for (const auto& index : source.indexes) {
    const auto* filter = FilterOf(index.column);
    if (!filter) {
      continue;
    }
    switch (index.lookup) {
      case SystemLookup::Kind: {
        const bool text = _table.Sql().columns[index.column].physical ==
                          duckdb::PhysicalType::VARCHAR;
        std::erase_if(types, [&](duckdb::CatalogType type) {
          return absl::c_none_of(index.kinds, [&](const SystemKindTypes& kind) {
            return absl::c_linear_search(kind.types, type) &&
                   (text ? Passes(index.column, static_cast<char>(kind.kind))
                         : Passes(index.column, kind.kind));
          });
        });
        break;
      }
      case SystemLookup::Namespace:
        if (const auto& names = filter->texts.keys) {
          scoped = true;
          for (const auto& name : *names) {
            scope(FindSchema(name, source.schemas));
          }
        }
        if (const auto& oids = filter->numbers.keys) {
          scoped = true;
          for (const auto oid : *oids) {
            scope(
              FindNamespace(static_cast<duckdb::idx_t>(oid), source.schemas));
          }
        }
        break;
      case SystemLookup::Object:
      case SystemLookup::Dependents:
        break;
    }
  }
  std::vector<duckdb::CatalogEntry*> entries;
  if (types.empty() || (scoped && schemas.empty())) {
    return entries;
  }
  const auto* index = Choose(source.indexes);
  if (!index) {
    if (!scoped) {
      schemas = Schemas(source.schemas);
    }
    const auto text = NamePrefix(source.indexes);
    const duckdb::Identifier prefix{std::string{text}};
    const bool narrow =
      !text.empty() &&
      !(text.starts_with('_') && absl::c_linear_search(types, TYPE_ENTRY));
    _indexed_complete =
      _collect_indexed && !narrow && absl::c_linear_search(types, INDEX_ENTRY);
    for (auto* schema : schemas) {
      if (absl::c_linear_search(types, SCHEMA_ENTRY)) {
        entries.emplace_back(schema);
      }
      AppendMembers(*schema, types, narrow ? &prefix : nullptr, entries);
    }
    return entries;
  }
  const auto& filter = *FilterOf(index->column);
  if (const auto& names = filter.texts.keys) {
    if (!scoped) {
      schemas = Schemas(source.schemas);
    }
    for (const auto& name : *names) {
      AppendNamed(name, types, source.schemas, schemas, entries);
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
              absl::c_linear_search(types, dependent.type)) {
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
  return entries;
}

std::vector<duckdb::CatalogEntry*> SystemScan::Resolve(
  const CatalogSetSource& source) const {
  duckdb::DuckCatalog& catalog = source.catalog == SystemCatalog::Cluster
                                   ? catalog::ClusterOf(_context)
                                   : _database;
  auto& set = catalog.GetCatalogSet(source.type);
  const auto transaction = catalog.GetCatalogTransaction(_context);
  std::vector<duckdb::CatalogEntry*> entries;
  const auto* index = Choose(source.indexes);
  if (!index) {
    const auto append = [&](duckdb::CatalogEntry& entry) {
      entries.emplace_back(&entry);
    };
    if (const auto prefix = NamePrefix(source.indexes); !prefix.empty()) {
      set.ScanWithPrefix(transaction, append,
                         duckdb::Identifier{std::string{prefix}});
    } else {
      set.Scan(transaction, append);
    }
    return entries;
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
  return entries;
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SystemScan::FindNamespace(
  duckdb::idx_t oid, SystemSchemas system) const {
  if (const auto* builtin = FindSystemNamespace(oid)) {
    return FindSchema(builtin->name, system);
  }
  auto entry = FindById(oid);
  if (!entry || entry->type != duckdb::CatalogType::SCHEMA_ENTRY ||
      entry->internal) {
    return nullptr;
  }
  return entry->Cast<duckdb::SchemaCatalogEntry>();
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

void SystemScan::AppendMembers(
  duckdb::SchemaCatalogEntry& schema,
  std::span<const duckdb::CatalogType> types, const duckdb::Identifier* prefix,
  std::vector<duckdb::CatalogEntry*>& entries) const {
  using enum duckdb::CatalogType;
  auto& sets = schema.Cast<duckdb::DuckSchemaEntry>();
  const auto* keyed = absl::c_linear_search(types, INDEX_ENTRY)
                        ? &sets.GetCatalogSet(TABLE_ENTRY)
                        : nullptr;
  const auto append = [&](duckdb::CatalogEntry& entry) {
    if (_indexed_complete && entry.type == INDEX_ENTRY) {
      _indexed.insert(entry.Cast<duckdb::IndexCatalogEntry>().table_oid);
    }
    if (absl::c_linear_search(types, entry.type) &&
        (!entry.internal || schema.internal)) {
      entries.emplace_back(&entry);
    }
  };
  auto* serene =
    dynamic_cast<catalog::SereneDBCatalog*>(&schema.ParentCatalog());
  absl::InlinedVector<const duckdb::CatalogSet*, 4> scanned;
  for (const auto type : types | kMemberTypes) {
    auto& set = sets.GetCatalogSet(type);
    if (absl::c_linear_search(scanned, &set)) {
      continue;
    }
    scanned.emplace_back(&set);
    if (!prefix || &set == keyed) {
      if (const auto snapshot =
            serene ? serene->Snapshot(_context, set) : nullptr) {
        for (auto* entry : snapshot->entries) {
          append(*entry);
        }
      } else {
        set.Scan(Transaction(), append);
      }
    } else {
      set.ScanWithPrefix(Transaction(), append, *prefix);
    }
  }
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

void SystemScan::AppendNamed(
  std::string_view name, std::span<const duckdb::CatalogType> types,
  SystemSchemas system, std::span<duckdb::SchemaCatalogEntry* const> scopes,
  std::vector<duckdb::CatalogEntry*>& entries) const {
  using enum duckdb::CatalogType;
  const duckdb::Identifier identifier{name};
  for (auto* schema : scopes) {
    const auto found = entries.size();
    for (const auto type : types | kMemberTypes) {
      if (auto entry = FindMember(Transaction(), *schema, type, identifier);
          entry && entry->type == type &&
          (!entry->internal || schema->internal)) {
        entries.emplace_back(entry.get());
      }
    }
    if (entries.size() == found && absl::c_linear_search(types, INDEX_ENTRY)) {
      if (const auto key = FindKeyIndex(_context, *schema, name);
          key.key_index) {
        entries.emplace_back(key.table);
      }
    }
  }
  if (absl::c_linear_search(types, SCHEMA_ENTRY)) {
    if (auto schema = FindSchema(name, system)) {
      entries.emplace_back(schema.get());
    }
  }
  if (absl::c_linear_search(types, TYPE_ENTRY) && name.starts_with('_')) {
    AppendNamed(name.substr(1), types, system, scopes, entries);
  }
}

}  // namespace sdb::pg
