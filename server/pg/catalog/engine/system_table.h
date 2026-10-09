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

#pragma once

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <bit>
#include <bitset>
#include <cstddef>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/catalog/permissions.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/parser/column_list.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <functional>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/serializer.hpp>
#include <iresearch/utils/system_compiler.hpp>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/entry/database.h"
#include "catalog/entry/foreign_server.h"
#include "catalog/entry/role.h"
#include "pg/types.h"

namespace duckdb {

class Expression;
class TableFilter;
struct TableFilterState;

}  // namespace duckdb
namespace sdb::catalog {

class TokenizerCatalogEntry;

}  // namespace sdb::catalog
namespace sdb::pg {

enum class SystemKey : uint8_t {
  None,
  Unique,
  Indexed,
};

using SystemCell = std::optional<std::string_view>;

struct SystemSqlColumn {
  std::string_view name;
  int32_t type;
  duckdb::PhysicalType physical;
  duckdb::PhysicalType element;
  bool not_null;
  SystemKey key;
  SystemCell default_value;
};

enum class SystemLookup : uint8_t {
  Object,
  Namespace,
  Kind,
  Dependents,
};

struct SystemKindTypes {
  int64_t kind;
  std::span<const duckdb::CatalogType> types;
};

struct SystemIndex {
  uint32_t column;
  SystemLookup lookup;
  std::span<const SystemKindTypes> kinds = {};
};

enum class SystemSchemas : bool {
  Skip,
  Visit,
};

enum class SystemCatalog : bool {
  Database,
  Cluster,
};

struct SystemSql {
  duckdb::idx_t oid;
  std::string_view schema;
  std::string_view name;
  char relkind;
  bool superuser_only;
  bool shared;
  duckdb::idx_t rows;
  std::span<const SystemSqlColumn> columns;

  consteval uint32_t operator[](std::string_view column) const {
    for (uint32_t i = 0; i < columns.size(); ++i) {
      if (columns[i].name == column) {
        return i;
      }
    }
    SDB_UNREACHABLE();
  }
};

template<irs::utils::detail::FixedString Name, typename Get>
struct SystemColumn {
  using Getter = Get;

  static constexpr std::string_view kName{Name};

  Get get;
};

template<irs::utils::detail::FixedString Name, typename Get>
consteval SystemColumn<Name, Get> Col(Get get) {
  return {get};
}

template<const SystemSql& Sql, typename Ctx, typename... Cols>
struct SystemShape {
  using Context = Ctx;

  static constexpr const SystemSql& kSql = Sql;
  static constexpr size_t kSize = sizeof...(Cols);
  static constexpr std::array<uint32_t, kSize> kColumns{Sql[Cols::kName]...};
  static constexpr uint64_t kProvided =
    (uint64_t{0} | ... | (uint64_t{1} << Sql[Cols::kName]));

  static consteval bool Distinct() {
    for (size_t i = 0; i < kSize; ++i) {
      for (size_t j = i + 1; j < kSize; ++j) {
        if (kColumns[i] == kColumns[j]) {
          return false;
        }
      }
    }
    return true;
  }

  static consteval uint64_t Required() {
    uint64_t required = 0;
    for (uint32_t i = 0; i < Sql.columns.size(); ++i) {
      if (Sql.columns[i].not_null && !Sql.columns[i].default_value) {
        required |= uint64_t{1} << i;
      }
    }
    return required;
  }

  static_assert(Distinct(), "a shape sets every column once");
  static_assert((Required() & ~kProvided) == 0,
                "a shape sets every NOT NULL column without a default");

  std::tuple<Cols...> columns;
};

template<typename Column>
constexpr std::tuple<Column> SystemColumns(Column column) {
  return {column};
}

template<typename... Columns>
constexpr std::tuple<Columns...> SystemColumns(std::tuple<Columns...> columns) {
  return columns;
}

template<const SystemSql& Sql, typename Ctx, typename... Cols>
consteval SystemShape<Sql, Ctx, Cols...> MakeShape(
  std::tuple<Cols...> columns) {
  return {columns};
}

template<const SystemSql& Sql, typename Ctx, typename... Args>
consteval auto Shape(Args... args) {
  return MakeShape<Sql, Ctx>(std::tuple_cat(SystemColumns(args)...));
}

template<const auto& Shape>
using SystemShapeOf = std::remove_cvref_t<decltype(Shape)>;

struct SystemScanFunctions {
  duckdb::table_function_init_global_t init;
  duckdb::table_function_t scan;
};

consteval size_t SystemWidth(duckdb::PhysicalType physical) {
  using enum duckdb::PhysicalType;
  if (physical == BOOL || physical == INT8) {
    return 1;
  }
  if (physical == INT16) {
    return 2;
  }
  if (physical == INT32 || physical == FLOAT) {
    return 4;
  }
  if (physical == INT64 || physical == DOUBLE) {
    return 8;
  }
  if (physical == VARCHAR) {
    return sizeof(duckdb::string_t);
  }
  SDB_UNREACHABLE();
}

struct SystemDefault {
  duckdb::Value value;
  std::array<std::byte, sizeof(duckdb::string_t)> bytes;
};

struct SystemFact {
  uint32_t column;
  int64_t min;
  int64_t max;
};

class SystemTable {
 public:
  SystemTable(const SystemSql& sql, std::span<const SystemCell> cells);
  SystemTable(const SystemSql& sql, SystemScanFunctions functions,
              std::span<const SystemFact> facts);

  SystemTable(const SystemTable&) = delete;
  SystemTable& operator=(const SystemTable&) = delete;

  void Init();

  const SystemSql& Sql() const noexcept { return _sql; }
  const duckdb::ColumnList& Columns() const noexcept { return _columns; }
  const SystemDefault& Default(duckdb::idx_t column) const {
    return _defaults[column];
  }
  const SystemScanFunctions& Functions() const noexcept { return _functions; }
  std::span<const SystemCell> Cells() const noexcept { return _cells; }
  std::span<const std::unique_ptr<duckdb::DataChunk>> Chunks() const noexcept {
    return _chunks;
  }
  const SystemFact* FactOf(uint32_t column) const {
    const auto it = absl::c_find_if(
      _facts, [&](const SystemFact& fact) { return fact.column == column; });
    return it == _facts.end() ? nullptr : &*it;
  }
  bool Holds(uint32_t column, int64_t value) const {
    const auto* fact = FactOf(column);
    return !fact || (fact->min <= value && value <= fact->max);
  }
  bool DefaultsHold(uint64_t provided) const;

 private:
  const SystemSql& _sql;
  SystemScanFunctions _functions;
  std::span<const SystemCell> _cells;
  std::span<const SystemFact> _facts;
  duckdb::ColumnList _columns;
  std::vector<SystemDefault> _defaults;
  std::vector<std::unique_ptr<std::byte[]>> _storage;
  std::vector<std::unique_ptr<duckdb::DataChunk>> _chunks;
};

duckdb::LogicalType SystemRowType(const SystemSql& sql);

class SystemScan;

template<typename V>
struct SystemOptional : std::false_type {
  using Type = V;
};

template<typename V>
struct SystemOptional<std::optional<V>> : std::true_type {
  using Type = V;
};

template<typename V>
constexpr bool kSystemCheckable =
  std::is_same_v<V, char> || std::is_same_v<V, duckdb::Identifier> ||
  std::is_convertible_v<const V&, std::string_view> || std::is_integral_v<V> ||
  std::is_enum_v<V>;

template<typename Get, typename Ctx>
struct SystemValueOf {
  using Type = std::remove_cvref_t<typename std::conditional_t<
    std::is_invocable_v<const Get&, Ctx&, SystemScan&>,
    std::invoke_result<const Get&, Ctx&, SystemScan&>,
    std::invoke_result<const Get&, Ctx&>>::type>;
};

struct CatalogSource {
  std::span<const duckdb::CatalogType> types;
  SystemSchemas schemas;
  std::span<const SystemIndex> indexes;
};

struct CatalogSetSource {
  SystemCatalog catalog;
  duckdb::CatalogType type;
  std::span<const SystemIndex> indexes;
};

template<typename Row, typename Holder = void>
class SystemRows {
 public:
  SystemRows(std::span<const Row> rows) : _rows{rows} {}
  SystemRows(std::span<const Row> rows, std::shared_ptr<const Holder> owner)
    : _rows{rows}, _owner{std::move(owner)} {}
  SystemRows(std::vector<Row> rows) {
    auto owned = std::make_shared<const std::vector<Row>>(std::move(rows));
    _rows = *owned;
    _owner = std::move(owned);
  }

  std::span<const Row> Rows() const noexcept { return _rows; }
  const auto& Owner() const noexcept
    requires(!std::is_void_v<Holder>)
  {
    return *_owner;
  }

 private:
  std::span<const Row> _rows;
  std::shared_ptr<const Holder> _owner;
};

template<typename T>
struct SystemBound {
  T value;
  bool inclusive;
};

template<typename T>
struct SystemCondition {
  std::optional<std::vector<T>> keys;
  std::vector<T> excluded;
  std::optional<SystemBound<T>> lower;
  std::optional<SystemBound<T>> upper;

  bool Empty() const noexcept {
    return !keys && excluded.empty() && !lower && !upper;
  }

  template<typename V>
  bool Passes(const V& value) const {
    if (keys) {
      return absl::c_binary_search(*keys, value);
    }
    return !absl::c_linear_search(excluded, value) &&
           (!lower || (lower->inclusive ? !(value < lower->value)
                                        : lower->value < value)) &&
           (!upper || (upper->inclusive ? !(upper->value < value)
                                        : value < upper->value));
  }
};

struct SystemFilter {
  uint32_t column;
  SystemCondition<int64_t> numbers;
  SystemCondition<std::string> texts;
  std::bitset<257> chars;
};

struct SystemResidual {
  uint32_t slot;
  duckdb::unique_ptr<duckdb::TableFilterState> state;
};

struct SystemPrune {
  uint32_t column;
  const duckdb::TableFilter* filter;
  duckdb::unique_ptr<duckdb::TableFilterState> state;
  mutable duckdb::BaseStatistics stats;
};

struct SystemVector {
  duckdb::Vector* vector;
  duckdb::data_ptr_t data;
  duckdb::ValidityMask* validity;
};

struct SystemRange {
  std::string lower;
  std::optional<SystemBound<std::string>> upper;
  bool exact;
};

std::optional<SystemRange> RangeOf(const duckdb::Expression& expr);
std::optional<bool> BooleanOf(const duckdb::Expression& expr);

template<typename Row, typename Holder = void>
struct ArrayKey {
  uint32_t column;
  void (*find)(const SystemRows<Row, Holder>& rows, const SystemFilter& filter,
               std::vector<const Row*>& picked);
};

template<typename Row, typename Holder = void>
struct ArraySource {
  SystemRows<Row, Holder> (*load)(SystemScan& scan);
  std::span<const ArrayKey<Row, Holder>> keys;
};

template<typename Row, typename Holder>
struct SystemArray {
  SystemRows<Row, Holder> rows;
  std::optional<std::vector<const Row*>> picked;
};

template<typename Row, auto Project, typename Holder = void>
void SortedBy(const SystemRows<Row, Holder>& rows, const SystemFilter& filter,
              std::vector<const Row*>& picked) {
  using V =
    std::remove_cvref_t<std::invoke_result_t<decltype(Project), const Row&>>;
  const auto pick = [&](const V& probe) {
    for (const auto& row : std::ranges::equal_range(
           rows.Rows(), probe,
           [](const V& lhs, const V& rhs) { return lhs < rhs; }, Project)) {
      picked.emplace_back(&row);
    }
  };
  if constexpr (std::is_integral_v<V>) {
    for (const auto key : *filter.numbers.keys) {
      pick(static_cast<V>(key));
    }
  } else {
    for (const auto& key : *filter.texts.keys) {
      pick(V{key});
    }
  }
}

class SystemScan {
 public:
  SystemScan(duckdb::ClientContext& context,
             duckdb::TableFunctionInitInput& input);
  ~SystemScan();

  SystemScan(const SystemScan&) = delete;
  SystemScan& operator=(const SystemScan&) = delete;

  duckdb::ClientContext& Context() const noexcept { return _context; }
  duckdb::DuckCatalog& Database() const noexcept { return _database; }
  const SystemTable& Table() const noexcept { return _table; }
  std::shared_ptr<const auth::RoleClosure> SessionClosure() const;
  const duckdb::CatalogTransaction& Transaction() const noexcept {
    return _transaction;
  }
  duckdb::DependencyManager& Dependencies() const {
    return *_database.GetDependencyManager();
  }

  std::vector<duckdb::CatalogEntry*> Resolve(const CatalogSource& source) const;
  std::vector<duckdb::CatalogEntry*> Resolve(
    const CatalogSetSource& source) const;
  template<typename Row, typename Holder>
  SystemArray<Row, Holder> Resolve(const ArraySource<Row, Holder>& source) {
    auto rows = source.load(*this);
    for (const auto& key : source.keys) {
      const auto* filter = FilterOf(key.column);
      if (filter && (filter->numbers.keys || filter->texts.keys)) {
        std::vector<const Row*> picked;
        key.find(rows, *filter, picked);
        absl::c_sort(picked);
        picked.erase(std::unique(picked.begin(), picked.end()), picked.end());
        return {std::move(rows), std::move(picked)};
      }
    }
    return {std::move(rows), std::nullopt};
  }

  template<typename V>
  bool Allows(uint32_t column, const V& value) const {
    if (Filtered(column) && !Passes(column, value)) {
      return false;
    }
    if constexpr (std::is_integral_v<V> && !std::is_same_v<V, char>) {
      return ((_pruned >> column) & 1) == 0 ||
             Survives(column, static_cast<int64_t>(value));
    } else {
      return true;
    }
  }

  void Begin(duckdb::DataChunk& output);
  void End(duckdb::DataChunk& output);
  template<typename F>
  bool Step(F&& row) {
    const auto start = _row;
    const auto resumed = _skip;
    row();
    if (!_full) {
      return false;
    }
    _skip = resumed + (_row - start);
    return true;
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> FindById(duckdb::idx_t oid) const {
    return _database.GetOidIndex().GetVisible(oid, Transaction().view);
  }

  template<const auto& Shape>
  bool Emit(typename SystemShapeOf<Shape>::Context& ctx) {
    using T = SystemShapeOf<Shape>;
    SDB_ASSERT(_table.DefaultsHold(T::kProvided));
    if ((_rejecting & ~T::kProvided) != 0) {
      return false;
    }
    if (_row == STANDARD_VECTOR_SIZE) {
      _full = true;
      return false;
    }
    for (auto bits = _filtered & T::kProvided; bits != 0; bits &= bits - 1) {
      const auto check = kChecks<Shape>[std::countr_zero(bits)];
      SDB_ASSERT(check);
      if (!check(*this, ctx)) {
        return false;
      }
    }
    if (_skip != 0) {
      --_skip;
      return true;
    }
    if ((_needed & ~T::kProvided) != 0) {
      [&]<uint32_t... C>(std::integer_sequence<uint32_t, C...>) {
        (
          [&] {
            if constexpr (((T::kProvided >> C) & 1) == 0) {
              if (((_needed >> C) & 1) != 0) {
                PutDefault<T::kSql, C>();
              }
            }
          }(),
          ...);
      }(std::make_integer_sequence<uint32_t, T::kSql.columns.size()>{});
    }
    if (const auto rest = _needed & ~_filtered & T::kProvided; rest != 0) {
      [&]<size_t... I>(std::index_sequence<I...>) {
        (((rest >> T::kColumns[I]) & 1
            ? void(Write<Shape, I, false>(*this, ctx))
            : void()),
         ...);
      }(std::make_index_sequence<T::kSize>{});
    }
    ++_row;
    return true;
  }

  void CollectIndexed() noexcept { _collect_indexed = true; }
  std::optional<bool> KnownIndexed(duckdb::idx_t relation) const {
    if (!_indexed_complete) {
      return std::nullopt;
    }
    return _indexed.contains(relation);
  }

  void CollectTriggered() noexcept { _collect_triggered = true; }
  std::optional<bool> KnownTriggered(duckdb::idx_t relation) const {
    if (!_triggered_complete) {
      return std::nullopt;
    }
    return _triggered.contains(relation);
  }

  bool Needs(uint32_t column) const noexcept {
    return ((_needed >> column) & 1) != 0;
  }

  bool Reads(uint32_t column) const noexcept {
    return (((_needed | _filtered) >> column) & 1) != 0;
  }

 protected:
  void EmitChunk(const duckdb::DataChunk& source);

 private:
  static constexpr uint32_t kNowhere = ~uint32_t{0};

  template<typename Get, typename Ctx>
  decltype(auto) Value(const Get& get, Ctx& ctx) {
    if constexpr (std::is_invocable_v<const Get&, Ctx&, SystemScan&>) {
      return std::invoke(get, ctx, *this);
    } else {
      return std::invoke(get, ctx);
    }
  }

  template<const auto& Shape, size_t I>
  using ValueOf = typename SystemValueOf<
    typename std::tuple_element_t<
      I, decltype(SystemShapeOf<Shape>::columns)>::Getter,
    typename SystemShapeOf<Shape>::Context>::Type;

  template<typename V>
  using Optional = SystemOptional<V>;

  template<const auto& Shape, size_t I, bool Checked>
  static bool Write(SystemScan& scan,
                    typename SystemShapeOf<Shape>::Context& ctx) {
    using T = SystemShapeOf<Shape>;
    constexpr auto kColumn = T::kColumns[I];
    constexpr auto& kMeta = T::kSql.columns[kColumn];
    constexpr bool kNullDefault = !kMeta.default_value.has_value();
    const auto put = [&](const auto& value) {
      static_assert(
        !std::is_same_v<std::remove_cvref_t<decltype(value)>, char> ||
          kMeta.type == kChar,
        "a char getter feeds a \"char\" column");
      if constexpr (Checked) {
        if (!scan.Passes(kColumn, value)) {
          return false;
        }
        if (!scan.Needs(kColumn)) {
          return true;
        }
      }
      scan.PutValue<kMeta.physical, kMeta.element, kNullDefault>(kColumn,
                                                                 value);
      return true;
    };
    decltype(auto) value = scan.Value(std::get<I>(Shape.columns).get, ctx);
    if constexpr (Optional<ValueOf<Shape, I>>::value) {
      static_assert(!(kMeta.not_null && kNullDefault),
                    "a NOT NULL column without a default is always set");
      if (value) {
        return put(*value);
      }
      if (Checked && ((scan._rejecting >> kColumn) & 1) != 0) {
        return false;
      }
      if (scan.Needs(kColumn)) {
        scan.template PutDefault<T::kSql, kColumn>();
      }
      return true;
    } else {
      return put(value);
    }
  }

  template<const auto& Shape, size_t... I>
  static consteval auto MakeChecks(std::index_sequence<I...>) {
    using T = SystemShapeOf<Shape>;
    using Checker = bool (*)(SystemScan&, typename T::Context&);
    std::array<Checker, 64> checks{};
    ((checks[T::kColumns[I]] = [] -> Checker {
       if constexpr (kSystemCheckable<
                       typename Optional<ValueOf<Shape, I>>::Type>) {
         return &Write<Shape, I, true>;
       } else {
         return nullptr;
       }
     }()),
     ...);
    return checks;
  }

  template<const auto& Shape>
  static constexpr auto kChecks =
    MakeChecks<Shape>(std::make_index_sequence<SystemShapeOf<Shape>::kSize>{});

  template<duckdb::PhysicalType P, duckdb::PhysicalType E, bool NullDefault,
           typename V>
  void PutValue(uint32_t column, V&& value) {
    const auto& target = _vectors[_slots[column]];
    if constexpr (NullDefault) {
      target.validity->SetValid(_row);
    }
    if constexpr (std::is_integral_v<std::remove_cvref_t<V>> ||
                  std::is_enum_v<std::remove_cvref_t<V>>) {
      SDB_ASSERT(_table.Holds(column, Integer<int64_t>(value)));
    }
    if constexpr (P == duckdb::PhysicalType::LIST) {
      PutList<E, NullDefault>(column, target, value);
    } else {
      Put<P>(target, _row, value);
    }
  }

  bool Filtered(uint32_t column) const noexcept {
    return ((_filtered >> column) & 1) != 0;
  }

  const SystemFilter* FilterOf(uint32_t column) const {
    return Filtered(column) ? &_filters[_filter_index[column]] : nullptr;
  }

  template<typename V>
  bool Passes(uint32_t column, const V& value) const {
    const auto& filter = _filters[_filter_index[column]];
    if constexpr (std::is_same_v<V, char>) {
      SDB_ASSERT(_table.Sql().columns[column].type == kChar);
      return filter.chars[value ? static_cast<unsigned char>(value) : 256];
    } else if constexpr (std::is_same_v<V, duckdb::Identifier>) {
      return filter.texts.Passes(value.GetIdentifierName());
    } else if constexpr (std::is_convertible_v<const V&, std::string_view>) {
      return filter.texts.Passes(std::string_view{value});
    } else {
      static_assert(std::is_integral_v<V> || std::is_enum_v<V>,
                    "a filtered column needs a text or integer value");
      return filter.numbers.Passes(Integer<int64_t>(value));
    }
  }
  template<typename T, typename V>
  static T Integer(const V& value) {
    if constexpr (std::is_enum_v<V>) {
      return static_cast<T>(std::to_underlying(value));
    } else {
      static_assert(std::is_integral_v<V>, "integer column needs an integer");
      return static_cast<T>(value);
    }
  }

  template<typename T>
  static T* Cells(const SystemVector& target) {
    return reinterpret_cast<T*>(target.data);
  }

  template<duckdb::PhysicalType P, typename V>
  void Put(const SystemVector& target, duckdb::idx_t row, const V& value) {
    using enum duckdb::PhysicalType;
    if constexpr (P == BOOL) {
      static_assert(std::is_convertible_v<const V&, bool> &&
                      !std::is_pointer_v<V> &&
                      !std::is_convertible_v<const V&, std::string_view>,
                    "bool column needs a bool");
      Cells<bool>(target)[row] = static_cast<bool>(value);
    } else if constexpr (P == INT16) {
      Cells<int16_t>(target)[row] = Integer<int16_t>(value);
    } else if constexpr (P == INT32) {
      Cells<int32_t>(target)[row] = Integer<int32_t>(value);
    } else if constexpr (P == INT64 &&
                         std::is_same_v<V, duckdb::timestamp_tz_t>) {
      Cells<duckdb::timestamp_tz_t>(target)[row] = value;
    } else if constexpr (P == INT64) {
      Cells<int64_t>(target)[row] = Integer<int64_t>(value);
    } else if constexpr (P == FLOAT) {
      static_assert(std::is_arithmetic_v<V>, "real column needs a number");
      Cells<float>(target)[row] = static_cast<float>(value);
    } else if constexpr (P == DOUBLE) {
      static_assert(std::is_arithmetic_v<V>, "real column needs a number");
      Cells<double>(target)[row] = static_cast<double>(value);
    } else if constexpr (P == VARCHAR) {
      if constexpr (std::is_same_v<V, char>) {
        PutText(target, row, std::string_view{&value, value ? 1U : 0U});
      } else if constexpr (std::is_same_v<V, duckdb::Identifier>) {
        PutText(target, row, value.GetIdentifierName());
      } else if constexpr (std::is_same_v<V, duckdb::AclItem>) {
        PutAcl(target, row, value);
      } else {
        static_assert(std::is_convertible_v<const V&, std::string_view>,
                      "text column needs a string");
        PutText(target, row, std::string_view{value});
      }
    } else {
      static_assert(false, "column type cannot be written");
    }
  }

  template<duckdb::PhysicalType E, bool NullDefault, typename R>
  void PutList(uint32_t column, const SystemVector& target, R& items) {
    if constexpr (NullDefault && std::is_same_v<std::ranges::range_value_t<R>,
                                                duckdb::AclItem>) {
      if (std::ranges::empty(items) && !_table.Sql().columns[column].not_null) {
        target.validity->SetInvalid(_row);
        return;
      }
    }
    auto& vector = *target.vector;
    const auto offset = duckdb::ListVector::GetListSize(vector);
    const auto length = static_cast<duckdb::idx_t>(std::ranges::size(items));
    duckdb::ListVector::Reserve(vector, offset + length);
    auto& child = duckdb::ListVector::GetChildMutable(vector);
    const SystemVector elements{&child,
                                duckdb::FlatVector::GetDataMutable(child),
                                &duckdb::FlatVector::ValidityMutable(child)};
    auto row = offset;
    for (const auto& item : items) {
      Put<E>(elements, row++, item);
    }
    Cells<duckdb::list_entry_t>(target)[_row] = {offset, length};
    duckdb::ListVector::SetListSize(vector, offset + length);
  }

  template<const SystemSql& Sql, uint32_t C>
  void PutDefault() {
    using enum duckdb::PhysicalType;
    constexpr auto& kMeta = Sql.columns[C];
    const auto& target = _vectors[_slots[C]];
    if constexpr (!kMeta.default_value) {
      target.validity->SetInvalid(_row);
    } else if constexpr (kMeta.physical == LIST ||
                         (kMeta.physical == VARCHAR &&
                          kMeta.default_value->size() >
                            duckdb::string_t::INLINE_LENGTH)) {
      target.vector->SetValue(_row, _table.Default(C).value);
    } else {
      constexpr auto kWidth = SystemWidth(kMeta.physical);
      std::memcpy(target.data + _row * kWidth, _table.Default(C).bytes.data(),
                  kWidth);
    }
  }

  static void PutText(const SystemVector& target, duckdb::idx_t row,
                      std::string_view value);
  void PutAcl(const SystemVector& target, duckdb::idx_t row,
              const duckdb::AclItem& item);
  bool Survives(uint32_t column, int64_t value) const;
  bool Compile(duckdb::column_t column, const duckdb::TableFilter& filter);
  const SystemIndex* Choose(std::span<const SystemIndex> indexes) const;
  std::string_view NamePrefix(std::span<const SystemIndex> indexes) const;
  duckdb::optional_ptr<duckdb::CatalogEntry> FindOwner(
    duckdb::idx_t oid) const {
    return _database.GetOidIndex().GetVisibleOwner(oid, Transaction().view);
  }
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> FindSchema(
    std::string_view name, SystemSchemas system) const;
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> FindNamespace(
    duckdb::idx_t oid, SystemSchemas system) const;
  std::vector<duckdb::SchemaCatalogEntry*> Schemas(SystemSchemas system) const;
  void AppendMembers(duckdb::SchemaCatalogEntry& schema,
                     std::span<const duckdb::CatalogType> types,
                     const duckdb::Identifier* prefix,
                     std::vector<duckdb::CatalogEntry*>& entries) const;
  void AppendOwned(duckdb::idx_t oid, SystemSchemas system,
                   std::vector<duckdb::CatalogEntry*>& entries) const;
  void AppendNamed(std::string_view name,
                   std::span<const duckdb::CatalogType> types,
                   SystemSchemas system,
                   std::span<duckdb::SchemaCatalogEntry* const> scopes,
                   std::vector<duckdb::CatalogEntry*>& entries) const;

  duckdb::ClientContext& _context;
  duckdb::DuckCatalog& _database;
  duckdb::CatalogTransaction _transaction;
  const SystemTable& _table;
  std::shared_ptr<const auth::RoleGraph> _roles;
  duckdb::vector<duckdb::column_t> _column_ids;
  std::vector<uint32_t> _places;
  std::vector<SystemFilter> _filters;
  std::array<uint32_t, 64> _slots{};
  std::vector<SystemResidual> _residuals;
  duckdb::DataChunk _side;
  std::vector<SystemVector> _vectors;
  duckdb::idx_t _row = 0;
  size_t _skip = 0;
  bool _full = false;
  uint64_t _needed = 0;
  uint64_t _filtered = 0;
  uint64_t _rejecting = 0;
  std::array<uint8_t, 64> _filter_index{};
  uint64_t _pruned = 0;
  std::vector<SystemPrune> _prunes;
  std::string _text;
  mutable irs::containers::FlatHashSet<duckdb::idx_t> _indexed;
  mutable bool _indexed_complete = false;
  bool _collect_indexed = false;
  mutable irs::containers::FlatHashSet<duckdb::idx_t> _triggered;
  mutable bool _triggered_complete = false;
  bool _collect_triggered = false;
};

template<const SystemSql& Sql>
class SystemTableScan : public SystemScan {
 public:
  static constexpr const SystemSql& kSql = Sql;

  using SystemScan::SystemScan;

  template<irs::utils::detail::FixedString Name, typename V>
  bool Allows(const V& value) const {
    return SystemScan::Allows(Sql[std::string_view{Name}], value);
  }

  template<irs::utils::detail::FixedString Name>
  bool Needs() const noexcept {
    return SystemScan::Needs(Sql[std::string_view{Name}]);
  }

  template<irs::utils::detail::FixedString Name>
  bool Reads() const noexcept {
    return SystemScan::Reads(Sql[std::string_view{Name}]);
  }
};

template<typename Entry, typename T>
void Visit(T& table, duckdb::CatalogEntry& entry) {
  if constexpr (requires(Entry& typed) { table.Row(typed); }) {
    table.Row(entry.Cast<Entry>());
  } else if constexpr (requires { table.Row(entry); }) {
    table.Row(entry);
  }
}

template<typename T>
void Dispatch(T& table, duckdb::CatalogEntry& entry) {
  using enum duckdb::CatalogType;
  switch (entry.type) {
    case TABLE_ENTRY:
      return Visit<duckdb::TableCatalogEntry>(table, entry);
    case VIEW_ENTRY:
      return Visit<duckdb::ViewCatalogEntry>(table, entry);
    case INDEX_ENTRY:
      return Visit<duckdb::IndexCatalogEntry>(table, entry);
    case SEQUENCE_ENTRY:
      return Visit<duckdb::SequenceCatalogEntry>(table, entry);
    case TYPE_ENTRY:
      return Visit<duckdb::TypeCatalogEntry>(table, entry);
    case SCHEMA_ENTRY:
      return Visit<duckdb::SchemaCatalogEntry>(table, entry);
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      return Visit<duckdb::MacroCatalogEntry>(table, entry);
    case TRIGGER_ENTRY:
      return Visit<duckdb::TriggerCatalogEntry>(table, entry);
    case ROLE_ENTRY:
      return Visit<catalog::RoleCatalogEntry>(table, entry);
    case DATABASE_ENTRY:
      return Visit<catalog::DatabaseCatalogEntry>(table, entry);
    case FOREIGN_SERVER_ENTRY:
      return Visit<catalog::ForeignServerCatalogEntry>(table, entry);
    case TOKENIZER_ENTRY:
      return Visit<catalog::TokenizerCatalogEntry>(table, entry);
    case INVALID:
    case PREPARED_STATEMENT:
    case COLLATION_ENTRY:
    case COORDINATE_SYSTEM_ENTRY:
    case JOB_ENTRY:
    case TABLE_FUNCTION_ENTRY:
    case SCALAR_FUNCTION_ENTRY:
    case AGGREGATE_FUNCTION_ENTRY:
    case PRAGMA_FUNCTION_ENTRY:
    case COPY_FUNCTION_ENTRY:
    case WINDOW_FUNCTION_ENTRY:
    case DELETED_ENTRY:
    case RENAMED_ENTRY:
    case SECRET_ENTRY:
    case SECRET_TYPE_ENTRY:
    case SECRET_FUNCTION_ENTRY:
    case DEPENDENCY_ENTRY:
      return Visit<duckdb::CatalogEntry>(table, entry);
  }
}

template<typename Item>
class SystemCursor final {
 public:
  explicit SystemCursor(std::vector<Item*> items) : _items{std::move(items)} {}

  template<typename T>
  bool Run(T& table) {
    for (; _next < _items.size(); ++_next) {
      if (table.Step([&] { Dispatch(table, *_items[_next]); })) {
        return true;
      }
    }
    return false;
  }

 private:
  std::vector<Item*> _items;
  size_t _next = 0;
};

template<typename Row, typename Holder>
class SystemCursor<SystemArray<Row, Holder>> final {
 public:
  explicit SystemCursor(SystemArray<Row, Holder> array)
    : _array{std::move(array)} {}

  size_t Size() const noexcept {
    return _array.picked ? _array.picked->size() : _array.rows.Rows().size();
  }

  template<typename T>
  bool Run(T& table) {
    for (; _next < Size(); ++_next) {
      if (table.Step([&] {
            table.Row(_array.picked ? *(*_array.picked)[_next]
                                    : _array.rows.Rows()[_next]);
          })) {
        return true;
      }
    }
    return false;
  }

 private:
  SystemArray<Row, Holder> _array;
  size_t _next = 0;
};

template<typename Item>
SystemCursor(std::vector<Item*>) -> SystemCursor<Item>;
template<typename Row, typename Holder>
SystemCursor(SystemArray<Row, Holder>)
  -> SystemCursor<SystemArray<Row, Holder>>;

template<typename T>
auto SystemCursors(T& table) {
  return std::apply(
    [&](const auto&... sources) {
      return std::tuple{SystemCursor{table.Resolve(sources)}...};
    },
    T::kSources);
}

template<typename T>
class SystemScanState final : public duckdb::GlobalTableFunctionState {
 public:
  SystemScanState(duckdb::ClientContext& context,
                  duckdb::TableFunctionInitInput& input)
    : _table{context, input}, _cursors{SystemCursors(_table)} {}

  void Next(duckdb::DataChunk& output) {
    while (!_finished) {
      _table.Begin(output);
      _finished = !std::apply(
        [&](auto&... cursors) { return (... || cursors.Run(_table)); },
        _cursors);
      _table.End(output);
      if (output.size() != 0) {
        return;
      }
    }
  }

 private:
  T _table;
  decltype(SystemCursors(std::declval<T&>())) _cursors;
  bool _finished = false;
};

template<typename T>
duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SystemScanInit(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  return duckdb::make_uniq<SystemScanState<T>>(context, input);
}

template<typename T>
void SystemScanRun(duckdb::ClientContext&, duckdb::TableFunctionInput& input,
                   duckdb::DataChunk& output) {
  input.global_state->Cast<SystemScanState<T>>().Next(output);
}

template<typename T>
SystemTable SystemTableOf() {
  if constexpr (requires { T::kFacts; }) {
    return SystemTable{
      T::kSql, {&SystemScanInit<T>, &SystemScanRun<T>}, T::kFacts};
  } else {
    return SystemTable{T::kSql, {&SystemScanInit<T>, &SystemScanRun<T>}, {}};
  }
}

}  // namespace sdb::pg
