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

#include <algorithm>
#include <compare>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/planner/table_filter_set.hpp>
#include <iresearch/utils/assert.hpp>

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

void SystemTable::Init() {
  for (const auto& column : _sql.columns) {
    auto type = SystemColumnType(column.type);
    SDB_ASSERT(
      type.InternalType() == column.physical &&
      (column.physical != duckdb::PhysicalType::LIST ||
       duckdb::ListType::GetChildType(type).InternalType() == column.element));
    _columns.AddColumn(duckdb::ColumnDefinition{duckdb::Identifier{column.name},
                                                std::move(type)});
  }
}

bool SystemTable::DefaultsHold(uint64_t provided) const {
  return absl::c_all_of(_facts, [&](const SystemFact& fact) {
    return ((provided >> fact.column) & 1) != 0 ||
           !_sql.columns[fact.column].zero_default ||
           Holds(fact.column, int64_t{0});
  });
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
    _table{connector::BoundSystemTable(input).Table()} {
  const auto& ids = input.column_ids;
  if (input.filters) {
    for (const auto& entry : *input.filters) {
      Compile(ids[entry.GetIndex()], entry.Filter());
    }
  }
  const auto& projection = input.projection_ids;
  const auto outputs = projection.empty() ? ids.size() : projection.size();
  for (uint32_t place = 0; place < outputs; ++place) {
    const auto column = ids[projection.empty() ? place : projection[place]];
    if (column == connector::kColumnIdentifierTableOid) {
      _constants.emplace_back(
        place, duckdb::Value::BIGINT(static_cast<int64_t>(_table.Sql().oid)));
    } else if (column == duckdb::COLUMN_IDENTIFIER_EMPTY) {
      _constants.emplace_back(place,
                              duckdb::Value{duckdb::LogicalType::BOOLEAN});
    } else {
      SDB_ASSERT(column < _table.Columns().LogicalColumnCount());
      _slots[column] = place;
      _needed |= uint64_t{1} << column;
      if (!_roles && _table.Sql().columns[column].type == kAclitemArray) {
        _roles = auth::RolesOf(&context);
      }
    }
  }
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
  output.SetChildCardinality(STANDARD_VECTOR_SIZE);
  for (auto& vector : output.data) {
    _vectors.emplace_back(
      SystemVector{.vector = &vector,
                   .data = duckdb::FlatVector::GetDataMutable(vector),
                   .validity = &duckdb::FlatVector::ValidityMutable(vector)});
  }
}

void SystemScan::End(duckdb::DataChunk& output) {
  output.SetChildCardinality(_row);
  for (const auto& [place, value] : _constants) {
    _vectors[place].vector->Reference(value, duckdb::count_t{_row});
  }
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

}  // namespace sdb::pg
