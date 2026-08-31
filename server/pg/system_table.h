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

#pragma once

#include <absl/functional/function_ref.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include <algorithm>
#include <array>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <optional>
#include <span>
#include <type_traits>

#include "auth/acl.h"
#include "auth/role_closure.h"
#include "basics/down_cast.h"
#include "catalog1/entry/role.h"
#include "catalog1/lookup.h"
#include "catalog1/permissions.h"
#include "connector/pg_logical_types.h"
#include "pg/information_schema/fwd.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_types.h"
#include "pg/virtual_table.h"
#include "query/config.h"

namespace sdb::pg {

struct PrivChar {
  catalog::AclMode mode;
  char chr;
};
inline constexpr std::array kPrivChars{
  PrivChar{catalog::AclMode::Insert, 'a'},
  PrivChar{catalog::AclMode::Select, 'r'},
  PrivChar{catalog::AclMode::Update, 'w'},
  PrivChar{catalog::AclMode::Delete, 'd'},
  PrivChar{catalog::AclMode::Truncate, 'D'},
  PrivChar{catalog::AclMode::References, 'x'},
  PrivChar{catalog::AclMode::Trigger, 't'},
  PrivChar{catalog::AclMode::Maintain, 'm'},
  PrivChar{catalog::AclMode::Execute, 'X'},
  PrivChar{catalog::AclMode::Usage, 'U'},
  PrivChar{catalog::AclMode::Create, 'C'},
  PrivChar{catalog::AclMode::CreateTemp, 'T'},
  PrivChar{catalog::AclMode::Connect, 'c'},
  PrivChar{catalog::AclMode::Set, 's'},
  PrivChar{catalog::AclMode::AlterSystem, 'A'},
};

inline void PutId(std::string& out, std::string_view name) {
  const bool safe = std::ranges::all_of(name, [](unsigned char c) {
    return !(c & 0x80) && (absl::ascii_isalnum(c) || c == '_');
  });
  if (safe) {
    out.append(name);
    return;
  }
  absl::StrAppend(&out, "\"", absl::StrReplaceAll(name, {{"\"", "\"\""}}),
                  "\"");
}

inline std::string AclToPgString(
  const catalog::AclItem& item,
  absl::FunctionRef<std::string_view(duckdb::idx_t)> name_of) {
  std::string out;
  if (item.grantee != kPublicGrantee) {
    PutId(out, name_of(item.grantee));
  }
  out.push_back('=');
  for (const auto& p : kPrivChars) {
    if ((item.privs & p.mode) != catalog::AclMode::NoRights) {
      out.push_back(p.chr);
      if ((item.grant_option & p.mode) != catalog::AclMode::NoRights) {
        out.push_back('*');
      }
    }
  }
  out.push_back('/');
  PutId(out, name_of(item.grantor));
  return out;
}

// Every entry of one type in the database being projected. duckdb keeps
// tables and views in a single set, so a scan of either type yields both and
// the entry's own type is what separates them.
template<typename T>
void VisitEntries(duckdb::ClientContext* context, duckdb::Catalog& database,
                  absl::FunctionRef<void(T&)> visitor) {
  database.ScanSchemas(context, [&](duckdb::SchemaCatalogEntry& schema_ref) {
    schema_ref.Scan(context, T::Type, [&](duckdb::CatalogEntry& entry) {
      if (entry.type == T::Type) {
        visitor(entry.template Cast<T>());
      }
    });
  });
}

template<typename T>
duckdb::LogicalType GetFieldType();

// Write a single field value into a DuckDB Vector at the given row.
template<typename Field>
void WriteField(duckdb::Vector& vec, duckdb::idx_t row, const Field& field,
                const auth::RoleGraph& roles) {
  if constexpr (std::is_enum_v<Field>) {
    WriteField(vec, row, std::to_underlying(field), roles);
  } else if constexpr (std::is_same_v<Field, Name>) {
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] =
      duckdb::StringVector::AddString(vec, field.v.data(), field.v.size());
  } else if constexpr (std::is_same_v<Field, std::string_view>) {
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] =
      duckdb::StringVector::AddString(vec, field.data(), field.size());
  } else if constexpr (std::is_same_v<Field, std::string>) {
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] =
      duckdb::StringVector::AddString(vec, field);
  } else if constexpr (std::is_same_v<Field, char>) {
    // Postgres prints "char" 0 as the empty string, and its own catalog views
    // compare against '' to mean "unset" (attgenerated, attidentity). A
    // one-byte string holding NUL is not that string.
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] =
      duckdb::StringVector::AddString(vec, &field, field ? 1 : 0);
  } else if constexpr (std::is_same_v<Field, bool>) {
    duckdb::FlatVector::GetDataMutable<bool>(vec)[row] = field;
  } else if constexpr (std::is_same_v<Field, int8_t>) {
    duckdb::FlatVector::GetDataMutable<int8_t>(vec)[row] = field;
  } else if constexpr (std::is_same_v<Field, int16_t>) {
    duckdb::FlatVector::GetDataMutable<int16_t>(vec)[row] = field;
  } else if constexpr (std::is_same_v<Field, int32_t>) {
    duckdb::FlatVector::GetDataMutable<int32_t>(vec)[row] = field;
  } else if constexpr (std::is_same_v<Field, Oid> ||
                       std::is_same_v<Field, Xid> ||
                       std::is_same_v<Field, Regproc> ||
                       std::is_same_v<Field, Regtype> ||
                       std::is_same_v<Field, Regclass> ||
                       std::is_same_v<Field, Cid> ||
                       std::is_same_v<Field, Xid8> ||
                       std::is_same_v<Field, Tid>) {
    // PG catalog OID-like types stored as int32
    duckdb::FlatVector::GetDataMutable<int64_t>(vec)[row] =
      static_cast<int64_t>(field);
  } else if constexpr (std::is_same_v<Field, int64_t>) {
    duckdb::FlatVector::GetDataMutable<int64_t>(vec)[row] =
      static_cast<int64_t>(field);
  } else if constexpr (std::is_same_v<Field, uint64_t>) {
    duckdb::FlatVector::GetDataMutable<uint64_t>(vec)[row] =
      static_cast<uint64_t>(field);
  } else if constexpr (std::is_same_v<Field, float>) {
    duckdb::FlatVector::GetDataMutable<float>(vec)[row] = field;
  } else if constexpr (std::is_same_v<Field, double>) {
    duckdb::FlatVector::GetDataMutable<double>(vec)[row] = field;
  } else if constexpr (std::is_same_v<Field, Bytea>) {
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] =
      duckdb::StringVector::AddStringOrBlob(
        vec, reinterpret_cast<const char*>(field.data()), field.size());
  } else if constexpr (IsArray<Field>::value) {
    auto list_size = field.size();
    auto current_size = duckdb::ListVector::GetListSize(vec);
    duckdb::ListVector::Reserve(vec, current_size + list_size);
    auto& entry = duckdb::ListVector::GetData(vec)[row];
    entry.offset = current_size;
    entry.length = list_size;
    auto& child = duckdb::ListVector::GetEntry(vec);
    for (duckdb::idx_t i = 0; i < list_size; i++) {
      WriteField(child, current_size + i, field[i], roles);
    }
    duckdb::ListVector::SetListSize(vec, current_size + list_size);
  } else if constexpr (IsAclColumn<Field>::value) {
    if (field.items.empty()) {
      duckdb::FlatVector::ValidityMutable(vec).SetInvalid(row);
    } else {
      auto list_size = field.items.size();
      auto current_size = duckdb::ListVector::GetListSize(vec);
      duckdb::ListVector::Reserve(vec, current_size + list_size);
      auto& entry = duckdb::ListVector::GetData(vec)[row];
      entry.offset = current_size;
      entry.length = list_size;
      auto& child = duckdb::ListVector::GetEntry(vec);
      for (duckdb::idx_t i = 0; i < list_size; i++) {
        std::string oid_fallback;
        auto text = AclToPgString(
          field.items[i], [&](duckdb::idx_t id) -> std::string_view {
            if (id == kPublicGrantee) {
              return {};
            }
            if (auto name = roles.NameOf(id); !name.empty()) {
              return name;
            }
            oid_fallback = std::to_string(id);
            return oid_fallback;
          });
        duckdb::FlatVector::GetDataMutable<duckdb::string_t>(
          child)[current_size + i] =
          duckdb::StringVector::AddString(child, text.data(), text.size());
      }
      duckdb::ListVector::SetListSize(vec, current_size + list_size);
    }
  } else if constexpr (std::is_same_v<Field, Timestamptz>) {
    if (field.is_null) {
      duckdb::FlatVector::ValidityMutable(vec).SetInvalid(row);
    } else {
      duckdb::FlatVector::GetDataMutable<int64_t>(vec)[row] = field.micros;
    }
  } else if constexpr (std::is_same_v<Field, Empty>) {
    duckdb::FlatVector::ValidityMutable(vec).SetInvalid(row);
  } else {
    static_assert(false);
  }
}

template<typename Field>
duckdb::LogicalType GetFieldType() {
  if constexpr (std::is_same_v<Field, Oid>) {
    return OID();
  } else if constexpr (std::is_same_v<Field, Regproc>) {
    return REGPROC();
  } else if constexpr (std::is_same_v<Field, Regtype>) {
    return REGTYPE();
  } else if constexpr (std::is_same_v<Field, Regclass>) {
    return REGCLASS();
  } else if constexpr (std::is_same_v<Field, Xid>) {
    return XID();
  } else if constexpr (std::is_same_v<Field, Name>) {
    return NAME();
  } else if constexpr (std::is_same_v<Field, Bytea>) {
    return duckdb::LogicalType::BLOB;
  } else if constexpr (std::is_same_v<Field, char>) {
    return CHAR();
  } else if constexpr (std::is_same_v<Field, bool>) {
    return duckdb::LogicalType::BOOLEAN;
  } else if constexpr (std::is_same_v<Field, int16_t>) {
    return duckdb::LogicalType::SMALLINT;
  } else if constexpr (std::is_same_v<Field, int32_t>) {
    return duckdb::LogicalType::INTEGER;
  } else if constexpr (std::is_same_v<Field, int64_t>) {
    return duckdb::LogicalType::BIGINT;
  } else if constexpr (std::is_same_v<Field, uint64_t>) {
    return duckdb::LogicalType::UBIGINT;
  } else if constexpr (std::is_same_v<Field, float>) {
    return duckdb::LogicalType::FLOAT;
  } else if constexpr (std::is_same_v<Field, double>) {
    return duckdb::LogicalType::DOUBLE;
  } else if constexpr (std::is_same_v<Field, std::string_view> ||
                       std::is_same_v<Field, std::string>) {
    return duckdb::LogicalType::VARCHAR;
  } else if constexpr (std::is_same_v<Field, Timestamptz>) {
    return duckdb::LogicalType::TIMESTAMP_TZ;
  } else if constexpr (std::is_same_v<Field, Empty>) {
    return duckdb::LogicalType::SQLNULL;
  } else if constexpr (std::is_same_v<Field, Aclitem>) {
    return ACLITEM();
  } else if constexpr (std::is_enum_v<Field>) {
    return GetFieldType<std::underlying_type_t<Field>>();
  } else if constexpr (IsAclColumn<Field>::value) {
    return duckdb::LogicalType::LIST(GetFieldType<Aclitem>());
  } else if constexpr (IsArray<Field>::value) {
    return duckdb::LogicalType::LIST(
      GetFieldType<typename Field::value_type>());
  } else {
    static_assert(false);
  }
}

// Create DuckDB Vectors with the right types for struct T.
template<typename T>
std::vector<duckdb::Vector> CreateColumns(duckdb::idx_t capacity) {
  std::vector<duckdb::Vector> result;
  result.reserve(boost::pfr::tuple_size_v<T>);
  boost::pfr::for_each_field(T{}, [&]<typename Field>(const Field&) {
    result.emplace_back(GetFieldType<Field>(), capacity);
  });
  return result;
}

// Write a row into DuckDB Vectors.
// null_mask: bitmask where bit N=1 means column N is NULL for this row.
template<typename T>
void WriteData(std::vector<duckdb::Vector>& columns, const T& value,
               uint64_t null_mask, duckdb::idx_t row,
               const auth::RoleGraph& roles) {
  uint32_t column = 0;
  boost::pfr::for_each_field(value, [&]<typename Field>(const Field& field) {
    if (null_mask & (uint64_t{1} << column)) {
      duckdb::FlatVector::ValidityMutable(columns[column]).SetInvalid(row);
    } else {
      WriteField(columns[column], row, field, roles);
    }
    ++column;
  });
}

template<typename T>
class SystemTable;

template<typename T>
class SystemTableSnapshot final : public VirtualTableSnapshot {
 public:
  explicit SystemTableSnapshot(const VirtualTable& table,
                               duckdb::Catalog& database, const Config& config)
    : VirtualTableSnapshot{table, database, table.Id(), table.GetName()},
      _config{config},
      // Once per snapshot, not once per row: resolving it walks the role
      // registry, and rebuilds the whole graph for a session that has created a
      // role itself. Every row of one snapshot answers from the same graph.
      _roles{auth::RolesOf(&config.GetClientContext())} {}

  duckdb::LogicalType RowType() const noexcept final {
    return _table->RowType();
  }

  const MaterializedData& GetData(std::vector<std::string> names) final {
    if (!_data) {
      _data = GetTableData();
    }
    return *_data;
  }

  MaterializedData GetTableData() { return {}; }

  const auth::RoleGraph& Roles() const noexcept { return *_roles; }

 private:
  const Config& _config;
  const std::shared_ptr<const auth::RoleGraph> _roles;
  std::optional<MaterializedData> _data;
};

template<typename T>
class SystemTable : public VirtualTable {
 public:
  constexpr SystemTable() {
    _id = T::kId;
    _name = T::kName;
    if constexpr (requires { T::kSuperuserOnly; }) {
      _acl = {};  // no PUBLIC grant -> superuser-only
    }
  }

  std::shared_ptr<VirtualTableSnapshot> CreateSnapshot(
    duckdb::Catalog& database, const Config& config) const final {
    return std::make_shared<SystemTableSnapshot<T>>(*this, database, config);
  }

  duckdb::LogicalType RowType() const noexcept final {
    static const duckdb::LogicalType kRowType = [] {
      duckdb::child_list_t<duckdb::LogicalType> children;
      children.reserve(boost::pfr::tuple_size_v<T>);
      boost::pfr::for_each_field_with_name(
        T{}, [&]<typename Field>(std::string_view name, const Field& field) {
          children.emplace_back(name, GetFieldType<Field>());
        });
      return duckdb::LogicalType::STRUCT(std::move(children));
    }();
    return kRowType;
  }
};

}  // namespace sdb::pg
