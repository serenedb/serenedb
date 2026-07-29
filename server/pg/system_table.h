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
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <optional>
#include <span>
#include <type_traits>

#include "auth/acl.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/object.h"
#include "catalog/role.h"
#include "catalog/virtual_table.h"
#include "connector/pg_logical_types.h"
#include "pg/information_schema/fwd.h"
#include "pg/pg_catalog/fwd.h"

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
  absl::FunctionRef<std::string_view(ObjectId)> name_of) {
  std::string out;
  if (item.grantee != catalog::kPublicGrantee) {
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

template<typename T>
duckdb::LogicalType GetFieldType();

// Write a single field value into a DuckDB Vector at the given row.
template<typename Field>
void WriteField(duckdb::Vector& vec, duckdb::idx_t row, const Field& field,
                const catalog::Snapshot& snapshot) {
  if constexpr (std::is_enum_v<Field>) {
    WriteField(vec, row, std::to_underlying(field), snapshot);
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
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[row] =
      duckdb::StringVector::AddString(vec, &field, 1);
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
      duckdb::StringVector::AddStringOrBlob(vec, field.data.data(),
                                            field.data.size());
  } else if constexpr (IsArray<Field>::value) {
    auto list_size = field.size();
    auto current_size = duckdb::ListVector::GetListSize(vec);
    duckdb::ListVector::Reserve(vec, current_size + list_size);
    auto& entry = duckdb::ListVector::GetData(vec)[row];
    entry.offset = current_size;
    entry.length = list_size;
    auto& child = duckdb::ListVector::GetEntry(vec);
    for (duckdb::idx_t i = 0; i < list_size; i++) {
      WriteField(child, current_size + i, field[i], snapshot);
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
        auto text =
          AclToPgString(field.items[i], [&](ObjectId id) -> std::string_view {
            if (id == catalog::kPublicGrantee) {
              return {};
            }
            if (auto role = snapshot.GetObject<catalog::Role>(id)) {
              return role->GetName();
            }
            oid_fallback = std::to_string(id.id());
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
               const catalog::Snapshot& snapshot) {
  uint32_t column = 0;
  boost::pfr::for_each_field(value, [&]<typename Field>(const Field& field) {
    if (null_mask & (uint64_t{1} << column)) {
      duckdb::FlatVector::ValidityMutable(columns[column]).SetInvalid(row);
    } else {
      WriteField(columns[column], row, field, snapshot);
    }
    ++column;
  });
}

template<typename T>
class SystemTable;

template<typename T>
class SystemTableSnapshot final : public catalog::VirtualTableSnapshot {
 public:
  explicit SystemTableSnapshot(const catalog::VirtualTable& table,
                               ObjectId database_id, const Config& config)
    : VirtualTableSnapshot{{},
                           database_id,
                           table.Id(),
                           std::string{table.GetName()},
                           catalog::ObjectType::Virtual},
      _config{config} {
    _table = &table;
  }

  duckdb::LogicalType RowType() const noexcept final {
    return _table->RowType();
  }

  ObjectId GetDatabaseId() const noexcept { return GetParentId(); }

  const catalog::MaterializedData& GetData(
    std::vector<std::string> names) final {
    if (!_data) {
      _data = GetTableData();
    }
    return *_data;
  }

  catalog::MaterializedData GetTableData() { return {}; }

 private:
  const Config& _config;
  std::optional<catalog::MaterializedData> _data;

  void Serialize(duckdb::Serializer&) const final {}
};

template<typename T>
class SystemTable : public catalog::VirtualTable {
 public:
  constexpr SystemTable() {
    _id = ObjectId{T::kId};
    _name = T::kName;
    if constexpr (requires { T::kSuperuserOnly; }) {
      _acl = {};  // no PUBLIC grant -> superuser-only
    }
  }

  std::shared_ptr<catalog::VirtualTableSnapshot> CreateSnapshot(
    ObjectId database, const Config& config) const final {
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
