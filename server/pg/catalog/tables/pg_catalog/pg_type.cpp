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

#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <type_traits>

#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"
#include "pg/types.h"

namespace sdb::pg {
namespace {

constexpr duckdb::CatalogType kTypes[] = {
  duckdb::CatalogType::TYPE_ENTRY,
  duckdb::CatalogType::TABLE_ENTRY,
  duckdb::CatalogType::VIEW_ENTRY,
};

constexpr duckdb::CatalogType kUserTypes[] = {duckdb::CatalogType::TYPE_ENTRY};

constexpr SystemKindTypes kTypeKinds[] = {
  {'b', kTypes},
  {'c', kTypes},
  {'d', kUserTypes},
  {'e', kUserTypes},
};

constexpr SystemIndex kIndexes[] = {
  {kPgTypeSql["oid"], SystemLookup::Object},
  {kPgTypeSql["typname"], SystemLookup::Object},
  {kPgTypeSql["typnamespace"], SystemLookup::Namespace},
  {kPgTypeSql["typtype"], SystemLookup::Kind, kTypeKinds},
  {kPgTypeSql["typrelid"], SystemLookup::Object},
  {kPgTypeSql["typelem"], SystemLookup::Object},
};

SystemRows<BuiltinType> LoadBuiltinTypes(SystemScan&) { return BuiltinTypes(); }

void FindNamed(const SystemRows<BuiltinType>&, const SystemFilter& filter,
               std::vector<const BuiltinType*>& picked) {
  for (const auto& name : *filter.texts.keys) {
    for (const auto& schema : kSystemNamespaces) {
      if (const auto* type = FindBuiltinType(schema.oid, name)) {
        picked.emplace_back(type);
      }
    }
  }
}

constexpr ArrayKey<BuiltinType> kBuiltinKeys[] = {
  {kPgTypeSql["oid"], SortedBy<BuiltinType, &BuiltinType::oid>},
  {kPgTypeSql["typname"], FindNamed},
};

const BuiltinType kComposite = *FindBuiltinType(kPgClassRowtype);
const BuiltinType kArray = *FindBuiltinType(kInt4Array);
const BuiltinType kDomain = *FindBuiltinType(kCardinalNumber);
const int32_t kEnumIn = BuiltinProcOid("enum_in");
const int32_t kEnumOut = BuiltinProcOid("enum_out");
const int32_t kEnumRecv = BuiltinProcOid("enum_recv");
const int32_t kEnumSend = BuiltinProcOid("enum_send");

consteval auto TypeColumns(auto base, auto align) {
  return std::tuple{
    Col<"typtype">([=](const auto& row) { return base(row).type; }),
    Col<"typlen">([=](const auto& row) { return base(row).len; }),
    Col<"typbyval">([=](const auto& row) { return base(row).byval; }),
    Col<"typcategory">([=](const auto& row) { return base(row).category; }),
    Col<"typsubscript">([=](const auto& row) { return base(row).subscript; }),
    Col<"typinput">([=](const auto& row) { return base(row).input; }),
    Col<"typoutput">([=](const auto& row) { return base(row).output; }),
    Col<"typreceive">([=](const auto& row) { return base(row).receive; }),
    Col<"typsend">([=](const auto& row) { return base(row).send; }),
    Col<"typanalyze">([=](const auto& row) { return base(row).analyze; }),
    Col<"typalign">([=](const auto& row) { return align(row); }),
    Col<"typstorage">([=](const auto& row) { return base(row).storage; })};
}

class UserType {
 public:
  explicit UserType(const duckdb::TypeCatalogEntry& entry) : entry{entry} {}

  const ColumnType& Base() const {
    if (!_base) {
      _base = DescribeColumnType(entry.user_type);
    }
    return *_base;
  }

  const BuiltinType* Builtin() const {
    if (!_builtin) {
      _builtin = FindBuiltinType(Base().oid);
    }
    return *_builtin;
  }

  const duckdb::TypeCatalogEntry& entry;

 private:
  mutable std::optional<ColumnType> _base;
  mutable std::optional<const BuiltinType*> _builtin;
};

struct ArrayType {
  const duckdb::CatalogEntry& owner;
  duckdb::idx_t element;
  char align;
  const UserType* domain;
  ArrayTypeNames& names;

  char Align() const { return domain ? domain->Base().align : align; }
};

constexpr std::tuple kUserTypeColumns{
  Col<"oid">([](const auto& row) { return row.entry.oid; }),
  Col<"typname">([](const auto& row) -> const auto& { return row.entry.name; }),
  Col<"typnamespace">(
    [](const auto& row) { return row.entry.ParentSchemaOid(); }),
  Col<"typowner">([](const auto& row) { return row.entry.permissions.owner; }),
  Col<"typarray">([](const auto& row) { return TypeArrayOid(row.entry.oid); }),
  Col<"typacl">(
    [](const auto& row) -> const auto& { return row.entry.permissions.acl; })};

class PgType final : public SystemTableScan<kPgTypeSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<BuiltinType>{&LoadBuiltinTypes, kBuiltinKeys},
    CatalogSource{kTypes, SystemSchemas::Skip, kIndexes}};

  static constexpr auto kBuiltinType = Shape<kSql, const BuiltinType>(
    Col<"oid">(&BuiltinType::oid), Col<"typname">(&BuiltinType::name),
    Col<"typnamespace">(&BuiltinType::nsp),
    Col<"typowner">([](const auto&) { return kRootUser; }),
    TypeColumns([](const auto& type) -> const BuiltinType& { return type; },
                [](const auto& type) { return type.align; }),
    Col<"typispreferred">(&BuiltinType::preferred),
    Col<"typdelim">(&BuiltinType::delim), Col<"typelem">(&BuiltinType::elem),
    Col<"typarray">(&BuiltinType::array), Col<"typmodin">(&BuiltinType::modin),
    Col<"typmodout">(&BuiltinType::modout),
    Col<"typbasetype">(&BuiltinType::basetype),
    Col<"typcollation">(&BuiltinType::collation),
    Col<"typrelid">(&BuiltinType::relid),
    Col<"typtypmod">(&BuiltinType::typmod),
    Col<"typnotnull">(&BuiltinType::notnull),
    Col<"typdefault">(
      [](const auto& type) { return NonEmpty(type.default_text); }));

  static constexpr auto kEnumType = Shape<kSql, const UserType>(
    kUserTypeColumns, Col<"typlen">([](const auto&) { return 4; }),
    Col<"typbyval">([](const auto&) { return true; }),
    Col<"typtype">([](const auto&) { return 'e'; }),
    Col<"typcategory">([](const auto&) { return 'E'; }),
    Col<"typinput">([](const auto&) { return kEnumIn; }),
    Col<"typoutput">([](const auto&) { return kEnumOut; }),
    Col<"typreceive">([](const auto&) { return kEnumRecv; }),
    Col<"typsend">([](const auto&) { return kEnumSend; }),
    Col<"typalign">([](const auto&) { return 'i'; }));

  static constexpr auto kStructType = Shape<kSql, const UserType>(
    kUserTypeColumns,
    TypeColumns([](const auto&) -> const BuiltinType& { return kComposite; },
                [](const auto&) { return kComposite.align; }),
    Col<"typrelid">([](const auto& row) { return row.entry.oid; }));

  static constexpr auto kDomainType = Shape<kSql, const UserType>(
    kUserTypeColumns,
    Col<"typlen">([](const auto& row) { return row.Base().len; }),
    Col<"typbyval">([](const auto& row) { return row.Base().byval; }),
    Col<"typtype">([](const auto&) { return kDomain.type; }),
    Col<"typcategory">([](const auto& row) {
      const auto* builtin = row.Builtin();
      return builtin ? builtin->category : 'U';
    }),
    Col<"typinput">([](const auto&) { return kDomain.input; }),
    Col<"typoutput">([](const auto& row) {
      const auto* builtin = row.Builtin();
      return builtin ? builtin->output : 0;
    }),
    Col<"typreceive">([](const auto&) { return kDomain.receive; }),
    Col<"typsend">([](const auto& row) {
      const auto* builtin = row.Builtin();
      return builtin ? builtin->send : 0;
    }),
    Col<"typanalyze">([](const auto& row) {
      const auto* builtin = row.Builtin();
      return builtin ? builtin->analyze : 0;
    }),
    Col<"typalign">([](const auto& row) { return row.Base().align; }),
    Col<"typstorage">([](const auto& row) { return row.Base().storage; }),
    Col<"typbasetype">([](const auto& row) { return row.Base().oid; }),
    Col<"typtypmod">([](const auto& row) { return row.Base().typmod; }),
    Col<"typndims">([](const auto& row) { return row.Base().ndims; }),
    Col<"typcollation">([](const auto& row) { return row.Base().collation; }));

  static constexpr auto kRowType = Shape<kSql, const duckdb::CatalogEntry>(
    Col<"oid">([](const auto& relation) { return RowTypeOid(relation.oid); }),
    Col<"typname">(&duckdb::CatalogEntry::name),
    Col<"typnamespace">(&duckdb::CatalogEntry::ParentSchemaOid),
    Col<"typowner">(kOwner), Col<"typarray">([](const auto& relation) {
      return TypeArrayOid(RowTypeOid(relation.oid));
    }),
    TypeColumns([](const auto&) -> const BuiltinType& { return kComposite; },
                [](const auto&) { return kComposite.align; }),
    Col<"typrelid">(&duckdb::CatalogEntry::oid));

  static constexpr auto kArrayType = Shape<kSql, const ArrayType>(
    Col<"oid">([](const auto& row) { return TypeArrayOid(row.element); }),
    Col<"typnamespace">(
      [](const auto& row) { return row.owner.ParentSchemaOid(); }),
    Col<"typowner">(
      [](const auto& row) { return row.owner.permissions.owner; }),
    TypeColumns([](const auto&) -> const BuiltinType& { return kArray; },
                [](const auto& row) { return row.Align() == 'd' ? 'd' : 'i'; }),
    Col<"typelem">(&ArrayType::element),
    Col<"typname">([](const auto& row) { return row.names.Of(row.owner); }));

  void Row(const BuiltinType& type) { Emit<kBuiltinType>(type); }

  void Row(const duckdb::TypeCatalogEntry& entry) {
    const UserType type{entry};
    const auto id = entry.user_type.id();
    if (id == duckdb::LogicalTypeId::ENUM) {
      Emit<kEnumType>(type);
      Emit<kArrayType>({entry, entry.oid, 'i', nullptr, _arrays});
    } else if (duckdb::StructType::IsStruct(id)) {
      Emit<kStructType>(type);
      Emit<kArrayType>({entry, entry.oid, kComposite.align, nullptr, _arrays});
    } else {
      Emit<kDomainType>(type);
      Emit<kArrayType>({entry, entry.oid, char{}, &type, _arrays});
    }
  }

  template<typename Relation>
    requires std::is_same_v<Relation, duckdb::TableCatalogEntry> ||
             std::is_same_v<Relation, duckdb::ViewCatalogEntry>
  void Row(const Relation& relation) {
    Emit<kRowType>(relation);
    Emit<kArrayType>(
      {relation, RowTypeOid(relation.oid), kComposite.align, nullptr, _arrays});
  }

 private:
  ArrayTypeNames _arrays{*this};
};

}  // namespace

SystemTable gPgType = SystemTableOf<PgType>();

}  // namespace sdb::pg
