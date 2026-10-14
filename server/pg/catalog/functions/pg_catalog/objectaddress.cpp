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

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "connector/pg_logical_types.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/oids.h"
#include "pg/sql_utils.h"

namespace sdb::pg {
namespace {

struct ObjectIdentity {
  std::string type;
  std::optional<std::string> description;
  std::optional<std::string> schema;
  std::optional<std::string> name;
  std::optional<std::string> identity;
};

struct ObjectAddress {
  uint64_t classid;
  uint64_t objid;
  int32_t objsubid;
};

ObjectIdentity Missing(std::string type) {
  return ObjectIdentity{.type = std::move(type)};
}

duckdb::optional_ptr<duckdb::CatalogEntry> ObjectEntry(const Session& session,
                                                       uint64_t oid) {
  if (!session.database || oid == kInvalidOid) {
    return nullptr;
  }
  return session.database->GetOidIndex().GetVisible(oid,
                                                    session.transaction->view);
}

std::string_view RelationKind(const RelationAccess& relation) {
  using enum duckdb::CatalogType;
  if (relation.key) {
    return "index";
  }
  if (relation.system) {
    return relation.system->Sql().relkind == 'v' ? "view" : "table";
  }
  const auto type = relation.entry->type;
  if (type == TABLE_ENTRY) {
    return "table";
  }
  if (type == VIEW_ENTRY) {
    return "view";
  }
  if (type == INDEX_ENTRY) {
    return "index";
  }
  if (type == SEQUENCE_ENTRY) {
    return "sequence";
  }
  return "composite type";
}

const duckdb::ColumnDefinition* TableColumn(const RelationAccess& relation,
                                            int32_t attnum) {
  const auto* columns = TableColumns(relation);
  if (!columns || attnum < 1 ||
      static_cast<duckdb::idx_t>(attnum) > columns->LogicalColumnCount()) {
    return nullptr;
  }
  return &columns->GetColumn(duckdb::LogicalIndex(attnum - 1));
}

template<typename R>
std::optional<std::string> IndexColumn(duckdb::ClientContext& context,
                                       duckdb::CatalogEntry& relation,
                                       const R& attnums, size_t column) {
  const auto view =
    relation.type == duckdb::CatalogType::VIEW_ENTRY
      ? ViewColumns(context, relation.Cast<duckdb::ViewCatalogEntry>())
      : nullptr;
  if (relation.type == duckdb::CatalogType::VIEW_ENTRY && !view) {
    return std::nullopt;
  }
  size_t i = 0;
  for (const int16_t attnum : attnums) {
    if (i++ == column) {
      return std::string{IndexColumnAt(relation, view.get(), attnum).name};
    }
  }
  return std::nullopt;
}

std::optional<std::string> ColumnName(const Session& session,
                                      RelationAccess& relation,
                                      int32_t attnum) {
  using enum duckdb::CatalogType;
  if (attnum < 1) {
    return std::nullopt;
  }
  const auto column = static_cast<size_t>(attnum - 1);
  if (relation.system) {
    const auto& columns = relation.system->Sql().columns;
    if (column >= columns.size()) {
      return std::nullopt;
    }
    return std::string{columns[column].name};
  }
  auto& context = *session.context;
  auto& entry = *relation.entry;
  if (relation.key) {
    return IndexColumn(
      context, entry,
      KeyAttnums(entry.Cast<duckdb::TableCatalogEntry>().GetColumns(),
                 *relation.key),
      column);
  }
  if (const auto* definition = TableColumn(relation, attnum)) {
    return definition->Name().GetIdentifierName();
  }
  if (entry.type == VIEW_ENTRY) {
    auto& view = entry.Cast<duckdb::ViewCatalogEntry>();
    const auto columns = ViewColumns(context, view);
    if (!columns || column >= columns->types.size()) {
      return std::nullopt;
    }
    return view.ColumnName(*columns, column).GetIdentifierName();
  }
  if (entry.type == INDEX_ENTRY) {
    const auto& index = entry.Cast<duckdb::IndexCatalogEntry>();
    auto table = EntryByOid(session, index.table_oid);
    if (!table) {
      return std::nullopt;
    }
    return IndexColumn(context, *table, IndexAttnums(context, index, *table),
                       column);
  }
  if (entry.type == SEQUENCE_ENTRY) {
    if (NumbersRows(entry) || column >= kSequenceColumns.size()) {
      return std::nullopt;
    }
    return std::string{kSequenceColumns[column]};
  }
  if (entry.type == TYPE_ENTRY) {
    const auto& type = entry.Cast<duckdb::TypeCatalogEntry>().user_type;
    const auto& children = duckdb::StructType::GetChildTypes(type);
    if (column >= children.size()) {
      return std::nullopt;
    }
    if (type.id() == duckdb::LogicalTypeId::TUPLE) {
      return std::string{duckdb::TupleType::GetChildName(column)};
    }
    return children[column].first.GetIdentifierName();
  }
  return std::nullopt;
}

ObjectIdentity IdentifyRelation(const Session& session, uint64_t oid,
                                int32_t attnum) {
  auto relation = RelationByOid(session, oid);
  if (!relation) {
    return Missing("relation");
  }
  const auto kind = RelationKind(*relation);
  const auto [schema, name] = NameOf(*session.transaction, *relation);
  auto description =
    absl::StrCat(kind, " ", RegOut<RegKind::Class>(session, oid));
  auto identity = QualifiedOutName(schema, name);
  if (attnum == 0) {
    return ObjectIdentity{.type = std::string{kind},
                          .description = std::move(description),
                          .schema = std::string{schema},
                          .name = std::string{name},
                          .identity = std::move(identity)};
  }
  const auto column = ColumnName(session, *relation, attnum);
  if (!column) {
    return Missing(absl::StrCat(kind, " column"));
  }
  return ObjectIdentity{
    .type = absl::StrCat(kind, " column"),
    .description = absl::StrCat("column ", *column, " of ", description),
    .schema = std::string{schema},
    .name = std::string{name},
    .identity = absl::StrCat(identity, ".", QuoteIdentifier(*column))};
}

ObjectIdentity IdentifyFunction(const Session& session, uint64_t oid) {
  const auto visible = FunctionIsVisible(session, oid);
  if (!visible) {
    return Missing("routine");
  }
  const auto builtins = GetBuiltinFunctions(*session.context);
  char kind = 'f';
  std::string schema;
  if (const auto* builtin = builtins->Find(oid)) {
    kind = builtin->kind;
    schema = FindSystemObject(kSystemNamespaces, builtin->nsp)->name;
  } else {
    const auto entry = EntryByOid(session, oid);
    const auto& macros = entry->Cast<duckdb::MacroCatalogEntry>().macros;
    kind = macros.front()->is_procedure ? 'p' : 'f';
    schema = NameOf(*session.transaction, *entry).schema;
  }
  std::string_view type = "function";
  if (kind == 'a') {
    type = "aggregate";
  } else if (kind == 'p') {
    type = "procedure";
  }
  const std::string signature{RegOut<RegKind::Procedure>(session, oid)};
  return ObjectIdentity{
    .type = std::string{type},
    .description = absl::StrCat("function ", signature),
    .schema = schema,
    .identity = *visible ? absl::StrCat(QuoteIdentifier(schema), ".", signature)
                         : signature};
}

ObjectIdentity IdentifyType(const Session& session, uint64_t oid) {
  if (!TypeIsVisible(session, oid)) {
    return Missing("type");
  }
  const std::string formatted{RegOut<RegKind::Type>(session, oid)};
  ObjectIdentity identity{.type = "type",
                          .description = absl::StrCat("type ", formatted),
                          .identity = formatted};
  if (const auto object = TypeObject(session, oid)) {
    identity.schema = std::string{object->schema};
    identity.name = std::string{object->name};
    if (formatted == object->name) {
      identity.identity = QualifiedOutName(object->schema, object->name);
    }
  }
  return identity;
}

ObjectIdentity IdentifySchema(const Session& session, uint64_t oid) {
  const auto entry = ObjectEntry(session, oid);
  if (!entry || entry->type != duckdb::CatalogType::SCHEMA_ENTRY) {
    return Missing("schema");
  }
  const auto& name = entry->name.GetIdentifierName();
  return ObjectIdentity{.type = "schema",
                        .description = absl::StrCat("schema ", name),
                        .name = name,
                        .identity = QuoteIdentifier(name)};
}

ObjectIdentity IdentifyRole(const Session& session, uint64_t oid) {
  const auto roles = auth::RolesOf(session.context);
  const std::string name{roles->NameOf(oid)};
  if (name.empty()) {
    return Missing("role");
  }
  return ObjectIdentity{.type = "role",
                        .description = absl::StrCat("role ", name),
                        .name = name,
                        .identity = QuoteIdentifier(name)};
}

ObjectIdentity IdentifyConstraint(const Session& session, uint64_t oid) {
  const auto table = OwnerTable(session, oid);
  if (!table) {
    return Missing("constraint");
  }
  for (const auto& constraint : table->GetConstraints()) {
    if (constraint->oid != oid) {
      continue;
    }
    std::string name;
    for (const auto part : ConstraintName(*table, *constraint).parts) {
      name.append(part);
    }
    const auto [schema, table_name] = NameOf(*session.transaction, *table);
    return ObjectIdentity{
      .type = "table constraint",
      .description = absl::StrCat("constraint ", name, " on table ",
                                  RegOut<RegKind::Class>(session, table->oid)),
      .schema = std::string{schema},
      .identity = absl::StrCat(QuoteIdentifier(name), " on ",
                               QualifiedOutName(schema, table_name))};
  }
  return Missing("constraint");
}

ObjectIdentity IdentifyDefault(const Session& session, uint64_t oid) {
  const auto table = OwnerTable(session, oid);
  if (!table) {
    return Missing("default value");
  }
  for (const auto& column : table->GetColumns().Logical()) {
    if (column.CatalogOid() != oid) {
      continue;
    }
    const auto [schema, table_name] = NameOf(*session.transaction, *table);
    const auto& name = column.Name().GetIdentifierName();
    return ObjectIdentity{
      .type = "default value",
      .description =
        absl::StrCat("default value for column ", name, " of table ",
                     RegOut<RegKind::Class>(session, table->oid)),
      .identity = absl::StrCat("for ", QualifiedOutName(schema, table_name),
                               ".", QuoteIdentifier(name))};
  }
  return Missing("default value");
}

ObjectIdentity IdentifyRule(const Session& session, uint64_t oid) {
  const auto relation = RelationByOid(session, oid);
  if (!relation || RelationKind(*relation) != "view") {
    return Missing("rule");
  }
  const auto [schema, name] = NameOf(*session.transaction, *relation);
  return ObjectIdentity{
    .type = "rule",
    .description = absl::StrCat("rule _RETURN on view ",
                                RegOut<RegKind::Class>(session, oid)),
    .identity = absl::StrCat(QuoteIdentifier("_RETURN"), " on ",
                             QualifiedOutName(schema, name))};
}

ObjectIdentity IdentifyTrigger(const Session& session, uint64_t oid) {
  const auto entry = EntryByOid(session, oid);
  if (!entry || entry->type != duckdb::CatalogType::TRIGGER_ENTRY) {
    return Missing("trigger");
  }
  const auto& trigger = entry->Cast<duckdb::TriggerCatalogEntry>();
  const auto table =
    SiblingTable(*session.transaction, trigger, trigger.base_table->Table());
  if (!table) {
    return Missing("trigger");
  }
  const auto [schema, table_name] = NameOf(*session.transaction, *table);
  const auto& name = trigger.name.GetIdentifierName();
  return ObjectIdentity{
    .type = "trigger",
    .description = absl::StrCat("trigger ", name, " on table ",
                                RegOut<RegKind::Class>(session, table->oid)),
    .identity = absl::StrCat(QuoteIdentifier(name), " on ",
                             QualifiedOutName(schema, table_name))};
}

ObjectIdentity IdentifyCollation(uint64_t oid) {
  const auto collations = BuiltinCollations();
  const auto it = absl::c_find_if(
    collations, [&](const BuiltinCollation& c) { return c.oid == oid; });
  if (it == collations.end()) {
    return Missing("collation");
  }
  const auto quoted = QuoteIdentifier(it->name);
  return ObjectIdentity{
    .type = "collation",
    .description = absl::StrCat("collation ", quoted),
    .schema = std::string{irs::StaticStrings::kPgCatalogSchema},
    .name = std::string{it->name},
    .identity =
      absl::StrCat(irs::StaticStrings::kPgCatalogSchema, ".", quoted)};
}

ObjectIdentity IdentifyNamed(std::string_view type, std::string_view name) {
  return ObjectIdentity{.type = std::string{type},
                        .description = absl::StrCat(type, " ", name),
                        .name = std::string{name},
                        .identity = QuoteIdentifier(name)};
}

template<typename R>
ObjectIdentity IdentifyNamed(std::string_view type, const R& objects,
                             uint64_t oid) {
  const auto it = absl::c_find_if(
    objects, [&](const auto& object) { return object.oid == oid; });
  return it == objects.end() ? Missing(std::string{type})
                             : IdentifyNamed(type, it->name);
}

ObjectIdentity IdentifyDatabase(const Session& session, uint64_t oid) {
  const auto database = FindDatabaseById(*session.context, oid);
  return database
           ? IdentifyNamed("database", database->name.GetIdentifierName())
           : Missing("database");
}

std::optional<ObjectIdentity> IdentifyObject(const Session& session,
                                             const ObjectAddress& address) {
  const auto [classid, objid, objsubid] = address;
  if (classid == kPgClassTable) {
    return IdentifyRelation(session, objid, objsubid);
  }
  if (classid == kPgProcTable) {
    return IdentifyFunction(session, objid);
  }
  if (classid == kPgTypeTable) {
    return IdentifyType(session, objid);
  }
  if (classid == kPgNamespaceTable) {
    return IdentifySchema(session, objid);
  }
  if (classid == kPgAuthidTable) {
    return IdentifyRole(session, objid);
  }
  if (classid == kPgConstraintTable) {
    return IdentifyConstraint(session, objid);
  }
  if (classid == kPgAttrdefTable) {
    return IdentifyDefault(session, objid);
  }
  if (classid == kPgRewriteTable) {
    return IdentifyRule(session, objid);
  }
  if (classid == kPgTriggerTable) {
    return IdentifyTrigger(session, objid);
  }
  if (classid == kPgDatabaseTable) {
    return IdentifyDatabase(session, objid);
  }
  if (classid == kPgCollationTable) {
    return IdentifyCollation(objid);
  }
  if (classid == kPgLanguageTable) {
    return IdentifyNamed("language", kSystemLanguages, objid);
  }
  if (classid == kPgAmTable) {
    return IdentifyNamed("access method", kSystemAccessMethods, objid);
  }
  if (classid == kPgTablespaceTable) {
    return IdentifyNamed("tablespace", kSystemTablespaces, objid);
  }
  return std::nullopt;
}

std::optional<std::span<const duckdb::AclItem>> ObjectAcl(
  const Session& session, const ObjectAddress& address) {
  using enum duckdb::CatalogType;
  const auto [classid, objid, objsubid] = address;
  if (classid == kPgClassTable) {
    const auto relation = RelationByOid(session, objid);
    if (!relation || relation->key ||
        (relation->entry && (relation->entry->type == INDEX_ENTRY ||
                             relation->entry->type == TYPE_ENTRY))) {
      return std::nullopt;
    }
    if (objsubid == 0) {
      return relation->permissions->acl;
    }
    if (const auto* column = TableColumn(*relation, objsubid)) {
      return column->Acl();
    }
    return std::nullopt;
  }
  if (classid == kPgProcTable || classid == kPgTypeTable ||
      classid == kPgNamespaceTable) {
    if (const auto entry = ObjectEntry(session, objid);
        entry && CatalogClassOid(entry->type) == classid) {
      return entry->permissions.acl;
    }
  }
  return std::nullopt;
}

template<typename Write>
void ForEachObject(duckdb::DataChunk& args, Write&& write) {
  const auto classids = args.data[0].Values<int64_t>();
  const auto objids = args.data[1].Values<int64_t>();
  const auto objsubids = args.data[2].Values<int32_t>();
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
    const auto classid = classids[row];
    const auto objid = objids[row];
    const auto objsubid = objsubids[row];
    if (!classid.IsValid() || !objid.IsValid() || !objsubid.IsValid()) {
      write(std::optional<ObjectAddress>{});
      continue;
    }
    write(std::optional{
      ObjectAddress{.classid = static_cast<uint64_t>(classid.GetValue()),
                    .objid = static_cast<uint64_t>(objid.GetValue()),
                    .objsubid = objsubid.GetValue()}});
  }
}

void PgDescribeObjectFunction(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  auto writer =
    duckdb::FlatVector::Writer<duckdb::string_t>(result, args.size());
  ForEachObject(args, [&](const std::optional<ObjectAddress>& object) {
    const auto identity =
      object ? IdentifyObject(session, *object) : std::nullopt;
    if (!identity || !identity->description) {
      writer.WriteNull();
      return;
    }
    writer.WriteValue(duckdb::string_t{*identity->description});
  });
}

void PgIdentifyObjectFunction(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  auto writer = duckdb::FlatVector::Writer<duckdb::VectorStructType<
    duckdb::string_t, duckdb::string_t, duckdb::string_t, duckdb::string_t>>(
    result, args.size());
  ForEachObject(args, [&](const std::optional<ObjectAddress>& object) {
    const auto identity =
      object ? IdentifyObject(session, *object) : std::nullopt;
    if (!identity) {
      writer.WriteNull();
      return;
    }
    const auto put = [&](auto& field, const std::optional<std::string>& text) {
      if (text) {
        field.WriteValue(duckdb::string_t{*text});
      } else {
        field.WriteNull();
      }
    };
    const auto quoted = [](const std::optional<std::string>& text) {
      return text ? std::optional{QuoteIdentifier(*text)} : std::nullopt;
    };
    writer.WriteValue([&](auto& type, auto& schema, auto& name, auto& id) {
      put(type, identity->type);
      put(schema, quoted(identity->schema));
      put(name, quoted(identity->name));
      put(id, identity->identity);
    });
  });
}

void PgGetAclFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                      duckdb::Vector& result) {
  const auto& session = SessionOf(state);
  const auto roles = auth::RolesOf(&state.GetContext());
  auto writer =
    duckdb::FlatVector::Writer<duckdb::VectorListType<duckdb::string_t>>(
      result, args.size());
  std::string text;
  ForEachObject(args, [&](const std::optional<ObjectAddress>& object) {
    const auto acl = object ? ObjectAcl(session, *object) : std::nullopt;
    if (!acl || acl->empty()) {
      writer.WriteNull();
      return;
    }
    WriteAclTexts(writer, *acl, *roles, text);
  });
}

}  // namespace

void RegisterObjectAddressFunctions(duckdb::ExtensionLoader& loader) {
  using enum duckdb::FunctionStability;
  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;
  const auto oid = OID();
  const auto int4 = duckdb::LogicalType::INTEGER;
  const auto text = duckdb::LogicalType::VARCHAR;
  RegisterScalar(
    loader, pg_catalog, "pg_describe_object", CONSISTENT_WITHIN_QUERY,
    InitSession,
    {duckdb::ScalarFunction{{oid, oid, int4}, text, PgDescribeObjectFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_identify_object", CONSISTENT_WITHIN_QUERY,
    InitSession,
    {duckdb::ScalarFunction{
      {oid, oid, int4},
      duckdb::LogicalType::STRUCT(
        {{"type", text}, {"schema", text}, {"name", text}, {"identity", text}}),
      PgIdentifyObjectFunction}});
  RegisterScalar(loader, pg_catalog, "pg_get_acl", CONSISTENT_WITHIN_QUERY,
                 InitSession,
                 {duckdb::ScalarFunction{{oid, oid, int4},
                                         duckdb::LogicalType::LIST(ACLITEM()),
                                         PgGetAclFunction}});
}

}  // namespace sdb::pg
