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

#include "pg/catalog/functions/pg_catalog/objectaddress.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <array>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/main/database_manager.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/oids.h"
#include "pg/catalog/utils/acl.h"
#include "pg/catalog/utils/columns.h"
#include "pg/catalog/utils/lookup.h"
#include "pg/sql_utils.h"
#include "pg/types/logical.h"

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

#include "pg/catalog/generated/object_classes.gen.inc"

ObjectIdentity Missing(std::string type) {
  return ObjectIdentity{.type = std::move(type)};
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
  auto& entry = *relation.entry;
  if (relation.key || entry.type == INDEX_ENTRY || entry.type == TYPE_ENTRY) {
    auto names = RelationAttributeNames(session, relation);
    if (column >= names.size()) {
      return std::nullopt;
    }
    return std::move(names[column]);
  }
  if (const auto* definition = TableColumn(relation, attnum)) {
    return definition->Name().GetIdentifierName();
  }
  if (entry.type == VIEW_ENTRY) {
    auto& view = entry.Cast<duckdb::ViewCatalogEntry>();
    const auto columns = ViewColumns(*session.context, view);
    if (!columns || column >= columns->types.size()) {
      return std::nullopt;
    }
    return columns->names[column].GetIdentifierName();
  }
  if (entry.type == SEQUENCE_ENTRY) {
    if (column >= kSequenceColumns.size()) {
      return std::nullopt;
    }
    return std::string{kSequenceColumns[column]};
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
    absl::StrCat(kind, " ", RegOut(session, RegKind::Class, oid));
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
  char kind = 'f';
  std::string_view schema;
  if (oid < kMaxSystem) {
    const auto builtins = GetBuiltinFunctions(*session.context);
    const auto* builtin = builtins->Find(oid);
    if (!builtin) {
      return Missing("routine");
    }
    kind = builtin->kind;
    schema = FindSystemObject(kSystemNamespaces, builtin->nsp)->name;
  } else {
    const auto found = FindMacro(session, oid);
    if (!found.entry) {
      return Missing("routine");
    }
    kind = found.macro->is_procedure ? 'p' : 'f';
    schema = NameOf(*session.transaction, *found.entry).schema;
  }
  std::string_view type = "function";
  if (kind == 'a') {
    type = "aggregate";
  } else if (kind == 'p') {
    type = "procedure";
  }
  return ObjectIdentity{
    .type = std::string{type},
    .description =
      absl::StrCat("function ", RegOut(session, RegKind::Procedure, oid)),
    .schema = std::string{schema},
    .identity = ProcIdentity(session, oid)};
}

ObjectIdentity IdentifyType(const Session& session, uint64_t oid) {
  if (!TypeExists(session, oid)) {
    return Missing("type");
  }
  ObjectIdentity identity{
    .type = "type",
    .description = absl::StrCat("type ", RegOut(session, RegKind::Type, oid)),
    .identity = TypeIdentity(session, oid)};
  if (const auto object = TypeObject(session, oid)) {
    identity.schema = std::string{object->schema};
    identity.name = std::string{object->name};
  } else if (const auto element = ArrayElementEntry(session, oid)) {
    identity.schema = std::string{
      NameOf(*session.transaction, Relation{element, nullptr, nullptr, nullptr})
        .schema};
    identity.name = ArrayTypeName(*session.transaction, *element);
  }
  return identity;
}

ObjectIdentity IdentifySchema(const Session& session, uint64_t oid) {
  auto entry = SchemaByOid(session, oid);
  if (!entry) {
    return Missing("schema");
  }
  const auto& name = entry->name.GetIdentifierName();
  return ObjectIdentity{.type = "schema",
                        .description = absl::StrCat("schema ", name),
                        .name = name,
                        .identity = QuoteIdentifier(name)};
}

ObjectIdentity IdentifyRole(const Session& session, uint64_t oid) {
  const auto roles = catalog::RolesOf(session.context);
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
  auto table = OwnerTable(session, oid);
  if (!table) {
    return Missing("constraint");
  }
  const auto& constraints = table->GetConstraints();
  const auto constraint = absl::c_find_if(
    constraints,
    [&](const auto& constraint) { return constraint->oid == oid; });
  if (constraint == constraints.end()) {
    return Missing("constraint");
  }
  const auto name =
    absl::StrJoin(ConstraintName(*table, **constraint).parts, "");
  const auto [schema, table_name] = NameOf(*session.transaction, *table);
  return ObjectIdentity{
    .type = "table constraint",
    .description = absl::StrCat("constraint ", name, " on table ",
                                RegOut(session, RegKind::Class, table->oid)),
    .schema = std::string{schema},
    .identity = absl::StrCat(QuoteIdentifier(name), " on ",
                             QualifiedOutName(schema, table_name))};
}

ObjectIdentity IdentifyDefault(const Session& session, uint64_t oid) {
  auto table = OwnerTable(session, oid);
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
                     RegOut(session, RegKind::Class, table->oid)),
      .identity = absl::StrCat("for ", QualifiedOutName(schema, table_name),
                               ".", QuoteIdentifier(name))};
  }
  return Missing("default value");
}

ObjectIdentity IdentifyRule(const Session& session, uint64_t oid) {
  const auto view_oid = RuleRelation(session, oid);
  if (view_oid == kInvalidOid) {
    return Missing("rule");
  }
  const auto relation = RelationByOid(session, view_oid);
  if (!relation || RelationKind(*relation) != "view") {
    return Missing("rule");
  }
  const auto [schema, name] = NameOf(*session.transaction, *relation);
  return ObjectIdentity{
    .type = "rule",
    .description = absl::StrCat("rule _RETURN on view ",
                                RegOut(session, RegKind::Class, view_oid)),
    .identity = absl::StrCat(QuoteIdentifier("_RETURN"), " on ",
                             QualifiedOutName(schema, name))};
}

ObjectIdentity IdentifyTrigger(const Session& session, uint64_t oid) {
  const auto entry = EntryByOid(session, oid);
  if (!entry || entry->type != duckdb::CatalogType::TRIGGER_ENTRY) {
    return Missing("trigger");
  }
  const auto& trigger = entry->Cast<duckdb::TriggerCatalogEntry>();
  const auto& table = TriggerTable(*session.transaction, trigger);
  const auto [schema, table_name] = NameOf(*session.transaction, table);
  const auto& name = trigger.name.GetIdentifierName();
  return ObjectIdentity{
    .type = "trigger",
    .description = absl::StrCat("trigger ", name, " on table ",
                                RegOut(session, RegKind::Class, table.oid)),
    .identity = absl::StrCat(QuoteIdentifier(name), " on ",
                             QualifiedOutName(schema, table_name))};
}

ObjectIdentity IdentifyCollation(uint64_t oid) {
  const auto* collation = FindBuiltinCollation(static_cast<int64_t>(oid));
  if (!collation) {
    return Missing("collation");
  }
  const auto quoted = QuoteIdentifier(collation->name);
  return ObjectIdentity{
    .type = "collation",
    .description = absl::StrCat("collation ", quoted),
    .schema = std::string{irs::StaticStrings::kPgCatalogSchema},
    .name = std::string{collation->name},
    .identity =
      absl::StrCat(irs::StaticStrings::kPgCatalogSchema, ".", quoted)};
}

ObjectIdentity IdentifyNamed(std::string_view type, std::string_view name) {
  return ObjectIdentity{.type = std::string{type},
                        .description = absl::StrCat(type, " ", name),
                        .name = std::string{name},
                        .identity = QuoteIdentifier(name)};
}

ObjectIdentity IdentifyNamed(std::string_view type, const auto& objects,
                             uint64_t oid) {
  const auto* object = FindSystemObject(objects, oid);
  return object ? IdentifyNamed(type, object->name)
                : Missing(std::string{type});
}

ObjectIdentity IdentifyDatabase(const Session& session, uint64_t oid) {
  auto database = FindDatabaseById(*session.context, oid);
  return database && Shown(session, *database)
           ? IdentifyNamed("database", database->name.GetIdentifierName())
           : Missing("database");
}

ObjectIdentity IdentifyTablespace(const Session& session, uint64_t oid) {
  auto& cluster = catalog::ClusterOf(*session.context);
  const auto tablespace = cluster.FindIn<catalog::TablespaceCatalogEntry>(
    cluster.GetCatalogTransaction(*session.context), oid);
  return tablespace
           ? IdentifyNamed("tablespace", tablespace->name.GetIdentifierName())
           : Missing("tablespace");
}

std::string_view DefaultAclTargets(duckdb::CatalogType type) {
  using enum duckdb::CatalogType;
  switch (type) {
    case SEQUENCE_ENTRY:
      return "sequences";
    case MACRO_ENTRY:
      return "functions";
    case TYPE_ENTRY:
      return "types";
    case SCHEMA_ENTRY:
      return "schemas";
    default:
      return "tables";
  }
}

ObjectIdentity IdentifyDefaultAcl(const Session& session, uint64_t oid) {
  auto& context = *session.context;
  auto& cluster = catalog::ClusterOf(context);
  const auto database =
    cluster.Set(duckdb::CatalogType::DATABASE_ENTRY)
      .Get(cluster.GetCatalogTransaction(context),
           duckdb::DatabaseManager::GetDefaultDatabase(context));
  auto identity = Missing("default acl");
  if (!database) {
    return identity;
  }
  VisitDefaultAcls(context, *database, [&](const duckdb::DefaultAcl& row) {
    if (row.oid != oid) {
      return;
    }
    const auto schema =
      row.scope == kInvalidOid ? nullptr : SchemaByOid(session, row.scope);
    if (row.scope != kInvalidOid && !schema) {
      return;
    }
    const std::string_view schema_name =
      schema ? std::string_view{schema->name.GetIdentifierName()}
             : std::string_view{};
    auto target = absl::StrCat(
      "for role ",
      QuoteIdentifier(catalog::RolesOf(&context)->NameOf(row.role)));
    if (schema) {
      absl::StrAppend(&target, " in schema ", QuoteIdentifier(schema_name));
    }
    absl::StrAppend(&target, " on ", DefaultAclTargets(row.objtype));
    identity.description = DescribeDefaultAcl(context, row, schema_name);
    identity.identity = std::move(target);
  });
  return identity;
}

std::optional<ObjectIdentity> IdentifyObject(const Session& session,
                                             const ObjectAddress& address) {
  const auto [classid, objid, objsubid] = address;
  if (!absl::c_linear_search(kObjectClasses, classid)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("unsupported object class: ", classid));
  }
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
  if (classid == kPgForeignDataWrapperTable) {
    return IdentifyNamed("foreign-data wrapper", kForeignDataWrappers, objid);
  }
  if (classid == kPgAmTable) {
    return IdentifyNamed("access method", kSystemAccessMethods, objid);
  }
  if (classid == kPgTablespaceTable) {
    return IdentifyTablespace(session, objid);
  }
  if (classid == kPgDefaultAclTable) {
    return IdentifyDefaultAcl(session, objid);
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
    writer.WriteValue([&](auto& type, auto& schema, auto& name, auto& id) {
      put(type, identity->type);
      put(schema, identity->schema
                    ? std::optional{QuoteIdentifier(*identity->schema)}
                    : std::nullopt);
      put(name, identity->name ? std::optional{QuoteIdentifier(*identity->name)}
                               : std::nullopt);
      put(id, identity->identity);
    });
  });
}

}  // namespace

std::string DescribeObject(duckdb::ClientContext& context,
                           const duckdb::CatalogEntry& entry, int32_t column) {
  using enum duckdb::CatalogType;
  uint64_t classid = 0;
  switch (entry.type) {
    case TABLE_ENTRY:
    case VIEW_ENTRY:
    case INDEX_ENTRY:
    case SEQUENCE_ENTRY:
      classid = kPgClassTable;
      break;
    case MACRO_ENTRY:
    case TABLE_MACRO_ENTRY:
      classid = kPgProcTable;
      break;
    case TYPE_ENTRY:
      classid = kPgTypeTable;
      break;
    case SCHEMA_ENTRY:
      classid = kPgNamespaceTable;
      break;
    case TOKENIZER_ENTRY:
      return absl::StrCat("text search dictionary ",
                          entry.name.GetIdentifierName());
    case TRIGGER_ENTRY:
      classid = kPgTriggerTable;
      break;
    case FOREIGN_SERVER_ENTRY:
      return absl::StrCat("server ", entry.name.GetIdentifierName());
    case FOREIGN_DATA_WRAPPER_ENTRY:
      classid = kPgForeignDataWrapperTable;
      break;
    default:
      return absl::StrCat(
        absl::AsciiStrToLower(duckdb::CatalogTypeToString(entry.type)), " ",
        entry.name.GetIdentifierName());
  }
  const auto session = MakeSession(&context);
  auto identity = IdentifyObject(
    session, {.classid = classid, .objid = entry.oid, .objsubid = column});
  if (identity && identity->description) {
    return std::move(*identity->description);
  }
  auto described =
    absl::StrCat(absl::AsciiStrToLower(duckdb::CatalogTypeToString(entry.type)),
                 " ", entry.name.GetIdentifierName());
  if (column > 0 && entry.type == TABLE_ENTRY) {
    const auto& columns = entry.Cast<duckdb::TableCatalogEntry>().GetColumns();
    const duckdb::LogicalIndex index{static_cast<duckdb::idx_t>(column - 1)};
    if (index.index < columns.LogicalColumnCount()) {
      return absl::StrCat("column ",
                          columns.GetColumn(index).Name().GetIdentifierName(),
                          " of ", described);
    }
  }
  return described;
}

std::string DescribeObject(duckdb::ClientContext& context, uint64_t classid,
                           uint64_t objid, int32_t objsubid) {
  auto identity =
    IdentifyObject(MakeSession(&context),
                   {.classid = classid, .objid = objid, .objsubid = objsubid});
  return identity && identity->description ? std::move(*identity->description)
                                           : std::string{};
}

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
  duckdb::ScalarFunction identify_object{
    {oid, oid, int4},
    duckdb::LogicalType::STRUCT(
      {{"type", text}, {"schema", text}, {"name", text}, {"identity", text}}),
    PgIdentifyObjectFunction};
  identify_object.GetSignature().SetRecordResult();
  RegisterScalar(loader, pg_catalog, "pg_identify_object",
                 CONSISTENT_WITHIN_QUERY, InitSession, {identify_object});
}

}  // namespace sdb::pg
