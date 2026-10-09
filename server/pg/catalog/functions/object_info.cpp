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

#include "pg/catalog/functions/object_info.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_split.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/common/vector_operations/unary_executor.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/function/scalar_function.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
#include <duckdb/parser/qualified_name.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/storage/data_table.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/search_table.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/functions/format_type.h"
#include "pg/catalog/functions/reg_types.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/oids.h"
#include "pg/connection_context.h"
#include "pg/progress_registry.h"
#include "pg/sql_utils.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

using duckdb::CatalogType;
using duckdb::Identifier;

using pg::EntryByOid;

duckdb::idx_t RoleByName(duckdb::ClientContext& context,
                         std::string_view name) {
  if (const auto role = pg::RoleOrPublic(context, name)) {
    return *role;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG("role \"", name, "\" does not exist"));
}

duckdb::idx_t CurrentRole(duckdb::ClientContext& context) {
  return RoleByName(context, GetSereneDBContext(context).EffectiveUserName());
}

std::string_view View(const duckdb::string_t& value) {
  return {value.GetData(), value.GetSize()};
}

void Register(duckdb::ExtensionLoader& loader, std::string_view schema,
              duckdb::ScalarFunctionSet set) {
  duckdb::CreateScalarFunctionInfo info{std::move(set)};
  info.SetSchema(Identifier{schema});
  info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  loader.RegisterFunction(std::move(info));
}

void RegisterPg(duckdb::ExtensionLoader& loader,
                duckdb::ScalarFunctionSet set) {
  Register(loader, irs::StaticStrings::kPgCatalogSchema, std::move(set));
}

duckdb::ScalarFunction WithinQuery(duckdb::ScalarFunction function) {
  function.SetStability(duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY);
  return function;
}

template<std::optional<bool> (*Visible)(const pg::Session&, uint64_t)>
void IsVisibleFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                       duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<int64_t, bool>(
    args.data[0], result, args.size(),
    [&](int64_t oid) -> duckdb::optional<bool> {
      if (oid <= 0) {
        return duckdb::nullopt;
      }
      return Visible(session, static_cast<uint64_t>(oid));
    });
}

void PgGetUserByIdFunction(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  const auto roles = auth::RolesOf(&state.GetContext());
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](int64_t oid) {
      const auto name = oid >= 0
                          ? roles->NameOf(static_cast<duckdb::idx_t>(oid))
                          : std::string_view{};
      if (!name.empty()) {
        return duckdb::StringVector::AddString(result, name);
      }
      return duckdb::StringVector::AddString(
        result, absl::StrCat("unknown (OID=", oid, ")"));
    });
}

duckdb::optional<duckdb::string_t> Comment(duckdb::Vector& result,
                                           const duckdb::Value& comment) {
  if (comment.IsNull()) {
    return duckdb::nullopt;
  }
  const auto& text = duckdb::StringValue::Get(comment);
  if (text.empty()) {
    return duckdb::nullopt;
  }
  return duckdb::StringVector::AddString(result, text);
}

void ObjDescription2Function(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<duckdb::string_t, int64_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t oid,
        duckdb::string_t catalog) -> duckdb::optional<duckdb::string_t> {
      const auto* table =
        pg::GetSystemTable(irs::StaticStrings::kPgCatalogSchema, View(catalog));
      auto entry = EntryByOid(session, oid);
      if (!table || !entry ||
          pg::CatalogClassOid(entry->type) != table->Sql().oid) {
        return duckdb::nullopt;
      }
      return Comment(result, entry->comment);
    });
}

void ObjDescription1Function(duckdb::DataChunk& args,
                             duckdb::ExpressionState& state,
                             duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<int64_t, duckdb::string_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) -> duckdb::optional<duckdb::string_t> {
      auto entry = EntryByOid(session, oid);
      if (!entry || pg::CatalogClassOid(entry->type) == pg::kInvalidOid) {
        return duckdb::nullopt;
      }
      return Comment(result, entry->comment);
    });
}

void ColDescriptionFunction(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<duckdb::string_t, int64_t, int32_t>(
    args, result,
    [&](int64_t oid, int32_t column) -> duckdb::optional<duckdb::string_t> {
      auto entry = EntryByOid(session, oid);
      if (!entry || column <= 0) {
        return duckdb::nullopt;
      }
      const auto index = static_cast<duckdb::idx_t>(column - 1);
      if (entry->type == CatalogType::TABLE_ENTRY) {
        const auto& columns =
          entry->Cast<duckdb::TableCatalogEntry>().GetColumns();
        if (index >= columns.LogicalColumnCount()) {
          return duckdb::nullopt;
        }
        return Comment(
          result, columns.GetColumn(duckdb::LogicalIndex(index)).Comment());
      }
      if (entry->type == CatalogType::VIEW_ENTRY) {
        auto& view = entry->Cast<duckdb::ViewCatalogEntry>();
        const auto info = view.GetColumnInfo();
        if (!info || index >= info->names.size()) {
          return duckdb::nullopt;
        }
        return Comment(result, view.GetColumnComment(index));
      }
      return duckdb::nullopt;
    });
}

template<typename T>
void TrueTypeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                      duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<T, T, duckdb::string_t, T>(
    args, result, [](T value, duckdb::string_t typtype, T base) {
      return typtype.GetSize() == 1 && typtype.GetData()[0] == 'd' ? base
                                                                   : value;
    });
}

using TypmodFn = std::optional<int32_t> (*)(int64_t, int32_t);

template<TypmodFn Fn>
void TypmodFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                    duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<int32_t, int64_t, int32_t>(
    args, result,
    [](int64_t typid, int32_t typmod) { return Fn(typid, typmod); });
}

void IntervalTypeFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                          duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<duckdb::string_t, int64_t, int32_t>(
    args, result,
    [&](int64_t typid, int32_t typmod) -> duckdb::optional<duckdb::string_t> {
      const auto type = pg::IntervalType(typid, typmod);
      if (!type) {
        return duckdb::nullopt;
      }
      return duckdb::StringVector::AddString(result, *type);
    });
}

std::vector<int16_t> IndexKey(const pg::Session& session, int64_t oid) {
  if (oid <= 0) {
    return {};
  }
  if (auto entry = EntryByOid(session, oid)) {
    if (entry->type != CatalogType::INDEX_ENTRY) {
      return {};
    }
    const auto& index = entry->Cast<duckdb::IndexCatalogEntry>();
    auto relation = index.GetRelation(*session.transaction);
    if (!relation) {
      return {};
    }
    return pg::IndexAttnums(*session.context, index, *relation);
  }
  if (const auto key = pg::FindKeyIndex(session, static_cast<uint64_t>(oid));
      key.table) {
    const auto& columns = key.table->GetColumns();
    return pg::Attnums(columns, key.key_index->GetLogicalIndexes(columns));
  }
  return {};
}

void IndexPositionFunction(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<int32_t, int64_t, int16_t>(
    args, result,
    [&](int64_t oid, int16_t column) -> duckdb::optional<int32_t> {
      const auto key = IndexKey(session, oid);
      const auto it = absl::c_find(key, column);
      if (it == key.end()) {
        return duckdb::nullopt;
      }
      return static_cast<int32_t>(it - key.begin()) + 1;
    });
}

void PgGetSerialSequenceFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  auto& context = state.GetContext();
  duckdb::VariadicExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t table_name,
        duckdb::string_t column_name) -> duckdb::optional<duckdb::string_t> {
      const auto name = duckdb::QualifiedName::Parse(table_name.GetString());
      const auto table = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
        context, name, duckdb::OnEntryNotFound::RETURN_NULL);
      if (!table) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_TABLE),
          ERR_MSG("relation \"", View(table_name), "\" does not exist"));
      }
      const Identifier column_key{View(column_name)};
      const auto& columns = table->GetColumns();
      if (!columns.ColumnExists(column_key)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
          ERR_MSG("column \"", View(column_name), "\" of relation \"",
                  table->name.GetIdentifierName(), "\" does not exist"));
      }
      const auto& column = columns.GetColumn(column_key);
      if (!column.HasDefaultValue()) {
        return duckdb::nullopt;
      }
      const auto argument = pg::NextvalArgument(column.DefaultValue());
      auto database = pg::SessionCatalog(context);
      if (!argument || !database) {
        return duckdb::nullopt;
      }
      const auto sequence_name = duckdb::QualifiedName::Parse(*argument);
      const auto transaction = database->GetCatalogTransaction(context);
      const auto schema_name = sequence_name.Schema().empty()
                                 ? table->ParentSchemaName()
                                 : sequence_name.Schema();
      auto schema = database->GetSchema(transaction, schema_name,
                                        duckdb::OnEntryNotFound::RETURN_NULL);
      if (!schema) {
        return duckdb::nullopt;
      }
      auto sequence = schema->LookupEntry(
        transaction, duckdb::EntryLookupInfo{CatalogType::SEQUENCE_ENTRY,
                                             sequence_name.Name()});
      if (!sequence) {
        return duckdb::nullopt;
      }
      return duckdb::StringVector::AddString(
        result, pg::QualifiedOutName(schema->name.GetIdentifierName(),
                                     sequence->name.GetIdentifierName()));
    });
}

constexpr int32_t kAllUpdateEvents = (1 << 2) | (1 << 3) | (1 << 4);

int32_t UpdatableEvents(const pg::Session& session, int64_t oid) {
  auto entry = EntryByOid(session, oid);
  return entry && entry->type == CatalogType::TABLE_ENTRY ? kAllUpdateEvents
                                                          : 0;
}

void PgRelationIsUpdatableFunction(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<int32_t, int64_t, bool>(
    args, result,
    [&](int64_t oid, bool) { return UpdatableEvents(session, oid); });
}

void PgColumnIsUpdatableFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, int16_t, bool>(
    args, result, [&](int64_t oid, int16_t attnum, bool) {
      return attnum > 0 && UpdatableEvents(session, oid) == kAllUpdateEvents;
    });
}

void PgSequenceLastValueFunction(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) -> duckdb::optional<int64_t> {
      auto entry = EntryByOid(session, oid);
      if (!entry) {
        return duckdb::nullopt;
      }
      if (entry->type != CatalogType::SEQUENCE_ENTRY) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                        ERR_MSG("\"", entry->name.GetIdentifierName(),
                                "\" is not a sequence"));
      }
      const auto data = entry->Cast<duckdb::SequenceCatalogEntry>().GetData();
      if (!data.last_value) {
        return duckdb::nullopt;
      }
      return *data.last_value;
    });
}

void ClockTimestampFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                            duckdb::Vector& result) {
  const duckdb::timestamp_tz_t now{duckdb::Timestamp::GetCurrentTimestamp()};
  result.Reference(duckdb::Value::TIMESTAMPTZ(now),
                   duckdb::count_t(args.size()));
}

constexpr std::string_view kEncodings[] = {
  "SQL_ASCII",    "EUC_JP",         "EUC_CN",        "EUC_KR",     "EUC_TW",
  "EUC_JIS_2004", "UTF8",           "MULE_INTERNAL", "LATIN1",     "LATIN2",
  "LATIN3",       "LATIN4",         "LATIN5",        "LATIN6",     "LATIN7",
  "LATIN8",       "LATIN9",         "LATIN10",       "WIN1256",    "WIN1258",
  "WIN866",       "WIN874",         "KOI8R",         "WIN1251",    "WIN1252",
  "ISO_8859_5",   "ISO_8859_6",     "ISO_8859_7",    "ISO_8859_8", "WIN1250",
  "WIN1253",      "WIN1254",        "WIN1255",       "WIN1257",    "KOI8U",
  "SJIS",         "BIG5",           "GBK",           "UHC",        "GB18030",
  "JOHAB",        "SHIFT_JIS_2004",
};

struct EncodingAlias {
  std::string_view alias;
  int32_t encoding;
};

constexpr EncodingAlias kEncodingAliases[] = {
  {"abc", 19},         {"alt", 20},         {"big5", 36},
  {"euccn", 2},        {"eucjis2004", 5},   {"eucjp", 1},
  {"euckr", 3},        {"euctw", 4},        {"gb18030", 39},
  {"gbk", 37},         {"iso88591", 8},     {"iso885910", 13},
  {"iso885913", 14},   {"iso885914", 15},   {"iso885915", 16},
  {"iso885916", 17},   {"iso88592", 9},     {"iso88593", 10},
  {"iso88594", 11},    {"iso88595", 25},    {"iso88596", 26},
  {"iso88597", 27},    {"iso88598", 28},    {"iso88599", 12},
  {"johab", 40},       {"koi8", 22},        {"koi8r", 22},
  {"koi8u", 34},       {"latin1", 8},       {"latin10", 17},
  {"latin2", 9},       {"latin3", 10},      {"latin4", 11},
  {"latin5", 12},      {"latin6", 13},      {"latin7", 14},
  {"latin8", 15},      {"latin9", 16},      {"mskanji", 35},
  {"muleinternal", 7}, {"shiftjis", 35},    {"shiftjis2004", 41},
  {"sjis", 35},        {"sqlascii", 0},     {"tcvn", 19},
  {"tcvn5712", 19},    {"uhc", 38},         {"unicode", 6},
  {"utf8", 6},         {"vscii", 19},       {"win", 23},
  {"win1250", 29},     {"win1251", 23},     {"win1252", 24},
  {"win1253", 30},     {"win1254", 31},     {"win1255", 32},
  {"win1256", 18},     {"win1257", 33},     {"win1258", 19},
  {"win866", 20},      {"win874", 21},      {"win932", 35},
  {"win936", 37},      {"win949", 38},      {"win950", 36},
  {"windows1250", 29}, {"windows1251", 23}, {"windows1252", 24},
  {"windows1253", 30}, {"windows1254", 31}, {"windows1255", 32},
  {"windows1256", 18}, {"windows1257", 33}, {"windows1258", 19},
  {"windows866", 20},  {"windows874", 21},  {"windows932", 35},
  {"windows936", 37},  {"windows949", 38},  {"windows950", 36},
};

void PgEncodingToCharFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<int32_t, duckdb::string_t>(
    args.data[0], result, args.size(), [&](int32_t encoding) {
      if (encoding < 0 ||
          static_cast<size_t>(encoding) >= std::size(kEncodings)) {
        return duckdb::StringVector::AddString(result, std::string_view{});
      }
      return duckdb::StringVector::AddString(result, kEncodings[encoding]);
    });
}

void PgCharToEncodingFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                              duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int32_t>(
    args.data[0], result, args.size(), [](duckdb::string_t name) {
      std::string clean;
      for (const auto c : View(name)) {
        if (absl::ascii_isalnum(c)) {
          clean.push_back(absl::ascii_tolower(c));
        }
      }
      const auto it = absl::c_find_if(
        kEncodingAliases, [&](const auto& a) { return a.alias == clean; });
      return it == std::end(kEncodingAliases) ? -1 : it->encoding;
    });
}

enum class PrivilegeObject : uint8_t {
  ForeignDataWrapper,
  ForeignServer,
  Tablespace,
};

struct PrivilegeRequest {
  bool plain = false;
  bool grant = false;
};

PrivilegeRequest ParsePrivileges(PrivilegeObject object,
                                 std::string_view text) {
  const bool tablespace = object == PrivilegeObject::Tablespace;
  const std::string_view keyword = tablespace ? "CREATE" : "USAGE";
  const std::string_view with_grant =
    tablespace ? "CREATE WITH GRANT OPTION" : "USAGE WITH GRANT OPTION";
  PrivilegeRequest request;
  for (const std::string_view token : absl::StrSplit(text, ',')) {
    const auto stripped = absl::StripAsciiWhitespace(token);
    if (absl::EqualsIgnoreCase(stripped, keyword)) {
      request.plain = true;
    } else if (absl::EqualsIgnoreCase(stripped, with_grant)) {
      request.grant = true;
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("unrecognized privilege type: \"", stripped, "\""));
    }
  }
  return request;
}

constexpr duckdb::idx_t kPgDefaultTablespace = 1663;
constexpr duckdb::idx_t kPgGlobalTablespace = 1664;

duckdb::Permissions TablespacePermissions() {
  return duckdb::Permissions{.owner = pg::kRootUser};
}

std::optional<duckdb::Permissions> PrivilegeTargetByName(
  duckdb::ClientContext& context, PrivilegeObject object,
  std::string_view name) {
  if (object == PrivilegeObject::ForeignServer) {
    if (auto database = pg::SessionCatalog(context)) {
      if (auto entry =
            database->GetCatalogSet(CatalogType::FOREIGN_SERVER_ENTRY)
              .GetEntry(database->GetCatalogTransaction(context),
                        Identifier{name})) {
        return entry->permissions;
      }
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("server \"", name, "\" does not exist"));
  }
  if (object == PrivilegeObject::Tablespace) {
    if (name == "pg_default" || name == "pg_global") {
      return TablespacePermissions();
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("tablespace \"", name, "\" does not exist"));
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
    ERR_MSG("foreign-data wrapper \"", name, "\" does not exist"));
}

std::optional<duckdb::Permissions> PrivilegeTargetByOid(
  duckdb::ClientContext& context, PrivilegeObject object, int64_t oid) {
  if (object == PrivilegeObject::ForeignServer) {
    if (auto entry = EntryByOid(pg::MakeSession(&context), oid);
        entry && entry->type == CatalogType::FOREIGN_SERVER_ENTRY) {
      return entry->permissions;
    }
    return std::nullopt;
  }
  if (object == PrivilegeObject::Tablespace &&
      (oid == kPgDefaultTablespace || oid == kPgGlobalTablespace)) {
    return TablespacePermissions();
  }
  return std::nullopt;
}

duckdb::optional<bool> HasPrivilege(
  duckdb::ClientContext& context, PrivilegeObject object, duckdb::idx_t role,
  const std::optional<duckdb::Permissions>& target, std::string_view text) {
  const auto request = ParsePrivileges(object, text);
  const auto closure = auth::ClosureFor(&context, role);
  if (closure->is_superuser) {
    return true;
  }
  if (!target) {
    return duckdb::nullopt;
  }
  const auto mode = object == PrivilegeObject::Tablespace
                      ? duckdb::AclMode::Create
                      : duckdb::AclMode::Usage;
  if (request.plain) {
    const bool held =
      object == PrivilegeObject::ForeignServer
        ? closure->CanAny(CatalogType::FOREIGN_SERVER_ENTRY, *target, mode)
        : closure->Owns(target->owner);
    if (held) {
      return true;
    }
  }
  if (request.grant) {
    return closure->Owns(target->owner) ||
           (closure->GrantableModes(target->acl) & mode) !=
             duckdb::AclMode::NoRights;
  }
  return false;
}

template<PrivilegeObject Object>
void RegisterPrivilegeFunctions(duckdb::ExtensionLoader& loader,
                                std::string_view name) {
  const auto text = duckdb::LogicalType::VARCHAR;
  const auto oid = pg::OID();
  const auto boolean = duckdb::LogicalType::BOOLEAN;
  duckdb::ScalarFunctionSet set{Identifier{name}};
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {text, text, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      duckdb::VariadicExecutor::Execute<bool, duckdb::string_t,
                                        duckdb::string_t, duckdb::string_t>(
        args, result,
        [&](duckdb::string_t role, duckdb::string_t target,
            duckdb::string_t priv) {
          const auto role_id = RoleByName(context, View(role));
          return HasPrivilege(
            context, Object, role_id,
            PrivilegeTargetByName(context, Object, View(target)), View(priv));
        });
    }}));
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {text, oid, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, int64_t,
                                        duckdb::string_t>(
        args, result,
        [&](duckdb::string_t role, int64_t target, duckdb::string_t priv) {
          const auto role_id = RoleByName(context, View(role));
          return HasPrivilege(context, Object, role_id,
                              PrivilegeTargetByOid(context, Object, target),
                              View(priv));
        });
    }}));
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {oid, text, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t,
                                        duckdb::string_t>(
        args, result,
        [&](int64_t role, duckdb::string_t target, duckdb::string_t priv) {
          return HasPrivilege(
            context, Object, static_cast<duckdb::idx_t>(role),
            PrivilegeTargetByName(context, Object, View(target)), View(priv));
        });
    }}));
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {oid, oid, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      duckdb::VariadicExecutor::Execute<bool, int64_t, int64_t,
                                        duckdb::string_t>(
        args, result, [&](int64_t role, int64_t target, duckdb::string_t priv) {
          return HasPrivilege(context, Object, static_cast<duckdb::idx_t>(role),
                              PrivilegeTargetByOid(context, Object, target),
                              View(priv));
        });
    }}));
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {text, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      const auto role_id = CurrentRole(context);
      duckdb::VariadicExecutor::Execute<bool, duckdb::string_t,
                                        duckdb::string_t>(
        args, result, [&](duckdb::string_t target, duckdb::string_t priv) {
          return HasPrivilege(
            context, Object, role_id,
            PrivilegeTargetByName(context, Object, View(target)), View(priv));
        });
    }}));
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {oid, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      const auto role_id = CurrentRole(context);
      duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
        args, result, [&](int64_t target, duckdb::string_t priv) {
          return HasPrivilege(context, Object, role_id,
                              PrivilegeTargetByOid(context, Object, target),
                              View(priv));
        });
    }}));
  RegisterPg(loader, std::move(set));
}

struct DefaultAcl {
  char objtype;
  duckdb::AclMode world;
  duckdb::AclMode owner;
};

constexpr DefaultAcl kDefaultAcls[] = {
  {'c', duckdb::AclMode::NoRights, duckdb::AclMode::NoRights},
  {'r', duckdb::AclMode::NoRights,
   duckdb::AclMode::Insert | duckdb::AclMode::Select | duckdb::AclMode::Update |
     duckdb::AclMode::Delete | duckdb::AclMode::Truncate |
     duckdb::AclMode::References | duckdb::AclMode::Trigger |
     duckdb::AclMode::Maintain},
  {'s', duckdb::AclMode::NoRights,
   duckdb::AclMode::Select | duckdb::AclMode::Update | duckdb::AclMode::Usage},
  {'d', duckdb::AclMode::CreateTemp | duckdb::AclMode::Connect,
   duckdb::AclMode::Create | duckdb::AclMode::CreateTemp |
     duckdb::AclMode::Connect},
  {'f', duckdb::AclMode::Execute, duckdb::AclMode::Execute},
  {'l', duckdb::AclMode::Usage, duckdb::AclMode::Usage},
  {'L', duckdb::AclMode::NoRights,
   duckdb::AclMode::Select | duckdb::AclMode::Update},
  {'n', duckdb::AclMode::NoRights,
   duckdb::AclMode::Usage | duckdb::AclMode::Create},
  {'t', duckdb::AclMode::NoRights, duckdb::AclMode::Create},
  {'F', duckdb::AclMode::NoRights, duckdb::AclMode::Usage},
  {'S', duckdb::AclMode::NoRights, duckdb::AclMode::Usage},
  {'T', duckdb::AclMode::Usage, duckdb::AclMode::Usage},
  {'p', duckdb::AclMode::NoRights,
   duckdb::AclMode::Set | duckdb::AclMode::AlterSystem},
};

void AclDefaultFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  const auto roles = auth::RolesOf(&state.GetContext());
  const auto count = args.size();
  const auto objtypes = args.data[0].Values<duckdb::string_t>();
  const auto owners = args.data[1].Values<int64_t>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::VectorListType<duckdb::string_t>>(result,
                                                                         count);
  std::vector<std::string> items;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto objtype = objtypes[row];
    const auto owner = owners[row];
    if (!objtype.IsValid() || !owner.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto type = View(objtype.GetValue());
    const auto it = absl::c_find_if(kDefaultAcls, [&](const DefaultAcl& acl) {
      return type.size() == 1 && type[0] == acl.objtype;
    });
    if (it == std::end(kDefaultAcls)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unrecognized object type abbreviation: ", type));
    }
    const auto owner_id = static_cast<duckdb::idx_t>(owner.GetValue());
    items.clear();
    if (it->world != duckdb::AclMode::NoRights) {
      pg::AppendAcl(items.emplace_back(),
                    duckdb::AclItem{.grantee = pg::kPublicGrantee,
                                    .grantor = owner_id,
                                    .privs = it->world},
                    *roles);
    }
    if (it->owner != duckdb::AclMode::NoRights) {
      pg::AppendAcl(
        items.emplace_back(),
        duckdb::AclItem{
          .grantee = owner_id, .grantor = owner_id, .privs = it->owner},
        *roles);
    }
    size_t i = 0;
    for (auto& child : writer.WriteList(items.size())) {
      child.WriteValue(duckdb::string_t{
        items[i].data(), static_cast<uint32_t>(items[i].size())});
      ++i;
    }
  }
}

struct PrivilegeName {
  duckdb::AclMode mode;
  std::string_view name;
};

constexpr PrivilegeName kPrivilegeNames[] = {
  {duckdb::AclMode::Insert, "INSERT"},
  {duckdb::AclMode::Select, "SELECT"},
  {duckdb::AclMode::Update, "UPDATE"},
  {duckdb::AclMode::Delete, "DELETE"},
  {duckdb::AclMode::Truncate, "TRUNCATE"},
  {duckdb::AclMode::References, "REFERENCES"},
  {duckdb::AclMode::Trigger, "TRIGGER"},
  {duckdb::AclMode::Execute, "EXECUTE"},
  {duckdb::AclMode::Usage, "USAGE"},
  {duckdb::AclMode::Create, "CREATE"},
  {duckdb::AclMode::CreateTemp, "TEMPORARY"},
  {duckdb::AclMode::Connect, "CONNECT"},
  {duckdb::AclMode::Set, "SET"},
  {duckdb::AclMode::AlterSystem, "ALTER SYSTEM"},
  {duckdb::AclMode::Maintain, "MAINTAIN"},
};

struct ExplodedAcl {
  int64_t grantor;
  int64_t grantee;
  std::string_view privilege;
  bool grantable;
};

class AclReader {
 public:
  explicit AclReader(std::string_view text) : _text{text} {}

  std::string_view Role() {
    if (!_text.empty() && _text.front() == '"') {
      _buffer.clear();
      size_t pos = 1;
      while (pos < _text.size()) {
        if (_text[pos] == '"') {
          if (pos + 1 < _text.size() && _text[pos + 1] == '"') {
            _buffer.push_back('"');
            pos += 2;
            continue;
          }
          ++pos;
          break;
        }
        _buffer.push_back(_text[pos]);
        ++pos;
      }
      _text.remove_prefix(pos);
      return _buffer;
    }
    const auto end = _text.find_first_of("=/");
    const auto role = _text.substr(0, end);
    _text.remove_prefix(role.size());
    return role;
  }

  bool Skip(char c) {
    if (_text.empty() || _text.front() != c) {
      return false;
    }
    _text.remove_prefix(1);
    return true;
  }

  std::string_view Privileges() {
    const auto privileges = _text.substr(0, _text.find('/'));
    _text.remove_prefix(privileges.size());
    return privileges;
  }

 private:
  std::string_view _text;
  std::string _buffer;
};

int64_t RoleIdOf(duckdb::ClientContext& context, std::string_view name) {
  if (name.empty()) {
    return static_cast<int64_t>(pg::kPublicGrantee);
  }
  if (const auto* role = pg::FindRole(context, name)) {
    return static_cast<int64_t>(role->oid);
  }
  int64_t oid = 0;
  if (absl::SimpleAtoi(name, &oid)) {
    return oid;
  }
  return 0;
}

void ExplodeAcl(duckdb::ClientContext& context, std::string_view text,
                std::vector<ExplodedAcl>& rows) {
  AclReader reader{text};
  const auto grantee = RoleIdOf(context, reader.Role());
  if (!reader.Skip('=')) {
    return;
  }
  const auto privileges = reader.Privileges();
  int64_t grantor = 0;
  if (reader.Skip('/')) {
    grantor = RoleIdOf(context, reader.Role());
  }
  duckdb::AclMode held = duckdb::AclMode::NoRights;
  duckdb::AclMode grantable = duckdb::AclMode::NoRights;
  for (size_t i = 0; i < privileges.size(); ++i) {
    const auto it = absl::c_find_if(pg::kPrivChars, [&](const pg::PrivChar& p) {
      return p.chr == privileges[i];
    });
    if (it == pg::kPrivChars.end()) {
      continue;
    }
    held |= it->mode;
    if (i + 1 < privileges.size() && privileges[i + 1] == '*') {
      grantable |= it->mode;
      ++i;
    }
  }
  for (const auto& privilege : kPrivilegeNames) {
    if ((held & privilege.mode) != duckdb::AclMode::NoRights) {
      rows.emplace_back(ExplodedAcl{.grantor = grantor,
                                    .grantee = grantee,
                                    .privilege = privilege.name,
                                    .grantable = (grantable & privilege.mode) !=
                                                 duckdb::AclMode::NoRights});
    }
  }
}

struct AclExplodeState final : duckdb::LocalTableFunctionState {
  std::vector<ExplodedAcl> rows;
  size_t next = 0;
  bool loaded = false;
};

duckdb::unique_ptr<duckdb::FunctionData> AclExplodeBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<Identifier>& names) {
  names.emplace_back("grantor");
  types.emplace_back(pg::OID());
  names.emplace_back("grantee");
  types.emplace_back(pg::OID());
  names.emplace_back("privilege_type");
  types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("is_grantable");
  types.emplace_back(duckdb::LogicalType::BOOLEAN);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> AclExplodeInit(
  duckdb::ExecutionContext&, duckdb::TableFunctionInitInput&,
  duckdb::GlobalTableFunctionState*) {
  return duckdb::make_uniq<AclExplodeState>();
}

duckdb::OperatorResultType AclExplodeFunction(duckdb::ExecutionContext& context,
                                              duckdb::TableFunctionInput& data,
                                              duckdb::DataChunk& input,
                                              duckdb::DataChunk& output) {
  auto& state = data.local_state->Cast<AclExplodeState>();
  if (!state.loaded) {
    state.rows.clear();
    state.next = 0;
    state.loaded = true;
    const auto lists =
      input.data[0].Values<duckdb::VectorListType<duckdb::string_t>>();
    for (duckdb::idx_t row = 0; row < input.size(); ++row) {
      const auto list = lists[row];
      if (!list.IsValid()) {
        continue;
      }
      for (const auto item : list.GetChildValues()) {
        if (item.IsValid()) {
          ExplodeAcl(context.client, View(item.GetValue()), state.rows);
        }
      }
    }
  }
  if (state.next == state.rows.size()) {
    state.loaded = false;
    output.SetCardinality(0);
    return duckdb::OperatorResultType::NEED_MORE_INPUT;
  }
  const auto count =
    std::min<size_t>(state.rows.size() - state.next, STANDARD_VECTOR_SIZE);
  auto grantors = duckdb::FlatVector::Writer<int64_t>(output.data[0], count);
  auto grantees = duckdb::FlatVector::Writer<int64_t>(output.data[1], count);
  auto privileges =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[2], count);
  auto grantables = duckdb::FlatVector::Writer<bool>(output.data[3], count);
  for (size_t i = 0; i < count; ++i) {
    const auto& row = state.rows[state.next + i];
    grantors.WriteValue(row.grantor);
    grantees.WriteValue(row.grantee);
    privileges.WriteValue(duckdb::string_t{
      row.privilege.data(), static_cast<uint32_t>(row.privilege.size())});
    grantables.WriteValue(row.grantable);
  }
  state.next += count;
  output.SetCardinality(count);
  return duckdb::OperatorResultType::HAVE_MORE_OUTPUT;
}

duckdb::unique_ptr<duckdb::Expression> BindZero(
  duckdb::FunctionBindExpressionInput& input) {
  const auto& type = input.bound_function.GetReturnType();
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value::INTEGER(0).DefaultCastAs(type));
}

duckdb::unique_ptr<duckdb::Expression> BindNull(
  duckdb::FunctionBindExpressionInput& input) {
  return duckdb::make_uniq<duckdb::BoundConstantExpression>(
    duckdb::Value{input.bound_function.GetReturnType()});
}

template<typename T>
void ZeroFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                  duckdb::Vector& result) {
  duckdb::UnaryExecutor::Execute<int64_t, T>(args.data[0], result, args.size(),
                                             [](int64_t) { return T{0}; });
}

enum class StatKind : uint8_t {
  Count,
  Time,
  Timestamp,
  Untracked,
};

struct StatFunction {
  std::string_view name;
  bool takes_oid;
  StatKind kind;
};

constexpr StatFunction kStatFunctions[] = {
  {"pg_stat_get_analyze_count", true, StatKind::Count},
  {"pg_stat_get_autoanalyze_count", true, StatKind::Count},
  {"pg_stat_get_autovacuum_count", true, StatKind::Count},
  {"pg_stat_get_bgwriter_buf_written_clean", false, StatKind::Count},
  {"pg_stat_get_bgwriter_maxwritten_clean", false, StatKind::Count},
  {"pg_stat_get_bgwriter_stat_reset_time", false, StatKind::Timestamp},
  {"pg_stat_get_blocks_fetched", true, StatKind::Count},
  {"pg_stat_get_blocks_hit", true, StatKind::Count},
  {"pg_stat_get_buf_alloc", false, StatKind::Count},
  {"pg_stat_get_checkpointer_buffers_written", false, StatKind::Count},
  {"pg_stat_get_checkpointer_num_performed", false, StatKind::Count},
  {"pg_stat_get_checkpointer_num_requested", false, StatKind::Count},
  {"pg_stat_get_checkpointer_num_timed", false, StatKind::Count},
  {"pg_stat_get_checkpointer_restartpoints_performed", false, StatKind::Count},
  {"pg_stat_get_checkpointer_restartpoints_requested", false, StatKind::Count},
  {"pg_stat_get_checkpointer_restartpoints_timed", false, StatKind::Count},
  {"pg_stat_get_checkpointer_slru_written", false, StatKind::Count},
  {"pg_stat_get_checkpointer_stat_reset_time", false, StatKind::Timestamp},
  {"pg_stat_get_checkpointer_sync_time", false, StatKind::Time},
  {"pg_stat_get_checkpointer_write_time", false, StatKind::Time},
  {"pg_stat_get_db_active_time", true, StatKind::Time},
  {"pg_stat_get_db_blk_read_time", true, StatKind::Time},
  {"pg_stat_get_db_blk_write_time", true, StatKind::Time},
  {"pg_stat_get_db_blocks_fetched", true, StatKind::Count},
  {"pg_stat_get_db_blocks_hit", true, StatKind::Count},
  {"pg_stat_get_db_checksum_failures", true, StatKind::Count},
  {"pg_stat_get_db_checksum_last_failure", true, StatKind::Timestamp},
  {"pg_stat_get_db_conflict_all", true, StatKind::Count},
  {"pg_stat_get_db_conflict_bufferpin", true, StatKind::Count},
  {"pg_stat_get_db_conflict_lock", true, StatKind::Count},
  {"pg_stat_get_db_conflict_logicalslot", true, StatKind::Count},
  {"pg_stat_get_db_conflict_snapshot", true, StatKind::Count},
  {"pg_stat_get_db_conflict_startup_deadlock", true, StatKind::Count},
  {"pg_stat_get_db_conflict_tablespace", true, StatKind::Count},
  {"pg_stat_get_db_deadlocks", true, StatKind::Count},
  {"pg_stat_get_db_idle_in_transaction_time", true, StatKind::Time},
  {"pg_stat_get_db_parallel_workers_launched", true, StatKind::Count},
  {"pg_stat_get_db_parallel_workers_to_launch", true, StatKind::Count},
  {"pg_stat_get_db_session_time", true, StatKind::Time},
  {"pg_stat_get_db_sessions", true, StatKind::Count},
  {"pg_stat_get_db_sessions_abandoned", true, StatKind::Count},
  {"pg_stat_get_db_sessions_fatal", true, StatKind::Count},
  {"pg_stat_get_db_sessions_killed", true, StatKind::Count},
  {"pg_stat_get_db_stat_reset_time", true, StatKind::Timestamp},
  {"pg_stat_get_db_temp_bytes", true, StatKind::Count},
  {"pg_stat_get_db_temp_files", true, StatKind::Count},
  {"pg_stat_get_db_tuples_deleted", true, StatKind::Count},
  {"pg_stat_get_db_tuples_fetched", true, StatKind::Count},
  {"pg_stat_get_db_tuples_inserted", true, StatKind::Count},
  {"pg_stat_get_db_tuples_returned", true, StatKind::Count},
  {"pg_stat_get_db_tuples_updated", true, StatKind::Count},
  {"pg_stat_get_db_xact_commit", true, StatKind::Count},
  {"pg_stat_get_db_xact_rollback", true, StatKind::Count},
  {"pg_stat_get_dead_tuples", true, StatKind::Count},
  {"pg_stat_get_function_calls", true, StatKind::Untracked},
  {"pg_stat_get_function_self_time", true, StatKind::Untracked},
  {"pg_stat_get_function_total_time", true, StatKind::Untracked},
  {"pg_stat_get_ins_since_vacuum", true, StatKind::Count},
  {"pg_stat_get_last_analyze_time", true, StatKind::Timestamp},
  {"pg_stat_get_last_autoanalyze_time", true, StatKind::Timestamp},
  {"pg_stat_get_last_autovacuum_time", true, StatKind::Timestamp},
  {"pg_stat_get_last_vacuum_time", true, StatKind::Timestamp},
  {"pg_stat_get_lastscan", true, StatKind::Timestamp},
  {"pg_stat_get_mod_since_analyze", true, StatKind::Count},
  {"pg_stat_get_numscans", true, StatKind::Count},
  {"pg_stat_get_total_analyze_time", true, StatKind::Time},
  {"pg_stat_get_total_autoanalyze_time", true, StatKind::Time},
  {"pg_stat_get_total_autovacuum_time", true, StatKind::Time},
  {"pg_stat_get_total_vacuum_time", true, StatKind::Time},
  {"pg_stat_get_tuples_deleted", true, StatKind::Count},
  {"pg_stat_get_tuples_fetched", true, StatKind::Count},
  {"pg_stat_get_tuples_hot_updated", true, StatKind::Count},
  {"pg_stat_get_tuples_inserted", true, StatKind::Count},
  {"pg_stat_get_tuples_newpage_updated", true, StatKind::Count},
  {"pg_stat_get_tuples_returned", true, StatKind::Count},
  {"pg_stat_get_tuples_updated", true, StatKind::Count},
  {"pg_stat_get_vacuum_count", true, StatKind::Count},
  {"pg_stat_get_xact_function_calls", true, StatKind::Untracked},
  {"pg_stat_get_xact_function_self_time", true, StatKind::Untracked},
  {"pg_stat_get_xact_function_total_time", true, StatKind::Untracked},
  {"pg_stat_get_xact_numscans", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_deleted", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_fetched", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_hot_updated", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_inserted", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_newpage_updated", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_returned", true, StatKind::Count},
  {"pg_stat_get_xact_tuples_updated", true, StatKind::Count},
};

duckdb::LogicalType StatType(const StatFunction& stat) {
  if (stat.kind == StatKind::Count) {
    return duckdb::LogicalType::BIGINT;
  }
  if (stat.kind == StatKind::Time) {
    return duckdb::LogicalType::DOUBLE;
  }
  if (stat.kind == StatKind::Timestamp) {
    return duckdb::LogicalType::TIMESTAMP_TZ;
  }
  return absl::EndsWith(stat.name, "_calls") ? duckdb::LogicalType::BIGINT
                                             : duckdb::LogicalType::DOUBLE;
}

void RegisterStatFunctions(duckdb::ExtensionLoader& loader) {
  for (const auto& stat : kStatFunctions) {
    const bool zero =
      stat.kind == StatKind::Count || stat.kind == StatKind::Time;
    const bool per_row = zero && stat.takes_oid;
    duckdb::ScalarFunction function{
      stat.takes_oid ? duckdb::vector<duckdb::LogicalType>{pg::OID()}
                     : duckdb::vector<duckdb::LogicalType>{},
      StatType(stat),
      per_row ? (stat.kind == StatKind::Count ? ZeroFunction<int64_t>
                                              : ZeroFunction<double>)
              : nullptr};
    if (!per_row) {
      function.SetBindExpressionCallback(zero ? BindZero : BindNull);
    }
    duckdb::ScalarFunctionSet set{Identifier{stat.name}};
    set.AddFunction(std::move(function));
    RegisterPg(loader, std::move(set));
  }
}

int64_t LiveTuples(const pg::Session& session, int64_t oid) {
  auto entry = EntryByOid(session, oid);
  if (!entry || entry->type != CatalogType::TABLE_ENTRY) {
    return 0;
  }
  if (const auto* search =
        dynamic_cast<const catalog::SearchTableEntry*>(&*entry)) {
    return static_cast<int64_t>(search->Storage()->GetStats().numLiveDocs);
  }
  if (auto* duck = dynamic_cast<duckdb::DuckTableEntry*>(&*entry)) {
    return static_cast<int64_t>(duck->GetStorage().GetTotalRows());
  }
  return 0;
}

void LiveTuplesFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<int64_t, int64_t>(
    args.data[0], result, args.size(),
    [&](int64_t oid) { return LiveTuples(session, oid); });
}

void NumBackendsFunction(duckdb::DataChunk& args, duckdb::ExpressionState&,
                         duckdb::Vector& result) {
  const auto sessions = pg::ProgressRegistry::Instance().GetSnapshots();
  duckdb::UnaryExecutor::Execute<int64_t, int32_t>(
    args.data[0], result, args.size(), [&](int64_t oid) {
      return static_cast<int32_t>(absl::c_count_if(
        sessions, [&](const auto& session) { return session.datid == oid; }));
    });
}

void RegisterScalar(duckdb::ExtensionLoader& loader, std::string_view schema,
                    std::string_view name,
                    std::initializer_list<duckdb::ScalarFunction> functions) {
  duckdb::ScalarFunctionSet set{Identifier{name}};
  for (const auto& function : functions) {
    set.AddFunction(function);
  }
  Register(loader, schema, std::move(set));
}

template<pg::RegKind Kind>
void ToRegFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                   duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int64_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t text) -> duckdb::optional<int64_t> {
      const auto oid = pg::RegIn<Kind>(session, View(text), true);
      if (!oid) {
        return duckdb::nullopt;
      }
      return static_cast<int64_t>(*oid);
    });
}

void ToRegtypmodFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::UnaryExecutor::Execute<duckdb::string_t, int32_t>(
    args.data[0], result, args.size(),
    [&](duckdb::string_t text) -> duckdb::optional<int32_t> {
      const auto typmod = pg::RegTypmodIn(session, View(text));
      if (!typmod) {
        return duckdb::nullopt;
      }
      return *typmod;
    });
}

void RegisterToRegFunction(duckdb::ExtensionLoader& loader,
                           std::string_view name, duckdb::LogicalType type,
                           duckdb::scalar_function_t function) {
  duckdb::ScalarFunction scalar{
    {duckdb::LogicalType::VARCHAR}, std::move(type), std::move(function)};
  scalar.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
  RegisterScalar(loader, irs::StaticStrings::kPgCatalogSchema, name, {scalar});
}

template<size_t I>
void RegisterToReg(duckdb::ExtensionLoader& loader) {
  constexpr const auto& kReg = pg::kRegTypes[I];
  if constexpr (!kReg.to_reg.empty()) {
    RegisterToRegFunction(loader, kReg.to_reg, pg::RegLogicalType(kReg),
                          ToRegFunction<kReg.kind>);
  }
}

}  // namespace

void RegisterPgHelperFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  const auto oid = pg::OID();
  const auto regclass = pg::REGCLASS();
  const auto text = duckdb::LogicalType::VARCHAR;
  const auto boolean = duckdb::LogicalType::BOOLEAN;
  const auto int2 = duckdb::LogicalType::SMALLINT;
  const auto int4 = duckdb::LogicalType::INTEGER;
  const auto int8 = duckdb::LogicalType::BIGINT;
  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;
  const std::string_view information_schema =
    irs::StaticStrings::kInformationSchema;

  RegisterScalar(loader, pg_catalog, "pg_table_is_visible",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid}, boolean, IsVisibleFunction<pg::RelationIsVisible>})});
  RegisterScalar(loader, pg_catalog, "pg_type_is_visible",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid}, boolean, IsVisibleFunction<pg::TypeIsVisible>})});
  RegisterScalar(loader, pg_catalog, "pg_function_is_visible",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid}, boolean, IsVisibleFunction<pg::FunctionIsVisible>})});
  RegisterScalar(loader, pg_catalog, "pg_get_userbyid",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid}, pg::NAME(), PgGetUserByIdFunction})});
  RegisterScalar(loader, pg_catalog, "obj_description",
                 {WithinQuery(duckdb::ScalarFunction{
                    {oid, text}, text, ObjDescription2Function}),
                  WithinQuery(duckdb::ScalarFunction{
                    {oid}, text, ObjDescription1Function})});
  RegisterScalar(loader, pg_catalog, "col_description",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid, int4}, text, ColDescriptionFunction})});
  RegisterScalar(loader, pg_catalog, "pg_get_serial_sequence",
                 {WithinQuery(duckdb::ScalarFunction{
                   {text, text}, text, PgGetSerialSequenceFunction})});
  RegisterScalar(loader, pg_catalog, "pg_relation_is_updatable",
                 {WithinQuery(duckdb::ScalarFunction{
                   {regclass, boolean}, int4, PgRelationIsUpdatableFunction})});
  RegisterScalar(
    loader, pg_catalog, "pg_column_is_updatable",
    {WithinQuery(duckdb::ScalarFunction{
      {regclass, int2, boolean}, boolean, PgColumnIsUpdatableFunction})});
  {
    duckdb::ScalarFunction function{
      {regclass}, int8, PgSequenceLastValueFunction};
    function.SetVolatile();
    RegisterScalar(loader, pg_catalog, "pg_sequence_last_value", {function});
  }
  {
    duckdb::ScalarFunction function{
      {}, duckdb::LogicalType::TIMESTAMP_TZ, ClockTimestampFunction};
    function.SetVolatile();
    RegisterScalar(loader, pg_catalog, "clock_timestamp", {function});
  }
  RegisterScalar(
    loader, pg_catalog, "pg_encoding_to_char",
    {duckdb::ScalarFunction{{int4}, pg::NAME(), PgEncodingToCharFunction}});
  RegisterScalar(
    loader, pg_catalog, "pg_char_to_encoding",
    {duckdb::ScalarFunction{{text}, int4, PgCharToEncodingFunction}});
  RegisterScalar(loader, pg_catalog, "acldefault",
                 {WithinQuery(duckdb::ScalarFunction{
                   {text, oid},
                   duckdb::LogicalType::LIST(pg::ACLITEM()),
                   AclDefaultFunction})});
  RegisterPrivilegeFunctions<PrivilegeObject::ForeignDataWrapper>(
    loader, "has_foreign_data_wrapper_privilege");
  RegisterPrivilegeFunctions<PrivilegeObject::ForeignServer>(
    loader, "has_server_privilege");
  RegisterPrivilegeFunctions<PrivilegeObject::Tablespace>(
    loader, "has_tablespace_privilege");
  [&]<size_t... I>(std::index_sequence<I...>) {
    (RegisterToReg<I>(loader), ...);
  }(std::make_index_sequence<pg::kRegTypes.size()>{});
  RegisterToRegFunction(loader, "to_regtypemod", int4, ToRegtypmodFunction);
  RegisterStatFunctions(loader);
  RegisterScalar(
    loader, pg_catalog, "pg_stat_get_live_tuples",
    {WithinQuery(duckdb::ScalarFunction{{oid}, int8, LiveTuplesFunction})});
  {
    duckdb::ScalarFunction function{{oid}, int4, NumBackendsFunction};
    function.SetVolatile();
    RegisterScalar(loader, pg_catalog, "pg_stat_get_db_numbackends",
                   {function});
  }
  {
    duckdb::TableFunction function{
      "aclexplode", {duckdb::LogicalType::LIST(pg::ACLITEM())},
      nullptr,      AclExplodeBind,
      nullptr,      AclExplodeInit};
    function.in_out_function = AclExplodeFunction;
    duckdb::CreateTableFunctionInfo info{std::move(function)};
    info.SetSchema(Identifier{pg_catalog});
    info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
    loader.RegisterFunction(std::move(info));
  }

  RegisterScalar(
    loader, information_schema, "_pg_truetypid",
    {duckdb::ScalarFunction{{oid, text, oid}, oid, TrueTypeFunction<int64_t>}});
  RegisterScalar(loader, information_schema, "_pg_truetypmod",
                 {duckdb::ScalarFunction{
                   {int4, text, int4}, int4, TrueTypeFunction<int32_t>}});
  RegisterScalar(loader, information_schema, "_pg_char_max_length",
                 {duckdb::ScalarFunction{
                   {oid, int4}, int4, TypmodFunction<pg::CharMaxLength>}});
  RegisterScalar(loader, information_schema, "_pg_char_octet_length",
                 {duckdb::ScalarFunction{
                   {oid, int4}, int4, TypmodFunction<pg::CharOctetLength>}});
  RegisterScalar(loader, information_schema, "_pg_numeric_precision",
                 {duckdb::ScalarFunction{
                   {oid, int4}, int4, TypmodFunction<pg::NumericPrecision>}});
  RegisterScalar(
    loader, information_schema, "_pg_numeric_precision_radix",
    {duckdb::ScalarFunction{
      {oid, int4}, int4, TypmodFunction<pg::NumericPrecisionRadix>}});
  RegisterScalar(loader, information_schema, "_pg_numeric_scale",
                 {duckdb::ScalarFunction{
                   {oid, int4}, int4, TypmodFunction<pg::NumericScale>}});
  RegisterScalar(loader, information_schema, "_pg_datetime_precision",
                 {duckdb::ScalarFunction{
                   {oid, int4}, int4, TypmodFunction<pg::DatetimePrecision>}});
  RegisterScalar(
    loader, information_schema, "_pg_interval_type",
    {duckdb::ScalarFunction{{oid, int4}, text, IntervalTypeFunction}});
  RegisterScalar(loader, information_schema, "_pg_index_position",
                 {WithinQuery(duckdb::ScalarFunction{
                   {oid, int2}, int4, IndexPositionFunction})});
}

}  // namespace sdb::connector
