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

#include "connector/functions/catalog_introspect.h"

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <magic_enum/magic_enum.hpp>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "auth/role_closure.h"
#include "basics/serializer.h"
#include "catalog1/catalog.h"
#include "catalog1/cluster.h"
#include "catalog1/entry/database.h"
#include "catalog1/entry/foreign_server.h"
#include "catalog1/entry/role.h"
#include "catalog1/permissions.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {
namespace {

// The dump spans every database and includes definitions a role could never
// SELECT (role rows carry password verifiers), so it is superuser-only.
void RequireSuperuser(duckdb::ClientContext& context, std::string_view what) {
  auto& conn = GetSereneDBContext(context);
  if (!auth::ClosureFor(&context, conn.GetRoleId())->is_superuser) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                    ERR_MSG("must be superuser to use ", what));
  }
}

// The output vectors are freshly initialized flat vectors, so write them
// directly instead of building a duckdb::Value per cell.
void SetUnsigned(duckdb::Vector& vec, duckdb::idx_t i, uint64_t v) {
  duckdb::FlatVector::GetDataMutable<uint64_t>(vec)[i] = v;
}

void SetString(duckdb::Vector& vec, duckdb::idx_t i, std::string_view v) {
  duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vec)[i] =
    duckdb::StringVector::AddString(vec, v);
}

struct CatalogSetRow {
  std::string schema;
  std::string entry_type;
  std::string name;
  uint64_t entry_oid = 0;
  bool visible = false;
};

struct CatalogSetsState final : duckdb::GlobalTableFunctionState {
  std::vector<CatalogSetRow> rows;
  size_t offset = 0;
  bool loaded = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<CatalogSetsState>();
  }
};

duckdb::unique_ptr<duckdb::FunctionData> CatalogSetsBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  RequireSuperuser(context, "sdb_catalog_sets()");
  names.emplace_back("schema_name");
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("entry_type");
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("name");
  return_types.emplace_back(duckdb::LogicalType::VARCHAR);
  // duckdb's own oid for the entry, which the port sets from the serenedb id --
  // so this column and pg_class.oid are the same number.
  names.emplace_back("entry_oid");
  return_types.emplace_back(duckdb::LogicalType::UBIGINT);
  names.emplace_back("visible");
  return_types.emplace_back(duckdb::LogicalType::BOOLEAN);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

// What is actually in the schema CatalogSets of the current database -- the one
// direct view of the entry port. `visible` says whether the version chain
// answers this transaction, so an entry another session has not committed shows
// up as present and invisible.
void CatalogSetsExecute(duckdb::ClientContext& context,
                        duckdb::TableFunctionInput& input,
                        duckdb::DataChunk& output) {
  auto& state = input.global_state->Cast<CatalogSetsState>();
  if (!state.loaded) {
    state.loaded = true;
    auto& duck_catalog = duckdb::Catalog::GetCatalog(
      context, duckdb::DatabaseManager::GetDefaultDatabase(context));
    if (duck_catalog.GetCatalogType() ==
        catalog::SereneDBCatalog::kStorageType) {
      auto& catalog = duck_catalog.Cast<catalog::SereneDBCatalog>();
      const auto transaction = catalog.GetCatalogTransaction(context);
      catalog.ScanSchemas(
        context, [&](duckdb::SchemaCatalogEntry& schema_entry) {
          auto& schema = schema_entry.Cast<duckdb::DuckSchemaEntry>();
          // The schema entry itself, which is what owns the sets below. The two
          // static schemas have no definition of their own and are reported by
          // name alone.
          state.rows.push_back(
            {.schema = schema.name.GetIdentifierName(),
             .entry_type = duckdb::CatalogTypeToString(schema.type),
             .name = schema.name.GetIdentifierName(),
             .entry_oid = schema.oid,
             .visible = true});
          // One type per set, and the entry's own type is reported rather than
          // the set's: tables, views and sequences share a set, as do the two
          // flavours of macro.
          for (const auto type : {duckdb::CatalogType::TABLE_ENTRY,
                                  duckdb::CatalogType::INDEX_ENTRY,
                                  duckdb::CatalogType::MACRO_ENTRY,
                                  duckdb::CatalogType::TYPE_ENTRY,
                                  duckdb::CatalogType::TOKENIZER_ENTRY}) {
            schema.GetCatalogSet(type).Scan(
              transaction, [&](duckdb::CatalogEntry& entry) {
                state.rows.push_back(
                  {.schema = schema.name.GetIdentifierName(),
                   .entry_type = duckdb::CatalogTypeToString(entry.type),
                   .name = entry.name.GetIdentifierName(),
                   .entry_oid = entry.oid,
                   .visible = true});
              });
          }
        });
      // Foreign servers are database children, so their set hangs off the
      // catalog and has no schema name to report.
      catalog.ScanForeignServers(transaction, [&](duckdb::CatalogEntry& entry) {
        state.rows.push_back(
          {.schema = {},
           .entry_type = duckdb::CatalogTypeToString(entry.type),
           .name = entry.name.GetIdentifierName(),
           .entry_oid = entry.oid,
           .visible = true});
      });
    }
    // The two cluster-global sets belong to no database at all, so they are
    // reported whichever one the session is in, with no schema name.
    auto& cluster = catalog::ClusterOf(context);
    {
      const auto transaction = cluster.GetCatalogTransaction(context);
      const auto row = [&](duckdb::CatalogEntry& entry) {
        state.rows.push_back(
          {.schema = {},
           .entry_type = duckdb::CatalogTypeToString(entry.type),
           .name = entry.name.GetIdentifierName(),
           .entry_oid = entry.oid,
           .visible = true});
      };
      cluster.ScanRoles(transaction, row);
      cluster.ScanDatabases(transaction, row);
    }
    // One row per recorded edge, from every attached manager: an edge is kept
    // by the dependent's own catalog, so no one of them holds the whole graph.
    // entry_oid is the referenced object and the name is the dependent.
    for (auto& attached :
         duckdb::DatabaseManager::Get(context).GetDatabases(context)) {
      auto manager = attached->GetCatalog().GetDependencyManager();
      if (!manager) {
        continue;
      }
      manager->Scan(context, [&](duckdb::CatalogEntry& referenced,
                                 duckdb::CatalogEntry& dependent,
                                 const duckdb::DependencyDependentFlags&) {
        state.rows.push_back({.schema = {},
                              .entry_type = duckdb::CatalogTypeToString(
                                duckdb::CatalogType::DEPENDENCY_ENTRY),
                              .name = std::to_string(dependent.oid),
                              .entry_oid = referenced.oid,
                              .visible = true});
      });
    }
  }
  const auto n =
    std::min<size_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.offset);
  for (size_t i = 0; i < n; ++i) {
    const auto& row = state.rows[state.offset + i];
    SetString(output.data[0], i, row.schema);
    SetString(output.data[1], i, row.entry_type);
    SetString(output.data[2], i, row.name);
    SetUnsigned(output.data[3], i, row.entry_oid);
    duckdb::FlatVector::GetDataMutable<bool>(output.data[4])[i] = row.visible;
  }
  state.offset += n;
  output.SetChildCardinality(n);
}

duckdb::unique_ptr<duckdb::FunctionData> DeferredCatalogBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  RequireSuperuser(context, "sdb_deferred_catalog()");
  names.emplace_back("catalog_version");
  return_types.emplace_back(duckdb::LogicalType::UBIGINT);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

struct OneRowState final : duckdb::GlobalTableFunctionState {
  bool emitted = false;

  static duckdb::unique_ptr<duckdb::GlobalTableFunctionState> Init(
    duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
    return duckdb::make_uniq<OneRowState>();
  }
};

// The catalog identity the running transaction's plans stand on.
void DeferredCatalogExecute(duckdb::ClientContext& context,
                            duckdb::TableFunctionInput& data,
                            duckdb::DataChunk& output) {
  auto& state = data.global_state->Cast<OneRowState>();
  if (std::exchange(state.emitted, true)) {
    output.SetCardinality(0);
    return;
  }
  SetUnsigned(output.data[0], 0, GetSereneDBContext(context).CatalogEpoch());
  output.SetCardinality(1);
}

}  // namespace

void RegisterCatalogIntrospectFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(duckdb::TableFunction{"sdb_deferred_catalog",
                                                {},
                                                DeferredCatalogExecute,
                                                DeferredCatalogBind,
                                                OneRowState::Init});
  loader.RegisterFunction(duckdb::TableFunction{"sdb_catalog_sets",
                                                {},
                                                CatalogSetsExecute,
                                                CatalogSetsBind,
                                                CatalogSetsState::Init});
}

}  // namespace sdb::connector
