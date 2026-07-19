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

#include "connector/duckdb_schema_entry.h"

#include <absl/algorithm/container.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/parsed_data/alter_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/comment_on_column_info.hpp>
#include <duckdb/parser/parsed_data/create_function_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>

#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "catalog/catalog.h"
#include "catalog/function.h"
#include "catalog/index.h"
#include "catalog/scorer_options.h"
#include "catalog/secondary_index.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_entry_cache.h"
#include "connector/duckdb_table_entry.h"
#include "connector/inverted_index_options_util.h"
#include "connector/pg_logical_types.h"
#include "connector/search_table_dispatch.h"
#include "connector/with_option_resolver.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "query/config_variable_names.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

[[noreturn]] void ThrowCreateUnsupported(std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                  ERR_MSG("CREATE ", what, " is not supported"));
}

// The single column a CHECK expression references, or empty if it references
// zero or multiple distinct columns. Drives PostgreSQL-style auto naming
// (<table>_<col>_check vs <table>_check), shared by CREATE TABLE and
// ALTER TABLE ADD CONSTRAINT.
std::string FindConstraintColumn(const duckdb::ParsedExpression& root) {
  std::string result;
  bool multiple = false;
  std::function<void(const duckdb::ParsedExpression&)> visit;
  visit = [&](const duckdb::ParsedExpression& expr) {
    if (multiple) {
      return;
    }
    if (expr.GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      const auto& name = expr.Cast<duckdb::ColumnRefExpression>()
                           .GetColumnName()
                           .GetIdentifierName();
      if (result.empty()) {
        result = name;
      } else if (result != name) {
        multiple = true;
      }
      return;
    }
    duckdb::ParsedExpressionIterator::EnumerateChildren(
      expr, [&](const duckdb::ParsedExpression& child) { visit(child); });
  };
  visit(root);
  return multiple ? std::string{} : result;
}

}  // namespace

ObjectId SereneDBSchemaEntry::GetDatabaseId() const {
  return catalog.Cast<SereneDBCatalog>().GetDatabaseId();
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::LookupEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& lookup_info) {
  auto& conn_ctx = GetSereneDBContext(transaction.GetContext());
  auto snapshot = conn_ctx.CatalogSnapshot();
  auto [result, object] = snapshot->GetDuckDBEntryCache().EnsureEntry(
    lookup_info.GetCatalogType(), catalog, *this, GetDatabaseId(),
    name.GetIdentifierName(), lookup_info.GetEntryName(), *snapshot);
  if (result) {
    if (object && name.GetIdentifierName() != StaticStrings::kPgCatalogSchema) {
      const auto need = [&] {
        switch (object->GetType()) {
          case catalog::ObjectType::PgSqlFunction:
            return catalog::AclMode::Execute;
          case catalog::ObjectType::PgSqlType:
            return catalog::AclMode::Usage;
          default:
            return catalog::AclMode::NoRights;
        }
      }();
      if (need != catalog::AclMode::NoRights) {
        snapshot->RequireAccess(conn_ctx.GetRoleId(), *object, need);
      }
    }
    return result;
  }
  if (name.GetIdentifierName() != StaticStrings::kPgCatalogSchema) {
    return result;
  }

  // Pg-compat fallback for `pg_catalog.<x>` that redirects to the system
  // catalog.
  switch (lookup_info.GetCatalogType()) {
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
    case duckdb::CatalogType::SCALAR_FUNCTION_ENTRY:
    case duckdb::CatalogType::TABLE_FUNCTION_ENTRY:
    case duckdb::CatalogType::AGGREGATE_FUNCTION_ENTRY:
    case duckdb::CatalogType::TYPE_ENTRY: {
      auto& sys = duckdb::Catalog::GetSystemCatalog(transaction.GetContext());
      auto main_schema = sys.GetSchema(transaction, DEFAULT_SCHEMA,
                                       duckdb::OnEntryNotFound::RETURN_NULL);
      if (main_schema) {
        return main_schema->LookupEntry(transaction, lookup_info);
      }
      break;
    }
    default:
      break;
  }
  return nullptr;
}

void SereneDBSchemaEntry::Scan(
  duckdb::ClientContext& context, duckdb::CatalogType type,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.CatalogSnapshot();
  snapshot->GetDuckDBEntryCache().ScanEntries(
    type, catalog, *this, GetDatabaseId(), name.GetIdentifierName(), callback,
    *snapshot);
}

void SereneDBSchemaEntry::Scan(
  duckdb::CatalogType type,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  // Without context -- no snapshot available, skip
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateTable(
  duckdb::CatalogTransaction transaction, duckdb::BoundCreateTableInfo& info) {
  auto& create_info = info.Base();
  auto& table_info = create_info.Cast<duckdb::CreateTableInfo>();

  catalog::CreateTableOptions options;
  options.name = table_info.GetTableName().GetIdentifierName();

  // Consume the SereneDB-specific `storage` WITH option (selects the table
  // engine) + any Search maintenance-interval options before validating that no
  // unknown options remain.
  ApplyStorageKind(transaction.GetContext(), options, table_info.options);

  if (!table_info.options.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized parameter \"",
                            table_info.options.begin()->first, "\""));
  }

  // PG-style constraint name generator with dedup.
  auto choose_constraint_name = [&](std::string_view tbl,
                                    std::string_view column,
                                    std::string_view label) -> std::string {
    std::string base_name;
    if (column.empty()) {
      base_name = absl::StrCat(tbl, "_", label);
    } else {
      base_name = absl::StrCat(tbl, "_", column, "_", label);
    }
    auto name_exists = [&](std::string_view candidate) {
      return absl::c_any_of(options.check_constraints, [&](const auto& c) {
        return c.GetName() == candidate;
      });
    };
    if (!name_exists(base_name)) {
      return base_name;
    }
    for (size_t counter = 1;; ++counter) {
      auto candidate = absl::StrCat(base_name, counter);
      if (!name_exists(candidate)) {
        return candidate;
      }
    }
  };

  // Dedup against duplicate NOT NULL adds; grows on demand because the
  // SERIAL path calls append_not_null mid column loop.
  std::vector<bool> has_not_null;

  auto append_not_null = [&](duckdb::idx_t col_idx,
                             std::string explicit_name = {}) {
    if (col_idx >= options.columns.size()) {
      return;
    }
    if (col_idx >= has_not_null.size()) {
      has_not_null.resize(col_idx + 1, false);
    }
    if (has_not_null[col_idx]) {
      return;
    }
    has_not_null[col_idx] = true;
    std::string col_name{options.columns[col_idx].GetName()};
    auto col_ref = duckdb::make_uniq<duckdb::ColumnRefExpression>(
      duckdb::Identifier{col_name});
    auto is_not_null = duckdb::make_uniq<duckdb::OperatorExpression>(
      duckdb::ExpressionType::OPERATOR_IS_NOT_NULL, std::move(col_ref));
    std::string nn_name =
      !explicit_name.empty()
        ? std::move(explicit_name)
        : choose_constraint_name(table_info.GetTableName().GetIdentifierName(),
                                 col_name, "not_null");
    options.check_constraints.push_back(catalog::CheckConstraint{
      ObjectId{}, catalog::NextId(), std::move(nn_name),
      std::make_shared<ColumnExpr>(std::move(is_not_null))});
  };

  // SERIAL expands to base int + nextval default + NOT NULL. The sequence
  // name and nextval default are resolved by Catalog under its mutex.
  for (auto& col : table_info.columns.Logical()) {
    auto& sdb_col =
      options.columns.emplace_back(ObjectId{}, catalog::NextId(),
                                   col.Name().GetIdentifierName(), col.Type());

    bool is_smallserial = pg::IsSmallserial(sdb_col.type);
    bool is_serial = pg::IsSerial(sdb_col.type);
    bool is_bigserial = pg::IsBigserial(sdb_col.type);
    if (is_smallserial || is_serial || is_bigserial) {
      catalog::SequenceOptions seq_opts;
      if (is_smallserial) {
        seq_opts.max_value = std::numeric_limits<int16_t>::max();
      } else if (is_serial) {
        seq_opts.max_value = std::numeric_limits<int32_t>::max();
      } else {
        SDB_ASSERT(is_bigserial);
        seq_opts.max_value = std::numeric_limits<int64_t>::max();
      }
      sdb_col.type = duckdb::LogicalType{sdb_col.type.id()};
      options.sequences.emplace_back(sdb_col.GetId(), seq_opts);
      append_not_null(options.columns.size() - 1);
    } else if (col.Generated()) {
      sdb_col.generated_type = catalog::Column::GeneratedType::kStored;
      sdb_col.expr =
        std::make_shared<ColumnExpr>(col.GeneratedExpression().Copy());
    } else if (col.HasDefaultValue()) {
      sdb_col.expr = std::make_shared<ColumnExpr>(col.DefaultValue().Copy());
    }
  }

  containers::FlatHashMap<catalog::Column::Id, size_t> col_idx_by_id;
  col_idx_by_id.reserve(options.columns.size());
  for (size_t i = 0; i < options.columns.size(); ++i) {
    col_idx_by_id.emplace(options.columns[i].GetId(), i);
  }
  auto find_column_idx = [&](catalog::Column::Id col_id) -> size_t {
    auto it = col_idx_by_id.find(col_id);
    SDB_ASSERT(it != col_idx_by_id.end());
    return it->second;
  };
  auto append_pk = [&](catalog::Column::Id col_id) {
    if (absl::c_contains(options.pk_columns, col_id)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
        ERR_MSG("column \"", options.columns[find_column_idx(col_id)].GetName(),
                "\" appears twice in primary key constraint"));
    }
    append_not_null(find_column_idx(col_id));  // PK implies NOT NULL
    options.pk_columns.push_back(col_id);
  };

  for (auto& constraint : table_info.constraints) {
    switch (constraint->type) {
      case duckdb::ConstraintType::UNIQUE: {
        auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
        if (!unique.IsPrimaryKey()) {
          std::vector<catalog::Column::Id> cols;
          if (unique.HasIndex()) {
            auto idx = unique.GetIndex().index;
            SDB_ASSERT(idx < options.columns.size());
            cols.push_back(options.columns[idx].GetId());
          } else {
            for (auto& col_name : unique.GetColumnNames()) {
              auto it = absl::c_find_if(options.columns, [&](const auto& col) {
                return col.GetName() == col_name;
              });
              if (it == options.columns.end()) {
                THROW_SQL_ERROR(
                  ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                  ERR_MSG("column \"", col_name.GetIdentifierName(),
                          "\" named in key does not exist"));
              }
              cols.push_back(it->GetId());
            }
          }
          std::string uq_name = unique.constraint_name;
          if (uq_name.empty()) {
            std::string_view col0 =
              cols.empty()
                ? std::string_view{}
                : options.columns[find_column_idx(cols[0])].GetName();
            uq_name = choose_constraint_name(options.name, col0, "key");
          }
          options.unique_constraints.push_back(
            catalog::TableUnique{std::move(uq_name), std::move(cols)});
          break;
        }
        if (unique.HasIndex()) {
          auto idx = unique.GetIndex().index;
          SDB_ASSERT(idx < options.columns.size());
          append_pk(options.columns[idx].GetId());
        } else {
          for (auto& pk_name : unique.GetColumnNames()) {
            auto it = absl::c_find_if(options.columns, [&](const auto& col) {
              return col.GetName() == pk_name;
            });
            if (it == options.columns.end()) {
              THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                              ERR_MSG("column \"", pk_name.GetIdentifierName(),
                                      "\" named in key does not exist"));
            }
            append_pk(it->GetId());
          }
        }
        if (!unique.constraint_name.empty()) {
          options.pk_name = unique.constraint_name;
        }
      } break;
      case duckdb::ConstraintType::NOT_NULL: {
        auto& nn = constraint->Cast<duckdb::NotNullConstraint>();
        append_not_null(nn.index.index, nn.constraint_name);
      } break;
      case duckdb::ConstraintType::CHECK: {
        auto& check = constraint->Cast<duckdb::CheckConstraint>();
        std::string name;
        if (!check.constraint_name.empty()) {
          name = check.constraint_name;
        } else {
          auto col = FindConstraintColumn(*check.expression);
          name = choose_constraint_name(
            table_info.GetTableName().GetIdentifierName(), col, "check");
        }
        options.check_constraints.push_back(catalog::CheckConstraint{
          ObjectId{}, catalog::NextId(), std::move(name),
          std::make_shared<ColumnExpr>(check.expression->Copy())});
        break;
      }
      case duckdb::ConstraintType::FOREIGN_KEY: {
        auto& fk = constraint->Cast<duckdb::ForeignKeyConstraint>();
        // FK_TYPE_PRIMARY_KEY_TABLE is the reciprocal entry on the referenced
        // table -- skip it (the FK is mirrored from the referencing side). A
        // self-referencing FK is FK_TYPE_SELF_REFERENCE_TABLE and must be kept,
        // else it is silently unenforced (the self_reference branch below
        // builds it).
        if (fk.info.type != duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE &&
            fk.info.type !=
              duckdb::ForeignKeyType::FK_TYPE_SELF_REFERENCE_TABLE) {
          break;
        }
        catalog::TableForeignKey out;
        for (auto& col_name : fk.fk_columns) {
          auto it = absl::c_find_if(options.columns, [&](const auto& col) {
            return col.GetName() == col_name;
          });
          if (it == options.columns.end()) {
            THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                            ERR_MSG("column \"", col_name.GetIdentifierName(),
                                    "\" named in foreign key does not exist"));
          }
          out.columns.push_back(it->GetId());
        }
        {
          std::string_view fk_col0 =
            out.columns.empty()
              ? std::string_view{}
              : options.columns[find_column_idx(out.columns[0])].GetName();
          out.name = fk.constraint_name.empty()
                       ? choose_constraint_name(options.name, fk_col0, "fkey")
                       : fk.constraint_name;
        }
        const bool self_reference =
          fk.info.table == table_info.GetTableName().GetIdentifierName();
        if (self_reference) {
          for (auto& col_name : fk.pk_columns) {
            auto it = absl::c_find_if(options.columns, [&](const auto& col) {
              return col.GetName() == col_name;
            });
            SDB_ASSERT(it != options.columns.end());
            out.referenced_columns.push_back(it->GetId());
          }
        } else {
          auto& conn_ctx = GetSereneDBContext(transaction.GetContext());
          auto snapshot = conn_ctx.CatalogSnapshot();
          auto referenced = snapshot->GetRelation(
            catalog::NoAccessCheck(), GetDatabaseId(),
            (fk.info.schema.empty() ? name : fk.info.schema)
              .GetIdentifierName(),
            fk.info.table.GetIdentifierName());
          if (!referenced ||
              referenced->GetType() != catalog::ObjectType::Table) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_UNDEFINED_TABLE),
              ERR_MSG("referenced table \"", fk.info.table.GetIdentifierName(),
                      "\" does not exist"));
          }
          auto& ref_table = basics::downCast<catalog::Table>(*referenced);
          out.referenced_table = ref_table.GetId();
          for (auto& col_name : fk.pk_columns) {
            auto it = absl::c_find_if(
              ref_table.Columns(),
              [&](const auto& col) { return col.GetName() == col_name; });
            if (it == ref_table.Columns().end()) {
              THROW_SQL_ERROR(
                ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                ERR_MSG("column \"", col_name.GetIdentifierName(),
                        "\" named in foreign key does not exist"));
            }
            out.referenced_columns.push_back(it->GetId());
          }
        }
        options.foreign_keys.push_back(std::move(out));
        break;
      }
      default:
        break;
    }
  }

  auto& catalog_impl = catalog::GetCatalog();
  auto database_id = GetDatabaseId();

  if (create_info.on_conflict ==
      duckdb::OnCreateConflict::REPLACE_ON_CONFLICT) {
    // CREATE OR REPLACE: drop the existing table first (DuckDB semantics; PG
    // has no OR REPLACE for tables). A missing table is fine -- replace then
    // degrades to a plain create.
    catalog_impl.DropTable(catalog::ActingAs(transaction.GetContext()),
                           catalog.GetName().GetIdentifierName(),
                           name.GetIdentifierName(),
                           table_info.GetTableName().GetIdentifierName(),
                           /*cascade=*/false, /*missing_ok=*/true);
  }

  bool replace =
    create_info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  catalog::CreateTableOperationOptions op_options;
  op_options.if_not_exists =
    create_info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  // CREATE OR REPLACE TABLE (non-AS): drop the pre-existing table (cascade)
  // then create the new one, mirroring native duckdb's REPLACE_ON_CONFLICT.
  // Only a real Table is dropped; a name held by a view/other relation falls
  // through to the duplicate-name path below.
  if (replace) {
    auto snapshot = catalog_impl.GetCatalogSnapshot();
    if (snapshot->GetTable(catalog::NoAccessCheck(), database_id,
                           name.GetIdentifierName(),
                           table_info.GetTableName().GetIdentifierName())) {
      catalog_impl.DropTable(catalog::ActingAs(transaction.GetContext()),
                             catalog.GetName().GetIdentifierName(),
                             name.GetIdentifierName(),
                             table_info.GetTableName().GetIdentifierName(),
                             /*cascade=*/true, /*missing_ok=*/false);
    }
  }

  // Creator owns the table (and its generated serial/PK sequences) via the
  // access context.
  const ObjectId role{GetSereneDBContext(transaction.GetContext()).GetRoleId()};
  if (catalog_impl.CreateTable(catalog::ActingAs(role), database_id,
                               name.GetIdentifierName(), std::move(options),
                               op_options)) {
    // Search tables maintain themselves in the background
    // (commit/consolidate/GC). Kick the maintenance chains now that the table
    // and its iresearch store exist; mirrors the inverted-index StartTasks in
    // CreateIndex.
    auto new_snapshot = catalog_impl.GetCatalogSnapshot();
    if (auto sdb_table = new_snapshot->GetTable(
          catalog::NoAccessCheck(), database_id, name.GetIdentifierName(),
          table_info.GetTableName().GetIdentifierName());
        sdb_table && sdb_table->GetEngine() == catalog::TableEngine::Search) {
      sdb_table->GetData()->StartTasks();  // GetData asserts the store is bound
    }
  }
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateIndex(
  duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
  duckdb::TableCatalogEntry& table) {
  auto& sdb_table_entry = RequireBaseTable(table);
  auto sdb_table = sdb_table_entry.GetSereneDBTable();

  auto& catalog_impl = catalog::GetCatalog();
  auto database_id = GetDatabaseId();

  RejectIfSearchTable(*sdb_table, "CREATE INDEX");

  // Map DuckDB index type to SereneDB IndexType
  // DuckDB default is empty or "ART"; PG default is "btree"
  catalog::ObjectType index_type;
  auto idx_type_str = info.index_type;
  std::transform(idx_type_str.begin(), idx_type_str.end(), idx_type_str.begin(),
                 ::tolower);
  if (idx_type_str.empty() || idx_type_str == "art" ||
      idx_type_str == "btree" || idx_type_str == "secondary") {
    index_type = catalog::ObjectType::SecondaryIndex;
  } else if (idx_type_str == "inverted") {
    index_type = catalog::ObjectType::InvertedIndex;
  } else {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
      ERR_MSG("access method \"", info.index_type, "\" does not exist"));
  }

  // Build CreateIndexColumn vector from DuckDB info.
  // At bind time, column_ids may not be populated yet -- use names/expressions.
  const auto& columns = sdb_table->Columns();
  std::vector<catalog::CreateIndexColumn> idx_columns;

  // parsed_expressions has the actual index columns (from CREATE INDEX ON t
  // (col)) info.names has ALL table scan columns -- don't use it for index
  // columns!
  for (auto& expr : info.parsed_expressions) {
    if (expr->GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
      auto& col_ref = expr->Cast<duckdb::ColumnRefExpression>();
      const auto& col_name = col_ref.GetColumnName().GetIdentifierName();
      const catalog::Column* cat_col = nullptr;
      for (const auto& col : columns) {
        if (col.GetName() == col_name) {
          cat_col = &col;
          break;
        }
      }
      if (!cat_col) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
          ERR_MSG("column \"", col_name, "\" not found in table"));
      }
      idx_columns.emplace_back(cat_col->GetName(), cat_col);
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("Expression-based index columns are not supported"));
    }
  }

  bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  auto& context = transaction.GetContext();
  bool created;
  if (index_type == catalog::ObjectType::InvertedIndex) {
    auto find_with = [&](std::string_view key) -> const duckdb::Value* {
      auto it = info.options.find(key);
      return it != info.options.end() ? &it->second : nullptr;
    };
    auto resolve_uint = [&](std::string_view key) -> uint32_t {
      return ResolveUintWithOption(context, key, find_with(key));
    };
    catalog::InvertedIndexOptions options{
      .row_group_size = resolve_uint(kRowGroupSizeSetting),
      .norm_row_group_size = resolve_uint(kNormRowGroupSizeSetting),
      .refresh_interval_ms = resolve_uint(kRefreshIntervalSetting),
      .compaction_interval_ms = resolve_uint(kCompactionIntervalSetting),
      .cleanup_interval_step = resolve_uint(kCleanupIntervalStepSetting),
    };
    if (auto* v = find_with("optimize_top_k")) {
      auto value =
        v->DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>();
      options.topk_scorer = catalog::ParseScorerExpression(context, value);
    }
    created = catalog_impl.CreateInvertedIndex(
      catalog::ActingAs(context), context, database_id,
      name.GetIdentifierName(), sdb_table->GetName(),
      info.GetIndexName().GetIdentifierName(), std::move(idx_columns),
      std::move(options),
      /*operation_options=*/{.if_not_exists = if_not_exists});
  } else {
    bool unique = (info.constraint_type == duckdb::IndexConstraintType::UNIQUE);
    created = catalog_impl.CreateSecondaryIndex(
      catalog::ActingAs(context), database_id, name.GetIdentifierName(),
      sdb_table->GetName(), info.GetIndexName().GetIdentifierName(),
      std::move(idx_columns), unique,
      /*operation_options=*/{.if_not_exists = if_not_exists});
  }
  if (!created) {
    return nullptr;
  }

  // Start background tasks for inverted indexes
  auto new_snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_index = new_snapshot->GetRelation(
    catalog::NoAccessCheck(), database_id, name.GetIdentifierName(),
    info.GetIndexName().GetIdentifierName());
  if (catalog_index) {
    auto inverted =
      new_snapshot->GetObject<catalog::InvertedIndex>(catalog_index->GetId());
    auto storage = inverted ? inverted->GetData() : nullptr;
    if (storage) {
      storage->StartTasks();
      // No backfill yet -- mark creation as finished so background commits
      // register the flush subscription and run periodically.
      storage->FinishCreation();
    }
  }
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateFunction(
  duckdb::CatalogTransaction transaction, duckdb::CreateFunctionInfo& info) {
  const ObjectId role{GetSereneDBContext(transaction.GetContext()).GetRoleId()};
  auto& catalog_impl = catalog::GetCatalog();
  auto database_id = GetDatabaseId();

  auto new_macro_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
      info.Copy());

  bool replace =
    info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;

  // Check for existing function to support overload merging.
  // PG semantics: multiple CREATE FUNCTION with the same name but
  // different parameter signatures are legal (they're distinct overloads).
  // CREATE OR REPLACE replaces only the matching overload, preserving
  // others.
  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto existing = snapshot->GetFunction(
    catalog::NoAccessCheck(), database_id, name.GetIdentifierName(),
    info.GetFunctionName().GetIdentifierName());

  if (existing) {
    // Clone the existing macros vector and merge the new overload(s).
    auto merged_info =
      duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateMacroInfo>(
        existing->GetInfo().Copy());

    for (auto& new_macro : new_macro_info->macros) {
      // Find an existing overload with the same parameter signature.
      bool found = false;
      for (size_t i = 0; i < merged_info->macros.size(); ++i) {
        if (merged_info->macros[i]->types == new_macro->types) {
          if (!replace) {
            // Plain CREATE FUNCTION: duplicate signature is an error.
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_DUPLICATE_FUNCTION),
              ERR_MSG("function \"", info.GetFunctionName().GetIdentifierName(),
                      "\" already exists with same argument types"));
          }
          // CREATE OR REPLACE: swap in the new overload.
          merged_info->macros[i] = new_macro->Copy();
          found = true;
          break;
        }
      }
      if (!found) {
        // New signature -- append as a new overload.
        merged_info->macros.push_back(new_macro->Copy());
      }
    }

    auto function = std::make_shared<catalog::PgSqlFunction>(
      role, ObjectId{}, ObjectId{}, info.GetFunctionName().GetIdentifierName(),
      std::move(merged_info));
    // Always replace=true for the catalog layer since we're replacing
    // the whole PgSqlFunction with the merged version. CreateFunction
    // preserves the prior owner on replace (PG semantics).
    catalog_impl.CreateFunction(catalog::ActingAs(role), database_id,
                                name.GetIdentifierName(), function,
                                /*replace=*/true, /*if_not_exists=*/false);
    return nullptr;
  }

  // No existing function -- create new.
  auto function = std::make_shared<catalog::PgSqlFunction>(
    role, ObjectId{}, ObjectId{}, info.GetFunctionName().GetIdentifierName(),
    std::move(new_macro_info));
  catalog_impl.CreateFunction(
    catalog::ActingAs(role), database_id, name.GetIdentifierName(), function,
    /*replace=*/false,
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT);
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateView(
  duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo& info) {
  const ObjectId role{GetSereneDBContext(transaction.GetContext()).GetRoleId()};
  auto& catalog_impl = catalog::GetCatalog();
  auto database_id = GetDatabaseId();

  auto view_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      info.Copy());
  auto view = std::make_shared<catalog::PgSqlView>(
    role, ObjectId{}, ObjectId{}, info.GetViewName().GetIdentifierName(),
    std::move(view_info));

  bool replace =
    info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  // CreateView preserves the prior owner on replace (PG: CREATE OR REPLACE
  // keeps the original owner).
  catalog_impl.CreateView(
    catalog::ActingAs(role), database_id, name.GetIdentifierName(), view,
    replace, info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT);
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateSequence(
  duckdb::CatalogTransaction transaction, duckdb::CreateSequenceInfo& info) {
  auto database_id = GetDatabaseId();

  if (info.increment <= 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("sequence INCREMENT must be positive (negative increments not "
              "yet supported)"));
  }
  if (info.min_value < 0 || info.max_value < 0 || info.start_value < 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("sequence MIN/MAX/START must be non-negative (negative "
              "sequences not yet supported)"));
  }
  if (info.start_value < info.min_value || info.start_value > info.max_value) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("sequence START is out of range [MIN, MAX]"));
  }

  const ObjectId role{GetSereneDBContext(transaction.GetContext()).GetRoleId()};
  catalog::SequenceOptions options;
  options.name = info.GetSequenceName().GetIdentifierName();
  options.start_value = static_cast<uint64_t>(info.start_value);
  options.increment = static_cast<uint64_t>(info.increment);
  options.min_value = static_cast<uint64_t>(info.min_value);
  options.max_value = static_cast<uint64_t>(info.max_value);
  options.cycle = info.cycle;
  options.perm = catalog::Permissions{role};

  bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  auto sequence = std::make_shared<catalog::Sequence>(ObjectId{}, ObjectId{},
                                                      std::move(options));

  auto& catalog_impl = catalog::GetCatalog();
  catalog_impl.CreateSequence(catalog::ActingAs(role), database_id,
                              name.GetIdentifierName(), sequence,
                              if_not_exists);
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreateTableFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateTableFunctionInfo& info) {
  ThrowCreateUnsupported("TABLE FUNCTION");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreateCopyFunction(duckdb::CatalogTransaction transaction,
                                        duckdb::CreateCopyFunctionInfo& info) {
  ThrowCreateUnsupported("COPY FUNCTION");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreatePragmaFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreatePragmaFunctionInfo& info) {
  ThrowCreateUnsupported("PRAGMA FUNCTION");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateCollation(
  duckdb::CatalogTransaction transaction, duckdb::CreateCollationInfo& info) {
  ThrowCreateUnsupported("COLLATION");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateType(
  duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo& info) {
  auto& catalog_impl = catalog::GetCatalog();
  auto database_id = GetDatabaseId();

  auto type_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTypeInfo>(
      info.Copy());
  const ObjectId role{GetSereneDBContext(transaction.GetContext()).GetRoleId()};
  auto type = std::make_shared<catalog::PgSqlType>(
    role, ObjectId{}, ObjectId{}, info.GetTypeName().GetIdentifierName(),
    std::move(type_info));
  catalog_impl.CreateType(
    catalog::ActingAs(role), database_id, name.GetIdentifierName(), type,
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT);
  return nullptr;
}

void SereneDBSchemaEntry::DropEntry(duckdb::ClientContext& context,
                                    duckdb::DropInfo& info) {
  info.SetCatalog(catalog.GetName());
  info.SetSchema(name);
  DropObject(context, info);
}

void SereneDBSchemaEntry::Alter(duckdb::CatalogTransaction transaction,
                                duckdb::AlterInfo& info) {
  auto& catalog_impl = catalog::GetCatalog();
  auto db = GetDatabaseId();
  const auto ax =
    catalog::ActingAs(GetSereneDBContext(transaction.GetContext()).GetRoleId());

  if (info.type == duckdb::AlterType::ALTER_SCALAR_FUNCTION) {
    auto& fn_info = info.Cast<duckdb::AlterScalarFunctionInfo>();
    if (fn_info.alter_scalar_function_type !=
        duckdb::AlterScalarFunctionType::RENAME_SCALAR_FUNCTION) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("only RENAME is supported for ALTER FUNCTION"));
    }
    auto& rename_info = fn_info.Cast<duckdb::RenameScalarFunctionInfo>();

    catalog_impl.RenameFunction(
      ax, db, name.GetIdentifierName(),
      info.GetQualifiedName().Name().GetIdentifierName(),
      rename_info.new_name.GetIdentifierName(),
      info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL);
    return;
  }

  if (info.type == duckdb::AlterType::ALTER_VIEW) {
    auto& view_info = info.Cast<duckdb::AlterViewInfo>();
    if (view_info.alter_view_type != duckdb::AlterViewType::RENAME_VIEW) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("only RENAME is supported for ALTER VIEW"));
    }
    auto& rename_info = view_info.Cast<duckdb::RenameViewInfo>();
    catalog_impl.RenameView(ax, db, name.GetIdentifierName(),
                            info.GetQualifiedName().Name().GetIdentifierName(),
                            rename_info.new_view_name.GetIdentifierName());
    return;
  }

  // COMMENT ON TABLE/COLUMN are top-level AlterTypes (not inside ALTER_TABLE),
  // so intercept them before the ALTER_TABLE guard. Both route through
  // ChangeTable copy-on-write; the comment surfaces in duckdb_tables()/
  // duckdb_columns(). NULL clears the comment (empty string).
  if (info.type == duckdb::AlterType::SET_COMMENT) {
    auto& comment_info = info.Cast<duckdb::SetCommentInfo>();
    std::string comment =
      comment_info.comment_value.IsNull()
        ? std::string{}
        : comment_info.comment_value.DefaultCastAs(duckdb::LogicalType::VARCHAR)
            .GetValue<std::string>();
    catalog_impl.ChangeTable(
      ax, db, name.GetIdentifierName(),
      info.GetQualifiedName().Name().GetIdentifierName(),
      [&](const catalog::Table& table,
          std::shared_ptr<catalog::Table>& updated) {
        table.SetComment(updated, comment);
      },
      {.missing_ok =
         info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL});
    return;
  }

  if (info.type == duckdb::AlterType::SET_COLUMN_COMMENT) {
    auto& comment_info = info.Cast<duckdb::SetColumnCommentInfo>();
    std::string comment =
      comment_info.comment_value.IsNull()
        ? std::string{}
        : comment_info.comment_value.DefaultCastAs(duckdb::LogicalType::VARCHAR)
            .GetValue<std::string>();
    catalog_impl.ChangeTable(
      ax, db, name.GetIdentifierName(),
      info.GetQualifiedName().Name().GetIdentifierName(),
      [&](const catalog::Table& table,
          std::shared_ptr<catalog::Table>& updated) {
        table.SetColumnComment(
          updated, comment_info.column_name.GetIdentifierName(), comment);
      },
      {.missing_ok =
         info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL});
    return;
  }

  if (info.type != duckdb::AlterType::ALTER_TABLE) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("this ALTER operation is not supported"));
  }

  auto& table_info = info.Cast<duckdb::AlterTableInfo>();
  auto table_name = info.GetQualifiedName().Name().GetIdentifierName();

  // ALTER INDEX <name> SET/RESET (option = ...): the maintenance/perf subset
  // of the inverted-index WITH options is alterable; SET writes the given
  // value, RESET restores the session-level default. Structural options
  // (row_group_size, store_pk, optimize_top_k, ...) shape the indexed data
  // and stay create-time only.
  if (table_info.alter_table_type ==
        duckdb::AlterTableType::SET_TABLE_OPTIONS ||
      table_info.alter_table_type ==
        duckdb::AlterTableType::RESET_TABLE_OPTIONS) {
    auto& context = transaction.GetContext();
    const auto require_alterable = [](std::string_view option) {
      if (!absl::c_contains(kAlterableInvertedOptions, option)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("option \"", option,
                                "\" cannot be changed with ALTER INDEX"));
      }
    };
    std::vector<std::pair<std::string, uint64_t>> changes;
    if (table_info.alter_table_type ==
        duckdb::AlterTableType::SET_TABLE_OPTIONS) {
      for (auto& [option, expr] :
           table_info.Cast<duckdb::SetTableOptionsInfo>().table_options) {
        if (!expr ||
            expr->GetExpressionClass() != duckdb::ExpressionClass::CONSTANT) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
            ERR_MSG("option \"", option, "\" requires a constant value"));
        }
        require_alterable(option);
        changes.emplace_back(
          option,
          ValidateInvertedIndexOptionValue(
            option, expr->Cast<duckdb::ConstantExpression>().GetValue()));
      }
    } else {
      // RESET stores the session-resolved value, which goes through the same
      // validator as an explicit SET.
      for (const auto& option :
           table_info.Cast<duckdb::ResetTableOptionsInfo>().table_options) {
        const auto& option_name = option.GetIdentifierName();
        require_alterable(option_name);
        changes.emplace_back(
          option_name,
          ValidateInvertedIndexOptionValue(
            option_name, duckdb::Value::UBIGINT(ResolveUbigintWithOption(
                           context, option_name, nullptr))));
      }
    }
    catalog_impl.AlterInvertedIndexOptions(
      ax, db, name.GetIdentifierName(), table_name,
      [&](catalog::InvertedIndexOptions& options) {
        for (const auto& [option, value] : changes) {
          if (option == kRefreshIntervalSetting) {
            options.refresh_interval_ms = static_cast<uint32_t>(value);
          } else if (option == kCompactionIntervalSetting) {
            options.compaction_interval_ms = static_cast<uint32_t>(value);
          } else if (option == kCleanupIntervalStepSetting) {
            options.cleanup_interval_step = static_cast<uint32_t>(value);
          } else if (option == kSegmentMemoryMaxSetting) {
            options.segment_memory_max = value;
          } else if (option == kSegmentDocsMaxSetting) {
            options.segment_docs_max = static_cast<uint32_t>(value);
          } else if (option == kCompactionMaxSegmentsSetting) {
            options.compaction_max_segments = static_cast<uint32_t>(value);
          } else if (option == kCompactionMaxSegmentsBytesSetting) {
            options.compaction_max_segments_bytes = value;
          } else {
            SDB_ASSERT(option == kCompactionFloorSegmentBytesSetting);
            options.compaction_floor_segment_bytes = value;
          }
        }
      },
      info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL);
    return;
  }

  // Search-backed tables have a fixed iresearch schema, so structural ALTERs
  // are rejected. Renames (table/column/constraint) are catalog-only metadata
  // -- iresearch fields and the scan are keyed by column id, not name -- so
  // they stay allowed.
  std::string_view unsupported_search_op;
  switch (table_info.alter_table_type) {
    case duckdb::AlterTableType::ADD_COLUMN:
      unsupported_search_op = "ALTER TABLE ADD COLUMN";
      break;
    case duckdb::AlterTableType::REMOVE_COLUMN:
      unsupported_search_op = "ALTER TABLE DROP COLUMN";
      break;
    case duckdb::AlterTableType::DROP_CONSTRAINT:
      unsupported_search_op = "ALTER TABLE DROP CONSTRAINT";
      break;
    case duckdb::AlterTableType::ALTER_COLUMN_TYPE:
      unsupported_search_op = "ALTER TABLE ALTER COLUMN TYPE";
      break;
    default:
      break;
  }
  if (!unsupported_search_op.empty()) {
    auto snapshot = catalog_impl.GetCatalogSnapshot();
    if (auto sdb_table = snapshot->GetTable(
          catalog::NoAccessCheck(), db, name.GetIdentifierName(), table_name)) {
      RejectIfSearchTable(*sdb_table, unsupported_search_op);
    }
  }

  switch (table_info.alter_table_type) {
    case duckdb::AlterTableType::DROP_CONSTRAINT: {
      auto& drop_info = table_info.Cast<duckdb::DropConstraintInfo>();

      catalog_impl.ChangeTable(
        ax, db, name.GetIdentifierName(), table_name,
        [&](const catalog::Table& table,
            std::shared_ptr<catalog::Table>& updated) {
          table.DropCheckConstraint(updated, drop_info.constraint_name,
                                    drop_info.if_constraint_not_found);
        },
        {.on_type_mismatch = [&](const catalog::Object& obj) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
            ERR_MSG("ALTER action DROP CONSTRAINT cannot be performed on "
                    "relation \"",
                    table_name, "\""),
            ERR_DETAIL("This operation is not supported for ",
                       basics::string_utils::GetPluralFormLowerCase(
                         pg::ToPgObjectTypeName(obj.GetType())),
                       "."));
        }});
      return;
    }

    case duckdb::AlterTableType::RENAME_TABLE: {
      auto& rename_info = table_info.Cast<duckdb::RenameTableInfo>();
      // RenameRelation routes by actual object type, so ALTER TABLE on a view
      // or index (which Postgres allows) still renames the correct object.
      catalog_impl.RenameRelation(
        ax, db, name.GetIdentifierName(), table_name,
        rename_info.new_table_name.GetIdentifierName());
      return;
    }

    case duckdb::AlterTableType::RENAME_CONSTRAINT: {
      auto& rename_info = table_info.Cast<duckdb::RenameConstraintInfo>();

      catalog_impl.ChangeTable(
        ax, db, name.GetIdentifierName(), table_name,
        [&](const catalog::Table& table,
            std::shared_ptr<catalog::Table>& updated) {
          table.RenameConstraint(updated, rename_info.old_name,
                                 rename_info.new_name);
        },
        {.on_type_mismatch = [&](const catalog::Object&) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
            ERR_MSG("constraint \"", rename_info.old_name, "\" for table \"",
                    table_name, "\" does not exist"));
        }});
      return;
    }

    case duckdb::AlterTableType::RENAME_COLUMN: {
      auto& rename_info = table_info.Cast<duckdb::RenameColumnInfo>();

      catalog_impl.ChangeTable(
        ax, db, name.GetIdentifierName(), table_name,
        [&](const catalog::Table& table,
            std::shared_ptr<catalog::Table>& updated) {
          table.RenameColumn(updated, rename_info.old_name.GetIdentifierName(),
                             rename_info.new_name.GetIdentifierName());
        },
        {.on_type_mismatch = [](const catalog::Object&) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
            ERR_MSG("cannot rename columns of a non-table relation"));
        }});
      return;
    }

    case duckdb::AlterTableType::SET_NOT_NULL: {
      auto& not_null_info = table_info.Cast<duckdb::SetNotNullInfo>();

      catalog_impl.ChangeTable(
        ax, db, name.GetIdentifierName(), table_name,
        [&](const catalog::Table& table,
            std::shared_ptr<catalog::Table>& updated) {
          table.SetNotNull(updated,
                           not_null_info.column_name.GetIdentifierName());
        });
      return;
    }

    case duckdb::AlterTableType::DROP_NOT_NULL: {
      auto& not_null_info = table_info.Cast<duckdb::DropNotNullInfo>();

      catalog_impl.ChangeTable(
        ax, db, name.GetIdentifierName(), table_name,
        [&](const catalog::Table& table,
            std::shared_ptr<catalog::Table>& updated) {
          table.DropNotNull(updated,
                            not_null_info.column_name.GetIdentifierName());
        });
      return;
    }

    case duckdb::AlterTableType::SET_DEFAULT: {
      auto& default_info = table_info.Cast<duckdb::SetDefaultInfo>();
      // expression is null for DROP DEFAULT.
      std::shared_ptr<ColumnExpr> expr;
      if (default_info.expression) {
        expr = std::make_shared<ColumnExpr>(default_info.expression->Copy());
      }
      catalog_impl.ChangeTable(ax, db, name.GetIdentifierName(), table_name,
                               [&](const catalog::Table& table,
                                   std::shared_ptr<catalog::Table>& updated) {
                                 table.SetDefault(
                                   updated,
                                   default_info.column_name.GetIdentifierName(),
                                   std::move(expr));
                               });
      return;
    }

    case duckdb::AlterTableType::ADD_CONSTRAINT: {
      auto& add_info = table_info.Cast<duckdb::AddConstraintInfo>();
      // ADD PRIMARY KEY (re-routed here from BindAlterAddIndex) and ADD UNIQUE:
      // map the constraint columns to catalog ids and add the PK/UNIQUE to the
      // catalog Table; the store recreate (catalog.cpp) validates existing
      // rows.
      if (add_info.constraint->type == duckdb::ConstraintType::UNIQUE) {
        auto& unique = add_info.constraint->Cast<duckdb::UniqueConstraint>();
        const bool is_pk = unique.IsPrimaryKey();
        catalog_impl.ChangeTable(
          ax, db, name.GetIdentifierName(), table_name,
          [&](const catalog::Table& table,
              std::shared_ptr<catalog::Table>& updated) {
            std::vector<catalog::Column::Id> ids;
            if (unique.HasIndex()) {
              auto idx = unique.GetIndex().index;
              if (idx >= table.Columns().size()) {
                THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                                ERR_MSG("column does not exist"));
              }
              ids.push_back(table.Columns()[idx].GetId());
            } else {
              for (const auto& cn : unique.GetColumnNames()) {
                auto it = std::ranges::find_if(
                  table.Columns(),
                  [&](const auto& c) { return c.GetName() == cn; });
                if (it == table.Columns().end()) {
                  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                                  ERR_MSG("column does not exist"));
                }
                ids.push_back(it->GetId());
              }
            }
            if (is_pk) {
              table.AddPrimaryKey(updated, std::move(ids),
                                  unique.constraint_name);
              return;
            }
            std::string uq_name = unique.constraint_name;
            if (uq_name.empty()) {
              std::string_view col0;
              if (!ids.empty()) {
                if (const auto* c = table.ColumnById(ids[0])) {
                  col0 = c->GetName();
                }
              }
              uq_name = col0.empty()
                          ? absl::StrCat(table_name, "_key")
                          : absl::StrCat(table_name, "_", col0, "_key");
            }
            table.AddUniqueConstraint(updated, std::move(ids),
                                      std::move(uq_name));
          });
        return;
      }
      if (add_info.constraint->type != duckdb::ConstraintType::CHECK) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("ALTER TABLE ADD CONSTRAINT supports only CHECK, UNIQUE, and "
                  "PRIMARY KEY constraints"));
      }
      auto& check = add_info.constraint->Cast<duckdb::CheckConstraint>();
      std::string cname = check.constraint_name;
      if (cname.empty()) {
        // PostgreSQL-style auto name, matching the CREATE TABLE path.
        auto col = FindConstraintColumn(*check.expression);
        cname = col.empty() ? absl::StrCat(table_name, "_check")
                            : absl::StrCat(table_name, "_", col, "_check");
      }
      auto expr = std::make_shared<ColumnExpr>(check.expression->Copy());
      catalog_impl.ChangeTable(ax, db, name.GetIdentifierName(), table_name,
                               [&](const catalog::Table& table,
                                   std::shared_ptr<catalog::Table>& updated) {
                                 table.AddCheckConstraint(
                                   updated, std::move(cname), std::move(expr));
                               });
      return;
    }

    case duckdb::AlterTableType::ADD_COLUMN: {
      auto& add_info = table_info.Cast<duckdb::AddColumnInfo>();
      const auto& cd = add_info.new_column;
      if (cd.Generated()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG("adding a generated column is not supported"));
      }
      catalog::Column column{ObjectId{}, catalog::NextId(),
                             cd.Name().GetIdentifierName(), cd.Type()};
      if (cd.HasDefaultValue()) {
        column.expr = std::make_shared<ColumnExpr>(cd.DefaultValue().Copy());
      }
      catalog_impl.ChangeTable(ax, db, name.GetIdentifierName(), table_name,
                               [&](const catalog::Table& table,
                                   std::shared_ptr<catalog::Table>& updated) {
                                 table.AddColumn(updated, column,
                                                 add_info.if_column_not_exists);
                               });
      return;
    }

    case duckdb::AlterTableType::REMOVE_COLUMN: {
      auto& remove_info = table_info.Cast<duckdb::RemoveColumnInfo>();
      catalog_impl.DropTableColumn(
        ax, db, name.GetIdentifierName(), table_name,
        remove_info.removed_column.GetIdentifierName(),
        remove_info.if_column_exists);
      return;
    }

    case duckdb::AlterTableType::ADD_FIELD:
    case duckdb::AlterTableType::REMOVE_FIELD:
    case duckdb::AlterTableType::RENAME_FIELD: {
      // Nested-STRUCT field DDL. Native DuckTableEntry turns each of these into
      // an ALTER COLUMN TYPE with a remap_struct(...) USING cast; reuse
      // duckdb's exact by-name remap (a positional/plain type cast silently
      // mis-maps renamed/dropped fields). We already support ALTER COLUMN TYPE
      // USING end-to-end, so route through it.
      const duckdb::vector<duckdb::Identifier>* column_path = nullptr;
      if (table_info.alter_table_type == duckdb::AlterTableType::ADD_FIELD) {
        column_path = &table_info.Cast<duckdb::AddFieldInfo>().column_path;
      } else if (table_info.alter_table_type ==
                 duckdb::AlterTableType::REMOVE_FIELD) {
        column_path = &table_info.Cast<duckdb::RemoveFieldInfo>().column_path;
      } else {
        column_path = &table_info.Cast<duckdb::RenameFieldInfo>().column_path;
      }
      const std::string& root_column = (*column_path)[0].GetIdentifierName();

      auto fld_snapshot = catalog_impl.GetCatalogSnapshot();
      auto table_obj = fld_snapshot->GetTable(
        catalog::NoAccessCheck(), db, name.GetIdentifierName(), table_name);
      if (!table_obj) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_TABLE),
          ERR_MSG("relation \"", table_name, "\" does not exist"));
      }
      auto col_it = absl::c_find_if(
        table_obj->Columns(),
        [&](const catalog::Column& c) { return c.GetName() == root_column; });
      if (col_it == table_obj->Columns().end()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                        ERR_MSG("column \"", root_column, "\" of relation \"",
                                table_name, "\" does not exist"));
      }

      // The remap below ignores IF [NOT] EXISTS, so short-circuit the no-op
      // (Add of an existing field / Drop of a missing one) here.
      auto field_exists = [](const duckdb::LogicalType& root,
                             const duckdb::vector<duckdb::Identifier>& path,
                             size_t path_end, std::string_view leaf) -> bool {
        // Direct child of struct `type` named `name` (case-insensitive), or
        // null.
        auto child = [](const duckdb::LogicalType& type,
                        std::string_view name) -> const duckdb::LogicalType* {
          if (type.id() != duckdb::LogicalTypeId::STRUCT) {
            return nullptr;
          }
          const auto& children = duckdb::StructType::GetChildTypes(type);
          auto found = absl::c_find_if(children, [&](const auto& field) {
            return absl::EqualsIgnoreCase(field.first.GetIdentifierName(),
                                          name);
          });
          return found == children.end() ? nullptr : &found->second;
        };
        // Walk path[1..path_end) into nested structs; bail if a segment is
        // absent.
        const duckdb::LogicalType* current = &root;
        for (size_t depth = 1; depth < path_end; ++depth) {
          current = child(*current, path[depth].GetIdentifierName());
          if (!current) {
            return false;
          }
        }
        return child(*current, leaf) != nullptr;
      };
      // A struct-field op requires the root column to be a struct.
      if (col_it->type.id() != duckdb::LogicalTypeId::STRUCT) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
                        ERR_MSG("field \"", root_column, "\" is not a struct"));
      }
      if (table_info.alter_table_type == duckdb::AlterTableType::ADD_FIELD) {
        const auto& add_field = table_info.Cast<duckdb::AddFieldInfo>();
        if (field_exists(col_it->type, *column_path, column_path->size(),
                         add_field.new_field.Name().GetIdentifierName())) {
          if (add_field.if_field_not_exists) {
            return;
          }
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
            ERR_MSG("field already exists in column \"", root_column, "\""));
        }
      } else if (table_info.alter_table_type ==
                 duckdb::AlterTableType::REMOVE_FIELD) {
        const auto& remove_field = table_info.Cast<duckdb::RemoveFieldInfo>();
        if (!field_exists(col_it->type, *column_path, column_path->size() - 1,
                          column_path->back().GetIdentifierName())) {
          if (remove_field.if_column_exists) {
            return;
          }
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                          ERR_MSG("column or field of \"", root_column,
                                  "\" does not exist in \"", table_name, "\""));
        }
      }

      duckdb::StructFieldRemap remap;
      try {
        if (table_info.alter_table_type == duckdb::AlterTableType::ADD_FIELD) {
          auto& add_field = table_info.Cast<duckdb::AddFieldInfo>();
          remap = duckdb::BuildAddFieldRemap(
            col_it->type, duckdb::Identifier{root_column},
            add_field.column_path, add_field.new_field);
        } else if (table_info.alter_table_type ==
                   duckdb::AlterTableType::REMOVE_FIELD) {
          remap = duckdb::BuildRemoveFieldRemap(col_it->type, *column_path);
        } else {
          remap = duckdb::BuildRenameFieldRemap(
            col_it->type, *column_path,
            table_info.Cast<duckdb::RenameFieldInfo>()
              .new_name.GetIdentifierName());
        }
      } catch (const std::exception& ex) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                        ERR_MSG(ex.what()));
      }

      std::string field_using_sql = remap.remap_expression->ToString();
      catalog_impl.ChangeColumnType(
        ax, db, name.GetIdentifierName(), table_name, root_column,
        std::move(remap.new_type), std::move(field_using_sql));
      return;
    }

    case duckdb::AlterTableType::ALTER_COLUMN_TYPE: {
      auto& type_info = table_info.Cast<duckdb::ChangeColumnTypeInfo>();
      std::string using_sql;
      if (type_info.expression) {
        using_sql = type_info.expression->ToString();
      }
      catalog_impl.ChangeColumnType(
        ax, db, name.GetIdentifierName(), table_name,
        type_info.column_name.GetIdentifierName(), type_info.target_type,
        std::move(using_sql));
      return;
    }

    default:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("this ALTER TABLE operation is not supported"));
  }
}

}  // namespace sdb::connector
