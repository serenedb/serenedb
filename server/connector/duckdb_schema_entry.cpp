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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/expression/cast_expression.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/parsed_data/alter_scalar_function_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_function_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_macro_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/create_type_info.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <iostream>

#include "app/app_server.h"
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
#include "connector/pg_logical_types.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/secondary_index_shard.h"

namespace sdb::connector {
namespace {

// Extracts column names from `[a, b, c]` -- the only accepted shape. Anything
// else (row, single ref, string) throws.
std::vector<std::string> ExtractColumnList(
  std::string_view option_key, const duckdb::ParsedExpression& expr) {
  if (expr.GetExpressionType() != duckdb::ExpressionType::FUNCTION) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("WITH option \"", option_key,
                            "\" expects a list literal, e.g. [col1, col2]"));
  }
  auto& fn = expr.Cast<duckdb::FunctionExpression>();
  if (duckdb::StringUtil::Lower(fn.function_name) != "list_value") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("WITH option \"", option_key,
              "\" expects a list literal [col1, col2, ...], got \"",
              fn.function_name, "\""));
  }
  std::vector<std::string> result;
  result.reserve(fn.children.size());
  for (auto& child : fn.children) {
    if (child->GetExpressionType() != duckdb::ExpressionType::COLUMN_REF) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                      ERR_MSG("WITH option \"", option_key,
                              "\" list element is not a column name"));
    }
    result.push_back(
      child->Cast<duckdb::ColumnRefExpression>().GetColumnName());
  }
  return result;
}

}  // namespace

void ApplyColumnModes(
  std::vector<catalog::Column>& columns,
  const duckdb::case_insensitive_map_t<
    duckdb::unique_ptr<duckdb::ParsedExpression>>& options) {
  static constexpr std::string_view kIndexOnlyKey = "sdb_indexonly";
  auto it = options.find(std::string{kIndexOnlyKey});
  if (it == options.end() || !it->second) {
    return;
  }
  for (auto& name : ExtractColumnList(kIndexOnlyKey, *it->second)) {
    auto col_it = absl::c_find_if(
      columns, [&](const auto& c) { return c.GetName() == name; });
    if (col_it == columns.end()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                      ERR_MSG("WITH option \"", kIndexOnlyKey,
                              "\" references unknown column \"", name, "\""));
    }
    // VIRTUAL columns aren't materialized at insert time, so there's no
    // value for the marker to carry.
    if (col_it->generated_type == catalog::Column::GeneratedType::kVirtual) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
        ERR_MSG("WITH option \"", kIndexOnlyKey,
                "\" cannot be applied to VIRTUAL generated column \"", name,
                "\""));
    }
    col_it->store_mode = catalog::ColumnStoreMode::kIndexOnly;
  }
}

ObjectId SereneDBSchemaEntry::GetDatabaseId() const {
  return catalog.Cast<SereneDBCatalog>().GetDatabaseId();
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::LookupEntry(
  duckdb::CatalogTransaction transaction,
  const duckdb::EntryLookupInfo& lookup_info) {
  auto& conn_ctx = GetSereneDBContext(transaction.GetContext());
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto result = snapshot->GetDuckDBEntryCache().EnsureEntry(
    lookup_info.GetCatalogType(), catalog, *this, GetDatabaseId(), name,
    lookup_info.GetEntryName(), *snapshot);
  if (result || name != StaticStrings::kPgCatalogSchema) {
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
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  snapshot->GetDuckDBEntryCache().ScanEntries(
    type, catalog, *this, GetDatabaseId(), name, callback, *snapshot);
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
  options.name = table_info.table;

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
      return std::ranges::any_of(options.check_constraints, [&](const auto& c) {
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

  auto find_constraint_column =
    [](const duckdb::ParsedExpression& root) -> std::string {
    std::string result;
    bool multiple = false;
    std::function<void(const duckdb::ParsedExpression&)> visit;
    visit = [&](const duckdb::ParsedExpression& expr) {
      if (multiple) {
        return;
      }
      if (expr.GetExpressionType() == duckdb::ExpressionType::COLUMN_REF) {
        auto& name = expr.Cast<duckdb::ColumnRefExpression>().GetColumnName();
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
  };

  // Dedup against duplicate NOT NULL adds; grows on demand because the
  // SERIAL path calls append_not_null mid column loop.
  std::vector<bool> has_not_null;

  auto append_not_null = [&](duckdb::idx_t col_idx) {
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
    auto col_ref = duckdb::make_uniq<duckdb::ColumnRefExpression>(col_name);
    auto is_not_null = duckdb::make_uniq<duckdb::OperatorExpression>(
      duckdb::ExpressionType::OPERATOR_IS_NOT_NULL, std::move(col_ref));
    options.check_constraints.push_back(catalog::CheckConstraint{
      ObjectId{}, catalog::NextId(),
      choose_constraint_name(table_info.table, col_name, "not_null"),
      std::make_shared<ColumnExpr>(std::move(is_not_null))});
  };

  // SERIAL expands to base int + nextval default + NOT NULL. The sequence
  // name and nextval default are resolved by LocalCatalog under its mutex.
  for (auto& col : table_info.columns.Logical()) {
    auto& sdb_col = options.columns.emplace_back(ObjectId{}, catalog::NextId(),
                                                 col.Name(), col.Type());

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
              throw duckdb::CatalogException(
                "column \"%s\" named in key does not exist", pk_name);
            }
            append_pk(it->GetId());
          }
        }
        break;
      }
      case duckdb::ConstraintType::NOT_NULL: {
        auto& nn = constraint->Cast<duckdb::NotNullConstraint>();
        append_not_null(nn.index.index);
        break;
      }
      case duckdb::ConstraintType::CHECK: {
        auto& check = constraint->Cast<duckdb::CheckConstraint>();
        std::string name;
        if (!check.constraint_name.empty()) {
          name = check.constraint_name;
        } else {
          auto col = find_constraint_column(*check.expression);
          name = choose_constraint_name(table_info.table, col, "check");
        }
        options.check_constraints.push_back(catalog::CheckConstraint{
          ObjectId{}, catalog::NextId(), std::move(name),
          std::make_shared<ColumnExpr>(check.expression->Copy())});
        break;
      }
      default:
        break;
    }
  }

  ApplyColumnModes(options.columns, table_info.options);

  auto& catalog_impl =
    catalog::CatalogFeature::instance().Global();
  auto database_id = GetDatabaseId();

  bool if_not_exists =
    create_info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  catalog::CreateTableOperationOptions op_options;

  auto r =
    catalog_impl.CreateTable(database_id, name, std::move(options), op_options);
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (if_not_exists) {
      return nullptr;
    }
    throw duckdb::CatalogException("relation \"%s\" already exists",
                                   table_info.table);
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateIndex(
  duckdb::CatalogTransaction transaction, duckdb::CreateIndexInfo& info,
  duckdb::TableCatalogEntry& table) {
  auto& sdb_table_entry = RequireBaseTable(table);
  auto sdb_table = sdb_table_entry.GetSereneDBTable();

  auto& catalog_feature =
    catalog::CatalogFeature::instance();
  auto& catalog_impl = catalog_feature.Global();
  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto database_id = GetDatabaseId();

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
    throw duckdb::CatalogException("access method \"%s\" does not exist",
                                   info.index_type);
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
      auto col_name = col_ref.GetColumnName();
      const catalog::Column* cat_col = nullptr;
      for (const auto& col : columns) {
        if (col.GetName() == col_name) {
          cat_col = &col;
          break;
        }
      }
      if (!cat_col) {
        throw duckdb::CatalogException("column \"%s\" not found in table",
                                       col_name);
      }
      idx_columns.emplace_back(cat_col, cat_col->GetName());
    } else {
      throw duckdb::CatalogException(
        "Expression-based index columns are not supported");
    }
  }

  bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  Result create_result;
  if (index_type == catalog::ObjectType::InvertedIndex) {
    auto& context = transaction.GetContext();
    auto find_with = [&](std::string_view key) -> const duckdb::Value* {
      auto it = info.options.find(key);
      return it != info.options.end() ? &it->second : nullptr;
    };
    auto resolve_uint = [&](std::string_view key) -> uint32_t {
      if (auto* v = find_with(key)) {
        return v->GetValue<uint32_t>();
      }
      duckdb::Value v;
      auto r = context.TryGetCurrentSetting(std::string{key}, v);
      SDB_ASSERT(r, "missing DB-level default for setting '", key, "'");
      return v.GetValue<uint32_t>();
    };
    catalog::InvertedIndexOptions options{
      .row_group_size = resolve_uint("row_group_size"),
      .norm_row_group_size = resolve_uint("norm_row_group_size"),
      .refresh_interval_ms = resolve_uint("refresh_interval"),
      .compaction_interval_ms = resolve_uint("compaction_interval"),
      .cleanup_interval_step = resolve_uint("cleanup_interval_step"),
    };
    if (auto* v = find_with("optimize_top_k")) {
      auto value =
        v->DefaultCastAs(duckdb::LogicalType::VARCHAR).GetValue<std::string>();
      options.topk_scorer = catalog::ParseScorerExpression(context, value);
    }
    create_result = catalog_impl.CreateInvertedIndex(
      database_id, name, sdb_table->GetName(), info.index_name,
      std::move(idx_columns), std::move(options), /*operation_options=*/{});
  } else {
    bool unique = (info.constraint_type == duckdb::IndexConstraintType::UNIQUE);
    create_result = catalog_impl.CreateSecondaryIndex(
      database_id, name, sdb_table->GetName(), info.index_name,
      std::move(idx_columns), unique, /*operation_options=*/{});
  }

  if (create_result.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (if_not_exists) {
      return nullptr;
    }
    throw duckdb::CatalogException("relation \"%s\" already exists",
                                   info.index_name);
  }
  if (!create_result.ok()) {
    SDB_THROW(std::move(create_result));
  }

  // Start background tasks for inverted indexes
  auto new_snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_index =
    new_snapshot->GetRelation(database_id, name, info.index_name);
  if (catalog_index) {
    auto shard = new_snapshot->GetIndexShard(catalog_index->GetId());
    if (shard && shard->GetType() == catalog::ObjectType::InvertedIndexShard) {
      auto& inverted_shard =
        basics::downCast<search::InvertedIndexShard>(*shard);
      inverted_shard.StartTasks();
      // No backfill yet -- mark creation as finished so background commits
      // register the flush subscription and run periodically.
      inverted_shard.FinishCreation();
    }
  }
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateFunction(
  duckdb::CatalogTransaction transaction, duckdb::CreateFunctionInfo& info) {
  auto& catalog_feature =
    catalog::CatalogFeature::instance();
  auto& catalog_impl = catalog_feature.Global();
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
  auto existing = snapshot->GetFunction(database_id, name, info.name);

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
            throw duckdb::CatalogException(
              "function \"%s\" already exists with same argument types",
              info.name);
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
      ObjectId{}, ObjectId{}, info.name, std::move(merged_info));
    // Always replace=true for the catalog layer since we're replacing
    // the whole PgSqlFunction with the merged version.
    auto r = catalog_impl.CreateFunction(database_id, name, function, true);
    if (!r.ok()) {
      SDB_THROW(std::move(r));
    }
    return nullptr;
  }

  // No existing function -- create new.
  auto function = std::make_shared<catalog::PgSqlFunction>(
    ObjectId{}, ObjectId{}, info.name, std::move(new_macro_info));
  auto r = catalog_impl.CreateFunction(database_id, name, function, false);

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    throw duckdb::CatalogException("relation \"%s\" already exists", info.name);
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateView(
  duckdb::CatalogTransaction transaction, duckdb::CreateViewInfo& info) {
  auto& catalog_feature =
    catalog::CatalogFeature::instance();
  auto& catalog_impl = catalog_feature.Global();
  auto database_id = GetDatabaseId();

  auto view_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateViewInfo>(
      info.Copy());
  auto view = std::make_shared<catalog::PgSqlView>(
    ObjectId{}, ObjectId{}, info.view_name, std::move(view_info));

  bool replace =
    info.on_conflict == duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  auto r = catalog_impl.CreateView(database_id, name, view, replace);

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    if (replace) {
      throw duckdb::CatalogException("\"%s\" is not a view", info.view_name);
    }
    throw duckdb::CatalogException("relation \"%s\" already exists",
                                   info.view_name);
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

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

  catalog::SequenceOptions opts;
  opts.start_value = static_cast<uint64_t>(info.start_value);
  opts.increment = static_cast<uint64_t>(info.increment);
  opts.min_value = static_cast<uint64_t>(info.min_value);
  opts.max_value = static_cast<uint64_t>(info.max_value);
  opts.cycle = info.cycle;

  bool if_not_exists =
    info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;

  auto sequence = std::make_shared<catalog::Sequence>(
    ObjectId{}, ObjectId{}, info.name, opts, ObjectId{});

  auto& catalog_impl =
    catalog::CatalogFeature::instance().Global();
  auto r =
    catalog_impl.CreateSequence(database_id, name, sequence, if_not_exists);
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("relation \"", info.name, "\" already exists"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return nullptr;
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreateTableFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateTableFunctionInfo& info) {
  throw duckdb::NotImplementedException("CREATE TABLE FUNCTION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreateCopyFunction(duckdb::CatalogTransaction transaction,
                                        duckdb::CreateCopyFunctionInfo& info) {
  throw duckdb::NotImplementedException("CREATE COPY FUNCTION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry>
SereneDBSchemaEntry::CreatePragmaFunction(
  duckdb::CatalogTransaction transaction,
  duckdb::CreatePragmaFunctionInfo& info) {
  throw duckdb::NotImplementedException(
    "CREATE PRAGMA FUNCTION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateCollation(
  duckdb::CatalogTransaction transaction, duckdb::CreateCollationInfo& info) {
  throw duckdb::NotImplementedException("CREATE COLLATION through DuckDB");
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBSchemaEntry::CreateType(
  duckdb::CatalogTransaction transaction, duckdb::CreateTypeInfo& info) {
  auto& catalog_feature =
    catalog::CatalogFeature::instance();
  auto& catalog_impl = catalog_feature.Global();
  auto database_id = GetDatabaseId();

  auto type_info =
    duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateTypeInfo>(
      info.Copy());
  auto type = std::make_shared<catalog::PgSqlType>(
    ObjectId{}, ObjectId{}, info.name, std::move(type_info));
  auto r = catalog_impl.CreateType(database_id, name, type);

  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
      return nullptr;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("type \"", info.name, "\" already exists"));
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  return nullptr;
}

void SereneDBSchemaEntry::DropEntry(duckdb::ClientContext& context,
                                    duckdb::DropInfo& info) {
  info.catalog = catalog.GetName();
  info.schema = name;
  DropObject(context, info);
}

namespace {

// Maps the Result of a relation-level rename (table / view / index) to a
// PG-compatible error. DuckDB's binder resolves the relation before we reach
// here, so ERROR_SERVER_DATA_SOURCE_NOT_FOUND / ERROR_SERVER_ILLEGAL_NAME can
// only happen on a race with a concurrent DROP -- handle defensively.
void HandleRenameRelationError(Result r, std::string_view name,
                               std::string_view new_name,
                               std::string_view expected_type) {
  if (r.ok()) {
    return;
  }
  if (r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG("\"", name, "\" is not ",
                            basics::string_utils::GetArticle(expected_type),
                            " ", expected_type));
  }
  if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND) ||
      r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                    ERR_MSG("relation \"", name, "\" does not exist"));
  }
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_TABLE),
                    ERR_MSG("relation \"", new_name, "\" already exists"));
  }
  SDB_THROW(std::move(r));
}

}  // namespace

void SereneDBSchemaEntry::Alter(duckdb::CatalogTransaction transaction,
                                duckdb::AlterInfo& info) {
  auto& catalog_impl =
    catalog::CatalogFeature::instance().Global();
  auto db = GetDatabaseId();

  if (info.type == duckdb::AlterType::ALTER_SCALAR_FUNCTION) {
    auto& fn_info = info.Cast<duckdb::AlterScalarFunctionInfo>();
    if (fn_info.alter_scalar_function_type !=
        duckdb::AlterScalarFunctionType::RENAME_SCALAR_FUNCTION) {
      throw duckdb::NotImplementedException("ALTER FUNCTION through DuckDB");
    }
    auto& rename_info = fn_info.Cast<duckdb::RenameScalarFunctionInfo>();

    Result r =
      catalog_impl.RenameFunction(db, name, info.name, rename_info.new_name);

    if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND) ||
        r.is(ERROR_SERVER_ILLEGAL_NAME)) {
      const bool missing_ok =
        info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL;
      if (!missing_ok) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_FUNCTION),
          ERR_MSG("could not find a function named \"", info.name, "\""));
      }
      return;
    }
    if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DUPLICATE_TABLE),
        ERR_MSG("relation \"", rename_info.new_name, "\" already exists"));
    }
    if (!r.ok()) {
      SDB_THROW(std::move(r));
    }
    return;
  }

  if (info.type == duckdb::AlterType::ALTER_VIEW) {
    auto& view_info = info.Cast<duckdb::AlterViewInfo>();
    if (view_info.alter_view_type != duckdb::AlterViewType::RENAME_VIEW) {
      throw duckdb::NotImplementedException("ALTER VIEW through DuckDB");
    }
    auto& rename_info = view_info.Cast<duckdb::RenameViewInfo>();
    Result r =
      catalog_impl.RenameView(db, name, info.name, rename_info.new_view_name);
    HandleRenameRelationError(std::move(r), info.name,
                              rename_info.new_view_name, "view");
    return;
  }

  if (info.type != duckdb::AlterType::ALTER_TABLE) {
    throw duckdb::NotImplementedException("ALTER through DuckDB");
  }

  auto& table_info = info.Cast<duckdb::AlterTableInfo>();
  auto table_name = info.name;

  switch (table_info.alter_table_type) {
    case duckdb::AlterTableType::DROP_CONSTRAINT: {
      auto& drop_info = table_info.Cast<duckdb::DropConstraintInfo>();

      Result r = catalog_impl.ChangeTable(
        db, name, table_name,
        [&](const catalog::Table& table,
            std::shared_ptr<catalog::Table>& updated) {
          return table.DropCheckConstraint(updated, drop_info.constraint_name);
        });

      if (r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH)) {
        auto actual_type =
          basics::string_utils::GetPluralFormLowerCase(r.errorMessage());
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
          ERR_MSG("ALTER action DROP CONSTRAINT cannot be performed on "
                  "relation \"",
                  table_name, "\""),
          ERR_DETAIL("This operation is not supported for ", actual_type, "."));
      }

      if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND)) {
        // Table not found -- DuckDB's binder already handles IF EXISTS,
        // so if we reach here the table should exist.
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_TABLE),
          ERR_MSG("relation \"", table_name, "\" does not exist"));
      }

      if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
        if (!drop_info.if_constraint_not_found) {
          THROW_SQL_ERROR(
            ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
            ERR_MSG("constraint \"", drop_info.constraint_name,
                    "\" of relation \"", table_name, "\" does not exist"));
        }
        return;
      }

      if (!r.ok()) {
        SDB_THROW(std::move(r));
      }
      return;
    }

    case duckdb::AlterTableType::RENAME_TABLE: {
      auto& rename_info = table_info.Cast<duckdb::RenameTableInfo>();
      // RenameRelation routes by actual object type, so ALTER TABLE on a view
      // or index (which Postgres allows) still renames the correct object.
      Result r = catalog_impl.RenameRelation(db, name, table_name,
                                             rename_info.new_table_name);
      HandleRenameRelationError(std::move(r), table_name,
                                rename_info.new_table_name, "table");
      return;
    }

    case duckdb::AlterTableType::RENAME_CONSTRAINT: {
      auto& rename_info = table_info.Cast<duckdb::RenameConstraintInfo>();

      Result r = catalog_impl.ChangeTable(
        db, name, table_name,
        [&](const catalog::Table& table,
            std::shared_ptr<catalog::Table>& updated) {
          return table.RenameConstraint(updated, rename_info.old_name,
                                        rename_info.new_name);
        });

      if (r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH) ||
          r.is(ERROR_SERVER_ILLEGAL_NAME)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
          ERR_MSG("constraint \"", rename_info.old_name, "\" for table \"",
                  table_name, "\" does not exist"));
      }

      if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_TABLE),
          ERR_MSG("relation \"", table_name, "\" does not exist"));
      }

      if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
          ERR_MSG("constraint \"", rename_info.new_name, "\" for relation \"",
                  table_name, "\" already exists"));
      }

      if (!r.ok()) {
        SDB_THROW(std::move(r));
      }
      return;
    }

    case duckdb::AlterTableType::RENAME_COLUMN: {
      auto& rename_info = table_info.Cast<duckdb::RenameColumnInfo>();

      Result r = catalog_impl.ChangeTable(
        db, name, table_name,
        [&](const catalog::Table& table,
            std::shared_ptr<catalog::Table>& updated) {
          return table.RenameColumn(updated, rename_info.old_name,
                                    rename_info.new_name);
        });

      if (r.is(ERROR_SERVER_OBJECT_TYPE_MISMATCH)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
          ERR_MSG("cannot rename columns of a non-table relation"));
      }

      if (r.is(ERROR_SERVER_DATA_SOURCE_NOT_FOUND)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_TABLE),
          ERR_MSG("relation \"", table_name, "\" does not exist"));
      }

      if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
          ERR_MSG("column \"", rename_info.old_name, "\" does not exist"));
      }

      if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DUPLICATE_COLUMN),
          ERR_MSG("column \"", rename_info.new_name, "\" of relation \"",
                  table_name, "\" already exists"));
      }

      if (!r.ok()) {
        SDB_THROW(std::move(r));
      }
      return;
    }

    default:
      throw duckdb::NotImplementedException("ALTER TABLE through DuckDB");
  }
}

}  // namespace sdb::connector
