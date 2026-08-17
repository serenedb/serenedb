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

#include "connector/duckdb_reindex_function.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/comparison_expression.hpp>
#include <duckdb/parser/expression/conjunction_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/expression/star_expression.hpp>
#include <duckdb/parser/expression/subquery_expression.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parser.hpp>
#include <duckdb/parser/query_node/delete_query_node.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/delete_statement.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/column_data_ref.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/search/all_filter.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "basics/primary_key.hpp"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/role.h"
#include "catalog/view.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/file_manifest.h"
#include "connector/search_remove_filter.hpp"
#include "connector/view_fast_path.h"
#include "core/deletes/iceberg_equality_delete.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception.h"
#include "pg/sql_exception_macro.h"
#include "planning/iceberg_multi_file_list.hpp"
#include "search/task.h"
#include "search/tick_domain.h"

namespace sdb::connector {
namespace {

enum class ReindexAction {
  UpToDate,
  Delta,
  Rebuild,
};

std::string ActionName(ReindexAction action) {
  switch (action) {
    case ReindexAction::UpToDate:
      return "up_to_date";
    case ReindexAction::Delta:
      return "delta";
    case ReindexAction::Rebuild:
      return "rebuild";
  }
  return {};
}

struct ReindexOutcome {
  ReindexAction action = ReindexAction::Rebuild;
  int64_t files_added = 0;
  int64_t files_changed = 0;
  int64_t files_removed = 0;
  // changed - rescanned = files refreshed in place (masks / removes).
  int64_t files_rescanned = 0;
};

struct ReindexTarget {
  std::string name;
  std::string database;
  std::string schema;
  std::shared_ptr<catalog::Database> db;
  std::shared_ptr<catalog::Index> index;
  std::shared_ptr<catalog::Object> relation;
};

// Resolution plus every REINDEX precondition error.
ReindexTarget ResolveTarget(ConnectionContext& conn_ctx,
                            const catalog::Snapshot& snapshot,
                            const std::string& name,
                            const std::string& schema_p,
                            const std::string& catalog_p) {
  if (name.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("serenedb_reindex requires an index name"));
  }
  ReindexTarget target;
  target.name = name;
  target.database = catalog_p.empty() ? conn_ctx.GetDatabase() : catalog_p;
  target.schema = schema_p.empty() ? conn_ctx.GetCurrentSchema() : schema_p;
  target.db = snapshot.GetDatabase(target.database);
  if (!target.db) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_CATALOG_NAME),
      ERR_MSG("database \"", target.database, "\" does not exist"));
  }
  auto resolved = snapshot.GetRelation(catalog::NoAccessCheck(),
                                       target.db->GetId(), target.schema, name);
  if (!resolved || resolved->GetType() != catalog::ObjectType::InvertedIndex) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("index \"", name, "\" does not exist"));
  }
  target.index = std::static_pointer_cast<catalog::Index>(std::move(resolved));
  target.relation = snapshot.GetObject(target.index->GetRelationId());
  if (!target.relation ||
      target.relation->GetType() != catalog::ObjectType::View) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("REINDEX is only supported for view-backed inverted indexes"));
  }
  // PG semantics: REINDEX needs MAINTAIN (same as VACUUM). Enforced here --
  // the passes run on internal connections and reach no other gate.
  snapshot.RequireAccess(conn_ctx.GetRoleId(), *target.relation,
                         catalog::AclMode::Maintain);
  return target;
}

// Internal connection impersonating the caller; notices forward to the
// caller on scope exit.
class PassConnection {
 public:
  PassConnection(duckdb::ClientContext& context, ConnectionContext& caller,
                 const ReindexTarget& target)
    : _caller{caller},
      _conn{*context.db},
      _ctx{std::make_shared<ConnectionContext>(
        *_conn.context, caller.user(), caller.GetRoleId(), target.database,
        target.db->GetId(), target.db, nullptr, nullptr, caller.GetBackendPid(),
        nullptr)} {
    SereneDBClientState::Register(*_conn.context, _ctx);
    _conn.context->session_user = caller.user();
    _conn.context->config.user_settings = context.config.user_settings;
  }
  ~PassConnection() {
    _ctx->ConsumeNotices(
      [&](auto& notice) { _caller.AddNotice(std::move(notice)); });
  }

  // The pass statement carries the index's persisted DEFINITION (positions
  // resolved against the view's CURRENT names, persisted predicate), so it
  // survives renames and ALTER INDEX SET. Throws on failure.
  void RunPass(const ReindexTarget& target,
               duckdb::unique_ptr<SereneDBCreateIndexInfo> info,
               std::string_view what) {
    info->index_type = "inverted";
    const auto& inverted =
      basics::downCast<const catalog::InvertedIndex>(*target.index);
    const auto& view_info =
      basics::downCast<const catalog::PgSqlView>(*target.relation).GetInfo();
    SDB_ASSERT(info->parsed_expressions.empty());
    for (const auto col : inverted.GetReferencedColumns()) {
      if (col >= view_info.names.size()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
          ERR_MSG("REINDEX of \"", target.name,
                  "\": the view no longer has the column at position ", col,
                  " the index references; drop and recreate the index"));
      }
      info->parsed_expressions.push_back(
        duckdb::make_uniq<duckdb::ColumnRefExpression>(view_info.names[col]));
    }
    if (const auto* predicate = inverted.Predicate()) {
      auto exprs =
        duckdb::Parser::ParseExpressionList(predicate->pretty_printed);
      SDB_ASSERT(exprs.size() == 1);
      info->where_clause = std::move(exprs[0]);
    }
    info->table = duckdb::Identifier{std::string{target.relation->GetName()}};
    info->SetSchema(duckdb::Identifier{std::string{target.schema}});
    info->SetCatalog(duckdb::Identifier{std::string{target.database}});
    auto statement = duckdb::make_uniq<duckdb::CreateStatement>();
    statement->info = std::move(info);
    auto result = _conn.Query(std::move(statement));
    if (result->HasError()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("REINDEX ", what, " of \"", target.name,
                              "\" failed: ", result->GetError()));
    }
  }

  duckdb::unique_ptr<duckdb::QueryResult> Query(
    duckdb::unique_ptr<duckdb::SQLStatement> statement) {
    return _conn.Query(std::move(statement));
  }

 private:
  ConnectionContext& _caller;
  duckdb::Connection _conn;
  std::shared_ptr<ConnectionContext> _ctx;
};

// The condition runs as a real `DELETE FROM <index> WHERE ...` plan: the
// index's own scan evaluates the rows, the sink removes the matched
// (file, row) pks. False = the query cannot run (rescan is the fallback).
bool RunPkScanRemoves(duckdb::ClientContext& context,
                      ConnectionContext& conn_ctx, const ReindexTarget& target,
                      duckdb::unique_ptr<duckdb::ParsedExpression> condition) {
  // Statement object, no SQL text: values travel verbatim.
  auto statement = duckdb::make_uniq<duckdb::DeleteStatement>();
  auto table = duckdb::make_uniq<duckdb::BaseTableRef>();
  table->SetQualifiedName(duckdb::Identifier{std::string{target.database}},
                          duckdb::Identifier{std::string{target.schema}},
                          duckdb::Identifier{std::string{target.name}});
  statement->node->table = std::move(table);
  statement->node->condition = std::move(condition);
  // PlanDelete admits the index target only on an internal connection.
  PassConnection pass{context, conn_ctx, target};
  auto result = pass.Query(std::move(statement));
  return !result->HasError();
}

// Delete kind 3 -- equality deletes (iceberg): covered files group by
// their applicable-delete set; ALL groups merge into ONE
// `DELETE FROM <index> WHERE (<rows> AND <files>) OR ...`.

// A KEEP filter (col != v / col IS NOT NULL) parsed: view output position
// plus the deleted value (NULL value = IS NULL).
struct EqConjunct {
  uint64_t pos;
  duckdb::Value value;
};
using EqRow = std::vector<EqConjunct>;

// One applicable-delete set and the covered files it applies to.
struct EqGroup {
  std::vector<const IcebergObserve::EqCovered*> files;
  duckdb::vector<duckdb::reference<const duckdb::IcebergEqualityDeleteFile>>
    deletes;
};

// Deterministic enumeration order makes the pointer sequence a stable key.
std::vector<EqGroup> GroupCoveredFiles(const IcebergObserve& observe) {
  std::vector<EqGroup> groups;
  containers::FlatHashMap<std::vector<const duckdb::IcebergEqualityDeleteFile*>,
                          size_t>
    group_of;
  for (const auto& covered : observe.eq_covered) {
    const auto entry =
      observe.Deletes().list->GetManifestEntry(covered.listing_idx);
    auto delete_files = observe.Deletes().list->GetEqualityDeletesForFile(
      entry, observe.SequenceNumber());
    const size_t total_rows = absl::c_accumulate(
      delete_files, size_t{0},
      [](size_t n, auto& df) { return n + df.get().rows.size(); });
    if (total_rows == 0) {
      // Seq moved but nothing applies per the spec (delete landed in the
      // same snapshot as the file).
      continue;
    }
    std::vector<const duckdb::IcebergEqualityDeleteFile*> group_key;
    group_key.reserve(delete_files.size());
    for (const auto& df : delete_files) {
      group_key.push_back(&df.get());
    }
    const auto [it, inserted] =
      group_of.emplace(std::move(group_key), groups.size());
    if (inserted) {
      groups.push_back({{}, std::move(delete_files)});
    }
    groups[it->second].files.push_back(&covered);
  }
  return groups;
}

// Empty = no road (non-constant filters, a column outside the view output,
// or no rows). Only IS-NULL-carrying rows are deduplicated: they become an
// OR branch each; NULL-free rows feed an IN semi-join, repeats can't hurt.
std::vector<EqRow> ParseEqRows(
  const duckdb::vector<
    duckdb::reference<const duckdb::IcebergEqualityDeleteFile>>& delete_files,
  const IcebergObserve& observe, const ViewFastPath& fast_path,
  const duckdb::CreateViewInfo& view_info) {
  // Field id -> view output position.
  const auto resolve_view_pos =
    [&](int32_t field_id) -> std::optional<uint64_t> {
    const auto& columns = observe.GlobalColumns();
    const auto source = absl::c_find_if(
      columns, [&](const duckdb::MultiFileColumnDefinition& column) {
        return !column.identifier.IsNull() &&
               column.identifier.GetValue<int32_t>() == field_id;
      });
    if (source == columns.end()) {
      return std::nullopt;
    }
    uint64_t pos = source - columns.begin();
    if (!fast_path.projection_columns.empty()) {
      const auto name = source->name.GetIdentifierName();
      const auto it = absl::c_find_if(
        fast_path.projection_columns,
        [&](const auto& p) { return duckdb::StringUtil::CIEquals(p, name); });
      if (it == fast_path.projection_columns.end()) {
        return std::nullopt;
      }
      pos = it - fast_path.projection_columns.begin();
    }
    if (pos >= view_info.types.size()) {
      return std::nullopt;
    }
    return pos;
  };
  std::vector<EqRow> rows;
  containers::FlatHashSet<std::string> seen_null_rows;
  std::string row_key;
  for (const auto& delete_file : delete_files) {
    for (const auto& row : delete_file.get().rows) {
      EqRow parsed;
      parsed.reserve(row.filters.size());
      bool has_null = false;
      for (const auto& [field_id, keep] : row.filters) {
        const auto pos = resolve_view_pos(field_id);
        if (!pos) {
          return {};
        }
        if (keep->GetExpressionType() ==
            duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) {
          parsed.push_back({*pos, duckdb::Value{}});
          has_null = true;
        } else if (keep->GetExpressionType() ==
                   duckdb::ExpressionType::COMPARE_NOTEQUAL) {
          const auto& right = duckdb::BoundComparisonExpression::Right(
            keep->Cast<duckdb::BoundFunctionExpression>());
          if (right.GetExpressionType() !=
              duckdb::ExpressionType::VALUE_CONSTANT) {
            return {};
          }
          auto value = right.Cast<duckdb::BoundConstantExpression>().GetValue();
          if (value.IsNull()) {
            // Would read as IS NULL below and over-delete.
            return {};
          }
          parsed.push_back({*pos, std::move(value)});
        } else {
          return {};
        }
      }
      absl::c_sort(parsed, [](const auto& lhs, const auto& rhs) {
        return lhs.pos < rhs.pos;
      });
      if (has_null) {
        row_key.clear();
        for (const auto& conjunct : parsed) {
          absl::StrAppend(&row_key, conjunct.pos,
                          conjunct.value.IsNull()
                            ? "!null"
                            : absl::StrCat("=", conjunct.value.ToString()),
                          ";");
        }
        if (!seen_null_rows.emplace(row_key).second) {
          continue;
        }
      }
      rows.push_back(std::move(parsed));
    }
  }
  return rows;
}

duckdb::unique_ptr<duckdb::ParsedExpression> CombineExprs(
  duckdb::ExpressionType type,
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> exprs) {
  if (exprs.size() == 1) {
    return std::move(exprs[0]);
  }
  return duckdb::make_uniq<duckdb::ConjunctionExpression>(type,
                                                          std::move(exprs));
}

// NULL-free rows ride `(cols...) IN (SELECT * FROM <materialized>)` per
// uniform column set -- one ColumnDataCollection (no per-row expression
// nodes), planned as a hash semi-join. IS-NULL rows cannot ride IN (NULL
// never matches) and stay plain OR branches.
duckdb::unique_ptr<duckdb::ParsedExpression> BuildEqWhere(
  const std::vector<EqRow>& rows, const duckdb::CreateViewInfo& view_info) {
  const auto column_ref = [&](uint64_t pos) {
    return duckdb::make_uniq<duckdb::ColumnRefExpression>(view_info.names[pos]);
  };
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> branches;
  std::vector<const EqRow*> in_rows;
  in_rows.reserve(rows.size());
  for (const auto& row : rows) {
    if (absl::c_any_of(row, [](const EqConjunct& conjunct) {
          return conjunct.value.IsNull();
        })) {
      duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> conjuncts;
      conjuncts.reserve(row.size());
      for (const auto& conjunct : row) {
        if (!conjunct.value.IsNull()) {
          conjuncts.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
            duckdb::ExpressionType::COMPARE_EQUAL, column_ref(conjunct.pos),
            duckdb::make_uniq<duckdb::ConstantExpression>(conjunct.value)));
        } else {
          conjuncts.push_back(duckdb::make_uniq<duckdb::OperatorExpression>(
            duckdb::ExpressionType::OPERATOR_IS_NULL,
            column_ref(conjunct.pos)));
        }
      }
      branches.push_back(CombineExprs(duckdb::ExpressionType::CONJUNCTION_AND,
                                      std::move(conjuncts)));
      continue;
    }
    in_rows.push_back(&row);
  }
  // A row's column set IS its position sequence: stable-sort brings equal
  // sets together, every consecutive run becomes one IN branch.
  const auto pos_less = [](const EqConjunct& lhs, const EqConjunct& rhs) {
    return lhs.pos < rhs.pos;
  };
  const auto same_columns = [](const EqRow& lhs, const EqRow& rhs) {
    return absl::c_equal(
      lhs, rhs,
      [](const EqConjunct& l, const EqConjunct& r) { return l.pos == r.pos; });
  };
  absl::c_stable_sort(in_rows, [&](const EqRow* lhs, const EqRow* rhs) {
    return absl::c_lexicographical_compare(*lhs, *rhs, pos_less);
  });
  for (size_t begin = 0; begin < in_rows.size();) {
    const auto& head = *in_rows[begin];
    size_t end = begin + 1;
    while (end < in_rows.size() && same_columns(head, *in_rows[end])) {
      ++end;
    }
    duckdb::vector<duckdb::LogicalType> types;
    duckdb::vector<duckdb::Identifier> names;
    types.reserve(head.size());
    names.reserve(head.size());
    for (const auto& conjunct : head) {
      types.push_back(view_info.types[conjunct.pos]);
      names.push_back(view_info.names[conjunct.pos]);
    }
    auto& allocator = duckdb::Allocator::DefaultAllocator();
    auto collection =
      duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator, types);
    duckdb::DataChunk chunk;
    chunk.Initialize(allocator, types);
    for (; begin < end; ++begin) {
      const auto row_idx = chunk.size();
      for (size_t c = 0; c < head.size(); ++c) {
        const auto& value = (*in_rows[begin])[c].value;
        chunk.SetValue(
          c, row_idx,
          value.type() == types[c] ? value : value.DefaultCastAs(types[c]));
      }
      chunk.SetCardinality(row_idx + 1);
      if (chunk.size() == STANDARD_VECTOR_SIZE) {
        collection->Append(chunk);
        chunk.Reset();
      }
    }
    if (chunk.size() != 0) {
      collection->Append(chunk);
    }
    auto values_ref = duckdb::make_uniq<duckdb::ColumnDataRef>(
      std::move(collection), std::move(names));
    // The subquery's star expansion qualifies columns by the binding alias;
    // an anonymous ref has none and the binder throws.
    values_ref->alias = duckdb::Identifier{"eq_delete_rows"};
    auto select_node = duckdb::make_uniq<duckdb::SelectNode>();
    select_node->select_list.push_back(
      duckdb::make_uniq<duckdb::StarExpression>());
    select_node->from_table = std::move(values_ref);
    auto select = duckdb::make_uniq<duckdb::SelectStatement>();
    select->node = std::move(select_node);

    duckdb::unique_ptr<duckdb::ParsedExpression> keys;
    if (head.size() == 1) {
      keys = column_ref(head[0].pos);
    } else {
      duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> parts;
      parts.reserve(head.size());
      for (const auto& conjunct : head) {
        parts.push_back(column_ref(conjunct.pos));
      }
      keys =
        duckdb::make_uniq<duckdb::FunctionExpression>("row", std::move(parts));
    }
    auto in_expr = duckdb::make_uniq<duckdb::SubqueryExpression>();
    in_expr->GetSubqueryTypeMutable() = duckdb::SubqueryType::ANY;
    in_expr->GetComparisonTypeMutable() = duckdb::ExpressionType::COMPARE_EQUAL;
    in_expr->SubqueryMutable() = std::move(select);
    in_expr->GetChildMutable() = std::move(keys);
    branches.push_back(std::move(in_expr));
  }
  return CombineExprs(duckdb::ExpressionType::CONJUNCTION_OR,
                      std::move(branches));
}

// (generated_pk).file_index IN (covered...)
duckdb::unique_ptr<duckdb::ParsedExpression> BuildFileScope(
  const std::vector<const IcebergObserve::EqCovered*>& files) {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> in_children;
  in_children.reserve(files.size() + 1);
  in_children.push_back(duckdb::make_uniq<duckdb::OperatorExpression>(
    duckdb::ExpressionType::STRUCT_EXTRACT,
    duckdb::make_uniq<duckdb::ColumnRefExpression>(
      duckdb::Identifier{"generated_pk"}),
    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value{
      duckdb::StructType::GetChildName(FileIndexRowNumberStructType(), 0)
        .GetIdentifierName()})));
  for (const auto* covered : files) {
    in_children.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(
      duckdb::Value::UBIGINT(covered->file_id)));
  }
  return duckdb::make_uniq<duckdb::OperatorExpression>(
    duckdb::ExpressionType::COMPARE_IN, std::move(in_children));
}

// False = no road -- the caller demotes the covered files to rescans.
bool RunEqualityRemoves(duckdb::ClientContext& context,
                        ConnectionContext& conn_ctx,
                        const ReindexTarget& target, const Source& src,
                        IcebergObserve& observe) {
  observe.EnsureDeletesProcessed();
  const auto& view_info =
    basics::downCast<const catalog::PgSqlView>(*target.relation).GetInfo();
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> branches;
  for (const auto& group : GroupCoveredFiles(observe)) {
    const auto rows =
      ParseEqRows(group.deletes, observe, src.fast_path, view_info);
    if (rows.empty()) {
      return false;
    }
    branches.push_back(duckdb::make_uniq<duckdb::ConjunctionExpression>(
      duckdb::ExpressionType::CONJUNCTION_AND, BuildEqWhere(rows, view_info),
      BuildFileScope(group.files)));
  }
  if (branches.empty()) {
    return true;
  }
  return RunPkScanRemoves(
    context, conn_ctx, target,
    CombineExprs(duckdb::ExpressionType::CONJUNCTION_OR, std::move(branches)));
}

// Every eq-covered file demotes to remove-and-rescan (its masks drop --
// the rescan supersedes them).
void DemoteEqCoveredToRescan(const Source& src, FileDiff& files,
                             IcebergObserve& observe) {
  for (auto& covered : observe.eq_covered) {
    std::erase_if(observe.del_masks, [&](const auto& mask) {
      return mask.file_id == covered.file_id;
    });
    files.del_files.push_back(covered.file_id);
    files.scan.push_back(std::move(covered.live));
  }
  observe.eq_covered.clear();
  // Restore listing order: the pass relies on scan order == assigned-id
  // order.
  containers::FlatHashMap<std::string_view, size_t> listing_pos;
  listing_pos.reserve(src.files.size());
  for (size_t i = 0; i < src.files.size(); ++i) {
    listing_pos.emplace(src.files[i].path, i);
  }
  absl::c_sort(files.scan, [&](const auto& a, const auto& b) {
    return listing_pos.find(a.path)->second < listing_pos.find(b.path)->second;
  });
}

// The referenced rows/buckets live on the observe's DeleteMask, alive
// until the remove commits.
SearchRemovePrefixFilter::DeadRowCursor MakeRowsCursor(
  const std::vector<int64_t>& rows) {
  return [&rows,
          idx = size_t{0}](int64_t min_row) mutable -> std::optional<int64_t> {
    idx =
      std::lower_bound(rows.begin() + idx, rows.end(), min_row) - rows.begin();
    if (idx == rows.size()) {
      return std::nullopt;
    }
    return rows[idx];
  };
}

// Buckets ascend by (high, low) = ascending row.
SearchRemovePrefixFilter::DeadRowCursor MakeDvCursor(
  const std::vector<std::pair<int32_t, roaring::Roaring>>& buckets) {
  return [&buckets, bucket = size_t{0}](
           int64_t min_row) mutable -> std::optional<int64_t> {
    while (bucket < buckets.size()) {
      const auto base = static_cast<int64_t>(buckets[bucket].first) << 32;
      if (min_row >= base + (int64_t{1} << 32)) {
        ++bucket;
        continue;
      }
      const uint32_t low = min_row <= base ? 0 : min_row - base;
      auto it = buckets[bucket].second.begin();
      if (!it.move_equalorlarger(low)) {
        ++bucket;
        continue;
      }
      return base | *it;
    }
    return std::nullopt;
  };
}

std::string PkFilePrefix(uint64_t file_id) {
  std::string key;
  primary_key::AppendUnsigned(key, file_id);
  return key;
}

// Delete kind 1 -- deleted files: one prefix entry per file. nullptr =
// none.
std::shared_ptr<SearchRemovePrefixFilter> BuildDeletedFilesRemove(
  std::vector<uint64_t>& del_files) {
  if (del_files.empty()) {
    return nullptr;
  }
  absl::c_sort(del_files);
  auto remove =
    std::make_shared<SearchRemovePrefixFilter>(catalog::term_dict::kPKFieldId);
  for (const auto id : del_files) {
    remove->AddFile(PkFilePrefix(id));
  }
  return remove;
}

// Delete kind 2 -- changed bitmasks (iceberg): kept files whose dead rows
// leapfrog the cursors against the sorted terms. nullptr = nothing to
// remove (row-less masks are eq-covered files).
std::shared_ptr<SearchRemovePrefixFilter> BuildMaskRemove(
  const std::vector<IcebergObserve::DeleteMask>& del_masks) {
  std::vector<const IcebergObserve::DeleteMask*> masks;
  masks.reserve(del_masks.size());
  for (const auto& mask : del_masks) {
    if (!mask.rows.empty() || !mask.dv.empty()) {
      masks.push_back(&mask);
    }
  }
  if (masks.empty()) {
    return nullptr;
  }
  absl::c_sort(masks, [](const auto* a, const auto* b) {
    return a->file_id < b->file_id;
  });
  auto remove =
    std::make_shared<SearchRemovePrefixFilter>(catalog::term_dict::kPKFieldId);
  for (const auto* mask : masks) {
    remove->AddFileRows(
      PkFilePrefix(mask->file_id),
      mask->dv.empty() ? MakeRowsCursor(mask->rows) : MakeDvCursor(mask->dv));
  }
  return remove;
}

// Old manifest minus dying files, plus fresh ids for the scans: a scan
// file's id is `base + its listing ordinal` (recorded by DiffListing), so
// the pass emits ids as plain `file_index + base`. Ids gap where the
// listing holds unchanged files -- only uniqueness matters. `file_base`
// returns the base for the pass statement.
std::shared_ptr<search::FileManifest> BuildNextManifest(
  const search::FileManifest& manifest, FileDiff& files, uint64_t& file_base) {
  auto manifest_next = std::make_shared<search::FileManifest>();
  manifest_next->entries.reserve(manifest.entries.size() + files.scan.size());
  containers::FlatHashSet<uint64_t> dead_ids{files.del_files.begin(),
                                             files.del_files.end()};
  file_base = 0;
  for (const auto& [id, entry] : manifest.entries) {
    if (dead_ids.contains(id)) {
      continue;
    }
    file_base = std::max(file_base, id + 1);
    manifest_next->entries.emplace(id, entry);
  }
  for (auto& entry : files.scan) {
    entry.file_id += file_base;
    manifest_next->entries.emplace(entry.file_id, entry);
  }
  manifest_next->version = files.version;
  return manifest_next;
}

// The three delete kinds, then rescan only the affected files through ONE
// pass into the existing storage; Finalize publishes docs + manifest as
// one flip. The caller holds the reindex claim.
template<typename Observe>
void RunDelta(duckdb::ClientContext& context, ConnectionContext& conn_ctx,
              const ReindexTarget& target, const Source& src, FileDiff& files,
              Observe& observe, const search::FileManifest& manifest,
              search::InvertedIndexStorage& storage) {
  constexpr bool kIceberg = std::is_same_v<Observe, IcebergObserve>;
  if constexpr (kIceberg) {
    // Kind 3 first: a group with no road demotes its covered files into
    // kind 1's input.
    if (!observe.eq_covered.empty() &&
        !RunEqualityRemoves(context, conn_ctx, target, src, observe)) {
      DemoteEqCoveredToRescan(src, files, observe);
    }
  }
  uint64_t file_base = 0;
  auto manifest_next = BuildNextManifest(manifest, files, file_base);
  auto file_removes = BuildDeletedFilesRemove(files.del_files);
  std::shared_ptr<SearchRemovePrefixFilter> mask_removes;
  if constexpr (kIceberg) {
    mask_removes = BuildMaskRemove(observe.del_masks);
  }

  // The removes commit first as their own transaction; the pass's docs land
  // above them (the pass transactions pull domain ticks). A crash between
  // the two loses the removed rows until the next tick: the manifest
  // version only moves at the end, so the tick re-runs the delta.
  if (file_removes || mask_removes) {
    auto trx = storage.GetTransaction();
    trx.SetTickSource([](uint64_t count) {
      return search::TickDomain::Instance().Advance(count);
    });
    if (file_removes) {
      trx.Remove(std::move(file_removes));
    }
    if (mask_removes) {
      trx.Remove(std::move(mask_removes));
    }
    if (!trx.Commit()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("REINDEX delta of \"", target.name,
                              "\": failed to commit the removes"));
    }
  }

  if (files.scan.empty()) {
    // Removes-only tick: no pass, publish directly -- REINDEX returning
    // means the deletions are visible.
    storage.SetFileManifest(std::move(manifest_next));
    auto code = search::RefreshResult::Undefined;
    if (auto refreshed = storage.RefreshUnsafe(/*wait=*/true, nullptr, code);
        !refreshed.res.ok()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("REINDEX delta of \"", target.name,
                "\": publish failed: ", refreshed.res.ToString()));
    }
    return;
  }

  // Scan road: ONE statement for the whole scan set, bound against the
  // REAL view (the bind hook narrows the leaf's file list, so the reader
  // still applies every delete flavor). The pass commits like an ordinary
  // CREATE INDEX; Finalize publishes manifest_next.
  PassConnection pass_conn{context, conn_ctx, target};
  auto info = duckdb::make_uniq<SereneDBCreateIndexInfo>();
  info->source_index = target.index->GetId();
  info->delta_file_base = file_base;
  info->delta_files.reserve(files.scan.size());
  for (const auto& file : files.scan) {
    info->delta_files.push_back(file.path);
  }
  info->manifest = std::move(manifest_next);
  // The narrowed bind would claim a single-file pk: carry the REAL type.
  info->generated_pk_type = src.fast_path.GeneratedPkType();
  pass_conn.RunPass(target, std::move(info), "delta");
}

// A committed remove-all, then the plain CREATE INDEX pipeline over the
// live index -- the pass's docs commit above the remove. Readers see an
// empty index until the pass lands; a died rebuild leaves it empty and
// the version mismatch relaunches.
void RunFullRebuild(duckdb::ClientContext& context, ConnectionContext& conn_ctx,
                    const ReindexTarget& target,
                    search::InvertedIndexStorage& storage) {
  auto trx = storage.GetTransaction();
  trx.SetTickSource([](uint64_t count) {
    return search::TickDomain::Instance().Advance(count);
  });
  trx.Remove(std::make_shared<irs::All>());
  if (!trx.Commit()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("REINDEX of \"", target.name,
                            "\": failed to commit the remove-all"));
  }
  PassConnection pass_conn{context, conn_ctx, target};
  auto info = duckdb::make_uniq<SereneDBCreateIndexInfo>();
  info->source_index = target.index->GetId();
  pass_conn.RunPass(target, std::move(info), "rebuild");
}

// Diff the listing, then delta or full rebuild. Stat regime diffs on stat
// identity; iceberg diffs against the indexed snapshot's sequence number
// (data files are write-once -- only deletes above the baseline change a
// file) and masks dead rows in place when possible.
template<typename Observe>
ReindexOutcome RunRefresh(duckdb::ClientContext& context,
                          ConnectionContext& conn_ctx,
                          const ReindexTarget& target, const Source& src,
                          const search::FileManifest& manifest,
                          search::InvertedIndexStorage& storage,
                          Observe& observe) {
  constexpr bool kIceberg = std::is_same_v<Observe, IcebergObserve>;
  auto files = DiffListing(src, manifest, observe);

  if (files.Empty()) {
    if constexpr (kIceberg) {
      if (files.version != manifest.version) {
        // Identical files under a moved pin: restamp so reads never pin an
        // expiring snapshot. Durability rides the next real commit.
        auto restamped = std::make_shared<search::FileManifest>(manifest);
        restamped->version = files.version;
        storage.SetFileManifest(std::move(restamped));
      }
    }
    return {ReindexAction::UpToDate, files.added, files.changed, files.removed};
  }
  // Delta needs the view's support (recorded at fast-path resolution) and
  // pk terms (term-less indexes take the rebuild road).
  const auto& options =
    basics::downCast<const catalog::InvertedIndex>(*target.index).GetOptions();
  if (src.fast_path.supports_delta && options.pk_term) {
    RunDelta(context, conn_ctx, target, src, files, observe, manifest, storage);
    return {ReindexAction::Delta, files.added, files.changed, files.removed,
            static_cast<int64_t>(files.scan.size()) - files.added};
  }
  RunFullRebuild(context, conn_ctx, target, storage);
  return {ReindexAction::Rebuild, files.added, files.changed, files.removed,
          static_cast<int64_t>(src.files.size())};
}

// resolve + claim -> observe -> plan (up_to_date / delta / rebuild) ->
// execute. TF / PRAGMA / REINDEX statement / periodic tick all run this.
ReindexOutcome RunReindex(duckdb::ClientContext& context,
                          const std::string& name, const std::string& schema_p,
                          const std::string& catalog_p) {
  auto& conn_ctx = GetSereneDBContext(context);
  auto snapshot = conn_ctx.CatalogSnapshot();
  const auto target =
    ResolveTarget(conn_ctx, *snapshot, name, schema_p, catalog_p);
  const auto& inverted =
    basics::downCast<const catalog::InvertedIndex>(*target.index);
  const auto storage = inverted.GetData();
  SDB_ASSERT(storage);
  search::InvertedIndexStorage::ReindexClaim claim{*storage};
  if (!claim.Claimed()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_IN_USE),
      ERR_MSG("REINDEX of \"", name, "\" is already in progress"));
  }

  const auto manifest = storage->GetFileManifest();
  std::optional<Source> src;
  if (manifest) {
    src =
      ResolveSource(context, *snapshot, *target.index, inverted.GetOptions());
  }
  // No manifest (external-pk index) or no observable source: full rebuild.
  if (!src) {
    RunFullRebuild(context, conn_ctx, target, *storage);
    return {};
  }
  if (src->version && src->version == manifest->version) {
    // An unmoved pin proves an empty diff (a died pass never advances the
    // manifest version). Most periodic ticks land here.
    return {ReindexAction::UpToDate, 0, 0, 0, 0};
  }
  src->files = ListSourceFiles(*src->list);
  if (src->iceberg_list) {
    if (manifest->version && src->version &&
        !SnapshotIsAncestor(*src->iceberg_list, manifest->version)) {
      // The indexed snapshot left the table's history: deletes may have
      // been UNDONE, invisible to any seq diff. Only a rebuild converges.
      RunFullRebuild(context, conn_ctx, target, *storage);
      return {ReindexAction::Rebuild, 0, 0, 0,
              static_cast<int64_t>(src->files.size())};
    }
    IcebergObserve observe{*src->iceberg_list,
                           src->bind->Cast<duckdb::MultiFileBindData>(),
                           manifest->version};
    return RunRefresh(context, conn_ctx, target, *src, *manifest, *storage,
                      observe);
  }
  StatObserve observe{context};
  return RunRefresh(context, conn_ctx, target, *src, *manifest, *storage,
                    observe);
}

struct ReindexBindData : public duckdb::FunctionData {
  std::string name;
  std::string schema;
  std::string catalog;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<ReindexBindData>(*this);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    auto& o = other.Cast<ReindexBindData>();
    return name == o.name && schema == o.schema && catalog == o.catalog;
  }
};

void FillReindexArgs(ReindexBindData& data,
                     const duckdb::vector<duckdb::Value>& args) {
  const auto arg = [&](duckdb::idx_t i) {
    return args.size() > i && !args[i].IsNull()
             ? args[i].GetValue<std::string>()
             : std::string{};
  };
  data.name = arg(0);
  data.schema = arg(1);
  data.catalog = arg(2);
}

duckdb::unique_ptr<duckdb::FunctionData> ReindexBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  auto data = duckdb::make_uniq<ReindexBindData>();
  FillReindexArgs(*data, input.inputs);
  return_types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
                  duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,
                  duckdb::LogicalType::BIGINT};
  names = {"action", "files_added", "files_changed", "files_removed",
           "files_rescanned"};
  return data;
}

struct ReindexGlobalState : public duckdb::GlobalTableFunctionState {
  bool done = false;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ReindexInitGlobal(
  duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
  return duckdb::make_uniq<ReindexGlobalState>();
}

void ReindexExecute(duckdb::ClientContext& context,
                    duckdb::TableFunctionInput& input,
                    duckdb::DataChunk& output) {
  auto& gstate = input.global_state->Cast<ReindexGlobalState>();
  if (gstate.done) {
    return;
  }
  auto& bind = input.bind_data->Cast<ReindexBindData>();
  const auto outcome =
    RunReindex(context, bind.name, bind.schema, bind.catalog);
  output.SetValue(0, 0, duckdb::Value(ActionName(outcome.action)));
  output.SetValue(1, 0, duckdb::Value::BIGINT(outcome.files_added));
  output.SetValue(2, 0, duckdb::Value::BIGINT(outcome.files_changed));
  output.SetValue(3, 0, duckdb::Value::BIGINT(outcome.files_removed));
  output.SetValue(4, 0, duckdb::Value::BIGINT(outcome.files_rescanned));
  output.SetCardinality(1);
  gstate.done = true;
}

void ReindexPragma(duckdb::ClientContext& context,
                   const duckdb::FunctionParameters& parameters) {
  ReindexBindData args;
  FillReindexArgs(args, parameters.values);
  // PRAGMA / REINDEX statement form: silent, PG-style.
  RunReindex(context, args.name, args.schema, args.catalog);
}

// ReindexLoop tick: one REINDEX on an internal session impersonating the
// index OWNER (the identity a manual owner-run REINDEX has). Quiet
// outcomes return OK -- vanished index/owner, claim lost to a manual run.
absl::Status RunReindexTick(duckdb::DatabaseInstance& db, ObjectId index_id) {
  try {
    auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
    const auto index = snapshot->GetObject(index_id);
    if (!index || index->GetType() != catalog::ObjectType::InvertedIndex) {
      return absl::OkStatus();
    }
    const auto schema = snapshot->GetObject(index->GetParentId());
    auto database = snapshot->GetDatabase(snapshot->GetDatabaseId(*index));
    const auto relation = snapshot->GetObject(
      basics::downCast<const catalog::Index>(*index).GetRelationId());
    if (!schema || !database || !relation) {
      return absl::OkStatus();
    }
    // Ownership lives on the RELATION; roles resolve by name only, so scan
    // the role list for the owner's id.
    auto roles = snapshot->GetRoles();
    const auto owner_it = absl::c_find_if(roles, [&](const auto& role) {
      return role->GetId() == relation->GetOwner();
    });
    const auto owner = owner_it == roles.end() ? nullptr : std::move(*owner_it);
    if (!owner) {
      // Surface it: skipping quietly would look like a healthy loop.
      return absl::NotFoundError(
        absl::StrCat("owner role of \"", relation->GetName(),
                     "\" no longer exists; the periodic refresh cannot "
                     "impersonate it"));
    }
    const std::string user{owner->GetName()};

    duckdb::Connection conn{db};
    auto ctx = std::make_shared<ConnectionContext>(
      *conn.context, user, owner->GetId(), database->GetName(),
      database->GetId(), database, nullptr, nullptr, /*backend_pid=*/0,
      nullptr);
    SereneDBClientState::Register(*conn.context, ctx);
    conn.context->session_user = user;
    ctx->SetCatalogSnapshot(snapshot);
    // No client behind this session: notices have nowhere to go (the
    // context asserts none survive destruction).
    absl::Cleanup drop_notices = [&] { ctx->ConsumeNotices([](auto&) {}); };
    // duckdb catalog lookups during the observe (object-store secrets)
    // require an active transaction; this connection never commits.
    conn.BeginTransaction();
    absl::Cleanup rollback = [&] {
      try {
        conn.Rollback();
      } catch (...) {
      }
    };

    RunReindex(*conn.context, std::string{index->GetName()},
               std::string{schema->GetName()},
               std::string{database->GetName()});
    return absl::OkStatus();
  } catch (const SqlException& ex) {
    if (ex.error().errcode == ERRCODE_OBJECT_IN_USE ||
        ex.error().errcode == ERRCODE_UNDEFINED_OBJECT) {
      // A manual REINDEX holds the claim / the index vanished mid-tick.
      return absl::OkStatus();
    }
    return absl::InternalError(ex.message());
  } catch (const std::exception& ex) {
    return absl::InternalError(ex.what());
  }
}

}  // namespace

void RegisterReindexFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");

  // `db` outlives the loops: SearchEngine::stop() joins them first.
  search::SetReindexRunner(
    [&db](ObjectId index_id) { return RunReindexTick(db, index_id); });

  duckdb::TableFunction func("serenedb_reindex", {}, ReindexExecute,
                             ReindexBind, ReindexInitGlobal);
  func.varargs = duckdb::LogicalType::VARCHAR;
  loader.RegisterFunction(func);

  auto pragma = duckdb::PragmaFunction::PragmaCall(
    "serenedb_reindex", ReindexPragma, {duckdb::LogicalType::VARCHAR});
  pragma.varargs = duckdb::LogicalType::VARCHAR;
  loader.RegisterFunction(pragma);
}

}  // namespace sdb::connector
