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

#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/string_util.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/struct_vector.hpp>
#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/comparison_expression.hpp>
#include <duckdb/parser/expression/conjunction_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/query_node/delete_query_node.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/parser/statement/delete_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/planner/expression/bound_comparison_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_operator_expression.hpp>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/search/filter_optimizer.hpp>
#include <iresearch/search/prefix_filter.hpp>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/log.h"
#include "basics/primary_key.hpp"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/pk_spec.h"
#include "catalog/role.h"
#include "catalog/view.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/file_manifest.h"
#include "connector/optimizer/iresearch_plan_common.hpp"
#include "connector/search_filter_builder.hpp"
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

struct ReindexOutcome {
  ReindexAction action = ReindexAction::Rebuild;
  int64_t files_added = 0;
  int64_t files_changed = 0;
  int64_t files_removed = 0;
  // Changed files whose docs were rebuilt by a scan (a rebuild scans the
  // whole listing). Changed minus rescanned = files refreshed in place
  // (masked rows / removed-by-query) -- the fallback-frequency signal.
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

// The guard ladder: argument and catalog resolution plus every REINDEX
// precondition error. Returns only a valid view-backed inverted index.
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
  // PG-17 semantics: REINDEX is in the MAINTAIN privilege family (owners
  // hold it implicitly), same check VACUUM runs. Enforced here for every
  // action -- the passes run on an internal connection and never reach a
  // catalog mutation's own gate.
  snapshot.RequireAccess(conn_ctx.GetRoleId(), *target.relation,
                         catalog::AclMode::Maintain);
  return target;
}

// One internal connection per pass batch, impersonating the caller
// (ownership was enforced by ResolveTarget). Notices raised by the passes
// forward to the caller's connection on scope exit.
class PassConnection {
 public:
  PassConnection(duckdb::ClientContext& context, ConnectionContext& caller,
                 const ReindexTarget& target)
    : _caller{caller},
      _conn{*context.db},
      _ctx{std::make_shared<ConnectionContext>(
        *_conn.context, caller.user(), caller.GetRoleId(), target.database,
        target.db->GetId(), target.db, nullptr, nullptr, caller.GetBackendPid(),
        nullptr)},
      _state{&SereneDBClientState::Register(*_conn.context, _ctx)} {
    _conn.context->session_user = caller.user();
    // Session impersonation covers the one setting view binding depends on:
    // version guessing (metadata discovery on hint-less iceberg tables)
    // crosses with the CALLER's value, so a pass binds exactly where the
    // caller's own scan of the view would -- no forced capability.
    duckdb::Value guessing;
    if (context.TryGetCurrentSetting("unsafe_enable_version_guessing",
                                     guessing) &&
        guessing.GetValue<bool>()) {
      auto result = _conn.Query("SET unsafe_enable_version_guessing=true");
      if (result->HasError()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INTERNAL_ERROR),
          ERR_MSG("REINDEX pass connection: ", result->GetError()));
      }
    }
  }
  ~PassConnection() {
    _ctx->ConsumeNotices(
      [&](auto& notice) { _caller.AddNotice(std::move(notice)); });
  }

  // A REINDEX pass statement carries NO user syntax -- no keys, no opclasses,
  // no WITH, no WHERE. Which pass it is (rebuild or one-file delta) travels
  // ON the statement as its SereneDBCreateIndexInfo: BindCreateIndex derives
  // the key list from the source index's referenced column positions and
  // splices the persisted predicate, so the pass is immune to view/column
  // renames and lossless through ALTER INDEX SET.
  duckdb::unique_ptr<duckdb::QueryResult> RunPass(
    const ReindexTarget& target,
    duckdb::unique_ptr<SereneDBCreateIndexInfo> info) {
    info->index_type = "inverted";
    info->SetIndexName(duckdb::Identifier{std::string{target.name}});
    info->table = duckdb::Identifier{std::string{target.relation->GetName()}};
    info->SetSchema(duckdb::Identifier{std::string{target.schema}});
    info->SetCatalog(duckdb::Identifier{std::string{target.database}});
    info->on_conflict = duckdb::OnCreateConflict::ERROR_ON_CONFLICT;
    auto statement = duckdb::make_uniq<duckdb::CreateStatement>();
    statement->info = std::move(info);
    return _conn.Query(std::move(statement));
  }

  // A raw internal statement under the same impersonation -- the equality
  // delete scan road runs its DELETE-from-index plan through this.
  duckdb::unique_ptr<duckdb::QueryResult> Query(
    duckdb::unique_ptr<duckdb::SQLStatement> statement) {
    return _conn.Query(std::move(statement));
  }

  // The connection's side-band state, for planting the DELETE-from-index
  // authorization around Query (reindex_delete_pass).
  SereneDBClientState& State() { return *_state; }

 private:
  ConnectionContext& _caller;
  duckdb::Connection _conn;
  std::shared_ptr<ConnectionContext> _ctx;
  SereneDBClientState* _state;
};

// The scan road for an untranslatable equality-delete group: the delete
// rows become a WHERE over the index's own SQL scan -- ANY view column
// evaluates (dictionaried columns push down, stored/included columns
// residual-filter from the columnstore, everything else materializes from
// the source) -- and the matching (file, row) pks remove exactly, scoped
// to the covered files. The DELETE removes with its own transaction on the
// live index, committed inside the statement -- commit order keeps it
// below every later remove and delta insert. Returns false when the query
// cannot run (rescan stays the only road).
bool RunPkScanRemoves(duckdb::ClientContext& context,
                      ConnectionContext& conn_ctx, const ReindexTarget& target,
                      duckdb::unique_ptr<duckdb::ParsedExpression> where,
                      const containers::FlatHashSet<uint64_t>& covered_ids) {
  // Built as a statement object, like RunPass builds its CreateStatement:
  // no SQL text, so values travel verbatim and names never print.
  auto statement = duckdb::make_uniq<duckdb::DeleteStatement>();
  auto table = duckdb::make_uniq<duckdb::BaseTableRef>();
  table->SetQualifiedName(duckdb::Identifier{std::string{target.database}},
                          duckdb::Identifier{std::string{target.schema}},
                          duckdb::Identifier{std::string{target.name}});
  statement->node->table = std::move(table);
  // Scope to the covered files in the WHERE itself -- the sink then deposits
  // every matched pk verbatim: (generated_pk).file_index IN (covered...).
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> in_children;
  in_children.reserve(covered_ids.size() + 1);
  in_children.push_back(duckdb::make_uniq<duckdb::OperatorExpression>(
    duckdb::ExpressionType::STRUCT_EXTRACT,
    duckdb::make_uniq<duckdb::ColumnRefExpression>(
      duckdb::Identifier{"generated_pk"}),
    duckdb::make_uniq<duckdb::ConstantExpression>(duckdb::Value{
      duckdb::StructType::GetChildName(FileIndexRowNumberStructType(), 0)
        .GetIdentifierName()})));
  for (const auto id : covered_ids) {
    in_children.push_back(duckdb::make_uniq<duckdb::ConstantExpression>(
      duckdb::Value::UBIGINT(id)));
  }
  statement->node->condition = duckdb::make_uniq<duckdb::ConjunctionExpression>(
    duckdb::ExpressionType::CONJUNCTION_AND, std::move(where),
    duckdb::make_uniq<duckdb::OperatorExpression>(
      duckdb::ExpressionType::COMPARE_IN, std::move(in_children)));
  PassConnection pass{context, conn_ctx, target};
  // The planted flag is what lets PlanDelete accept the index target: the
  // scan runs as a real DELETE plan whose sink removes the matched pks.
  pass.State().reindex_delete_pass = true;
  auto result = pass.Query(std::move(statement));
  pass.State().reindex_delete_pass = false;
  return result && !result->HasError();
}

// The scan road for an untranslatable equality-delete group lives in
// RunPkScanRemoves above; the translation itself follows.

// New equality deletes become a remove-by-query instead of a rescan.
// Covered files GROUP by their applicable-delete set (the reader's own
// GetEqualityDeletesForFile: sequence strictly above the data file,
// partition match), each group translating its deduplicated rows ONCE.
// A group whose delete set is applicable to EVERY kept live file runs
// UNSCOPED -- re-removing an already-applied row is idempotent, so no
// per-file scope node is needed (the common bulk-delete shape; scans
// rebuild after the removes, so scanned files are immune by order).
// Otherwise the group falls back to per-file (pk-prefix AND rows) --
// rows re-inserted into files the deletes don't reach stay untouched.
// A group the dictionaries cannot answer (included / analyzed columns,
// failed casts) drops to the pk SCAN road instead. False = no road at
// all (a column outside the view output, non-constant shapes, or the pk
// scan cannot run) -- the caller demotes the covered files to rescans.
bool TranslateEqualityRemoves(
  duckdb::ClientContext& context, ConnectionContext& conn_ctx,
  const std::shared_ptr<const catalog::Snapshot>& snapshot,
  const ReindexTarget& target, const Source& src, const FileDiff& files,
  IcebergObserve& observe) {
  const auto& fast_path = src.fast_path;
  observe.EnsureDeletesProcessed();
  const auto& index =
    basics::downCast<const catalog::InvertedIndex>(*target.index);
  const auto& view =
    basics::downCast<const catalog::PgSqlView>(*target.relation);
  const auto& view_info = view.GetInfo();

  // Iceberg field id -> the view's output position, which IS the index's
  // column id (view scans key columns positionally); source columns map
  // by name through the view's projection when it has one.
  containers::FlatHashMap<uint64_t, const catalog::InvertedIndexEntryInfo*>
    infos;
  // Field id -> view output position, index shape aside: the scan road
  // needs only the position (its SQL references the view column by name);
  // the filter road layers the term-dict checks on top.
  const auto resolve_view_pos =
    [&](int32_t field_id) -> std::optional<uint64_t> {
    const auto& columns = observe.GlobalColumns();
    size_t source_pos = columns.size();
    for (size_t i = 0; i < columns.size(); ++i) {
      if (!columns[i].identifier.IsNull() &&
          columns[i].identifier.GetValue<int32_t>() == field_id) {
        source_pos = i;
        break;
      }
    }
    if (source_pos == columns.size()) {
      return std::nullopt;
    }
    uint64_t pos = source_pos;
    if (!fast_path.projection_columns.empty()) {
      const auto name = columns[source_pos].name.GetIdentifierName();
      const auto it = absl::c_find_if(
        fast_path.projection_columns,
        [&](const auto& p) { return duckdb::StringUtil::CIEquals(p, name); });
      if (it == fast_path.projection_columns.end()) {
        return std::nullopt;
      }
      pos = static_cast<uint64_t>(it - fast_path.projection_columns.begin());
    }
    if (pos >= view_info.types.size()) {
      return std::nullopt;
    }
    return pos;
  };
  // Per-position dictionary gate for the filter road, memoized: the
  // column must carry an UNANALYZED term dictionary (analyzed terms
  // answer MATCH semantics, not value equality -- a stemmed term could
  // remove rows whose stored value differs). Fills `infos` for the
  // getter.
  containers::FlatHashMap<uint64_t, bool> dict_verdict;
  const auto dict_ok = [&](uint64_t pos) {
    if (const auto it = dict_verdict.find(pos); it != dict_verdict.end()) {
      return it->second;
    }
    const auto* info =
      index.FindColumnInfo(static_cast<catalog::Column::Id>(pos));
    const bool ok = info && info->IsTermDict() &&
                    index.GetTokenizer(snapshot, pos).analyzer->type() ==
                      irs::Type<irs::StringTokenizer>::id();
    if (ok) {
      infos.emplace(pos, info);
    }
    dict_verdict.emplace(pos, ok);
    return ok;
  };
  // Built fresh per reference: SearchColumnInfo owns its tokenizer and is
  // move-only, same as the optimizer's own getter. The null-marker
  // registry feeds the same Optimize pass the read road runs -- it also
  // normalizes the degenerate boolean nodes built below.
  containers::FlatHashMap<irs::field_id, irs::field_id> null_markers;
  const ColumnGetter getter = [&](const duckdb::BoundColumnRefExpression& ref)
    -> std::optional<SearchColumnInfo> {
    const auto pos = ref.Binding().column_index.GetIndex();
    const auto it = infos.find(pos);
    if (it == infos.end()) {
      return std::nullopt;
    }
    auto info = optimizer::MakeSearchColumnInfo(
      pos, it->second, view_info.types[pos], index.GetTokenizer(snapshot, pos));
    if (irs::field_limits::valid(info.null_field_id)) {
      null_markers[info.null_field_id] = info.field_id;
    }
    return info;
  };

  // The reader stores KEEP filters (col != v / col IS NOT NULL); a
  // parsed conjunct is the view output position plus the deleted value
  // (nullopt = IS NULL). Rows are parsed and deduplicated ONCE per group
  // -- the filter and scan roads are thin emitters over this, so the
  // shape checks (constants only, columns inside the view output) live
  // in a single place.
  struct EqConjunct {
    uint64_t pos;
    std::optional<duckdb::Value> value;
  };
  using EqRow = std::vector<EqConjunct>;
  const auto parse_rows =
    [&](const duckdb::vector<
        duckdb::reference<const duckdb::IcebergEqualityDeleteFile>>&
          delete_files) -> std::optional<std::vector<EqRow>> {
    std::vector<EqRow> rows;
    containers::FlatHashSet<std::string> seen_rows;
    std::string row_key;
    for (const auto& delete_file : delete_files) {
      for (const auto& row : delete_file.get().rows) {
        EqRow parsed;
        parsed.reserve(row.filters.size());
        for (const auto& [field_id, keep] : row.filters) {
          const auto pos = resolve_view_pos(field_id);
          if (!pos) {
            return std::nullopt;
          }
          if (keep->GetExpressionType() ==
              duckdb::ExpressionType::OPERATOR_IS_NOT_NULL) {
            parsed.push_back({*pos, std::nullopt});
          } else if (keep->GetExpressionType() ==
                     duckdb::ExpressionType::COMPARE_NOTEQUAL) {
            const auto& right = duckdb::BoundComparisonExpression::Right(
              keep->Cast<duckdb::BoundFunctionExpression>());
            if (right.GetExpressionType() !=
                duckdb::ExpressionType::VALUE_CONSTANT) {
              return std::nullopt;
            }
            parsed.push_back(
              {*pos, right.Cast<duckdb::BoundConstantExpression>().GetValue()});
          } else {
            return std::nullopt;
          }
        }
        absl::c_sort(parsed, [](const auto& lhs, const auto& rhs) {
          return lhs.pos < rhs.pos;
        });
        // Rows repeat across CDC commits: dedup by the canonical
        // (position, value) tuple.
        row_key.clear();
        for (const auto& conjunct : parsed) {
          absl::StrAppend(&row_key, conjunct.pos,
                          conjunct.value
                            ? absl::StrCat("=", conjunct.value->ToString())
                            : "!null",
                          ";");
        }
        if (!seen_rows.emplace(row_key).second) {
          continue;
        }
        rows.push_back(std::move(parsed));
      }
    }
    if (rows.empty()) {
      return std::nullopt;
    }
    return rows;
  };

  // Filter-road emitter: EQ/IS NULL over the view's columns through
  // MakeSearchFilter. False = a column without a queryable dictionary or
  // a value that will not cast -- the scan road takes over.
  const auto translate_rows = [&](const std::vector<EqRow>& rows,
                                  irs::Or& rows_or) -> bool {
    for (const auto& row : rows) {
      std::vector<duckdb::unique_ptr<duckdb::Expression>> conjuncts;
      conjuncts.reserve(row.size());
      for (const auto& conjunct : row) {
        if (!dict_ok(conjunct.pos)) {
          return false;
        }
        auto colref = duckdb::make_uniq<duckdb::BoundColumnRefExpression>(
          view_info.types[conjunct.pos],
          duckdb::ColumnBinding{duckdb::TableIndex{0},
                                duckdb::ProjectionIndex{conjunct.pos}});
        if (conjunct.value) {
          auto value = *conjunct.value;
          if (!value.DefaultTryCastAs(view_info.types[conjunct.pos])) {
            return false;
          }
          conjuncts.push_back(duckdb::BoundComparisonExpression::Create(
            duckdb::ExpressionType::COMPARE_EQUAL, std::move(colref),
            duckdb::make_uniq<duckdb::BoundConstantExpression>(
              std::move(value))));
        } else {
          auto is_null = duckdb::make_uniq<duckdb::BoundOperatorExpression>(
            duckdb::ExpressionType::OPERATOR_IS_NULL,
            duckdb::LogicalType::BOOLEAN);
          is_null->GetChildrenMutable().push_back(std::move(colref));
          conjuncts.push_back(std::move(is_null));
        }
      }
      auto& row_and = rows_or.add<irs::And>();
      if (!MakeSearchFilter(row_and, conjuncts, getter, context).ok()) {
        return false;
      }
    }
    return true;
  };

  // Scan-road emitter: the same rows as a parsed WHERE tree over the view's
  // column NAMES, the Values carried verbatim (the binder casts them to the
  // column types) -- parse_rows already guaranteed positions and constants,
  // so this cannot fail.
  const auto combine =
    [](duckdb::ExpressionType type,
       duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> exprs)
    -> duckdb::unique_ptr<duckdb::ParsedExpression> {
    if (exprs.size() == 1) {
      return std::move(exprs[0]);
    }
    return duckdb::make_uniq<duckdb::ConjunctionExpression>(type,
                                                            std::move(exprs));
  };
  const auto build_where = [&](const std::vector<EqRow>& rows) {
    duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> row_exprs;
    row_exprs.reserve(rows.size());
    for (const auto& row : rows) {
      duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> conjuncts;
      conjuncts.reserve(row.size());
      for (const auto& conjunct : row) {
        auto colref = duckdb::make_uniq<duckdb::ColumnRefExpression>(
          view_info.names[conjunct.pos]);
        if (conjunct.value) {
          conjuncts.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
            duckdb::ExpressionType::COMPARE_EQUAL, std::move(colref),
            duckdb::make_uniq<duckdb::ConstantExpression>(*conjunct.value)));
        } else {
          conjuncts.push_back(duckdb::make_uniq<duckdb::OperatorExpression>(
            duckdb::ExpressionType::OPERATOR_IS_NULL, std::move(colref)));
        }
      }
      row_exprs.push_back(
        combine(duckdb::ExpressionType::CONJUNCTION_AND, std::move(conjuncts)));
    }
    return combine(duckdb::ExpressionType::CONJUNCTION_OR,
                   std::move(row_exprs));
  };

  // Group covered files by their applicable-delete set (deterministic
  // enumeration order makes the pointer sequence a stable key).
  struct Group {
    std::vector<const IcebergObserve::EqCovered*> files;
    duckdb::vector<duckdb::reference<const duckdb::IcebergEqualityDeleteFile>>
      deletes;
  };
  std::vector<Group> groups;
  containers::FlatHashMap<std::string, size_t> group_of;
  for (const auto& covered : observe.eq_covered) {
    const auto entry =
      observe.Deletes().list->GetManifestEntry(covered.listing_idx);
    auto delete_files = observe.Deletes().list->GetEqualityDeletesForFile(
      entry, static_cast<int64_t>(covered.old_seq));
    const size_t total_rows = absl::c_accumulate(
      delete_files, size_t{0},
      [](size_t n, auto& df) { return n + df.get().rows.size(); });
    if (total_rows == 0) {
      // The file's seq moved but nothing new applies to it per the spec
      // (e.g. the delete landed in the same snapshot): restamp only.
      continue;
    }
    std::string group_key;
    for (const auto& df : delete_files) {
      absl::StrAppend(&group_key, reinterpret_cast<uintptr_t>(&df.get()), ";");
    }
    const auto [it, inserted] =
      group_of.emplace(std::move(group_key), groups.size());
    if (inserted) {
      groups.push_back({{}, std::move(delete_files)});
    }
    groups[it->second].files.push_back(&covered);
  }
  if (groups.empty()) {
    return true;
  }

  // The applicable set of every KEPT live file (scans rebuild after the
  // removes, so they are immune by order): a group whose whole delete set
  // is applicable to all of them may run unscoped -- re-removal of an
  // already-applied row is idempotent, and no kept file can hold a row
  // one of the group's deletes must not touch.
  containers::FlatHashSet<std::string_view> scan_paths;
  scan_paths.reserve(files.scan.size());
  for (const auto& entry : files.scan) {
    scan_paths.emplace(entry.path);
  }
  std::vector<containers::FlatHashSet<const void*>> kept_applicable;
  for (size_t i = 0; i < src.files.size(); ++i) {
    if (scan_paths.contains(src.files[i].path)) {
      continue;
    }
    const auto entry = observe.Deletes().list->GetManifestEntry(i);
    const auto applicable = observe.Deletes().list->GetEqualityDeletesForFile(
      entry, std::numeric_limits<int64_t>::min());
    auto& set = kept_applicable.emplace_back();
    set.reserve(applicable.size());
    for (const auto& df : applicable) {
      set.insert(&df.get());
    }
  }

  std::string key;
  for (const auto& group : groups) {
    const auto rows = parse_rows(group.deletes);
    if (!rows) {
      // A shape no road can evaluate (non-constant filters, a column
      // outside the view output): the rescan road.
      return false;
    }
    const bool safe = absl::c_all_of(kept_applicable, [&](const auto& set) {
      return absl::c_all_of(
        group.deletes, [&](const auto& df) { return set.contains(&df.get()); });
    });
    irs::Filter::ptr filter;
    bool translated = true;
    if (safe) {
      auto rows_or = std::make_unique<irs::Or>();
      translated = translate_rows(*rows, *rows_or);
      if (translated) {
        filter = std::move(rows_or);
      }
    } else {
      // Per-file scope: (pk-prefix AND rows) for each covered file. Pays
      // the stock ByPrefix prepare cost over the file's terms -- the rare
      // road (partition-scoped deletes, CDC same-commit re-inserts).
      auto per_file = std::make_unique<irs::Or>();
      for (const auto* covered : group.files) {
        auto& file_and = per_file->add<irs::And>();
        auto& prefix = file_and.add<irs::ByPrefix>();
        *prefix.mutable_field_id() = catalog::term_dict::kPKFieldId;
        key.clear();
        primary_key::AppendSigned(key, static_cast<int64_t>(covered->file_id));
        prefix.mutable_options()->term.assign(
          reinterpret_cast<const irs::byte_type*>(key.data()), key.size());
        auto& rows_or = file_and.add<irs::Or>();
        if (!(translated = translate_rows(*rows, rows_or))) {
          break;
        }
      }
      if (translated) {
        filter = std::move(per_file);
      }
    }
    if (!translated) {
      // The dictionaries cannot answer this group (included / analyzed /
      // foreign columns): evaluate the rows through the index's own SQL
      // scan and remove the matching pks exactly -- always scoped by pk,
      // so the safe/unsafe split does not apply.
      containers::FlatHashSet<uint64_t> covered_ids;
      covered_ids.reserve(group.files.size());
      for (const auto* covered : group.files) {
        covered_ids.insert(covered->file_id);
      }
      if (!RunPkScanRemoves(context, conn_ctx, target, build_where(*rows),
                            covered_ids)) {
        return false;  // the scan cannot run (store_pk, shapes): rescan
      }
      continue;
    }
    irs::Optimize(filter, {.null_markers = &null_markers});
    observe.eq_removes.push_back(std::move(filter));
  }
  return true;
}

// Sorted dead-row cursors the range-mask filter leapfrogs over. The rows
// vector lives on the observe's DeleteMask, alive until the remove commits.
SearchRemoveRangeMaskFilter::DeadRowCursor MakeRowsCursor(
  const std::vector<int64_t>& rows) {
  return [&rows,
          idx = size_t{0}](int64_t min_row) mutable -> std::optional<int64_t> {
    idx = static_cast<size_t>(
      std::lower_bound(rows.begin() + static_cast<int64_t>(idx), rows.end(),
                       min_row) -
      rows.begin());
    if (idx == rows.size()) {
      return std::nullopt;
    }
    return rows[idx];
  };
}

// The DV-diff buckets live on the observe's DeleteMask, alive until the
// remove commits. Buckets ascend by (high, low) = ascending row.
SearchRemoveRangeMaskFilter::DeadRowCursor MakeDvDiffCursor(
  const std::vector<std::pair<int32_t, roaring::Roaring>>& buckets) {
  return [&buckets, bucket = size_t{0}](
           int64_t min_row) mutable -> std::optional<int64_t> {
    while (bucket < buckets.size()) {
      const auto base = static_cast<int64_t>(buckets[bucket].first) << 32;
      if (min_row >= base + (int64_t{1} << 32)) {
        ++bucket;
        continue;
      }
      const uint32_t low =
        min_row <= base ? 0u : static_cast<uint32_t>(min_row - base);
      auto it = buckets[bucket].second.begin();
      if (!it.move_equalorlarger(low)) {
        ++bucket;
        continue;
      }
      return base | static_cast<int64_t>(*it);
    }
    return std::nullopt;
  };
}

// The publish every road shares: the same plain refresh CREATE INDEX's
// finalize uses.
void PublishRefresh(search::InvertedIndexStorage& storage,
                    const ReindexTarget& target, std::string_view what) {
  auto code = search::RefreshResult::Undefined;
  if (auto refreshed = storage.RefreshUnsafe(/*wait=*/true, nullptr, code);
      !refreshed.res.ok()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("REINDEX of \"", target.name, "\": ", what, ": ",
                            refreshed.res.ToString()));
  }
}

// The delta executor: remove the dead files' docs by pk-term prefix
// (self-published), then rescan only the affected files through ONE pass
// into the EXISTING storage -- the pass's Finalize publishes fresh docs and
// manifest v_next as one flip under the refresh gate. The caller holds the
// reindex claim.
template<typename Observe>
void RunDelta(duckdb::ClientContext& context, ConnectionContext& conn_ctx,
              const ReindexTarget& target, FileDiff& files,
              const Observe& observe, const search::FileManifest& manifest,
              search::InvertedIndexStorage& storage) {
  constexpr bool kIceberg = std::is_same_v<Observe, IcebergObserve>;
  // The next manifest = the old one minus dying files, plus fresh ids for
  // the scans (an id freed by the removal of the HIGHEST entry may be
  // reused: fresh ids start after the surviving max).
  auto manifest_next = std::make_shared<search::FileManifest>();
  manifest_next->entries.reserve(manifest.entries.size() + files.scan.size());
  containers::FlatHashSet<uint64_t> dead_ids{files.del_files.begin(),
                                             files.del_files.end()};
  containers::FlatHashMap<uint64_t, const IcebergObserve::DeleteMask*> mask_of;
  if constexpr (kIceberg) {
    mask_of.reserve(observe.del_masks.size());
    for (const auto& mask : observe.del_masks) {
      mask_of.emplace(mask.file_id, &mask);
    }
  }
  uint64_t next_id = 0;
  for (const auto& [id, entry] : manifest.entries) {
    if (dead_ids.contains(id)) {
      continue;
    }
    next_id = std::max(next_id, id + 1);
    auto [kept, emplaced] = manifest_next->entries.emplace(id, entry);
    SDB_ASSERT(emplaced);
    if (const auto it = mask_of.find(id); it != mask_of.end()) {
      kept->second.delete_seq = it->second->delete_seq;
      if (it->second->v3_delete_masks) {
        kept->second.v3_delete_masks = *it->second->v3_delete_masks;
      }
    }
  }
  // Consecutive ids in files.scan (= listing) order: the pass rebases the
  // narrowed listing's file_index by scan_base to recover them.
  const uint64_t scan_base = next_id;
  for (auto& entry : files.scan) {
    entry.file_id = next_id;
    manifest_next->entries.emplace(next_id, entry);
    ++next_id;
  }

  bool has_removes = !files.del_files.empty();
  if constexpr (kIceberg) {
    has_removes =
      has_removes || !observe.del_masks.empty() || !observe.eq_removes.empty();
  }
  if (has_removes) {
    // Removes ride the same modification-query road as DML deletes. v1
    // contract: they publish their own effect -- a reader may briefly see
    // dead files' rows gone before the rescans below land.
    auto trx = storage.GetTransaction();
    uint64_t queries = 0;
    if (!files.del_files.empty()) {
      // A dead file is a contiguous pk-term range: the (file, row) encoding
      // is order-preserving, so every one of its docs carries the 8-byte
      // enc(file) term prefix. One prefix per file; each segment's dictionary
      // walk enumerates exactly the rows it holds -- no keys to precompute,
      // no count to trust.
      absl::c_sort(files.del_files);
      auto filter = std::make_shared<SearchRemovePrefixFilter>(
        files.del_files.size(), catalog::term_dict::kPKFieldId);
      std::string key;
      for (const auto id : files.del_files) {
        key.clear();
        primary_key::AppendSigned(key, static_cast<int64_t>(id));
        filter->Add(key);
      }
      trx.Remove(std::move(filter));
      ++queries;
    }
    if constexpr (kIceberg) {
      // Delete masks: small row lists (including small DV diffs) stay
      // DML-style point lookups; big lists and big DV diffs LEAPFROG the
      // sorted dead rows against the file's pk-term range on one shared
      // forward iterator (ascending encoded-prefix order required). A
      // row-less mask (the file's seq moved on equality deletes alone)
      // restamps the manifest entry but adds no query.
      const auto seek_road = [](const auto& mask) {
        return !mask.dv_diff.empty() ||
               mask.rows.size() > Observe::kMaskRangeWalkThreshold;
      };
      size_t exact_rows = 0;
      std::vector<size_t> seek_masks;
      for (size_t i = 0; i < observe.del_masks.size(); ++i) {
        if (seek_road(observe.del_masks[i])) {
          seek_masks.push_back(i);
        } else {
          exact_rows += observe.del_masks[i].rows.size();
        }
      }
      if (exact_rows) {
        auto filter = std::make_shared<SearchRemoveFilter>(
          exact_rows, catalog::term_dict::kPKFieldId);
        std::string key;
        for (const auto& mask : observe.del_masks) {
          if (seek_road(mask)) {
            continue;
          }
          for (const auto row : mask.rows) {
            key.clear();
            primary_key::AppendSigned(key, static_cast<int64_t>(mask.file_id));
            primary_key::AppendSigned(key, row);
            filter->Add(key);
          }
        }
        trx.Remove(std::move(filter));
        ++queries;
      }
      if (!seek_masks.empty()) {
        absl::c_sort(seek_masks, [&](size_t a, size_t b) {
          return observe.del_masks[a].file_id < observe.del_masks[b].file_id;
        });
        auto range_filter = std::make_shared<SearchRemoveRangeMaskFilter>(
          catalog::term_dict::kPKFieldId);
        std::string prefix;
        for (const auto i : seek_masks) {
          const auto& mask = observe.del_masks[i];
          prefix.clear();
          primary_key::AppendSigned(prefix, static_cast<int64_t>(mask.file_id));
          range_filter->Add(prefix, mask.dv_diff.empty()
                                      ? MakeRowsCursor(mask.rows)
                                      : MakeDvDiffCursor(mask.dv_diff));
        }
        trx.Remove(std::move(range_filter));
        ++queries;
      }
      for (const auto& eq_remove : observe.eq_removes) {
        // New equality deletes, one translated query per group of covered
        // files sharing the same applicable-delete set.
        trx.Remove(eq_remove);
        ++queries;
      }
    }
    if (queries) {
      // Tick-bound queries land in [commit - queries, commit): reserve one
      // more tick so the lowest sits strictly above the last published tick
      // (and below any insert staged afterwards).
      if (!trx.Commit(search::TickDomain::Instance().Advance(queries + 1))) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                        ERR_MSG("REINDEX delta of \"", target.name,
                                "\": failed to commit the removes"));
      }
      PublishRefresh(storage, target, "remove publish failed");
    }
  }

  // The version travels with the manifest: reads and the next observe
  // target the source state these docs came from.
  manifest_next->version = files.version;

  if (files.scan.empty()) {
    // Removes-only tick (dead files, masks, restamps): no pass to ride, so
    // swap the manifest and publish here.
    SDB_IF_FAILURE("crash_before_delta_publish") { SDB_IMMEDIATE_ABORT(); }
    auto gate = storage.AcquireRefreshGate();
    storage.SetFileManifest(std::move(manifest_next));
    gate.unlock();
    PublishRefresh(storage, target, "publish failed");
    return;
  }

  // ONE statement for the whole scan set, bound against the REAL view: the
  // bind hook narrows the leaf's file list to these files (iceberg keeps
  // its own list type, so the reader still applies every delete flavor)
  // and the engine parallelizes across them. The pass commits its batches
  // as it goes -- the plain create road -- under the refresh gate its init
  // acquired; Finalize swaps in the manifest carried here and publishes.
  // The next REINDEX's removes or rebuild mask whatever a failed delta
  // committed.
  PassConnection pass_conn{context, conn_ctx, target};
  auto info = duckdb::make_uniq<SereneDBCreateIndexInfo>();
  info->source_index = target.index->GetId();
  info->delta_file_base = scan_base;
  info->delta_files.reserve(files.scan.size());
  for (const auto& file : files.scan) {
    info->delta_files.push_back(file.path);
  }
  info->manifest = std::move(manifest_next);
  auto result = pass_conn.RunPass(target, std::move(info));
  if (result->HasError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("REINDEX delta of \"", target.name,
                            "\" failed: ", result->GetError()));
  }
}

// EXECUTE(rebuild): the unmodified CREATE INDEX pipeline on an internal
// connection into the live index. The operator empties the index at init
// (an EMPTY manifest riding the remove-all) and commits batches as it goes;
// its Finalize publishes old->new as one reader swap. An interruption
// leaves the empty manifest observable, so the next refresh tick heals by
// full rescan.
void RunFullRebuild(duckdb::ClientContext& context, ConnectionContext& conn_ctx,
                    const ReindexTarget& target) {
  PassConnection pass_conn{context, conn_ctx, target};
  auto info = duckdb::make_uniq<SereneDBCreateIndexInfo>();
  info->source_index = target.index->GetId();
  auto result = pass_conn.RunPass(target, std::move(info));
  if (result->HasError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("REINDEX of \"", target.name,
                            "\" failed to rebuild: ", result->GetError()));
  }
}

// The multi-file refresh: the standard listing diff on stat identity; an
// unchanged listing is the whole currency proof.
ReindexOutcome MultiFileRefresh(duckdb::ClientContext& context,
                                ConnectionContext& conn_ctx,
                                const ReindexTarget& target, const Source& src,
                                const search::FileManifest& manifest,
                                search::InvertedIndexStorage& storage,
                                const catalog::InvertedIndexOptions& options) {
  StatObserve observe{context};
  auto files = DiffListing(src, manifest, observe);

  if (files.Empty()) {
    return {ReindexAction::UpToDate, files.added, files.changed, files.removed};
  }
  // Delta needs the view's support (recorded at fast-path resolution) and a
  // pk-term index (pre-pk-term indexes cannot term-remove -- one rebuild
  // migrates them).
  if (src.fast_path.supports_delta && options.pk_term) {
    RunDelta(context, conn_ctx, target, files, observe, manifest, storage);
    return {ReindexAction::Delta, files.added, files.changed, files.removed,
            static_cast<int64_t>(files.scan.size()) - files.added};
  }
  RunFullRebuild(context, conn_ctx, target);
  return {ReindexAction::Rebuild, files.added, files.changed, files.removed,
          static_cast<int64_t>(src.files.size())};
}

// The iceberg refresh: files are versioned by their max applicable delete
// sequence number (data files are write-once -- only the delete side can
// move), changed files mask their dead rows in place when possible, and the
// snapshot pin chases the current snapshot even when no file moved.
ReindexOutcome IcebergRefresh(
  duckdb::ClientContext& context, ConnectionContext& conn_ctx,
  const ReindexTarget& target, const Source& src,
  const search::FileManifest& manifest, search::InvertedIndexStorage& storage,
  const catalog::InvertedIndexOptions& options,
  const std::shared_ptr<const catalog::Snapshot>& snapshot) {
  IcebergObserve observe{*src.iceberg_list,
                         src.bind->Cast<duckdb::MultiFileBindData>()};
  auto files = DiffListing(src, manifest, observe);

  if (files.Empty()) {
    if (files.version != manifest.version) {
      // Identical files under a moved pin (a no-op commit): republish the
      // same entries with the version restamped, so reads never pin an
      // expiring snapshot. No docs move -- the no-changes refresh re-pairs
      // the live reader with this manifest. Durability rides the next real
      // commit; a restart before that re-observes the moved pin here.
      auto restamped = std::make_shared<search::FileManifest>(manifest);
      restamped->version = files.version;
      storage.SetFileManifest(std::move(restamped));
      PublishRefresh(storage, target, "restamp publish failed");
    }
    return {ReindexAction::UpToDate, files.added, files.changed, files.removed};
  }
  // Delta needs the view's support (recorded at fast-path resolution) and a
  // pk-term index (pre-pk-term indexes cannot term-remove -- one rebuild
  // migrates them).
  if (src.fast_path.supports_delta && options.pk_term) {
    if (!observe.eq_covered.empty() &&
        !TranslateEqualityRemoves(context, conn_ctx, snapshot, target, src,
                                  files, observe)) {
      // Translation refused (a delete column the index cannot query): the
      // covered files take the rescan road after all.
      for (auto& covered : observe.eq_covered) {
        std::erase_if(observe.del_masks, [&](const auto& mask) {
          return mask.file_id == covered.file_id;
        });
        files.del_files.push_back(covered.file_id);
        files.scan.push_back(std::move(covered.live));
      }
      observe.eq_covered.clear();
      // The appends broke the diff's listing order, which the batched pass
      // relies on (narrowed enumeration order == scan order == assigned-id
      // order) -- restore it.
      containers::FlatHashMap<std::string_view, size_t> listing_pos;
      listing_pos.reserve(src.files.size());
      for (size_t i = 0; i < src.files.size(); ++i) {
        listing_pos.emplace(src.files[i].path, i);
      }
      absl::c_sort(files.scan, [&](const auto& a, const auto& b) {
        return listing_pos.find(a.path)->second <
               listing_pos.find(b.path)->second;
      });
    }
    RunDelta(context, conn_ctx, target, files, observe, manifest, storage);
    return {ReindexAction::Delta, files.added, files.changed, files.removed,
            static_cast<int64_t>(files.scan.size()) - files.added};
  }
  RunFullRebuild(context, conn_ctx, target);
  return {ReindexAction::Rebuild, files.added, files.changed, files.removed,
          static_cast<int64_t>(src.files.size())};
}

// The REFRESH pipeline for one view-backed inverted index: gate -> observe
// the source -> plan (up_to_date / delta / rebuild) -> execute. Everything
// the serenedb_reindex TF / PRAGMA / REINDEX INDEX statement does after
// argument parsing; the periodic tick runs it through the same road. Empty
// schema/catalog resolve to the connection's defaults. Throws on failure.
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
  // No manifest (an external-pk view index never captured one) or no
  // observable source behind the view: full rebuild, nothing to count.
  if (!src) {
    RunFullRebuild(context, conn_ctx, target);
    return {};
  }
  if (src->version && src->version == manifest->version) {
    // Snapshots are immutable: an unmoved pin proves an empty diff. Skip
    // the listing and the delete-manifest walk entirely -- with a periodic
    // refresh most ticks land exactly here.
    return {ReindexAction::UpToDate, 0, 0, 0, 0};
  }
  src->files = src->list->GetAllFiles();
  if (src->iceberg_list) {
    return IcebergRefresh(context, conn_ctx, target, *src, *manifest, *storage,
                          inverted.GetOptions(), snapshot);
  }
  return MultiFileRefresh(context, conn_ctx, target, *src, *manifest, *storage,
                          inverted.GetOptions());
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

struct ReindexGlobalState : public duckdb::GlobalTableFunctionState {
  bool done = false;
};

void ReindexExecute(duckdb::ClientContext& context,
                    duckdb::TableFunctionInput& input,
                    duckdb::DataChunk& output) {
  auto* gstate = input.global_state
                   ? &input.global_state->Cast<ReindexGlobalState>()
                   : nullptr;
  if (gstate && gstate->done) {
    return;
  }
  auto& bind = input.bind_data->Cast<ReindexBindData>();
  const auto outcome =
    RunReindex(context, bind.name, bind.schema, bind.catalog);
  if (!gstate) {
    return;  // PRAGMA / REINDEX statement form: silent, PG-style.
  }
  const auto action_name = [](ReindexAction action) -> std::string {
    switch (action) {
      case ReindexAction::UpToDate:
        return "up_to_date";
      case ReindexAction::Delta:
        return "delta";
      case ReindexAction::Rebuild:
        return "rebuild";
    }
    return {};
  };
  output.SetValue(0, 0, duckdb::Value(action_name(outcome.action)));
  output.SetValue(1, 0, duckdb::Value::BIGINT(outcome.files_added));
  output.SetValue(2, 0, duckdb::Value::BIGINT(outcome.files_changed));
  output.SetValue(3, 0, duckdb::Value::BIGINT(outcome.files_removed));
  output.SetValue(4, 0, duckdb::Value::BIGINT(outcome.files_rescanned));
  output.SetCardinality(1);
  gstate->done = true;
}

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

void ReindexPragma(duckdb::ClientContext& context,
                   const duckdb::FunctionParameters& parameters) {
  ReindexBindData bind_data;
  FillReindexArgs(bind_data, parameters.values);

  duckdb::DataChunk dummy;
  duckdb::TableFunctionInput input{&bind_data, nullptr, nullptr};
  ReindexExecute(context, input, dummy);
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> ReindexInitGlobal(
  duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
  return duckdb::make_uniq<ReindexGlobalState>();
}

// The periodic-refresh tick body (search's SourceRefreshLoop calls it): one
// REINDEX of `index_id` on an internal session impersonating the index
// OWNER -- the exact identity a manual owner-run REINDEX has, so the
// MAINTAIN check and the global-settings mirror behave identically (a
// fresh internal connection reads GLOBAL values, and PassConnection
// replays them into the passes). Quiet outcomes return OK: the index or
// its owner vanished between ticks, or the claim was lost to a manual
// REINDEX -- the next tick re-evaluates.
absl::Status RunSourceRefreshTick(duckdb::DatabaseInstance& db,
                                  ObjectId index_id) {
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
      // Dropped between ticks -- the loop dies with the storage.
      return absl::OkStatus();
    }
    // An index object carries no owner of its own -- ownership (and the
    // MAINTAIN check ResolveTarget runs) lives on the RELATION. Roles
    // resolve by NAME in the snapshot (GetObject-by-id does not cover
    // them), so scan the role list for the relation owner's id.
    std::shared_ptr<catalog::Role> owner;
    for (auto& role : snapshot->GetRoles()) {
      if (role->GetId() == relation->GetOwner()) {
        owner = std::move(role);
        break;
      }
    }
    if (!owner) {
      // A live index whose owner role is gone: skipping quietly would look
      // like a healthy loop that never refreshes -- surface it so the loop
      // warns and backs off.
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
    // A wire session plants the statement snapshot at message boundaries;
    // this internal tick is its own statement boundary.
    ctx->SetCatalogSnapshot(snapshot);
    // No client sits behind this session: notices the passes forward here
    // have nowhere to go (the context asserts none survive destruction).
    absl::Cleanup drop_notices = [&] { ctx->ConsumeNotices([](auto&) {}); };
    // The observe binds the view on THIS connection; duckdb catalog lookups
    // along the way (secrets for object stores among them) require an
    // active transaction. The tick's own connection never commits anything
    // -- every real write goes through the pass connections.
    conn.BeginTransaction();
    absl::Cleanup rollback = [&] {
      try {
        conn.Rollback();
      } catch (...) {
      }
    };

    const auto outcome = RunReindex(
      *conn.context, std::string{index->GetName()},
      std::string{schema->GetName()}, std::string{database->GetName()});
    if (outcome.action != ReindexAction::UpToDate) {
      SDB_INFO(
        SEARCH, "periodic source refresh of index '", index->GetName(),
        "': ", outcome.action == ReindexAction::Delta ? "delta" : "rebuild",
        ", files added ", outcome.files_added, ", changed ",
        outcome.files_changed, ", removed ", outcome.files_removed,
        ", rescanned ", outcome.files_rescanned);
    }
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

  // The search layer's SourceRefreshLoop drives periodic REINDEX ticks
  // through this callback -- installed here because the tick needs this
  // layer (an internal connection on the facade instance + the REINDEX
  // road). `db` outlives the loops: SearchEngine::stop() joins them
  // before the engine tears the instance down.
  search::SetSourceRefreshRunner(
    [&db](ObjectId index_id) { return RunSourceRefreshTick(db, index_id); });

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
