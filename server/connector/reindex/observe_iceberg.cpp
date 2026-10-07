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

#include <duckdb/common/multi_file/multi_file_states.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/query_result.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/comparison_expression.hpp>
#include <duckdb/parser/expression/conjunction_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/expression/function_expression.hpp>
#include <duckdb/parser/expression/operator_expression.hpp>
#include <duckdb/parser/expression/star_expression.hpp>
#include <duckdb/parser/expression/subquery_expression.hpp>
#include <duckdb/parser/parsed_data/create_view_info.hpp>
#include <duckdb/parser/query_node/select_node.hpp>
#include <duckdb/parser/statement/select_statement.hpp>
#include <duckdb/parser/tableref/basetableref.hpp>
#include <duckdb/parser/tableref/column_data_ref.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <map>
#include <optional>

#include "connector/reindex/observe.h"
#include "connector/view_fast_path.h"
#include "core/deletes/iceberg_deletion_vector.hpp"
#include "core/deletes/iceberg_equality_delete.hpp"
#include "core/deletes/iceberg_positional_delete.hpp"
#include "core/metadata/iceberg_table_metadata.hpp"
#include "core/metadata/snapshot/iceberg_snapshot.hpp"
#include "planning/iceberg_multi_file_list.hpp"
#include "planning/snapshot/iceberg_snapshot_scan_info.hpp"
#include "search/inverted_index_storage.h"

namespace sdb::connector {
namespace {

using EqualityDeleteFiles =
  duckdb::vector<duckdb::reference<const duckdb::IcebergEqualityDeleteFile>>;

const duckdb::vector<duckdb::MultiFileColumnDefinition>& GlobalScanColumns(
  const duckdb::MultiFileBindData& bind) {
  return bind.reader_bind.schema.empty() ? bind.columns
                                         : bind.reader_bind.schema;
}

// True when `snapshot_id` is the list's pinned snapshot or one of its
// ancestors. False = the indexed snapshot left the table's history
// (expired, rollback, replaced table): deletes may have been UNDONE, which
// no sequence-number comparison can see -- only a rebuild converges.
bool SnapshotIsAncestor(const duckdb::IcebergScanPlanner& planner,
                        int64_t snapshot_id) {
  const auto& metadata = planner.GetMetadata();
  auto snapshot = planner.GetSnapshot().snapshot;
  while (snapshot) {
    if (snapshot->snapshot_id == snapshot_id) {
      return true;
    }
    if (!snapshot->parent_snapshot_id) {
      return false;
    }
    snapshot = metadata.FindSnapshotByIdInternal(*snapshot->parent_snapshot_id);
  }
  return false;
}

uint64_t SequenceNumberOf(const duckdb::IcebergScanPlanner& planner,
                          int64_t snapshot_id) {
  const auto snapshot =
    planner.GetMetadata().FindSnapshotByIdInternal(snapshot_id);
  return snapshot ? static_cast<uint64_t>(snapshot->sequence_number.value_or(0))
                  : 0;
}

bool IsNew(const std::optional<int64_t>& sequence_number, uint64_t baseline) {
  return !sequence_number || static_cast<uint64_t>(*sequence_number) > baseline;
}

bool ExistedAtBaseline(const duckdb::IcebergScanPlanner& planner,
                       uint64_t listing_idx, uint64_t baseline) {
  if (baseline == 0) {
    return false;
  }
  const auto file = planner.GetDataFileDescriptor(listing_idx);
  return file && !IsNew(file->sequence_number, baseline);
}

void ReadPositionalDeletes(const duckdb::IcebergMultiFileList& list,
                           const duckdb::IcebergFileScanTask& task,
                           roaring::Roaring64Map& rows) {
  const auto data =
    list.GetExistingPositionalDeleteData(task.original_file_path);
  if (!data) {
    return;
  }
  switch (data->type) {
    case duckdb::IcebergDeleteType::POSITIONAL_DELETE: {
      const auto* positions =
        static_cast<const duckdb::IcebergPositionalDeleteData&>(*data)
          .invalid_rows.get();
      std::vector<uint64_t> values(
        roaring::api::roaring64_bitmap_get_cardinality(positions));
      roaring::api::roaring64_bitmap_to_uint64_array(positions, values.data());
      rows.addMany(values.size(), values.data());
    } break;
    case duckdb::IcebergDeleteType::DELETION_VECTOR:
      for (const auto& [high, bitmap] :
           static_cast<const duckdb::IcebergDeletionVectorData&>(*data)
             .bitmaps) {
        const auto base = static_cast<uint64_t>(high) << 32;
        for (const auto low : bitmap) {
          rows.add(base | low);
        }
      }
      break;
  }
}

struct TouchedFile {
  uint64_t listing_idx;
  duckdb::IcebergFileScanTask task;
  const HeldFile& held;
};

struct EqualityCovered {
  uint64_t file_id;
  uint64_t listing_idx;
  EqualityDeleteFiles deletes;
};

void MaskTouched(const duckdb::IcebergMultiFileList& list,
                 const TouchedFile& touched, RefreshPlan& plan,
                 std::vector<EqualityCovered>& covered) {
  const auto file_id = touched.held.ids.front();
  auto deletes = list.ProcessDeletes(touched.task);
  roaring::Roaring64Map rows;
  ReadPositionalDeletes(list, touched.task, rows);
  if (!rows.isEmpty()) {
    plan.masks.emplace(file_id, std::move(rows));
  }
  if (absl::c_any_of(deletes.equality_deletes, [](const auto& delete_file) {
        return delete_file.get().equality_values.size() != 0;
      })) {
    covered.push_back(
      {file_id, touched.listing_idx, std::move(deletes.equality_deletes)});
  }
}

struct EqualityRow {
  std::vector<uint64_t> columns;
  std::vector<duckdb::Value> values;

  bool HasNull() const {
    return absl::c_any_of(
      values, [](const duckdb::Value& value) { return value.IsNull(); });
  }
};

// One applicable-delete set and the covered files it applies to.
struct EqualityGroup {
  const EqualityDeleteFiles* deletes;
  std::vector<const EqualityCovered*> files;
};

std::vector<EqualityGroup> GroupByDeletes(
  const std::vector<EqualityCovered>& covered) {
  std::vector<EqualityGroup> groups;
  irs::containers::FlatHashMap<
    std::vector<const duckdb::IcebergEqualityDeleteFile*>, size_t>
    group_of;
  for (const auto& file : covered) {
    // Deterministic enumeration order makes the pointer sequence a stable key.
    std::vector<const duckdb::IcebergEqualityDeleteFile*> key;
    key.reserve(file.deletes.size());
    for (const auto& delete_file : file.deletes) {
      key.push_back(&delete_file.get());
    }
    const auto [it, inserted] = group_of.emplace(std::move(key), groups.size());
    if (inserted) {
      groups.push_back({.deletes = &file.deletes});
    }
    groups[it->second].files.push_back(&file);
  }
  return groups;
}

// Field id -> view output position.
std::optional<uint64_t> ViewColumnOf(
  int32_t field_id,
  const duckdb::vector<duckdb::MultiFileColumnDefinition>& columns,
  const ViewFastPath& fast_path, const duckdb::CreateViewInfo& view_info) {
  const auto source = absl::c_find_if(
    columns, [&](const duckdb::MultiFileColumnDefinition& column) {
      return !column.identifier.IsNull() &&
             column.GetIdentifierFieldId() == field_id;
    });
  if (source == columns.end()) {
    return std::nullopt;
  }
  uint64_t position = source - columns.begin();
  if (!fast_path.projection_columns.empty()) {
    const auto projected = absl::c_find(fast_path.projection_columns,
                                        source->name.GetIdentifierName());
    if (projected == fast_path.projection_columns.end()) {
      return std::nullopt;
    }
    position = projected - fast_path.projection_columns.begin();
  }
  if (position >= view_info.types.size() ||
      view_info.types[position] != source->type) {
    return std::nullopt;
  }
  return position;
}

std::string NullRowKey(const EqualityRow& row) {
  std::string key;
  for (size_t i = 0; i < row.columns.size(); ++i) {
    absl::StrAppend(&key, row.columns[i], "=", row.values[i].ToSQLString(),
                    ";");
  }
  return key;
}

// Empty = no road (a column outside the view output, or no rows). Only
// IS-NULL-carrying rows are deduplicated: they become an OR branch each;
// NULL-free rows feed an IN semi-join, repeats can't hurt.
std::vector<EqualityRow> ParseEqualityRows(
  const EqualityDeleteFiles& deletes,
  const duckdb::vector<duckdb::MultiFileColumnDefinition>& columns,
  const ViewFastPath& fast_path, const duckdb::CreateViewInfo& view_info) {
  std::vector<EqualityRow> rows;
  irs::containers::FlatHashSet<std::string> null_rows;
  for (const auto& delete_file : deletes) {
    const auto& file = delete_file.get();
    std::vector<uint64_t> positions;
    positions.reserve(file.equality_ids.size());
    for (const auto field_id : file.equality_ids) {
      const auto column = ViewColumnOf(field_id, columns, fast_path, view_info);
      if (!column) {
        return {};
      }
      positions.push_back(*column);
    }
    const auto& values = file.equality_values;
    for (duckdb::idx_t row = 0; row < values.size(); ++row) {
      std::map<uint64_t, duckdb::Value> conjuncts;
      for (size_t column = 0; column < positions.size(); ++column) {
        conjuncts.emplace(positions[column], values.GetValue(column, row));
      }
      EqualityRow parsed;
      for (auto& [column, value] : conjuncts) {
        parsed.columns.push_back(column);
        parsed.values.push_back(std::move(value));
      }
      if (parsed.HasNull() && !null_rows.emplace(NullRowKey(parsed)).second) {
        continue;
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

duckdb::unique_ptr<duckdb::ParsedExpression> ViewColumn(
  const duckdb::CreateViewInfo& view_info, uint64_t position) {
  return duckdb::make_uniq<duckdb::ColumnRefExpression>(
    view_info.names[position]);
}

duckdb::unique_ptr<duckdb::ParsedExpression> MatchesRow(
  const EqualityRow& row, const duckdb::CreateViewInfo& view_info) {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> conjuncts;
  conjuncts.reserve(row.columns.size());
  for (size_t i = 0; i < row.columns.size(); ++i) {
    auto column = ViewColumn(view_info, row.columns[i]);
    if (row.values[i].IsNull()) {
      conjuncts.push_back(duckdb::make_uniq<duckdb::OperatorExpression>(
        duckdb::ExpressionType::OPERATOR_IS_NULL, std::move(column)));
    } else {
      conjuncts.push_back(duckdb::make_uniq<duckdb::ComparisonExpression>(
        duckdb::ExpressionType::COMPARE_EQUAL, std::move(column),
        duckdb::ConstantExpression::FromValue(row.values[i])));
    }
  }
  return CombineExprs(duckdb::ExpressionType::CONJUNCTION_AND,
                      std::move(conjuncts));
}

duckdb::unique_ptr<duckdb::ParsedExpression> InDeletedValues(
  const std::vector<uint64_t>& columns,
  const std::vector<const EqualityRow*>& rows,
  const duckdb::CreateViewInfo& view_info) {
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::Identifier> names;
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> keys;
  types.reserve(columns.size());
  names.reserve(columns.size());
  keys.reserve(columns.size());
  for (const auto column : columns) {
    types.push_back(view_info.types[column]);
    names.push_back(view_info.names[column]);
    keys.push_back(ViewColumn(view_info, column));
  }
  auto& allocator = duckdb::Allocator::DefaultAllocator();
  auto collection =
    duckdb::make_uniq<duckdb::ColumnDataCollection>(allocator, types);
  duckdb::DataChunk chunk;
  chunk.Initialize(allocator, types);
  for (const auto* row : rows) {
    const auto index = chunk.size();
    for (size_t c = 0; c < columns.size(); ++c) {
      const auto& value = row->values[c];
      chunk.SetValue(
        c, index,
        value.type() == types[c] ? value : value.DefaultCastAs(types[c]));
    }
    chunk.SetCardinality(index + 1);
    if (chunk.size() == STANDARD_VECTOR_SIZE) {
      collection->Append(chunk);
      chunk.Reset();
    }
  }
  if (chunk.size() != 0) {
    collection->Append(chunk);
  }
  auto values = duckdb::make_uniq<duckdb::ColumnDataRef>(std::move(collection),
                                                         std::move(names));
  // The subquery's star expansion qualifies columns by the binding alias;
  // an anonymous ref has none and the binder throws.
  values->alias = duckdb::Identifier{"eq_delete_rows"};
  auto select_node = duckdb::make_uniq<duckdb::SelectNode>();
  select_node->select_list.push_back(
    duckdb::make_uniq<duckdb::StarExpression>());
  select_node->from_table = std::move(values);
  auto select = duckdb::make_uniq<duckdb::SelectStatement>();
  select->node = std::move(select_node);
  auto in = duckdb::make_uniq<duckdb::SubqueryExpression>();
  in->GetSubqueryTypeMutable() = duckdb::SubqueryType::ANY;
  in->GetComparisonTypeMutable() = duckdb::ExpressionType::COMPARE_EQUAL;
  in->SubqueryMutable() = std::move(select);
  in->GetChildMutable() =
    keys.size() == 1
      ? std::move(keys[0])
      : duckdb::make_uniq<duckdb::FunctionExpression>("row", std::move(keys));
  return in;
}

// NULL-free rows ride `(cols...) IN (SELECT * FROM <materialized>)` per
// uniform column set -- one ColumnDataCollection (no per-row expression
// nodes), planned as a hash semi-join. IS-NULL rows cannot ride IN (NULL
// never matches) and stay plain OR branches.
duckdb::unique_ptr<duckdb::ParsedExpression> EqualityCondition(
  const std::vector<EqualityRow>& rows,
  const duckdb::CreateViewInfo& view_info) {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> branches;
  std::map<std::vector<uint64_t>, std::vector<const EqualityRow*>> by_columns;
  for (const auto& row : rows) {
    if (row.HasNull()) {
      branches.push_back(MatchesRow(row, view_info));
    } else {
      by_columns[row.columns].push_back(&row);
    }
  }
  for (const auto& [columns, group] : by_columns) {
    branches.push_back(InDeletedValues(columns, group, view_info));
  }
  return CombineExprs(duckdb::ExpressionType::CONJUNCTION_OR,
                      std::move(branches));
}

// file_index IN (covered...) -- the pk's flat file half.
duckdb::unique_ptr<duckdb::ParsedExpression> FileScope(
  const std::vector<const EqualityCovered*>& files) {
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> children;
  children.reserve(files.size() + 1);
  children.push_back(duckdb::make_uniq<duckdb::ColumnRefExpression>(
    duckdb::Identifier{"file_index"}));
  for (const auto* file : files) {
    children.push_back(duckdb::ConstantExpression::FromValue(
      duckdb::Value::UBIGINT(file->file_id)));
  }
  return duckdb::make_uniq<duckdb::OperatorExpression>(
    duckdb::ExpressionType::COMPARE_IN, std::move(children));
}

void CollectDeadRows(const ObserveInput& in,
                     duckdb::unique_ptr<duckdb::ParsedExpression> condition,
                     std::map<uint64_t, roaring::Roaring64Map>& masks) {
  // Statement object, no SQL text: values travel verbatim.
  auto select = duckdb::make_uniq<duckdb::SelectNode>();
  select->select_list.emplace_back(
    duckdb::make_uniq<duckdb::ColumnRefExpression>(
      duckdb::Identifier{"file_index"}));
  select->select_list.emplace_back(
    duckdb::make_uniq<duckdb::ColumnRefExpression>(
      duckdb::Identifier{"row_number"}));
  auto table = duckdb::make_uniq<duckdb::BaseTableRef>();
  table->SetQualifiedName(in.index);
  select->from_table = std::move(table);
  select->where_clause = std::move(condition);
  auto statement = duckdb::make_uniq<duckdb::SelectStatement>();
  statement->node = std::move(select);
  auto result =
    in.context.Query(std::move(statement), duckdb::QueryParameters{});
  if (result->HasError()) {
    result->ThrowError();
  }
  for (auto chunk = result->Fetch(); chunk && chunk->size() != 0;
       chunk = result->Fetch()) {
    for (duckdb::idx_t row = 0; row < chunk->size(); ++row) {
      const auto file = chunk->GetValue(0, row);
      const auto row_number = chunk->GetValue(1, row);
      if (file.IsNull() || row_number.IsNull()) {
        continue;
      }
      masks[file.GetValue<uint64_t>()].add(row_number.GetValue<uint64_t>());
    }
  }
}

// False = no road -- the caller demotes the covered files to rescans.
bool RemoveEqualityRows(const ObserveInput& in,
                        const std::vector<EqualityCovered>& covered,
                        RefreshPlan& plan) {
  // A real view column shadows the flat pk half the file scope binds by:
  // no eq road, the covered files rescan instead.
  if (absl::c_linear_search(in.view_info.names, "file_index") ||
      absl::c_linear_search(in.view_info.names, "row_number")) {
    return false;
  }
  const auto& columns = GlobalScanColumns(*in.bind);
  duckdb::vector<duckdb::unique_ptr<duckdb::ParsedExpression>> branches;
  for (const auto& group : GroupByDeletes(covered)) {
    const auto rows =
      ParseEqualityRows(*group.deletes, columns, *in.fast_path, in.view_info);
    if (rows.empty()) {
      return false;
    }
    branches.push_back(duckdb::make_uniq<duckdb::ConjunctionExpression>(
      duckdb::ExpressionType::CONJUNCTION_AND,
      EqualityCondition(rows, in.view_info), FileScope(group.files)));
  }
  CollectDeadRows(
    in,
    CombineExprs(duckdb::ExpressionType::CONJUNCTION_OR, std::move(branches)),
    plan.masks);
  return true;
}

void ApplyRowDeletes(const ObserveInput& in,
                     const duckdb::IcebergMultiFileList& list,
                     const std::vector<TouchedFile>& touched, RefreshPlan& plan,
                     std::vector<uint64_t>& scan) {
  std::vector<EqualityCovered> covered;
  for (const auto& file : touched) {
    MaskTouched(list, file, plan, covered);
  }
  if (covered.empty() || RemoveEqualityRows(in, covered, plan)) {
    return;
  }
  // Every eq-covered file demotes to remove-and-rescan (its masks drop --
  // the rescan supersedes them).
  for (const auto& file : covered) {
    plan.masks.erase(file.file_id);
    plan.drop.push_back(file.file_id);
    scan.push_back(file.listing_idx);
  }
}

RefreshPlan ObserveRowDeletes(const ObserveInput& in,
                              const duckdb::IcebergMultiFileList& list,
                              uint64_t baseline, const SourceListing& listing,
                              RefreshPlan plan) {
  const auto& planner = list.GetScanPlanner();
  const auto& held_position = in.snapshot.position;
  if (held_position.snapshot_id != 0 && plan.position.snapshot_id != 0 &&
      !SnapshotIsAncestor(planner, held_position.snapshot_id)) {
    PlanRebuild(plan, listing, in.next_file_id);
    return plan;
  }
  const auto held = CollectHeldFiles(in.snapshot.reader, *in.snapshot.files);
  if (!held.complete) {
    PlanRebuild(plan, listing, in.next_file_id);
    return plan;
  }
  const auto deletes_from =
    static_cast<duckdb::sequence_number_t>(baseline + 1);
  const bool new_deletes = planner.HasDeleteManifestsFrom(deletes_from);
  std::vector<uint64_t> scan;
  std::vector<TouchedFile> touched;
  for (size_t i = 0; i < listing.files.size(); ++i) {
    const auto it = held.by_path.find(listing.files[i].path);
    const auto* file = it == held.by_path.end() ? nullptr : &it->second;
    if (!file && !ExistedAtBaseline(planner, i, baseline)) {
      ++plan.outcome.files_added;
      scan.push_back(i);
      continue;
    }
    if (!new_deletes) {
      continue;
    }
    auto task = planner.GetScanTask(i, deletes_from);
    if (!task) {
      continue;
    }
    std::erase_if(task->delete_files, [&](const auto& delete_file) {
      return !IsNew(delete_file.sequence_number, baseline);
    });
    if (task->delete_files.empty()) {
      continue;
    }
    ++plan.outcome.files_changed;
    if (!file) {
      continue;
    }
    if (file->ids.size() > 1) {
      plan.drop.insert(plan.drop.end(), file->ids.begin(), file->ids.end());
      scan.push_back(i);
      continue;
    }
    touched.push_back({i, std::move(*task), *file});
  }
  DropUnlisted(plan, listing, held);
  if (!plan.outcome.FilesChanged()) {
    return plan;
  }
  if (!in.delta) {
    PlanRebuild(plan, listing, in.next_file_id);
    return plan;
  }
  if (!touched.empty()) {
    ApplyRowDeletes(in, list, touched, plan, scan);
  }
  PlanDelta(plan, listing, std::move(scan), in.next_file_id, held);
  return plan;
}

}  // namespace

RefreshPlan ObserveIceberg(const ObserveInput& in) {
  auto& list = dynamic_cast<duckdb::IcebergMultiFileList&>(*in.bind->file_list);
  auto& planner = list.GetScanPlanner();
  planner.DisableServerSidePlanning();
  const auto baseline =
    SequenceNumberOf(planner, in.snapshot.position.snapshot_id);
  const auto& snapshot = planner.GetSnapshot().snapshot;
  RefreshPlan plan;
  plan.position = {
    .definition = in.definition,
    .snapshot_id = snapshot ? snapshot->snapshot_id.value_or(0) : 0};
  if (plan.position == in.snapshot.position) {
    list.GetTotalFileCount();
    return plan;
  }
  const auto listing = ListSource(in.context, list, /*versioned=*/false);
  if (in.snapshot.position.definition != in.definition) {
    PlanRebuild(plan, listing, in.next_file_id);
    return plan;
  }
  switch (planner.GetMetadata().iceberg_version) {
    case 1:
      return PlanFileDiff(in, listing, std::move(plan));
    case 2:
    case 3:
      return ObserveRowDeletes(in, list, baseline, listing, std::move(plan));
    default:
      PlanRebuild(plan, listing, in.next_file_id);
      return plan;
  }
}

}  // namespace sdb::connector
