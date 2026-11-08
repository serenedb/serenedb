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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>

#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <ranges>
#include <span>

#include "connector/column_id.h"
#include "connector/functions/vector.h"
#include "connector/scan/scan_bind.h"
#include "connector/scan/scan_function.h"
#include "connector/search_filter_printer.hpp"
#include "connector/term_dict.h"
#include "pg/types/logical.h"
#include "search/scorer_options.h"

namespace sdb::connector {
namespace {

void AppendTypeSuffix(std::string& name, const duckdb::LogicalType& type) {
  using enum duckdb::LogicalTypeId;
  if (pg::IsInetOrCidr(type)) {
    name += pg::IsCidr(type) ? "(cidr)" : "(inet)";
    return;
  }
  switch (type.id()) {
    case SQLNULL:
      name += "(null)";
      break;
    case BOOLEAN:
      name += "(bool)";
      break;
    case VARCHAR:
    case BLOB:
    case GEOMETRY:
      name += "(string)";
      break;
    default:
      if (term_dict::IsNumeric(type.id())) {
        name += "(numeric)";
      }
      break;
  }
}

std::string_view DictSuffix(irs::DictRef dict) {
  const bool json_leaves = irs::IsJson(dict.layout);
  switch (dict.member) {
    case irs::DictMember::Nulls:
      return "(null)";
    case irs::DictMember::JsonNulls:
      return "(json null)";
    case irs::DictMember::NullElements:
      return "(null element)";
    case irs::DictMember::False:
    case irs::DictMember::True:
      return json_leaves ? "(bool)" : "";
    case irs::DictMember::Numbers:
      return json_leaves ? "(numeric)" : "";
    case irs::DictMember::Strings:
      return {};
  }
  return {};
}

const catalog::ConstraintKey* KeyOfField(const ScanBindData& bind_data,
                                         irs::field_id field) {
  if (!bind_data.relation.IsSearchTable()) {
    return nullptr;
  }
  for (const auto& key : bind_data.relation.ScannedIndex().constraint_keys) {
    if (key.field == field && key.columns.size() == 1 &&
        (key.expressions.empty() || key.expressions.front().empty())) {
      return &key;
    }
  }
  return nullptr;
}

auto MakeFieldNameResolver(const ScanBindData& bind_data) {
  return [&bind_data](ColumnId col_id, irs::DictRef dict) -> std::string {
    if (const auto* key = KeyOfField(bind_data, col_id)) {
      return absl::StrCat(bind_data.DisplayColumnName(key->columns.front()),
                          "(key)");
    }
    const auto& config = *bind_data.relation.inverted_config;
    auto base = bind_data.DisplayColumnName(col_id);
    const auto column_type = bind_data.ColumnTypeById(col_id);
    const bool found_type = column_type.id() != duckdb::LogicalTypeId::INVALID;
    const auto lookup = config.LookupField(col_id);
    if (lookup.entry_field_id == term_dict::kPKFieldId) {
      const auto name = bind_data.ColumnNameById(kGeneratedPKId);
      return std::string{name.empty() ? std::string_view{"sdb_generated_pk"}
                                      : name} +
             "(pk)";
    }
    if (lookup.entry) {
      const auto& entry = *lookup.entry;
      if (col_id == lookup.entry_field_id) {
        if (const auto suffix = DictSuffix(dict); !suffix.empty()) {
          return absl::StrCat(base, suffix);
        }
        const auto expr_type_id = config.ExpressionType(col_id).id();
        const bool is_expr = expr_type_id != duckdb::LogicalTypeId::INVALID;
        if (is_expr) {
          AppendTypeSuffix(base, duckdb::LogicalType{expr_type_id});
        } else if (found_type) {
          AppendTypeSuffix(base, column_type);
        } else if (entry.HasTextDictionary()) {
          base += "(string)";
        }
        return base;
      }
      if (col_id == entry.synthetic_column) {
        return bind_data.DisplayColumnName(lookup.entry_field_id) +
               "(synthetic)";
      }
    }
    if (found_type) {
      AppendTypeSuffix(base, column_type);
    }
    return base;
  };
}

duckdb::LogicalType ElementType(const duckdb::LogicalType& type) {
  const auto* leaf = &type;
  for (;;) {
    switch (leaf->id()) {
      case duckdb::LogicalTypeId::LIST:
        leaf = &duckdb::ListType::GetChildType(*leaf);
        continue;
      case duckdb::LogicalTypeId::ARRAY:
        leaf = &duckdb::ArrayType::GetChildType(*leaf);
        continue;
      default:
        return *leaf;
    }
  }
}

auto MakeFieldTypeResolver(const ScanBindData& bind_data) {
  return [&bind_data](ColumnId col_id,
                      irs::DictRef dict) -> duckdb::LogicalType {
    if (const auto* key = KeyOfField(bind_data, col_id)) {
      return bind_data.ColumnTypeById(key->columns.front());
    }
    const auto& config = *bind_data.relation.inverted_config;
    const auto lookup = config.LookupField(col_id);
    if (lookup.entry_field_id == term_dict::kPKFieldId) {
      return duckdb::LogicalType::BIGINT;
    }
    switch (dict.member) {
      case irs::DictMember::Nulls:
      case irs::DictMember::JsonNulls:
      case irs::DictMember::NullElements:
        return duckdb::LogicalType::SQLNULL;
      case irs::DictMember::False:
      case irs::DictMember::True:
        return duckdb::LogicalType::BOOLEAN;
      case irs::DictMember::Numbers:
        if (irs::IsJson(dict.layout)) {
          return duckdb::LogicalType::DOUBLE;
        }
        break;
      case irs::DictMember::Strings:
        break;
    }
    if (lookup.entry && col_id == lookup.entry_field_id) {
      if (const auto expr_type = config.ExpressionType(col_id);
          expr_type.id() != duckdb::LogicalTypeId::INVALID) {
        return ElementType(expr_type);
      }
      const auto column_type =
        bind_data.ColumnTypeById(config.ColumnOf(col_id));
      if (column_type.id() != duckdb::LogicalTypeId::INVALID) {
        return ElementType(column_type);
      }
      return duckdb::LogicalType::VARCHAR;
    }
    const auto column_type = bind_data.ColumnTypeById(config.ColumnOf(col_id));
    if (column_type.id() != duckdb::LogicalTypeId::INVALID) {
      return ElementType(column_type);
    }
    return duckdb::LogicalType{};
  };
}

std::string_view VectorMetricFunctionName(irs::VectorMetric metric) {
  switch (metric) {
    case irs::VectorMetric::L2Sqr:
      return kL2Distance;
    case irs::VectorMetric::L1:
      return kL1Distance;
    case irs::VectorMetric::Cosine:
      return kCosineDistance;
    case irs::VectorMetric::InnerProduct:
      return kIP;
  }
  SDB_UNREACHABLE();
}

struct ProjectionEntry {
  std::string name;
  bool from_index = false;
  bool is_virtual = false;
};

std::string ProjectionDisplayName(const ScanBindData& bind,
                                  const duckdb::ColumnIndex& column_index,
                                  const duckdb::vector<std::string>& names) {
  const auto col_id = column_index.GetPrimaryIndex();
  if (col_id < names.size()) {
    if (column_index.IsPushdownExtract() && column_index.HasChildren() &&
        col_id < bind.columns.types.size()) {
      std::vector<std::string_view> path{names[col_id]};
      DecodeExtractPath(column_index, bind.columns.types[col_id], path);
      return absl::StrJoin(path, ".");
    }
    return names[col_id];
  }
  if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
    return "row_id";
  }
  if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_NUMBER) {
    return "row_number";
  }
  if (col_id == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX) {
    return "file_index";
  }
  if (col_id == duckdb::MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
    return "file_row_number";
  }
  if (col_id == kColumnIdentifierTableOid) {
    return "tableoid";
  }
  if (col_id == kColumnIdentifierGeneratedPk) {
    return "pk";
  }
  if (col_id == kColumnIdentifierPkRowNumber) {
    return "row_number";
  }
  if (col_id == kColumnIdentifierDocAddress) {
    return bind.relation.IsSearchTable() ? "rowid" : "iresearch_pk";
  }
  return absl::StrCat("column_", col_id);
}

bool ProjectionIsFromIndex(const ScanBindData& bind,
                           const duckdb::ColumnIndex& column_index) {
  if (!bind.relation.IsInvertedIndex()) {
    return false;
  }
  const auto col_id = column_index.GetPrimaryIndex();
  if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID ||
      col_id >= duckdb::VIRTUAL_COLUMN_START ||
      col_id >= bind.columns.ids.size()) {
    return false;
  }
  return bind.relation.Stores(bind.columns.ids[col_id]);
}

bool ProjectionIsVirtual(const ScanBindData& bind,
                         const duckdb::ColumnIndex& column_index) {
  const auto col_id = column_index.GetPrimaryIndex();
  if (col_id >= duckdb::VIRTUAL_COLUMN_START) {
    return true;
  }
  if (col_id >= bind.columns.ids.size()) {
    return false;
  }
  const auto catalog_col_id = bind.columns.ids[col_id];
  return catalog_col_id == kInvertedIndexScoreId ||
         catalog_col_id == kInvertedIndexOffsetsId ||
         catalog_col_id == kInvertedIndexTermId ||
         catalog_col_id == kInvertedIndexTermRawId ||
         catalog_col_id == kInvertedIndexTermCountId ||
         catalog_col_id == kInvertedIndexTermFreqId ||
         catalog_col_id == kInvertedIndexTermScoreId;
}

std::vector<ProjectionEntry> BuildProjectionEntries(
  const ScanBindData& bind, const duckdb::TableFunctionToStringInput& input) {
  std::vector<ProjectionEntry> entries;
  if (!input.projected_column_ids || !input.projected_names) {
    return entries;
  }
  const auto& column_ids = *input.projected_column_ids;
  const auto& names = *input.projected_names;
  const auto count =
    input.projected_filter_prune
      ? (input.projection_ids ? input.projection_ids->size() : 0)
      : column_ids.size();
  entries.reserve(count);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    const auto base_index =
      input.projected_filter_prune ? (*input.projection_ids)[i] : i;
    if (base_index >= column_ids.size()) {
      continue;
    }
    const auto& column_index = column_ids[base_index];
    const auto col_id = column_index.GetPrimaryIndex();
    if (col_id == duckdb::COLUMN_IDENTIFIER_EMPTY) {
      continue;
    }
    entries.push_back({
      .name = ProjectionDisplayName(bind, column_index, names),
      .from_index = ProjectionIsFromIndex(bind, column_index),
      .is_virtual = ProjectionIsVirtual(bind, column_index),
    });
  }
  return entries;
}

std::string FormatProjections(const std::vector<ProjectionEntry>& entries,
                              bool annotate) {
  std::string out;
  for (const auto& e : entries) {
    if (!out.empty()) {
      absl::StrAppend(&out, "\n");
    }
    if (annotate && !e.is_virtual) {
      absl::StrAppend(&out, e.name, " (", e.from_index ? "i" : "l", ")");
    } else {
      absl::StrAppend(&out, e.name);
    }
  }
  return out;
}

struct Projections {
  std::vector<ProjectionEntry> entries;
  bool has_index = false;
  bool has_lookup = false;
};

Projections CollectProjections(
  const ScanBindData& bind, const duckdb::TableFunctionToStringInput& input) {
  Projections result{.entries = BuildProjectionEntries(bind, input)};
  for (const auto& e : result.entries) {
    if (!e.is_virtual) {
      (e.from_index ? result.has_index : result.has_lookup) = true;
    }
  }
  return result;
}

void AppendLookup(
  const ScanBindData& bind, const Projections& projections, bool count_only,
  bool has_lookup_filter,
  duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>& out) {
  const bool suppress_lookup =
    bind.relation.kind == ScanEntryKind::SearchTable ||
    (bind.relation.kind == ScanEntryKind::InvertedIndex && !has_lookup_filter &&
     (count_only || (!projections.entries.empty() && !projections.has_lookup)));
  if (!bind.lookup.label.empty() && !suppress_lookup) {
    out.insert("Lookup", bind.lookup.label);
  }
}

void AppendProjections(
  const Projections& projections,
  duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>& out) {
  if (!projections.entries.empty()) {
    out.insert("Projections", FormatProjections(projections.entries,
                                                projections.has_index &&
                                                  projections.has_lookup));
  }
}

duckdb::ExplainNode TableFilterNode(std::span<const DeferredCheck> checks,
                                    const irs::FieldNameResolver& name_of,
                                    const irs::FieldTypeResolver& type_of,
                                    const irs::FieldKeyResolver& key_of) {
  const auto node_of = [&](const DeferredCheck& check) {
    auto node = irs::ToExplainNode(*check.source, name_of, type_of, key_of);
    node.attributes["Verify"] = "table filter";
    return node;
  };
  if (checks.size() == 1) {
    return node_of(checks.front());
  }
  duckdb::ExplainNode node{"And"};
  for (const auto& check : checks) {
    node.children.push_back(node_of(check));
  }
  return node;
}

}  // namespace

void ScanBindData::AppendSummary(
  duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>& out) const {
  const auto& bind = *this;
  const auto& vector = score.vector;
  std::unique_ptr<irs::Scorer> query_scorer;
  const auto name_of = MakeFieldNameResolver(bind);
  const auto type_of = MakeFieldTypeResolver(bind);
  const irs::FieldKeyResolver key_of = [&bind](ColumnId field) {
    return KeyOfField(bind, field) != nullptr;
  };
  const bool vector_is_range =
    vector && vector->radius != std::numeric_limits<float>::max();
  if (vector_is_range) {
    const auto display =
      MakeVectorFilter(*vector, search.filter, vector->radius);
    out.insert("Index Filter", duckdb::ExplainValue(irs::ToExplainNode(
                                 *display, name_of, type_of, key_of)));
  } else if (search.filter) {
    out.insert("Index Filter", duckdb::ExplainValue(irs::ToExplainNode(
                                 *search.filter, name_of, type_of, key_of)));
  }
  const auto rows_text = [](std::span<const RowFilter> rows) {
    return absl::StrJoin(rows | std::views::transform([](const RowFilter& row) {
                           return duckdb::ExpressionFilter::GetExpressionFilter(
                                    *row.filter, "AppendSummary")
                             .expr->ToString();
                         }),
                         "\n");
  };
  if (!search.duals.empty() && KeepsColumnForms()) {
    out.insert("Index or Column Filter", rows_text(search.duals));
  }
  if (!search.row_filters.empty()) {
    out.insert("Row Filter", rows_text(search.row_filters));
  }
  if (!search.deferred.empty()) {
    out.insert("Table Filter", duckdb::ExplainValue(TableFilterNode(
                                 search.deferred, name_of, type_of, key_of)));
  }
  for (const auto& req : ts_dict.requests) {
    if (!req.having_filter) {
      continue;
    }
    auto key =
      ts_dict.requests.size() == 1
        ? std::string{"Index Filter"}
        : absl::StrCat("Index Filter(", DisplayColumnName(req.display_id), ")");
    out.insert(std::move(key),
               duckdb::ExplainValue(irs::ToExplainNode(
                 *req.having_filter, name_of, type_of, key_of)));
  }
  if (vector && !vector_is_range) {
    const auto col_id = vector->field_id;
    auto ctype = ColumnTypeById(col_id);
    if (ctype.id() == duckdb::LogicalTypeId::INVALID) {
      ctype = relation.inverted_config->ExpressionType(vector->field_id);
    }
    out.insert("Score",
               absl::StrCat(VectorMetricFunctionName(vector->metric), "(",
                            name_of(col_id, irs::StringLayout::kStrings), ", ",
                            ctype.ToString(), ")"));
    if (vector->exact || !score.HasTopK()) {
      out.insert("Exact", "brute force over the stored vectors");
    }
  }
  if (score.text) {
    query_scorer = search::MakeScorer(*score.text);
    out.insert("Score", query_scorer->ToString());
  }
  if (score.HasTopK()) {
    std::string topk_val =
      score.top_k ? absl::StrCat(*score.top_k -
                                 (score.top_n_consumed ? score.top_offset : 0))
                  : score.top_k_expr->ToString();
    if (score.top_n_consumed && score.top_offset != 0) {
      absl::StrAppend(&topk_val, ", offset ", score.top_offset);
    }
    if (!score.top_k && score.top_offset_expr) {
      absl::StrAppend(&topk_val, score.top_n_consumed ? ", offset " : " + ",
                      score.top_offset_expr->ToString());
    }
    const auto* pruning = ResolvePruneScorer(score.prune, query_scorer.get());
    if (pruning) {
      absl::StrAppend(&topk_val, ", optimized");
    }
    out.insert("Top", std::move(topk_val));
    if (pruning && score.prune != score.text) {
      out.insert("Bounds", search::MakeScorer(*score.prune)->ToString());
    }
  }
  if (offsets.Active()) {
    auto cols = absl::StrJoin(
      offsets.requests | std::views::transform([&](const auto& off) {
        return DisplayColumnName(off.display_id);
      }),
      ", ");
    out.insert("Offsets", std::move(cols));
  }
  if (ts_dict.Active()) {
    auto names = absl::StrJoin(
      ts_dict.requests | std::views::transform([&](const auto& req) {
        return DisplayColumnName(req.display_id);
      }),
      ", ");
    out.insert("TsDict", std::move(names));
  }
}

duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> ScanToStringValue(
  duckdb::TableFunctionToStringInput& input) {
  duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> result;
  if (!input.bind_data) {
    return result;
  }
  auto& bind = input.bind_data->Cast<ScanBindData>();
  result.insert(
    bind.relation.kind == ScanEntryKind::SearchTable ? "Table" : "Index",
    std::string{bind.RelationName()});
  const auto projections = CollectProjections(bind, input);
  bool count_only = input.projected_column_ids;
  if (count_only) {
    const auto& column_ids = *input.projected_column_ids;
    const bool use_projection = input.projected_filter_prune &&
                                input.projection_ids &&
                                !input.projection_ids->empty();
    const auto count =
      use_projection ? input.projection_ids->size() : column_ids.size();
    for (duckdb::idx_t i = 0; i < count; ++i) {
      const auto base_index = use_projection ? (*input.projection_ids)[i] : i;
      if (base_index < column_ids.size() &&
          column_ids[base_index].GetPrimaryIndex() !=
            duckdb::COLUMN_IDENTIFIER_EMPTY) {
        count_only = false;
        break;
      }
    }
  }
  bool has_lookup_filter = false;
  if (input.filters && input.projected_column_ids &&
      bind.relation.kind == ScanEntryKind::InvertedIndex) {
    const auto& column_ids = *input.projected_column_ids;
    for (const auto& entry : *input.filters) {
      const duckdb::idx_t proj = entry.GetIndex();
      if (proj >= column_ids.size() || column_ids[proj].IsVirtualColumn()) {
        continue;
      }
      const auto bind_idx = column_ids[proj].GetPrimaryIndex();
      if (bind_idx >= bind.columns.ids.size()) {
        continue;
      }
      const auto col_id = bind.columns.ids[bind_idx];
      if (col_id == kInvertedIndexScoreId) {
        continue;
      }
      if (!bind.relation.Stores(col_id)) {
        has_lookup_filter = true;
        break;
      }
    }
  }
  AppendLookup(bind, projections, count_only, has_lookup_filter, result);
  bind.AppendSummary(result);
  if (bind.score.static_floor > std::numeric_limits<float>::lowest() &&
      (bind.score.HasTopK() || bind.score.text)) {
    result.insert("Min Score", absl::StrCat(bind.score.static_floor));
  }
  if (count_only) {
    result.insert("Output", "row-count only");
  }
  AppendProjections(projections, result);
  return result;
}

duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>
IndexLookupToStringValue(duckdb::TableFunctionToStringInput& input) {
  duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue> result;
  const auto& bind = input.bind_data->Cast<ScanBindData>();
  result.insert("Index", std::string{bind.RelationName()});
  const auto projections = CollectProjections(bind, input);
  AppendLookup(bind, projections, false, false, result);
  AppendProjections(projections, result);
  return result;
}

}  // namespace sdb::connector
