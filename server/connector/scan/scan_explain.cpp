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
#include <ranges>
#include <span>

#include "catalog/entry/duckdb_table_entry.h"
#include "connector/functions/vector.h"
#include "connector/scan/scan_bind.h"
#include "connector/scan/scan_function.h"
#include "connector/search_filter_printer.hpp"

namespace sdb::connector {
namespace {

std::string ColumnNameFor(const ScanBindData& bind, catalog::ColumnId col_id) {
  auto name = bind.ColumnNameById(col_id);
  if (!name.empty()) {
    return std::string{name};
  }
  return absl::StrCat("col", col_id);
}

struct FieldOwner {
  const catalog::InvertedIndex* index = nullptr;
  catalog::InvertedIndex::FieldLookup lookup;
};

FieldOwner FindFieldOwner(
  std::span<const catalog::InvertedIndex* const> indexes, irs::field_id fid) {
  for (const auto* index : indexes) {
    const auto lookup = index->LookupField(fid);
    if (lookup.entry || irs::field_limits::valid(lookup.entry_field_id)) {
      return {index, lookup};
    }
  }
  return {};
}

auto MakeFieldNameResolver(
  const ScanBindData& bind,
  std::span<const catalog::InvertedIndex* const> indexes) {
  return [&bind, indexes](catalog::ColumnId col_id) -> std::string {
    const auto fid = static_cast<irs::field_id>(col_id);
    const auto [index, lookup] = FindFieldOwner(indexes, fid);
    auto base = std::string{bind.ColumnNameById(col_id)};
    const auto column_type = bind.ColumnTypeById(col_id);
    const bool found_type = column_type.id() != duckdb::LogicalTypeId::INVALID;
    auto entry_base = [&](irs::field_id entry_fid) {
      std::string s;
      const auto* expr = index->ExpressionByFieldId(entry_fid);
      if (expr && !expr->pretty_printed.empty()) {
        s = expr->pretty_printed;
      } else {
        s = bind.ColumnNameById(catalog::ColumnId{entry_fid});
      }
      if (s.empty()) {
        s = bind.ColumnNameById(index->ColumnForTermField(entry_fid));
      }
      if (s.empty()) {
        s = absl::StrCat("col", entry_fid);
      }
      return s;
    };
    if (lookup.entry_field_id == catalog::term_dict::kPKFieldId) {
      const auto name = bind.ColumnNameById(catalog::kGeneratedPKId);
      return std::string{name.empty() ? std::string_view{"sdb_generated_pk"}
                                      : name} +
             "(pk)";
    }
    if (lookup.entry) {
      const auto& entry = *lookup.entry;
      if (fid == lookup.entry_field_id) {
        const auto* expr = index->ExpressionByFieldId(fid);
        if (base.empty() && expr && !expr->pretty_printed.empty()) {
          base = expr->pretty_printed;
        }
        if (base.empty()) {
          base =
            std::string{bind.ColumnNameById(index->ColumnForTermField(fid))};
        }
        if (base.empty()) {
          base = absl::StrCat("col", fid);
        }
        if (expr) {
          catalog::InvertedIndex::AppendKindSuffix(base, expr->return_type);
        } else if (found_type) {
          catalog::InvertedIndex::AppendKindSuffix(base, column_type);
        } else if (entry.text_dictionary.isSet()) {
          base += "(string)";
        }
        return base;
      }
      if (fid == entry.null_field_id) {
        return entry_base(lookup.entry_field_id) + "(null)";
      }
      if (fid == entry.bool_field_id) {
        return entry_base(lookup.entry_field_id) + "(bool)";
      }
      if (fid == entry.numeric_field_id) {
        return entry_base(lookup.entry_field_id) + "(numeric)";
      }
      if (fid == entry.synthetic_column) {
        return entry_base(lookup.entry_field_id) + "(synthetic)";
      }
    }
    if (base.empty()) {
      base = absl::StrCat("col", fid);
    }
    if (found_type) {
      catalog::InvertedIndex::AppendKindSuffix(base, column_type);
    }
    return base;
  };
}

catalog::term_dict::Kind ClassifyTerms(const duckdb::LogicalType& type) {
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
        return catalog::term_dict::Classify(leaf->id());
    }
  }
}

auto MakeFieldKindResolver(
  const ScanBindData& bind,
  std::span<const catalog::InvertedIndex* const> indexes) {
  return
    [&bind, indexes](catalog::ColumnId col_id) -> catalog::term_dict::Kind {
      using catalog::term_dict::Kind;
      const auto fid = static_cast<irs::field_id>(col_id);
      const auto [index, lookup] = FindFieldOwner(indexes, fid);
      if (lookup.entry_field_id == catalog::term_dict::kPKFieldId) {
        return Kind::NumericI64;
      }
      if (lookup.entry) {
        const auto& entry = *lookup.entry;
        if (fid == lookup.entry_field_id) {
          const auto* expr = index->ExpressionByFieldId(fid);
          if (expr) {
            return ClassifyTerms(expr->return_type);
          }
          const auto column_type = bind.ColumnTypeById(col_id);
          if (column_type.id() != duckdb::LogicalTypeId::INVALID) {
            return ClassifyTerms(column_type);
          }
          return Kind::String;
        }
        if (fid == entry.null_field_id) {
          return Kind::Null;
        }
        if (fid == entry.bool_field_id) {
          return Kind::Bool;
        }
        if (fid == entry.numeric_field_id) {
          return Kind::NumericF64;
        }
      }
      const auto column_type = bind.ColumnTypeById(col_id);
      if (column_type.id() != duckdb::LogicalTypeId::INVALID) {
        return ClassifyTerms(column_type);
      }
      return Kind::Unsupported;
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
  if (const auto pk_idx =
        catalog::SereneDBTableEntry::VirtualToPKColumnIndex(col_id);
      pk_idx != duckdb::DConstants::INVALID_INDEX) {
    if (!bind.view && bind.relation.table_entry) {
      const auto& cols = bind.relation.table_entry->GetColumns();
      if (pk_idx < cols.LogicalColumnCount()) {
        return std::string{cols.GetColumn(duckdb::LogicalIndex(pk_idx))
                             .Name()
                             .GetIdentifierName()};
      }
    }
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
  if (col_id == catalog::kColumnIdentifierTableOid) {
    return "tableoid";
  }
  if (col_id == catalog::kColumnIdentifierGeneratedPk) {
    return "generated_pk";
  }
  if (col_id == catalog::kColumnIdentifierPkRowNumber) {
    return "row_number";
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
  const auto catalog_col_id = bind.columns.ids[col_id];
  if (catalog_col_id == catalog::kGeneratedPKId) {
    return true;
  }
  const auto* info =
    bind.relation.ScannedIndex().FindColumnInfo(catalog_col_id);
  return info != nullptr && info->IsStored();
}

bool ProjectionIsVirtual(const ScanBindData& bind,
                         const duckdb::ColumnIndex& column_index) {
  const auto col_id = column_index.GetPrimaryIndex();
  if (col_id >= duckdb::VIRTUAL_COLUMN_START ||
      col_id >= bind.columns.ids.size()) {
    return false;
  }
  const auto catalog_col_id = bind.columns.ids[col_id];
  return catalog_col_id == catalog::kInvertedIndexScoreId ||
         catalog_col_id == catalog::kInvertedIndexOffsetsId ||
         catalog_col_id == catalog::kInvertedIndexTermId ||
         catalog_col_id == catalog::kInvertedIndexTermRawId ||
         catalog_col_id == catalog::kInvertedIndexTermCountId ||
         catalog_col_id == catalog::kInvertedIndexTermFreqId ||
         catalog_col_id == catalog::kInvertedIndexTermScoreId;
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

}  // namespace

void ScanBindData::AppendSummary(
  duckdb::InsertionOrderPreservingMap<duckdb::ExplainValue>& out) const {
  const auto& bind = *this;
  const auto display_field = [&](catalog::ColumnId id) -> std::string {
    if (relation.IsIndexRelation()) {
      if (const auto* expr = relation.ScannedIndex().ExpressionByFieldId(
            static_cast<irs::field_id>(id));
          expr && !expr->pretty_printed.empty()) {
        return expr->pretty_printed;
      }
    }
    return ColumnNameFor(bind, id);
  };
  const auto indexes = relation.InvertedIndexes();
  const auto name_of = MakeFieldNameResolver(bind, indexes);
  const auto kind_of = MakeFieldKindResolver(bind, indexes);
  const auto& vector = score.vector;
  const bool vector_is_range =
    vector && vector->radius != std::numeric_limits<float>::max();
  if (vector_is_range) {
    const auto display =
      MakeVectorFilter(*vector, search.filter, vector->radius);
    out.insert("Index Filter", duckdb::ExplainValue(irs::ToExplainNode(
                                 *display, name_of, kind_of)));
  } else if (search.filter) {
    out.insert("Index Filter", duckdb::ExplainValue(irs::ToExplainNode(
                                 *search.filter, name_of, kind_of)));
  }
  for (const auto& req : ts_dict.requests) {
    if (!req.having_filter) {
      continue;
    }
    auto key =
      ts_dict.requests.size() == 1
        ? std::string{"Index Filter"}
        : absl::StrCat(
            "Index Filter(",
            display_field(static_cast<catalog::ColumnId>(req.display_id)), ")");
    out.insert(std::move(key), duckdb::ExplainValue(irs::ToExplainNode(
                                 *req.having_filter, name_of, kind_of)));
  }
  if (vector && !vector_is_range) {
    const auto col_id = static_cast<catalog::ColumnId>(vector->field_id);
    const auto fname = name_of(col_id);
    auto ctype = ColumnTypeById(col_id);
    if (ctype.id() == duckdb::LogicalTypeId::INVALID) {
      if (const auto* expr =
            relation.ScannedIndex().ExpressionByFieldId(vector->field_id)) {
        ctype = expr->return_type;
      }
    }
    out.insert("Score", absl::StrCat(VectorMetricFunctionName(vector->metric),
                                     "(", fname, ", ", ctype.ToString(), ")"));
  }
  std::unique_ptr<irs::Scorer> query_scorer;
  if (score.text) {
    query_scorer = catalog::MakeScorer(*score.text);
    if (query_scorer) {
      out.insert("Score", query_scorer->ToString());
    }
  }
  if (score.top_k) {
    std::string topk_val = absl::StrCat(
      *score.top_k - (score.top_n_consumed ? score.top_offset : 0));
    if (score.top_n_consumed && score.top_offset != 0) {
      absl::StrAppend(&topk_val, ", offset ", score.top_offset);
    }
    const auto* pruning = ResolvePruneScorer(score.prune, query_scorer.get());
    if (pruning) {
      absl::StrAppend(&topk_val, ", optimized");
    }
    out.insert("Top", std::move(topk_val));
    if (const auto& prune = score.prune;
        pruning && prune && prune != score.text) {
      if (auto bounds = catalog::MakeScorer(*prune)) {
        out.insert("Bounds", bounds->ToString());
      }
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
        return display_field(catalog::ColumnId{req.display_id});
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
  if (bind.relation.table_entry) {
    const char* kind =
      bind.relation.kind == ScanEntryKind::BaseTable ? "Table" : "Index";
    result.insert(
      kind, std::string{bind.relation.table_entry->name.GetIdentifierName()});
  } else {
    const char* kind = bind.IsViewBacked() ? "View" : "Table";
    result.insert(kind, std::string{bind.RelationName()});
  }
  const auto entries = BuildProjectionEntries(bind, input);
  bool has_index = false;
  bool has_lookup = false;
  for (const auto& e : entries) {
    if (e.is_virtual) {
      continue;
    }
    if (e.from_index) {
      has_index = true;
    } else {
      has_lookup = true;
    }
  }
  bool count_only = input.projected_column_ids != nullptr;
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
      bind.relation.IsInvertedIndex()) {
    const auto& column_ids = *input.projected_column_ids;
    for (const auto& entry : *input.filters) {
      const auto proj = static_cast<duckdb::idx_t>(entry.GetIndex());
      if (proj >= column_ids.size() || column_ids[proj].IsVirtualColumn()) {
        continue;
      }
      const auto bind_idx = column_ids[proj].GetPrimaryIndex();
      if (bind_idx >= bind.columns.ids.size()) {
        continue;
      }
      const auto col_id = bind.columns.ids[bind_idx];
      if (col_id == catalog::kInvertedIndexScoreId) {
        continue;
      }
      const auto* info = bind.relation.ScannedIndex().FindColumnInfo(col_id);
      if (!info || !info->IsStored()) {
        has_lookup_filter = true;
        break;
      }
    }
  }
  const bool suppress_lookup =
    bind.relation.IsInvertedIndex() && !has_lookup_filter &&
    (count_only || (!entries.empty() && !has_lookup));
  if (!bind.lookup.label.empty() && !suppress_lookup) {
    result.insert("Lookup", bind.lookup.label);
  }
  bind.AppendSummary(result);
  if (bind.score.static_floor > std::numeric_limits<float>::lowest() &&
      (bind.score.top_k || bind.score.text)) {
    result.insert("Min Score", absl::StrCat(bind.score.static_floor));
  }
  if (count_only) {
    result.insert("Output", "row-count only");
  }
  if (!entries.empty()) {
    const bool annotate = has_index && has_lookup;
    result.insert("Projections", FormatProjections(entries, annotate));
  }
  return result;
}

}  // namespace sdb::connector
