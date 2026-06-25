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

#include "connector/duckdb_term_stats_function.h"

#include <span>

#include <duckdb.hpp>
#include <duckdb/main/database.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/utils/utf8_utils.hpp>

#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/sql_utils.h"
#include "search/inverted_index_storage.h"

namespace sdb::connector {
namespace {

bool IsValidUtf8(irs::bytes_view data) noexcept {
  const auto* it = data.data();
  const auto* end = it + data.size();
  while (it < end) {
    if (irs::utf8_utils::ToChar32(it, end) == irs::utf8_utils::kInvalidChar32) {
      return false;
    }
  }
  return true;
}

struct TermStatsBindData : public duckdb::FunctionData {
  // Keeps the reader (and the rocksdb snapshot under it) alive for the scan.
  search::InvertedIndexSnapshotPtr idx_snapshot;
  std::shared_ptr<catalog::InvertedIndex> index;
  std::shared_ptr<catalog::Table> table;

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    auto copy = duckdb::make_uniq<TermStatsBindData>();
    copy->idx_snapshot = idx_snapshot;
    copy->index = index;
    copy->table = table;
    return copy;
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    auto& o = other.Cast<TermStatsBindData>();
    return idx_snapshot == o.idx_snapshot && index == o.index &&
           table == o.table;
  }
};

duckdb::unique_ptr<duckdb::FunctionData> TermStatsBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  if (input.inputs.empty() || input.inputs[0].IsNull()) {
    throw duckdb::BinderException(
      "sdb_term_stats expects an inverted index name: "
      "sdb_term_stats('[schema.]index')");
  }
  const auto raw_name = input.inputs[0].GetValue<std::string>();

  auto& conn_ctx = GetSereneDBContext(context);
  auto cat_snapshot = conn_ctx.CatalogSnapshot();
  const auto db_id = conn_ctx.GetDatabaseId();
  const auto target =
    pg::ParseObjectName(raw_name, conn_ctx.GetCurrentSchema());

  auto data = duckdb::make_uniq<TermStatsBindData>();
  for (auto& index : cat_snapshot->GetIndexes(db_id, target.schema)) {
    if (index->GetType() != catalog::ObjectType::InvertedIndex ||
        index->GetName() != target.relation) {
      continue;
    }
    const auto& storage =
      basics::downCast<const catalog::InvertedIndex>(*index).GetData();
    if (!storage) {
      break;
    }
    data->idx_snapshot = storage->GetInvertedIndexSnapshot();
    data->index = basics::downCast<catalog::InvertedIndex>(std::move(index));
    data->table = cat_snapshot->GetObject<catalog::Table>(
      data->index->GetRelationId());
    break;
  }
  if (!data->idx_snapshot) {
    throw duckdb::CatalogException("inverted index '%s' not found.",
                                   std::string{target.relation});
  }

  return_types.push_back(duckdb::LogicalType::BIGINT);
  names.push_back("segment");
  return_types.push_back(duckdb::LogicalType::VARCHAR);
  names.push_back("column_name");
  return_types.push_back(duckdb::LogicalType::BLOB);
  names.push_back("term");
  // The term decoded as text, NULL when the bytes are not valid UTF-8 (e.g.
  // shingle terms, whose 0xFF token separator is deliberately invalid). The
  // frequentwords derivation is then just `WHERE term_text IS NOT NULL`.
  return_types.push_back(duckdb::LogicalType::VARCHAR);
  names.push_back("term_text");
  return_types.push_back(duckdb::LogicalType::BIGINT);
  names.push_back("doc_freq");
  return_types.push_back(duckdb::LogicalType::BIGINT);
  names.push_back("total_term_freq");
  return data;
}

struct TermStatsGlobalState : public duckdb::GlobalTableFunctionState {
  size_t segment_idx = 0;
  std::span<const irs::field_id> field_ids;  // of the current segment
  size_t field_idx = 0;                      // cursor into field_ids
  irs::SeekTermIterator::ptr terms;          // over the current field
  const irs::TermMeta* term_meta = nullptr;
  std::string column_name;                   // of the current field
  bool finished = false;

  idx_t MaxThreads() const final { return 1; }
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> TermStatsInitGlobal(
  duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
  return duckdb::make_uniq<TermStatsGlobalState>();
}

// The new .idx format identifies fields by a numeric `field_id` (no string
// mangling). Resolves the field to its table column name; only text columns
// carry dictionary-tunable string terms. Non-text columns and the per-kind
// null / bool / numeric slice ids (which never match a column id) yield nullopt
// and are skipped -- their "terms" are encoded scalars, useless for dictionary
// tuning.
std::optional<std::string> ResolveColumnName(const TermStatsBindData& bind,
                                             irs::field_id field_id) {
  std::string out;
  const auto column_id = static_cast<catalog::Column::Id>(field_id);
  if (bind.table != nullptr) {
    for (const auto& col : bind.table->Columns()) {
      if (col.GetId() == column_id) {
        const auto type_id = col.type.id();
        if (type_id != duckdb::LogicalTypeId::VARCHAR &&
            type_id != duckdb::LogicalTypeId::BLOB) {
          return std::nullopt;  // encoded-scalar terms, not dictionary tuning
        }
        out = col.GetName();
        break;
      }
    }
  }
  if (out.empty()) {
    // Indexed expressions and synthetic ids have no table column; label by
    // the entry's pretty-printed expression when the catalog knows it.
    if (const auto* expr = bind.index->ExpressionByFieldId(field_id);
        expr != nullptr) {
      out = expr->pretty_printed;
    } else {
      return std::nullopt;
    }
  }
  return out;
}

void TermStatsExecute(duckdb::ClientContext&, duckdb::TableFunctionInput& data,
                      duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<TermStatsGlobalState>();
  auto& bind = data.bind_data->Cast<TermStatsBindData>();
  const auto& reader = bind.idx_snapshot->reader;

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  auto* segments = duckdb::FlatVector::GetDataMutable<int64_t>(output.data[0]);
  auto* columns =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output.data[1]);
  auto* terms =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output.data[2]);
  auto* term_texts =
    duckdb::FlatVector::GetDataMutable<duckdb::string_t>(output.data[3]);
  auto& term_text_validity =
    duckdb::FlatVector::ValidityMutable(output.data[3]);
  auto* doc_freqs = duckdb::FlatVector::GetDataMutable<int64_t>(output.data[4]);
  auto* total_freqs =
    duckdb::FlatVector::GetDataMutable<int64_t>(output.data[5]);

  duckdb::idx_t produced = 0;
  while (produced < STANDARD_VECTOR_SIZE) {
    if (gstate.terms != nullptr && gstate.terms->next()) {
      gstate.terms->read();  // fill TermMeta
      const auto term = gstate.terms->value();
      const auto* term_chars = reinterpret_cast<const char*>(term.data());
      segments[produced] = static_cast<int64_t>(gstate.segment_idx);
      columns[produced] = duckdb::StringVector::AddString(
        output.data[1], gstate.column_name);
      terms[produced] = duckdb::StringVector::AddStringOrBlob(
        output.data[2], term_chars, term.size());
      if (IsValidUtf8(term)) {
        term_texts[produced] = duckdb::StringVector::AddString(
          output.data[3], term_chars, term.size());
      } else {
        // The slot must still hold a well-formed string_t: downstream row
        // serialization (sort payloads) touches the data regardless of the
        // validity mask.
        term_texts[produced] = duckdb::string_t{};
        term_text_validity.SetInvalid(produced);
      }
      const auto* meta = gstate.term_meta;
      doc_freqs[produced] = meta != nullptr ? meta->docs_count : 0;
      total_freqs[produced] = meta != nullptr ? meta->freq : 0;
      ++produced;
      continue;
    }
    gstate.terms.reset();
    // Advance to the next string field, opening segments as needed.
    for (;;) {
      if (gstate.segment_idx >= reader.size()) {
        break;
      }
      const auto& segment = reader[gstate.segment_idx];
      if (gstate.field_idx == 0) {
        gstate.field_ids = segment.field_ids();
      }
      if (gstate.field_idx >= gstate.field_ids.size()) {
        gstate.field_idx = 0;
        ++gstate.segment_idx;
        continue;
      }
      const auto fid = gstate.field_ids[gstate.field_idx++];
      const auto* field = segment.field(fid);
      if (field == nullptr) {
        continue;
      }
      auto column = ResolveColumnName(bind, fid);
      if (!column) {
        continue;
      }
      gstate.column_name = *std::move(column);
      gstate.terms = field->iterator(irs::SeekMode::NORMAL);
      if (gstate.terms != nullptr) {
        gstate.term_meta = irs::get<irs::TermMeta>(*gstate.terms);
        break;
      }
    }
    if (gstate.terms == nullptr) {
      gstate.finished = true;
      break;
    }
  }
  // SetChildCardinality (not SetCardinality): this fork sizes each vector
  // individually, and downstream row serialization reads those sizes.
  output.SetChildCardinality(produced);
}

}  // namespace

void RegisterTermStatsFunction(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  duckdb::TableFunction func{"sdb_term_stats",
                             {duckdb::LogicalType::VARCHAR},
                             TermStatsExecute,
                             TermStatsBind,
                             TermStatsInitGlobal};
  loader.RegisterFunction(func);
}

}  // namespace sdb::connector
