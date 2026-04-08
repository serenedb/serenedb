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

#include "connector/duckdb_table_function.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/expression/bound_columnref_expression.hpp>
#include <duckdb/planner/expression/bound_constant_expression.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/boolean_filter.hpp>
#include <iresearch/search/phrase_filter.hpp>
#include <iresearch/search/term_filter.hpp>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "catalog/mangling.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_entry.h"
#include "connector/key_utils.hpp"
#include "connector/search_remove_filter.hpp"
#include "functions/search.h"
#include "pg/connection_context.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

constexpr size_t kTablePrefixSize = sizeof(ObjectId);
constexpr size_t kKeyPrefixSize =
  kTablePrefixSize + sizeof(catalog::Column::Id);

// --- SereneDBScanBindData ---

duckdb::unique_ptr<duckdb::FunctionData> SereneDBScanBindData::Copy() const {
  auto copy = duckdb::make_uniq<SereneDBScanBindData>();
  copy->table = table;
  copy->column_ids = column_ids;
  copy->column_types = column_types;
  copy->has_rowid = has_rowid;
  copy->table_entry = table_entry;
  return copy;
}

static duckdb::BindInfo SereneDBGetBindInfo(
  const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  auto& data =
    const_cast<SereneDBScanBindData&>(bind_data->Cast<SereneDBScanBindData>());
  if (data.table_entry) {
    return duckdb::BindInfo(*data.table_entry);
  }
  return duckdb::BindInfo(duckdb::ScanType::TABLE);
}

bool SereneDBScanBindData::Equals(const duckdb::FunctionData& other) const {
  auto& o = other.Cast<SereneDBScanBindData>();
  return table == o.table && column_ids == o.column_ids;
}

// --- Global/Local state ---

struct SereneDBScanGlobalState : public duckdb::GlobalTableFunctionState {
  // Only iterators for the projected (requested) columns
  std::vector<std::unique_ptr<rocksdb::Iterator>> iterators;
  // Maps output column index -> bind_data column index
  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;
  std::vector<std::string> column_keys;
  std::string upper_bound_data;
  std::vector<rocksdb::Slice> upper_bound_slices;
  const rocksdb::Snapshot* snapshot = nullptr;
  bool finished = false;
  bool scan_rowid = false;
  duckdb::idx_t rowid_output_idx = 0;

  // Search scan state
  size_t search_segment_idx = 0;
  irs::DocIterator::ptr search_doc;
  irs::DocIterator::ptr search_pk_iter;
  const irs::PayAttr* search_pk_value = nullptr;

  ~SereneDBScanGlobalState() override {
    iterators.clear();  // Release iterators before snapshot
    if (snapshot) {
      GetServerEngine().db()->ReleaseSnapshot(snapshot);
      snapshot = nullptr;
    }
  }
};

struct SereneDBScanLocalState : public duckdb::LocalTableFunctionState {};

// --- Callbacks ---

static duckdb::unique_ptr<duckdb::FunctionData> SereneDBScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  throw duckdb::InternalException(
    "SereneDBScanBind: should be provided via GetScanFunction");
}

static duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
SereneDBScanInitGlobal(duckdb::ClientContext& context,
                       duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SereneDBScanGlobalState>();

  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  state->snapshot = db->GetSnapshot();

  auto table_id = bind_data.table->GetId();
  std::string table_key = key_utils::PrepareTableKey(table_id);

  // Determine which columns DuckDB actually wants (projection pushdown)
  const auto num_bind_columns = bind_data.column_ids.size();
  std::cerr << "SereneDB scan init: requested columns [";
  for (auto col_id : input.column_ids) {
    std::cerr << col_id << " ";
  }
  std::cerr << "]" << std::endl;
  for (auto col_id : input.column_ids) {
    if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
      // Dummy sequential rowid
      state->scan_rowid = true;
      state->rowid_output_idx = state->projected_columns.size();
      state->projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state->projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id >= duckdb::VIRTUAL_COLUMN_START) {
      // Virtual PK column: VIRTUAL_COLUMN_START + real_col_index
      auto real_idx = SereneDBTableEntry::VirtualToPKColumnIndex(col_id);
      SDB_ASSERT(real_idx != duckdb::DConstants::INVALID_INDEX);
      SDB_ASSERT(real_idx < bind_data.column_ids.size());
      state->projected_columns.push_back(real_idx);
      state->projected_types.push_back(bind_data.column_types[real_idx]);
    } else if (col_id < num_bind_columns) {
      state->projected_columns.push_back(col_id);
      state->projected_types.push_back(bind_data.column_types[col_id]);
    }
  }

  // Create iterators only for the real (non-rowid) projected columns
  std::vector<catalog::Column::Id> scan_column_ids;
  for (auto proj_idx : state->projected_columns) {
    if (proj_idx != duckdb::DConstants::INVALID_INDEX) {
      scan_column_ids.push_back(bind_data.column_ids[proj_idx]);
    }
  }

  // If only rowid is requested, we still need one column iterator to drive scan
  if (scan_column_ids.empty() && !bind_data.column_ids.empty()) {
    scan_column_ids.push_back(bind_data.column_ids[0]);
  }

  const auto num_scan = scan_column_ids.size();
  state->column_keys.reserve(num_scan);
  state->upper_bound_data.reserve(kKeyPrefixSize * num_scan);
  state->upper_bound_slices.reserve(num_scan);

  for (auto column_id : scan_column_ids) {
    auto key = table_key;
    basics::StrResize(key, kTablePrefixSize);

    state->upper_bound_data.append(key);
    key_utils::AppendColumnKey(state->upper_bound_data, column_id + 1);

    key_utils::AppendColumnKey(key, column_id);
    state->column_keys.push_back(std::move(key));
  }

  for (size_t i = 0; i < num_scan; ++i) {
    state->upper_bound_slices.emplace_back(
      state->upper_bound_data.data() + i * kKeyPrefixSize, kKeyPrefixSize);
  }

  rocksdb::ReadOptions ro;
  ro.snapshot = state->snapshot;
  ro.async_io = false;
  ro.adaptive_readahead = true;
  ro.auto_prefix_mode = true;

  for (size_t i = 0; i < num_scan; ++i) {
    ro.iterate_upper_bound = &state->upper_bound_slices[i];
    auto it = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(ro, cf));
    it->Seek(state->column_keys[i]);
    state->iterators.push_back(std::move(it));
  }

  return state;
}

static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
SereneDBScanInitLocal(duckdb::ExecutionContext& context,
                      duckdb::TableFunctionInitInput& input,
                      duckdb::GlobalTableFunctionState* global_state) {
  return duckdb::make_uniq<SereneDBScanLocalState>();
}

static void SereneDBSearchScanFunction(duckdb::ClientContext& context,
                                       duckdb::TableFunctionInput& data,
                                       duckdb::DataChunk& output);

static void SereneDBScanFunction(duckdb::ClientContext& context,
                                 duckdb::TableFunctionInput& data,
                                 duckdb::DataChunk& output) {
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();
  if (bind_data.IsSearchScan()) {
    return SereneDBSearchScanFunction(context, data, output);
  }

  auto& gstate = data.global_state->Cast<SereneDBScanGlobalState>();

  if (gstate.finished || gstate.iterators.empty()) {
    output.SetCardinality(0);
    gstate.finished = true;
    return;
  }

  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;

  // First iterator drives the row count.
  // If rowid is needed AND the first projected column is rowid, we need a
  // special path. Otherwise, read the first real column.
  duckdb::idx_t count = 0;
  duckdb::idx_t iter_idx = 0;  // which iterator we're consuming

  // Find the first real (non-rowid) output column to drive the scan
  duckdb::idx_t first_real_output = duckdb::DConstants::INVALID_INDEX;
  for (duckdb::idx_t out = 0; out < gstate.projected_columns.size(); ++out) {
    if (gstate.projected_columns[out] != duckdb::DConstants::INVALID_INDEX) {
      first_real_output = out;
      break;
    }
  }

  if (first_real_output != duckdb::DConstants::INVALID_INDEX) {
    // Read first real column to determine row count
    count = ReadColumnIntoDuckDB(
      *gstate.iterators[0], output.data[first_real_output],
      gstate.projected_types[first_real_output], batch_size);
    iter_idx = 1;
  } else if (!gstate.iterators.empty()) {
    // Only rowid requested -- still need to iterate to count rows
    // Use a dummy read that just counts
    auto& it = *gstate.iterators[0];
    while (it.Valid() && count < batch_size) {
      ++count;
      it.Next();
    }
    rocksutils::CheckIteratorStatus(it);
    iter_idx = 1;
  }

  if (count == 0) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  // Read remaining real columns
  for (duckdb::idx_t out =
         (first_real_output == duckdb::DConstants::INVALID_INDEX
            ? 0
            : first_real_output + 1);
       out < gstate.projected_columns.size(); ++out) {
    if (gstate.projected_columns[out] == duckdb::DConstants::INVALID_INDEX) {
      continue;  // rowid -- already handled
    }
    SDB_ASSERT(iter_idx < gstate.iterators.size());
    auto col_count =
      ReadColumnIntoDuckDB(*gstate.iterators[iter_idx], output.data[out],
                           gstate.projected_types[out], count);
    SDB_ASSERT(col_count == count);
    ++iter_idx;
  }

  // Fill dummy rowid column with sequential numbers if requested
  if (gstate.scan_rowid) {
    auto* rowid_data = duckdb::FlatVector::GetData<int64_t>(
      output.data[gstate.rowid_output_idx]);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      rowid_data[i] = static_cast<int64_t>(i);
    }
  }

  output.SetCardinality(count);
}

// --- Search scan: IResearch query → PKs → RocksDB materialization ---

static void SereneDBSearchScanFunction(duckdb::ClientContext& context,
                                       duckdb::TableFunctionInput& data,
                                       duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SereneDBScanGlobalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;
  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);

  std::vector<std::string> pk_bytes;
  pk_bytes.reserve(batch_size);

  auto& reader = *bind_data.search_reader;
  auto& query = *bind_data.search_query;

  while (pk_bytes.size() < batch_size) {
    if (!gstate.search_doc) {
      if (gstate.search_segment_idx >= reader.size()) {
        break;
      }
      auto& segment = reader[gstate.search_segment_idx++];
      gstate.search_doc = segment.mask(query.execute({.segment = segment}));
      const auto* pk_column = segment.column(kPkFieldName);
      if (!pk_column) {
        gstate.search_doc.reset();
        continue;
      }
      gstate.search_pk_iter = pk_column->iterator(irs::ColumnHint::Normal);
      gstate.search_pk_value = irs::get<irs::PayAttr>(*gstate.search_pk_iter);
    }

    auto doc_id = gstate.search_doc->advance();
    if (irs::doc_limits::eof(doc_id)) {
      gstate.search_doc.reset();
      continue;
    }

    SDB_ASSERT(doc_id == gstate.search_pk_iter->seek(doc_id));
    auto pk_view = gstate.search_pk_value->value;
    pk_bytes.emplace_back(
      reinterpret_cast<const char*>(pk_view.data()), pk_view.size());
  }

  if (pk_bytes.empty()) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  const auto num_rows = pk_bytes.size();
  std::string table_key = key_utils::PrepareTableKey(bind_data.table->GetId());
  std::string key_buffer;
  rocksdb::ReadOptions ro;
  rocksdb::PinnableSlice value;

  for (duckdb::idx_t proj = 0; proj < gstate.projected_columns.size(); ++proj) {
    auto bind_col = gstate.projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      auto* rowid_data =
        duckdb::FlatVector::GetData<int64_t>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < num_rows; ++i) {
        rowid_data[i] = static_cast<int64_t>(i);
      }
      continue;
    }

    auto col_id = bind_data.column_ids[bind_col];
    auto& type = gstate.projected_types[proj];

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      key_buffer = table_key;
      basics::StrResize(key_buffer, kTablePrefixSize);
      key_utils::AppendColumnKey(key_buffer, col_id);
      key_buffer.append(pk_bytes[row]);

      value.Reset();
      auto s = db->Get(ro, cf, key_buffer, &value);
      if (s.IsNotFound()) {
        duckdb::FlatVector::Validity(output.data[proj]).SetInvalid(row);
        continue;
      }
      SDB_ASSERT(s.ok(), "RocksDB read failed: ", s.ToString());
      DeserializeValueIntoDuckDB(value.ToStringView(), output.data[proj],
                                 type, row);
    }
  }

  output.SetCardinality(num_rows);
}

// --- pushdown_complex_filter: intercept search predicates ---

static void SereneDBPushdownComplexFilter(
  duckdb::ClientContext& context, duckdb::LogicalGet& get,
  duckdb::FunctionData* bind_data_ptr,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>>& filters) {
  if (!bind_data_ptr) {
    return;
  }
  // Guard: only process our own scan function's bind data.
  // Check the function name to avoid processing DuckDB's internal scans
  // (e.g., from CREATE INDEX which returns a table scan plan).
  if (get.function.name != "serenedb_scan") {
    return;
  }
  auto& bind_data = bind_data_ptr->Cast<SereneDBScanBindData>();
  if (!bind_data.table) {
    return;
  }

  auto table_id = bind_data.table->GetId();
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();

  // Find inverted index — prefer the one from the table entry (FROM idx_name)
  std::shared_ptr<const catalog::InvertedIndex> inverted_index;
  if (bind_data.table_entry &&
      dynamic_cast<const SereneDBTableEntry*>(&*bind_data.table_entry)) {
    auto& sdb_entry = bind_data.table_entry->Cast<SereneDBTableEntry>();
    inverted_index = sdb_entry.GetInvertedIndex();
  }

  // Fallback: search for any inverted index on the table
  if (!inverted_index) {
    auto indexes = snapshot->GetIndexesByTable(table_id);
    for (auto& index : indexes) {
      if (index->GetType() == catalog::ObjectType::InvertedIndex) {
        inverted_index =
          std::dynamic_pointer_cast<const catalog::InvertedIndex>(index);
        break;
      }
    }
  }

  if (!inverted_index) {
    return;
  }

  // Check filters for search function calls
  irs::And conjunct_root;
  duckdb::vector<duckdb::idx_t> consumed;
  const auto& table_columns = bind_data.table->Columns();

  for (duckdb::idx_t i = 0; i < filters.size(); ++i) {
    auto& expr = *filters[i];
    if (expr.expression_class != duckdb::ExpressionClass::BOUND_FUNCTION) {
      continue;
    }

    auto& func_expr = expr.Cast<duckdb::BoundFunctionExpression>();
    if (func_expr.function.name != functions::kPhrase &&
        func_expr.function.name != functions::kTermEq) {
      continue;
    }

    if (func_expr.children.size() < 2) {
      continue;
    }

    // Child 0: column reference — get name
    if (func_expr.children[0]->expression_class !=
          duckdb::ExpressionClass::BOUND_COLUMN_REF &&
        func_expr.children[0]->expression_class !=
          duckdb::ExpressionClass::BOUND_REF) {
      continue;
    }

    // Find column ID by name
    auto col_name = func_expr.children[0]->GetName();
    catalog::Column::Id col_id = 0;
    bool col_found = false;
    for (const auto& col : table_columns) {
      if (col.name == col_name) {
        col_id = col.id;
        col_found = true;
        break;
      }
    }
    if (!col_found) {
      continue;
    }

    // Check column is in the inverted index
    bool is_indexed = false;
    for (auto idx_col_id : inverted_index->GetColumnIds()) {
      if (idx_col_id == col_id) {
        is_indexed = true;
        break;
      }
    }
    if (!is_indexed) {
      continue;
    }

    // Child 1: constant string
    if (func_expr.children[1]->expression_class !=
        duckdb::ExpressionClass::BOUND_CONSTANT) {
      continue;
    }
    auto& const_expr =
      func_expr.children[1]->Cast<duckdb::BoundConstantExpression>();
    if (const_expr.value.IsNull() ||
        const_expr.value.type().id() != duckdb::LogicalTypeId::VARCHAR) {
      continue;
    }
    auto search_text = const_expr.value.GetValue<std::string>();

    // Build IResearch field name (big-endian Column::Id + MangleString)
    std::string field_name;
    basics::StrResize(field_name, sizeof(col_id));
    absl::big_endian::Store(field_name.data(), col_id);
    search::mangling::MangleString(field_name);

    if (func_expr.function.name == functions::kPhrase) {
      auto analyzer = inverted_index->GetColumnAnalyzer(snapshot, col_id);

      auto& phrase = conjunct_root.add<irs::ByPhrase>();
      *phrase.mutable_field() = field_name;
      auto* phrase_opts = phrase.mutable_options();

      std::cerr << "Search: col_name=" << col_name << " col_id=" << col_id
                << " field_name_size=" << field_name.size()
                << " search_text='" << search_text << "'"
                << " has_analyzer=" << (analyzer.analyzer != nullptr)
                << std::endl;

      auto& tokenizer = *analyzer.analyzer;
      tokenizer.reset(search_text);
      int token_count = 0;
      while (tokenizer.next()) {
        auto& term_attr = *irs::get<irs::TermAttr>(tokenizer);
        phrase_opts->push_back<irs::ByTermOptions>()
          .term.assign(term_attr.value.data(), term_attr.value.size());
        ++token_count;
      }
      std::cerr << "Search: phrase token_count=" << token_count << std::endl;

      consumed.push_back(i);
    } else if (func_expr.function.name == functions::kTermEq) {
      auto& term = conjunct_root.add<irs::ByTerm>();
      *term.mutable_field() = field_name;
      term.mutable_options()->term.assign(
        reinterpret_cast<const irs::byte_type*>(search_text.data()),
        search_text.size());

      consumed.push_back(i);
    }
  }

  if (consumed.empty()) {
    return;
  }

  // Get search snapshot via connection context.
  // EnsureSearchSnapshot takes the INDEX ID (not shard ID) — it finds the
  // shard internally via GetIndexShard.
  // BUT GetIndexShard actually takes a SHARD ID. The old Velox code passes
  // shard.GetId(). Let's use the connection context's snapshot to find the shard.
  // Get search reader directly from a fresh catalog snapshot.
  // Don't use conn_ctx.EnsureSearchSnapshot — it uses the connection's cached
  // catalog snapshot which may be stale (from before CREATE INDEX).
  auto fresh_snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  std::shared_ptr<IndexShard> inverted_shard;
  for (auto& shard : fresh_snapshot->GetIndexShardsByTable(table_id)) {
    if (shard->GetIndexId() == inverted_index->GetId() &&
        shard->GetType() == catalog::ObjectType::InvertedIndexShard) {
      inverted_shard = shard;
      break;
    }
  }
  if (!inverted_shard) {
    return;
  }
  auto& irs_shard =
    basics::downCast<search::InvertedIndexShard>(*inverted_shard);
  auto irs_snapshot = irs_shard.GetInvertedIndexSnapshot();
  bind_data.search_snapshot = irs_snapshot;  // keep snapshot alive
  auto& reader = irs_snapshot->reader;

  std::cerr << "Search: reader.size()=" << reader.size()
            << " reader.docs_count()=" << reader.docs_count() << std::endl;
  bind_data.search_query = conjunct_root.prepare({.index = reader});
  bind_data.search_reader = &reader;

  // Remove consumed filters
  std::sort(consumed.rbegin(), consumed.rend());
  for (auto idx : consumed) {
    filters.erase(filters.begin() + idx);
  }
}

// --- Factory ---

duckdb::TableFunction CreateSereneDBScanFunction() {
  duckdb::TableFunction func("serenedb_scan", {}, SereneDBScanFunction,
                             SereneDBScanBind);
  func.init_global = SereneDBScanInitGlobal;
  func.init_local = SereneDBScanInitLocal;
  func.get_bind_info = SereneDBGetBindInfo;
  func.projection_pushdown = true;
  func.filter_pushdown = false;
  func.pushdown_complex_filter = SereneDBPushdownComplexFilter;
  return func;
}

}  // namespace sdb::connector
