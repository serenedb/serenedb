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

#include "connector/duckdb_pk_full_scan.hpp"

#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/search/boolean_filter.hpp>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/inverted_index.h"
#include "connector/duckdb_index_scan_entry.h"
#include "connector/duckdb_search_full_scan.hpp"
#include "connector/duckdb_table_function.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {
namespace {

// Set up a match-all SearchScan on the bind_data so the SearchFullScan
// init/function path can drive a view-backed iresearch index without a
// pre-existing PHRASE/range/scoring filter -- iresearch_plan rules will
// have replaced it already when one is present, so this only fires for
// bare scans (`SELECT * FROM idx_name` etc.).
void EnsureDefaultMatchAllSearchScan(SereneDBScanBindData& bind_data) {
  if (bind_data.scan_source &&
      bind_data.scan_source->Kind() == ScanSourceKind::Search) {
    return;
  }
  SDB_ASSERT(bind_data.IsInvertedIndexEntry());
  SDB_ASSERT(bind_data.inverted_index);

  auto cat_snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  std::shared_ptr<search::InvertedIndexShard> shard;
  for (auto& s : cat_snapshot->GetIndexShardsByRelation(
         bind_data.inverted_index->GetRelationId())) {
    if (s->GetIndexId() == bind_data.inverted_index->GetId() &&
        s->GetType() == catalog::ObjectType::InvertedIndexShard) {
      shard = basics::downCast<search::InvertedIndexShard>(std::move(s));
      break;
    }
  }
  SDB_ASSERT(shard);
  auto idx_snapshot = shard->GetInvertedIndexSnapshot();
  SDB_ASSERT(idx_snapshot);
  auto& reader = *idx_snapshot->reader;

  auto root = std::make_shared<irs::And>();
  root->add<irs::All>();
  auto search = std::make_unique<SearchScan>();
  search->snapshot = std::move(idx_snapshot);
  search->reader = &reader;
  search->stored_filter = root;
  search->query = root->prepare({.index = reader});
  search->filter_summary = "All";
  bind_data.scan_source = std::move(search);
}

}  // namespace

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> PKFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  // View-backed inverted index: rocksdb has no rows for the underlying
  // (the rows live in the source reader's storage), so the bare-scan
  // path walks the iresearch index docs directly and materialises each
  // PK against the underlying reader. Set up a default match-all
  // SearchScan and forward to the SearchFullScan execution path; the
  // iresearch_plan rules already replace the SearchScan with a more
  // specific one before init runs when a PHRASE/range/scoring predicate
  // is present.
  if (bind_data.IsViewBacked()) {
    // bind_data is the binder's cached FunctionData (input.bind_data is
    // optional_ptr<const FunctionData>). The SearchScan we're about to
    // install is reused across the full lifetime of the scan, so writing
    // it here is safe even though the const cast is unusual.
    EnsureDefaultMatchAllSearchScan(
      const_cast<SereneDBScanBindData&>(bind_data));
    return SearchFullScanInitGlobal(context, input);
  }
  auto state = duckdb::make_uniq<PKFullScanGlobalState>();

  InitCommonState(*state, context, bind_data, input);
  auto column_keys = InitPKScanColumns(*state, bind_data);

  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  const auto num_scan = column_keys.size();
  rocksdb::ReadOptions ro;
  ro.snapshot = state->snapshot;
  ro.async_io = false;
  ro.adaptive_readahead = true;
  ro.auto_prefix_mode = true;

  for (size_t i = 0; i < num_scan; ++i) {
    ro.iterate_upper_bound = &state->upper_bound_slices[i];
    auto it = std::unique_ptr<rocksdb::Iterator>(
      state->txn ? state->txn->GetIterator(ro, cf) : db->NewIterator(ro, cf));
    it->Seek(column_keys[i]);
    state->iterators.push_back(std::move(it));
  }

  return state;
}

void PKFullScanFunction(duckdb::ClientContext& context,
                        duckdb::TableFunctionInput& data,
                        duckdb::DataChunk& output) {
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();
  // Match the dispatch in PKFullScanInitGlobal: view-backed inverted
  // indexes use the SearchFullScan execution path (the init function set
  // up a match-all SearchScan).
  if (bind_data.IsViewBacked()) {
    SearchFullScanFunction(context, data, output);
    return;
  }
  auto& gstate = data.global_state->Cast<PKFullScanGlobalState>();
  PKScanFunctionImpl(gstate, gstate.iterators, output);
}

}  // namespace sdb::connector
