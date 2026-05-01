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

#pragma once

#include <faiss/impl/IDSelector.h>

#include <duckdb.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/proxy_filter.hpp>
#include <limits>
#include <memory>
#include <vector>

#include "connector/lookup.h"
#include "connector/search_pk_lookup.h"

namespace sdb::connector {

struct SereneDBScanBindData;

class ScanFilter {
 public:
  virtual ~ScanFilter() = default;

  virtual bool Accept(faiss::idx_t id) const = 0;

  virtual int Cost() const { return 100; }
};

class CompositeScanFilter final : public faiss::IDSelector {
 public:
  CompositeScanFilter() = default;
  CompositeScanFilter(std::vector<std::unique_ptr<ScanFilter>> filters)
    : _filters{std::move(filters)} {
    absl::c_sort(_filters, [](const auto& a, const auto& b) {
      return a->Cost() < b->Cost();
    });
  }

  bool Empty() const { return _filters.empty(); }

  bool is_member(faiss::idx_t id) const final;

 private:
  std::vector<std::unique_ptr<ScanFilter>> _filters;
};

struct VectorSearchScan;

struct ANNFilterContext {
  duckdb::ClientContext& context;
  const VectorSearchScan& scan;
  const SereneDBScanBindData& bind_data;
  const rocksdb::Snapshot* rocksdb_snapshot;

  std::vector<duckdb::LogicalType> filter_types;
  std::vector<duckdb::idx_t> filter_projection;
};

class ANNFilter final : public ScanFilter {
 public:
  ANNFilter(const ANNFilterContext& ctx, const irs::SubReader& segment);

  bool Accept(faiss::idx_t id) const final;

 private:
  const ANNFilterContext& _ctx;
  const irs::SubReader& _segment;
  mutable duckdb::ExpressionExecutor _executor;
  mutable duckdb::DataChunk _scratch;
  mutable duckdb::DataChunk _bool_out;

  // Cached File-backed lookup session (lazy, reused across Accept calls).
  // Empty for RocksDB-backed tables -- LookupRows dispatches to RocksDBLookup
  // which doesn't need a session. mutable so const Accept can lazily fill.
  mutable std::shared_ptr<FileLookupSession> _file_lookup_session;
  mutable SegmentPkIterator _it;
};

void InitAnnFilterContext(std::unique_ptr<ANNFilterContext>& filter,
                          duckdb::ClientContext& context,
                          const VectorSearchScan& scan,
                          const rocksdb::Snapshot* rocks_snapshot,
                          const SereneDBScanBindData& bind_data);

class TextScanFilter final : public ScanFilter {
 public:
  TextScanFilter(const irs::Filter::Query& proxy_query,
                 const irs::SubReader& segment);

  bool Accept(faiss::idx_t id) const final;
  int Cost() const final { return 1; }

 private:
  mutable irs::DocIterator::ptr _it;
};

}  // namespace sdb::connector
