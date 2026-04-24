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

#include <duckdb.hpp>
#include <duckdb/execution/physical_operator.hpp>
#include <duckdb/parser/statement/insert_statement.hpp>
#include <iresearch/analysis/token_attributes.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/filter.hpp>

#include "catalog/index.h"
#include "catalog/table.h"
#include "connector/search_pk_lookup.h"
#include "search/inverted_index_shard.h"

namespace sdb::connector {

// Collected results from an HNSW search: parallel arrays of PK bytes and
// the corresponding distances (smaller = closer).
struct SearchResults {
  std::vector<std::string> pk_keys;
  std::vector<float> distances;
};

// Base class for HNSW-backed DuckDB source operators.
// Subclasses implement RunSearch() to fill SearchResults.
// Output columns: [0] BLOB (pk bytes), [1] FLOAT (distance, optional).
class SereneDBPhysicalVectorSearchBase : public duckdb::PhysicalOperator {
 public:
  SereneDBPhysicalVectorSearchBase(
    duckdb::PhysicalPlan& plan,
    std::shared_ptr<search::InvertedIndexShard> index, std::string field_name,
    std::vector<float> query_vector, duckdb::vector<duckdb::LogicalType> types,
    duckdb::idx_t estimated_cardinality,
    duckdb::OnConflictAction on_conflict = duckdb::OnConflictAction::THROW);

  bool IsSink() const override { return false; }
  bool IsSource() const override { return true; }

  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const override;

  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const override;

 protected:
  // Subclasses run their specific search and populate results.
  virtual void RunSearch(const irs::DirectoryReader& reader,
                         SearchResults& results) const = 0;

  std::shared_ptr<search::InvertedIndexShard> _index;
  std::string _field_name;
  std::vector<float> _query_vector;
  duckdb::OnConflictAction _on_conflict;
};

// ANN (top-k nearest neighbour) search operator.
class SereneDBPhysicalANNSearch final
  : public SereneDBPhysicalVectorSearchBase {
 public:
  SereneDBPhysicalANNSearch(
    duckdb::PhysicalPlan& plan,
    std::shared_ptr<search::InvertedIndexShard> index, std::string field_name,
    std::vector<float> query_vector, size_t top_k,
    duckdb::vector<duckdb::LogicalType> types,
    duckdb::idx_t estimated_cardinality,
    duckdb::OnConflictAction on_conflict = duckdb::OnConflictAction::THROW);

 protected:
  void RunSearch(const irs::DirectoryReader& reader,
                 SearchResults& results) const override;

 private:
  size_t _top_k;
};

// Range search operator -- returns all vectors within squared-L2 distance
// <= radius from the query vector.
class SereneDBPhysicalRangeSearch final
  : public SereneDBPhysicalVectorSearchBase {
 public:
  SereneDBPhysicalRangeSearch(
    duckdb::PhysicalPlan& plan,
    std::shared_ptr<search::InvertedIndexShard> index, std::string field_name,
    std::vector<float> query_vector, float radius,
    duckdb::vector<duckdb::LogicalType> types,
    duckdb::idx_t estimated_cardinality,
    duckdb::OnConflictAction on_conflict = duckdb::OnConflictAction::THROW);

 protected:
  void RunSearch(const irs::DirectoryReader& reader,
                 SearchResults& results) const override;

 private:
  float _radius;
};

// Streaming state for the FTS physical operator.
// One instance per query execution; holds the current scan position across
// successive GetDataInternal() calls.
struct FTSearchGlobalSourceState : public duckdb::GlobalSourceState {
  size_t segment_idx = 0;
  irs::DocIterator::ptr doc;
  SegmentPkIterator segment_pk;
};

// Full-text search (IResearch boolean filter) physical operator.
// Iterates IResearch segments to collect PKs, then materialises rows from
// RocksDB for each projected column. Emits one batch per GetDataInternal()
// call (up to STANDARD_VECTOR_SIZE rows).
class SereneDBPhysicalFTSearch final : public duckdb::PhysicalOperator {
 public:
  // Sentinel value meaning "special column" (rowid / tableoid); filled with
  // NULL in the output.
  static constexpr catalog::Column::Id kInvalidColId =
    std::numeric_limits<catalog::Column::Id>::max();

  // One entry per projected output column.
  struct ProjColumn {
    catalog::Column::Id col_id = kInvalidColId;
    duckdb::LogicalType type;
  };

  SereneDBPhysicalFTSearch(duckdb::PhysicalPlan& plan,
                           std::shared_ptr<catalog::Table> table,
                           std::vector<ProjColumn> proj_columns,
                           search::InvertedIndexSnapshotPtr snapshot,
                           irs::Filter::Query::ptr query,
                           duckdb::vector<duckdb::LogicalType> output_types,
                           duckdb::idx_t estimated_cardinality);

  bool IsSink() const override { return false; }
  bool IsSource() const override { return true; }

  duckdb::unique_ptr<duckdb::GlobalSourceState> GetGlobalSourceState(
    duckdb::ClientContext& context) const override;

  duckdb::SourceResultType GetDataInternal(
    duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
    duckdb::OperatorSourceInput& input) const override;

 private:
  std::shared_ptr<catalog::Table> _table;
  std::vector<ProjColumn> _proj_columns;
  search::InvertedIndexSnapshotPtr _snapshot;
  irs::Filter::Query::ptr _query;
};

}  // namespace sdb::connector
