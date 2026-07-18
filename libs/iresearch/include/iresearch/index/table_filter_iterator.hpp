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

#include <array>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/iterators.hpp"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

// A DocIterator = the inner search DocIterator + `.col` table filters. It
// yields only doc-ids whose stored (INCLUDE'd) column values pass the filters,
// narrowing the selection during the read via per-block zonemap skip + codec
// ColumnSegment::Filter (ColumnReader::GatherFilter) -- exactly like
// RowGroup::Scan. Because it IS a DocIterator, every consumer works unchanged:
// count() (count-only), Collect() (top-k, feeding NthPartitionScoreCollector),
// and EmitScoredDocs()/EmitDocs() (streaming). So `.col` table filters apply to
// count-only, top-k and streaming through one wrapper.
class TableFilterDocIterator : public irs::DocIterator {
 public:
  struct FilterSpec {
    irs::field_id field;
    const duckdb::TableFilter* filter;
    // Filter on the computed score column (applied on the score vector after
    // scoring) rather than a `.col` field.
    bool is_score = false;
  };

  TableFilterDocIterator(irs::DocIterator::ptr inner,
                         const irs::ColReader& col_reader,
                         std::span<const FilterSpec> filters,
                         duckdb::ClientContext& context);

  irs::doc_id_t advance() final {
    _doc = _inner->advance();
    return _doc;
  }
  irs::doc_id_t seek(irs::doc_id_t target) final {
    _doc = _inner->seek(target);
    return _doc;
  }
  irs::doc_id_t LazySeek(irs::doc_id_t target) final {
    _doc = _inner->LazySeek(target);
    return _doc;
  }
  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    return _inner->GetMutable(id);
  }
  irs::ScoreFunction PrepareScore(const irs::PrepareScoreContext& ctx) final {
    return _inner->PrepareScore(ctx);
  }
  void FetchScoreArgs(uint16_t index) final { _inner->FetchScoreArgs(index); }
  uint32_t GetFreq() const final { return _inner->GetFreq(); }

  uint32_t count() final;
  uint32_t EmitDocs(irs::doc_id_t* out, irs::doc_id_t min,
                    irs::doc_id_t max) final;
  uint32_t EmitScoredDocs(irs::doc_id_t* out, irs::score_t* scores,
                          irs::doc_id_t max, const irs::ScoreFunction& scorer,
                          irs::ColumnArgsFetcher* fetcher,
                          irs::doc_id_t min) final;
  void Collect(const irs::ScoreFunction& scorer,
               irs::ColumnArgsFetcher& fetcher,
               irs::ScoreCollector& collector) final;
  std::pair<irs::doc_id_t, bool> FillBlock(
    irs::doc_id_t min, irs::doc_id_t max, uint64_t* mask,
    irs::FillBlockScoreContext score, irs::FillBlockMatchContext match) final {
    // Not reached on the filtered paths (count/Collect/EmitScoredDocs cover
    // them); forwarding keeps the interface total.
    return _inner->FillBlock(min, max, mask, score, match);
  }

 private:
  struct FilterCol {
    const irs::ColumnReader* reader = nullptr;
    const duckdb::TableFilter* filter = nullptr;
    duckdb::unique_ptr<duckdb::TableFilterState> state;
    irs::ColumnReader::ScanState scan;
    // Codec Scan/Filter may morph the vector (e.g. dict_fsst emits a
    // DICTIONARY view over codec-owned buffers), so every use goes through
    // VectorScratch::Reset() -- never reuse it dirty.
    std::unique_ptr<irs::ColumnReader::VectorScratch> scratch;
  };

  // Compacts docs[0..n) (and scores[0..n) when non-null, ascending) in place to
  // the subset passing every `.col` filter, block by block: whole-block zonemap
  // skip, then codec GatherFilter narrowing. Returns the survivor count.
  duckdb::idx_t FilterBlock(irs::doc_id_t* docs, irs::score_t* scores,
                            duckdb::idx_t n);

  irs::DocIterator::ptr _inner;
  irs::ReadContext _ctx;
  std::vector<FilterCol> _filters;
  // Filter on the computed score (not a `.col` field): applied on the score
  // vector after scoring. Null when there is no score filter.
  const duckdb::TableFilter* _score_filter = nullptr;
  duckdb::unique_ptr<duckdb::TableFilterState> _score_state;
  duckdb::buffer_ptr<duckdb::SelectionData> _sel_data;
  duckdb::SelectionVector _sel;
  std::array<irs::doc_id_t, STANDARD_VECTOR_SIZE> _docbuf;
  std::array<irs::score_t, STANDARD_VECTOR_SIZE> _scorebuf;
};

}  // namespace sdb::connector
