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

#include "iresearch/formats/hnsw/hnsw_writer.hpp"

#include <algorithm>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <utility>

#include "basics/assert.h"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/hnsw/column_distance.hpp"
#include "iresearch/formats/hnsw/hnsw_reader.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {
namespace {

template<typename T>
void WriteVector(DataOutput& out, const T& vec) {
  out.WriteU32(vec.size());
  if (vec.size() == 0) {
    return;
  }
  out.WriteData(reinterpret_cast<const byte_type*>(vec.data()),
                sizeof(*vec.data()) * vec.size());
}

}  // namespace

void WriteHNSW(DataOutput& out, const faiss::HNSW& hnsw) {
  WriteVector(out, hnsw.assign_probas);
  WriteVector(out, hnsw.cum_nneighbor_per_level);
  WriteVector(out, hnsw.levels);
  WriteVector(out, hnsw.offsets);
  WriteVector(out, hnsw.neighbors);

  out.WriteU32(hnsw.entry_point);
  out.WriteU32(hnsw.max_level);
  out.WriteU32(hnsw.efConstruction);
  out.WriteU32(hnsw.efSearch);
}

HnswWriter::HnswWriter(HNSWInfo info)
  : _info{std::move(info)}, _hnsw{std::make_shared<faiss::HNSW>(_info.m)} {
  _hnsw->efConstruction = _info.ef_construction;
}

HnswWriter::~HnswWriter() = default;

void HnswWriter::Build(const ColumnReader& vector_column, ReadContext& ctx) {
  const auto* child = vector_column.Child();
  SDB_ASSERT(child);
  const auto array_size = vector_column.ArraySize();
  const auto rows = vector_column.RowCount();

  const auto graph_nodes = rows + doc_limits::min();
  faiss::VisitedTable vt{static_cast<int>(graph_nodes)};
  auto& hnsw = *_hnsw;
  hnsw.prepare_level_tab(graph_nodes, false);

  ChunkedVectorCache cache;
  cache.Rebind(*child, array_size, ctx);
  ColumnDistance dis{_info, &cache};

  const uint64_t chunk_rows = cache.ChunkRows();
  const auto add_row = [&](uint64_t row, const float* base, uint64_t base_row) {
    dis.set_query(base + (row - base_row) * array_size);
    const faiss::idx_t id = static_cast<faiss::idx_t>(row + doc_limits::min());
    const int level = hnsw.levels[id] - 1;
    vt.advance();
    hnsw.add_with_locks(dis, level, id, vt, false);
  };

  if (!vector_column.HasValidity()) {
    for (uint64_t start = 0; start < rows; start += chunk_rows) {
      const uint64_t take = std::min<uint64_t>(chunk_rows, rows - start);
      const float* base = cache.Pin(start);
      for (uint64_t k = 0; k < take; ++k) {
        add_row(start + k, base, start);
      }
      cache.Unpin();
    }
    return;
  }

  duckdb::Vector vbatch{vector_column.Type(), /*capacity=*/0};
  vbatch.BufferMutable().GetValidityMask().Initialize(STANDARD_VECTOR_SIZE);
  const ColumnReader& vcol = *vector_column.Validity();
  ColumnReader::ScanState vscan = vcol.InitScan(ctx);
  for (uint64_t start = 0; start < rows; start += chunk_rows) {
    const uint64_t take = std::min<uint64_t>(chunk_rows, rows - start);
    const float* base = cache.Pin(start);
    uint64_t sub = 0;
    while (sub < take) {
      const auto vtake =
        std::min<duckdb::idx_t>(take - sub, STANDARD_VECTOR_SIZE);
      vbatch.BufferMutable().GetValidityMask().SetAllValid(
        STANDARD_VECTOR_SIZE);
      vcol.ScanCount(vscan, vbatch, vtake, /*result_offset=*/0);
      const auto& vmask = vbatch.Buffer().GetValidityMask();
      for (uint64_t k = 0; k < vtake; ++k) {
        if (vmask.RowIsValid(k)) {
          add_row(start + sub + k, base, start);
        }
      }
      sub += vtake;
    }
    cache.Unpin();
  }
}

void HnswWriter::Serialize(DataOutput& out) { irs::WriteHNSW(out, *_hnsw); }

}  // namespace irs
