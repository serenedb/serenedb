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

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>

#include "iresearch/formats/column/codecs/codecs.hpp"

namespace irs::codecs {
namespace {

using duckdb::idx_t;

std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment&) {
  return nullptr;
}

void Scan(const ColumnSegment&, ColumnScanState&, idx_t, duckdb::Vector&, idx_t,
          duckdb::ScanVectorType) {}

void Select(const ColumnSegment&, ColumnScanState&, idx_t, duckdb::Vector&,
            idx_t, const duckdb::SelectionVector&, idx_t,
            duckdb::ScanVectorType) {}

void FetchRows(const ColumnSegment&, ColumnFetchState&, const idx_t*, idx_t,
               duckdb::Vector&, const duckdb::SelectionVector&, idx_t) {}

constexpr DecompressionFunction kDecompression{
  .init_scan = InitScan,
  .scan = Scan,
  .select = Select,
  .fetch_rows = FetchRows,
};

bool Compress(const Codec& codec, ColumnDataCheckpointData& data) {
  if (absl::c_any_of(data.chunks, [](const WriteChunk& chunk) {
        return !duckdb::FlatVector::Validity(chunk.data)
                  .CheckAllValid(chunk.data.size());
      })) {
    return false;
  }
  data.flush(codec, duckdb::BaseStatistics::CreateEmpty(data.type), data.rows,
             {}, {});
  return true;
}

constexpr CompressionFunction kFunction{.compress = Compress};

constexpr Codec kCodec{
  .type = CodecType::Empty,
  .compression = {&kFunction, 1},
  .decompression = kDecompression,
};

}  // namespace

const Codec* EmptyCodec(duckdb::PhysicalType physical) {
  return physical == duckdb::PhysicalType::BIT ? &kCodec : nullptr;
}

}  // namespace irs::codecs
