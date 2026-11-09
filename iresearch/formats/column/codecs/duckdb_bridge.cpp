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

#include "iresearch/formats/column/codecs/duckdb_bridge.hpp"

#include <absl/algorithm/container.h>

#include <array>
#include <duckdb/function/compression/compression.hpp>
#include <duckdb/function/compression_function.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/checkpoint/string_checkpoint_state.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table/column_data_checkpointer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <optional>
#include <vector>

#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

using duckdb::CompressionType;
using duckdb::PhysicalType;

constexpr PhysicalType kPhysicalTypes[] = {
  PhysicalType::BOOL,    PhysicalType::UINT8,   PhysicalType::INT8,
  PhysicalType::UINT16,  PhysicalType::INT16,   PhysicalType::UINT32,
  PhysicalType::INT32,   PhysicalType::UINT64,  PhysicalType::INT64,
  PhysicalType::FLOAT,   PhysicalType::DOUBLE,  PhysicalType::INTERVAL,
  PhysicalType::LIST,    PhysicalType::STRUCT,  PhysicalType::ARRAY,
  PhysicalType::VARCHAR, PhysicalType::UINT128, PhysicalType::INT128,
  PhysicalType::BIT,
};

std::optional<size_t> PhysicalIndex(PhysicalType physical) noexcept {
  const auto* it = absl::c_find(kPhysicalTypes, physical);
  if (it == std::end(kPhysicalTypes)) {
    return std::nullopt;
  }
  return static_cast<size_t>(it - std::begin(kPhysicalTypes));
}

struct BridgeAnalyze final : AnalyzeState {
  const duckdb::CompressionFunction* duck = nullptr;
  duckdb::unique_ptr<duckdb::AnalyzeState> state;
};

class BridgeCompress final : public CompressionState {
 public:
  BridgeCompress(ColumnDataCheckpointData& data,
                 const duckdb::CompressionFunction& duck)
    : duck{&duck},
      _flush{data.flush},
      _ckp{
        data.type,
        data.ctx.Database(),
        duckdb::StorageVersion::SERENEDB_LATEST,
        data.type.InternalType() == PhysicalType::VARCHAR ? &data.ctx : nullptr,
        data.type.InternalType() == PhysicalType::VARCHAR ? &data.ctx : nullptr,
        [this](duckdb::unique_ptr<duckdb::ColumnSegment> seg,
               duckdb::BufferHandle handle,
               duckdb::idx_t size) { Capture(*seg, std::move(handle), size); },
        [this](duckdb::unique_ptr<duckdb::ColumnSegment> seg,
               duckdb::idx_t size) {
          Capture(*seg, duckdb::BufferHandle{}, size);
        },
        data.ctx,
      } {}

  duckdb::ColumnDataCheckpointData& Checkpoint() noexcept { return _ckp; }

  const duckdb::CompressionFunction* duck;
  duckdb::unique_ptr<duckdb::CompressionState> state;

 private:
  void Capture(duckdb::ColumnSegment& seg, duckdb::BufferHandle handle,
               duckdb::idx_t size) {
    if (size != 0 && !handle.IsValid() && seg.GetBlockHandle()) {
      handle = seg.GetDatabase().GetBufferManager().Pin(seg.GetBlockHandle());
    }
    const std::string_view part =
      handle.IsValid()
        ? std::string_view{reinterpret_cast<const char*>(handle.Ptr()), size}
        : std::string_view{};
    _flush(std::move(seg.GetStatsMutable()), seg.count.load(), part);
  }

  FlushSegmentFn _flush;
  duckdb::ColumnDataCheckpointData _ckp;
};

struct BridgeScan final : SegmentScanState {
  std::unique_ptr<duckdb::ColumnSegment> segment;
  duckdb::ColumnScanState st{nullptr};
};

struct BridgeFetch final : SegmentScanState {
  std::unique_ptr<duckdb::ColumnSegment> segment;
  duckdb::ColumnFetchState state;
};

const duckdb::CompressionFunction* Duck(CompressionType type,
                                        PhysicalType physical);

const duckdb::CompressionFunction& DuckOf(const ColumnSegment& segment) {
  const auto* duck =
    Duck(segment.function.type, segment.stats.GetType().InternalType());
  SDB_ASSERT(duck);
  return *duck;
}

std::unique_ptr<duckdb::ColumnSegment> DuckSegment(
  const ColumnSegment& segment) {
  auto& db = segment.ctx.Database();
  auto block = segment.block;
  auto seg = std::make_unique<duckdb::ColumnSegment>(
    db, std::move(block), duckdb::ColumnSegmentType::PERSISTENT,
    static_cast<duckdb::idx_t>(segment.count), DuckOf(segment),
    segment.stats.Copy(), /*block_id=*/0, /*offset=*/0,
    static_cast<duckdb::idx_t>(segment.size), /*segment_state=*/nullptr);
  if (segment.stats.GetType().InternalType() == PhysicalType::VARCHAR) {
    if (auto seg_state = seg->GetSegmentState()) {
      auto& str_state =
        seg_state->Cast<duckdb::UncompressedStringSegmentState>();
      str_state.overflow_reader = &segment.ctx;
      str_state.stream_reader = &segment.ctx;
    }
  }
  return seg;
}

template<CompressionType kType>
std::unique_ptr<AnalyzeState> BridgeInitAnalyze(CompressionAnalyzeContext& ctx,
                                                PhysicalType physical) {
  const auto* duck = Duck(kType, physical);
  SDB_ASSERT(duck && duck->init_analyze);
  duckdb::CompressionAnalyzeContext actx{
    ctx.ctx, ctx.ctx.Database(), duckdb::StorageVersion::SERENEDB_LATEST};
  auto state = duck->init_analyze(actx, physical);
  if (!state) {
    return nullptr;
  }
  auto bridge = std::make_unique<BridgeAnalyze>();
  bridge->duck = duck;
  bridge->state = std::move(state);
  return bridge;
}

bool BridgeAnalyzeData(AnalyzeState& state, const duckdb::Vector& input) {
  auto& bridge = state.Cast<BridgeAnalyze>();
  return bridge.duck->analyze(*bridge.state, input);
}

duckdb::idx_t BridgeFinalAnalyze(AnalyzeState& state) {
  auto& bridge = state.Cast<BridgeAnalyze>();
  return bridge.duck->final_analyze(*bridge.state);
}

template<CompressionType kType>
std::unique_ptr<CompressionState> BridgeInitCompression(
  ColumnDataCheckpointData& data, std::unique_ptr<AnalyzeState> state) {
  const auto* duck = Duck(kType, data.type.InternalType());
  SDB_ASSERT(duck && duck->init_compression);
  auto bridge = std::make_unique<BridgeCompress>(data, *duck);
  duckdb::unique_ptr<duckdb::AnalyzeState> duck_state;
  if (state) {
    duck_state = std::move(state->Cast<BridgeAnalyze>().state);
  }
  bridge->state =
    duck->init_compression(bridge->Checkpoint(), std::move(duck_state));
  return bridge;
}

void BridgeCompressData(CompressionState& state, const duckdb::Vector& input) {
  auto& bridge = state.Cast<BridgeCompress>();
  bridge.duck->compress(*bridge.state, input);
}

void BridgeCompressFinalize(CompressionState& state) {
  auto& bridge = state.Cast<BridgeCompress>();
  bridge.duck->compress_finalize(*bridge.state);
}

std::unique_ptr<SegmentScanState> BridgeInitScan(const ColumnSegment& segment) {
  auto scan = std::make_unique<BridgeScan>();
  scan->segment = DuckSegment(segment);
  scan->segment->InitializeScan(scan->st);
  return scan;
}

BridgeScan& Sync(ColumnScanState& state) {
  auto& bridge = state.scan_state->Cast<BridgeScan>();
  bridge.st.offset_in_column = state.offset_in_column;
  bridge.st.internal_index = state.internal_index;
  return bridge;
}

void BridgeScanVector(const ColumnSegment&, ColumnScanState& state,
                      duckdb::idx_t scan_count, duckdb::Vector& result) {
  auto& bridge = Sync(state);
  bridge.segment->GetCompressionFunction().scan_vector(
    *bridge.segment, bridge.st, scan_count, result);
}

void BridgeScanPartial(const ColumnSegment&, ColumnScanState& state,
                       duckdb::idx_t scan_count, duckdb::Vector& result,
                       duckdb::idx_t result_offset) {
  auto& bridge = Sync(state);
  bridge.segment->GetCompressionFunction().scan_partial(
    *bridge.segment, bridge.st, scan_count, result, result_offset);
}

void BridgeSelect(const ColumnSegment&, ColumnScanState& state,
                  duckdb::idx_t vector_count, duckdb::Vector& result,
                  const duckdb::SelectionVector& sel, duckdb::idx_t sel_count) {
  auto& bridge = Sync(state);
  bridge.segment->GetCompressionFunction().select(
    *bridge.segment, bridge.st, vector_count, result, sel, sel_count);
}

void BridgeFilter(const ColumnSegment&, ColumnScanState& state,
                  duckdb::idx_t vector_count, duckdb::Vector& result,
                  duckdb::SelectionVector& sel, duckdb::idx_t& sel_count,
                  const duckdb::TableFilter& filter,
                  duckdb::TableFilterState& filter_state) {
  auto& bridge = Sync(state);
  bridge.segment->GetCompressionFunction().filter(
    *bridge.segment, bridge.st, vector_count, result, sel, sel_count, filter,
    filter_state);
}

void BridgeSkip(const ColumnSegment&, ColumnScanState& state,
                duckdb::idx_t skip_count) {
  auto& bridge = Sync(state);
  bridge.segment->GetCompressionFunction().skip(*bridge.segment, bridge.st,
                                                skip_count);
}

void BridgeFetchRows(const ColumnSegment& segment, ColumnFetchState& state,
                     const duckdb::idx_t* rows, duckdb::idx_t count,
                     duckdb::Vector& result,
                     const duckdb::SelectionVector& result_sel,
                     duckdb::idx_t result_offset) {
  auto& bridge = state.GetOrCreateSegmentState<BridgeFetch>([&] {
    auto fetch = std::make_unique<BridgeFetch>();
    fetch->segment = DuckSegment(segment);
    return fetch;
  });
  bridge.segment->FetchRows(bridge.state, rows, count, result, result_sel,
                            result_offset);
}

duckdb::InsertionOrderPreservingMap<std::string> BridgeSegmentInfo(
  const ColumnSegment& segment) {
  auto seg = DuckSegment(segment);
  return seg->GetCompressionFunction().get_segment_info(duckdb::QueryContext{},
                                                        *seg);
}

struct Bridged {
  CompressionType type;
  duckdb::CompressionFunction (*get)(PhysicalType type);
  bool (*supports)(const PhysicalType type);
  std::unique_ptr<AnalyzeState> (*init_analyze)(CompressionAnalyzeContext& ctx,
                                                PhysicalType type);
  std::unique_ptr<CompressionState> (*init_compression)(
    ColumnDataCheckpointData& data, std::unique_ptr<AnalyzeState> state);
};

template<CompressionType kType>
constexpr Bridged Bridge(duckdb::CompressionFunction (*get)(PhysicalType),
                         bool (*supports)(const PhysicalType)) {
  return {kType, get, supports, BridgeInitAnalyze<kType>,
          BridgeInitCompression<kType>};
}

constexpr Bridged kBridged[] = {
  Bridge<CompressionType::COMPRESSION_ROARING>(
    duckdb::RoaringCompressionFun::GetFunction,
    duckdb::RoaringCompressionFun::TypeIsSupported),
};

struct Entry {
  duckdb::CompressionFunction duck;
  CompressionFunction ours;
};

using Table =
  std::array<std::array<std::optional<Entry>, std::size(kPhysicalTypes)>,
             std::size(kBridged)>;

Table MakeTable() {
  Table table;
  for (size_t t = 0; t < std::size(kBridged); ++t) {
    const auto& bridged = kBridged[t];
    for (size_t p = 0; p < std::size(kPhysicalTypes); ++p) {
      const auto physical = kPhysicalTypes[p];
      if (!bridged.supports(physical)) {
        continue;
      }
      auto duck = bridged.get(physical);
      CompressionFunction ours{
        .type = bridged.type,
        .data_type = physical,
        .init_scan = BridgeInitScan,
        .scan_vector = BridgeScanVector,
        .scan_partial = BridgeScanPartial,
        .fetch_rows = BridgeFetchRows,
        .skip = BridgeSkip,
      };
      if (duck.init_analyze) {
        ours.init_analyze = bridged.init_analyze;
        ours.analyze = BridgeAnalyzeData;
        ours.final_analyze = BridgeFinalAnalyze;
      }
      if (duck.init_compression) {
        ours.init_compression = bridged.init_compression;
        ours.compress = BridgeCompressData;
        ours.compress_finalize = BridgeCompressFinalize;
      }
      if (duck.select) {
        ours.select = BridgeSelect;
      }
      if (duck.filter) {
        ours.filter = BridgeFilter;
      }
      if (duck.get_segment_info) {
        ours.get_segment_info = BridgeSegmentInfo;
      }
      table[t][p].emplace(Entry{std::move(duck), ours});
    }
  }
  return table;
}

const Table& GetTable() {
  static const Table kTable = MakeTable();
  return kTable;
}

const Entry* Find(CompressionType type, PhysicalType physical) {
  const auto* bridged =
    absl::c_find_if(kBridged, [&](const Bridged& b) { return b.type == type; });
  const auto p = PhysicalIndex(physical);
  if (bridged == std::end(kBridged) || !p) {
    return nullptr;
  }
  const auto& entry =
    GetTable()[static_cast<size_t>(bridged - std::begin(kBridged))][*p];
  return entry ? &*entry : nullptr;
}

const duckdb::CompressionFunction* Duck(CompressionType type,
                                        PhysicalType physical) {
  const auto* entry = Find(type, physical);
  return entry ? &entry->duck : nullptr;
}

}  // namespace

const CompressionFunction* DuckDBFunction(CompressionType type,
                                          PhysicalType physical) {
  const auto* entry = Find(type, physical);
  return entry ? &entry->ours : nullptr;
}

}  // namespace irs::codecs
