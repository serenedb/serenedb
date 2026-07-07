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

// Measures duckdb compression methods DIRECTLY on a single ColumnSegment of
// the desired size -- no reader abstractions in the timed path. Per
// (codec/logical type, block size W, hits k) the arms are:
//   fetch      -- per-hit ColumnSegment::FetchRow (when the codec has one)
//   select     -- one native codec select over the block window; skipped
//                 where the codec's select can't take a block-sized window
//   entire     -- decode the whole block + dictionary Slice of the hits;
//                 no slice at full density (W <= 2048 scans straight into
//                 the output, wider blocks scan flat into a big scratch)
//   partial1   -- per-hit skip + scan_partial(1)
//   partialrun -- run-coalesced skip + scan_partial(run)
// Hits are 1 and 1..99..100% of the block, capped at one output chunk (2048).
// Validity tracks are benched as segments of their own; a null column's arm
// cost composes as data-segment cost + validity-segment cost.
//
// Build in build_perf: ninja serenedb-bench-micro-select_thresholds
// Run: ./bin/serenedb-bench-micro-select_thresholds [--benchmark_filter=...]

#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <duckdb.hpp>
#include <duckdb/common/enum_util.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>
#include <memory>
#include <string>
#include <vector>

#include "basics/duckdb_engine.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/store/memory_directory.hpp"

namespace {

constexpr irs::field_id kField = 0;
constexpr std::string_view kSegName = "bench_seg";

duckdb::DatabaseInstance& CsDb() {
  return sdb::DuckDBEngine::Instance().instance();
}

struct Shape {
  const char* name;
  // SQL expression over range(W) t(i) producing the column.
  const char* expr;
  // Bench the column's validity track instead of its data.
  bool validity = false;
};

// Data patterns steer the writer's codec analysis; the codec each (shape, W)
// actually got is printed at build time and labels the result.
constexpr Shape kShapes[] = {
  {"i64_seq", "i::BIGINT"},
  {"i64_rle", "(i / 512)::BIGINT"},
  {"i16", "(i % 2000)::SMALLINT"},
  {"f64", "(i * 0.001)::DOUBLE"},
  {"str_dict", "'cat_' || (i % 50)::VARCHAR"},
  {"str_high", "'s' || i::VARCHAR || '_' || (i * 7919)::VARCHAR"},
  {"i64_const", "42::BIGINT"},
  {"str_const", "'serene'::VARCHAR"},
  {"validity10", "CASE WHEN i % 10 = 0 THEN NULL ELSE i END::BIGINT", true},
  {"validity50", "CASE WHEN i % 2 = 0 THEN NULL ELSE i END::BIGINT", true},
};

// One opened block of ~W rows plus everything keeping it alive.
struct Blk {
  irs::MemoryDirectory dir{};
  std::unique_ptr<irs::ColReader> reader;
  std::unique_ptr<irs::ReadContext> ctx;
  std::unique_ptr<duckdb::ColumnSegment> block;
  duckdb::LogicalType type;
  uint64_t rows = 0;  // rows in block 0 (== W unless the codec split it)
};

std::unique_ptr<Blk> BuildBlk(const Shape& shape, uint64_t w) {
  auto blk = std::make_unique<Blk>();
  duckdb::Connection con{CsDb()};
  const auto sql = std::string{"SELECT "} + shape.expr + " AS v FROM range(" +
                   std::to_string(w) + ") t(i)";
  auto qr = con.Query(sql);
  if (qr->HasError()) {
    std::fprintf(stderr, "BuildBlk(%s): %s\n", shape.name,
                 qr->GetError().c_str());
    std::abort();
  }
  const auto col_type = qr->types[0];
  {
    irs::ColWriter writer{blk->dir, kSegName, CsDb()};
    auto& cw = writer.OpenColumn(kField, col_type);
    uint64_t pos = 0;
    while (auto chunk = qr->Fetch()) {
      const auto take = chunk->size();
      if (take == 0) {
        break;
      }
      chunk->data[0].Flatten(take);
      cw.Append(pos, chunk->data[0], take);
      pos += take;
    }
    writer.Commit(w);
  }
  blk->reader =
    std::make_unique<irs::ColReader>(blk->dir, std::string{kSegName}, CsDb());
  const auto* col = blk->reader->Column(kField);
  if (col == nullptr) {
    std::fprintf(stderr, "BuildBlk(%s): column missing\n", shape.name);
    std::abort();
  }
  if (shape.validity) {
    col = col->Validity();
    if (col == nullptr) {
      std::fprintf(stderr, "BuildBlk(%s): no validity track\n", shape.name);
      std::abort();
    }
  }
  blk->type = col->Type();
  blk->ctx = std::make_unique<irs::ReadContext>(*blk->reader);
  blk->block = col->OpenSegment(0, *blk->ctx);
  blk->rows = blk->block->count.load();
  const auto& fn = blk->block->GetCompressionFunction();
  std::fprintf(stderr,
               "shape %-12s W=%-6llu type=%-10s blocks=%zu block0=%llu "
               "codec=%s select=%d fetch=%d\n",
               shape.name, static_cast<unsigned long long>(w),
               blk->type.ToString().c_str(), col->DataBlocks().size(),
               static_cast<unsigned long long>(blk->rows),
               duckdb::EnumUtil::ToChars<duckdb::CompressionType>(fn.type),
               fn.select != nullptr, fn.fetch_row != nullptr);
  return blk;
}

constexpr uint64_t kWindows[] = {1, 10, 100, 2048, 4096, 8192, 16384};

const Blk& GetBlk(size_t shape_idx, uint64_t w) {
  static std::unique_ptr<Blk> gBlks[std::size(kShapes)][std::size(kWindows)];
  size_t wi = 0;
  while (kWindows[wi] != w) {
    ++wi;
  }
  auto& slot = gBlks[shape_idx][wi];
  if (!slot) {
    slot = BuildBlk(kShapes[shape_idx], w);
  }
  return *slot;
}

enum class Arm { Fetch, Select, Entire, Partial1, PartialRun };

const char* ArmName(Arm a) {
  switch (a) {
    case Arm::Fetch:
      return "fetch";
    case Arm::Select:
      return "select";
    case Arm::Entire:
      return "entire";
    case Arm::Partial1:
      return "partial1";
    case Arm::PartialRun:
      return "partialrun";
  }
  return "?";
}

void BenchArm(benchmark::State& state, size_t shape_idx, Arm arm) {
  const auto w = static_cast<uint64_t>(state.range(0));
  const auto k = static_cast<uint64_t>(state.range(1));
  const auto& blk = GetBlk(shape_idx, w);
  if (blk.rows < w) {
    state.SkipWithError("codec split the block");
    return;
  }
  auto& segment = *blk.block;
  const auto& fn = segment.GetCompressionFunction();
  if (arm == Arm::Select && fn.select == nullptr) {
    state.SkipWithError("no select");
    return;
  }
  if (arm == Arm::Fetch && fn.fetch_row == nullptr) {
    state.SkipWithError("no fetch_row");
    return;
  }

  // k hit offsets uniformly spaced in [0, w), plus the coalesced runs.
  duckdb::SelectionVector sel{STANDARD_VECTOR_SIZE};
  std::vector<std::pair<uint64_t, uint64_t>> runs;  // (start, len)
  for (uint64_t j = 0; j < k; ++j) {
    const uint64_t target = j * w / k;
    sel.set_index(j, target);
    if (!runs.empty() && runs.back().first + runs.back().second == target) {
      ++runs.back().second;
    } else {
      runs.emplace_back(target, 1);
    }
  }
  constexpr uint64_t kVec = STANDARD_VECTOR_SIZE;
  if (arm == Arm::Select) {
    // Select takes the whole block window in one call. RLE / FSST /
    // string_uncompressed selects are position/skip based and accept any
    // window; dict_fsst's DICT-mode fallback scans the window into the
    // 2048-capacity result and throws above it. Probe once: where the codec
    // can't take a block-sized window the arm is simply unavailable.
    try {
      duckdb::ColumnScanState probe_st{nullptr};
      duckdb::Vector probe{blk.type};
      segment.InitializeScan(probe_st);
      segment.Select(probe_st, w, probe, sel, k);
    } catch (const std::exception&) {
      state.SkipWithError("select can't take a block-sized window");
      return;
    }
  }

  duckdb::VectorCache out_cache{duckdb::Allocator::DefaultAllocator(),
                                blk.type};
  duckdb::Vector out{out_cache};
  duckdb::Vector scratch{blk.type, static_cast<duckdb::idx_t>(w)};

  for (auto _ : state) {
    out_cache.ResetFromCache(out);
    duckdb::ColumnScanState st{nullptr};
    switch (arm) {
      case Arm::Fetch: {
        duckdb::ColumnFetchState fs;
        for (uint64_t j = 0; j < k; ++j) {
          segment.FetchRow(fs, static_cast<duckdb::row_t>(sel.get_index(j)),
                           out, j);
        }
        break;
      }
      case Arm::Select:
        segment.InitializeScan(st);
        segment.Select(st, w, out, sel, k);
        break;
      case Arm::Entire: {
        segment.InitializeScan(st);
        if (w <= kVec) {
          segment.Scan(st, w, out, 0,
                       duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR);
          if (k < w) {
            out.Slice(sel, k);
          }
          break;
        }
        // duckdb's own scan loop granularity: <=2048-row flat scans into one
        // big scratch, then a single dictionary slice of the hits. scan_partial
        // reads its start from the state, so advance it like
        // ColumnScanState::Next does.
        duckdb::FlatVector::ValidityMutable(scratch).Reset();
        for (uint64_t off = 0; off < w; off += kVec) {
          const auto n = std::min(kVec, w - off);
          segment.Scan(st, n, scratch, off,
                       duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
          st.offset_in_column += n;
          st.internal_index = st.offset_in_column;
        }
        out.Slice(scratch, sel, k);
        break;
      }
      case Arm::Partial1: {
        segment.InitializeScan(st);
        for (uint64_t j = 0; j < k; ++j) {
          st.offset_in_column = sel.get_index(j);
          segment.Skip(st);
          segment.Scan(st, 1, out, j, duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
          st.offset_in_column += 1;
          st.internal_index = st.offset_in_column;
        }
        break;
      }
      case Arm::PartialRun: {
        segment.InitializeScan(st);
        duckdb::idx_t out_off = 0;
        for (const auto& [start, len] : runs) {
          st.offset_in_column = start;
          segment.Skip(st);
          segment.Scan(st, len, out, out_off,
                       duckdb::ScanVectorType::SCAN_FLAT_VECTOR);
          st.offset_in_column += len;
          st.internal_index = st.offset_in_column;
          out_off += len;
        }
        break;
      }
    }
    benchmark::DoNotOptimize(out);
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
  state.counters["hits"] = static_cast<double>(k);
  state.counters["window"] = static_cast<double>(w);
  state.counters["nruns"] = static_cast<double>(runs.size());
}

// Hit counts per block: 1 hit, then 1/10/20/../90/99/100% of the block,
// deduped and capped at one output chunk (2048).
std::vector<int64_t> HitCounts(uint64_t w) {
  static constexpr double kPcts[] = {0.01, 0.1, 0.2, 0.3, 0.4,  0.5,
                                     0.6,  0.7, 0.8, 0.9, 0.99, 1.0};
  std::vector<int64_t> ks{1};
  for (const double p : kPcts) {
    const auto k = static_cast<int64_t>(static_cast<double>(w) * p);
    if (k >= 1 && k <= static_cast<int64_t>(w) && k <= STANDARD_VECTOR_SIZE &&
        k != ks.back()) {
      ks.push_back(k);
    }
  }
  return ks;
}

void RegisterAll() {
  for (size_t si = 0; si < std::size(kShapes); ++si) {
    for (const Arm arm : {Arm::Fetch, Arm::Select, Arm::Entire, Arm::Partial1,
                          Arm::PartialRun}) {
      const auto name = std::string{kShapes[si].name} + "/" + ArmName(arm);
      auto* b = benchmark::RegisterBenchmark(
        name.c_str(),
        [si, arm](benchmark::State& st) { BenchArm(st, si, arm); });
      for (const auto w : kWindows) {
        for (const auto k : HitCounts(w)) {
          b->Args({static_cast<int64_t>(w), k});
        }
      }
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  sdb::DuckDBEngine::Instance().Initialize();
  RegisterAll();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  sdb::DuckDBEngine::Instance().Shutdown();
  return 0;
}
