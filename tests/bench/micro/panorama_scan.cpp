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

#include <benchmark/benchmark.h>
#include <faiss/MetricType.h>
#include <faiss/impl/Panorama.h>
#include <faiss/utils/ordered_key_value.h>

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <functional>
#include <iresearch/formats/ivf/clustering.hpp>
#include <iresearch/index/column_info.hpp>
#include <iresearch/types.hpp>
#include <iresearch/utils/vector.hpp>
#include <limits>
#include <map>
#include <random>
#include <type_traits>
#include <utility>
#include <vector>

// Establishes the (dim, block-size) bounds at which Panorama progressive
// pruning beats the plain scalar distance scan, for the IVF *centroid* scan
// rather than the posting-list scan Panorama already ships in.
//
// The consumer's regime is different from the postings': centroid nodes are
// small (kMaxFanout is 1024, a real root node is ~100 rows), and the query
// rotation is O(d^2) while the per-centroid saving is only O(d) -- so there is
// a minimum scanned-set size below which Panorama loses outright.
//
// Compares, per (d, n):
//   - Scalar{NoGate,Gate}   the current centroids.hpp loop over ComputeDistance
//   - PanoA_*               level-major 128-row batches through faiss
//                           Panorama::progressive_filter_batch (what postings do)
//   - PanoB_*               row-major centroids + a separate suffix-norm array,
//                           bounded per centroid with per-level early exit, so
//                           the top-k gate tightens every centroid instead of
//                           every batch
//   - Tree*                 a multi-node descent, where the one query rotation
//                           is amortized over nodes*n centroids -- this is the
//                           arm that answers the real question, since the
//                           rotation is paid per query, not per node
//
// Reading the table:
//   - PanoA_NoGate answers "is the level-major layout alone ever slower than the
//     scalar row-major loop".
//   - PanoC_Dense / TreeC are that layout scored densely, with none of the
//     active-set machinery, which is the cheap fallback a level-major node does
//     have: each (row, level) slice is contiguous, so a full distance is n_levels
//     dot products. Measured verdict: with a cold gate (nodes=1) it beats
//     PanoA_Warm by 15-42% for n <= 128, which is why a small-node fast path looks
//     attractive -- but once the gate carries across nodes it loses by 1.4-11.5x
//     at every n down to 16, because a warm gate prunes a 16-row node just as hard
//     (dims_frac 0.04-0.32 at nodes=64). So there is no node size at which
//     skipping the pruning machinery pays, and the scan keeps one path.
//   - PanoA_WarmRotated / PanoB_WarmRotated answer "at what n does one rotation
//     amortize", i.e. n_min(d). Rotation grows d^2 while the saving grows d, so
//     n_min rises with d: wide vectors prune better per centroid but are harder
//     to amortize.
//   - PanoA_WarmPadded prices PanoramaQuantizerWriter::Finish's zero-padding of
//     the tail batch to 128 -- a 2-centroid node stored that way is scanned as
//     128 records.
//   - TreeA_Carry vs TreeA prices carrying the pruning threshold across nodes.
//
// Caveats that must not be misread as results:
//   - Rows with n <= beam cannot prune at all (the k-th best is the worst seen),
//     so those rows are pure layout comparisons and dims_frac reads 1.0. This is
//     why beam is a registered dimension.
//   - The scalar L2 kernel computes sum (x-y)^2 (sub + FMA per dim) while
//     Panorama computes |y|^2 + |q|^2 - 2<q,y> from precomputed norms (one FMA
//     per dim), so PanoA can beat scalar on L2 for reasons unrelated to pruning.
//     The IP arms have no such asymmetry.
//   - The rotated arms charge a real RotateQuery + compute_query_cum_sums per
//     scan but discard the result and score with the unrotated query, so the
//     cost is faithful while `best` stays comparable across every arm.
//   - Small-n rows are L1-resident and re-scanned every iteration; that is
//     realistic for a centroid tree and optimistic for a cold posting list.
//   - compact_active_kernel's BMI2/AVX2 path is gated on COMPILE_SIMD_AVX2,
//     which faiss sets PRIVATE to faiss_avx2. progressive_filter_batch is a
//     header template instantiated here, so this bench takes the scalar
//     compaction tail exactly as quantizer.cpp does.
//
// PanoramaLevels / PanoramaRecordSize / MakePanorama / RotateQuery are copied
// from the anonymous namespace of formats/ivf/quantizer.cpp (only
// PanoramaApplies is exported); keep them in sync.

namespace {

constexpr uint32_t kLevelWidth = 32;
constexpr uint32_t kSeed = 12345;
constexpr size_t kGroupRows = 8;
constexpr size_t kPcaTrainRows = 4096;

uint32_t PanoramaLevels(uint32_t d) noexcept {
  return (d + kLevelWidth - 1) / kLevelWidth;
}

faiss::Panorama MakePanorama(uint32_t d, size_t batch_size) {
  return faiss::Panorama{size_t{d} * sizeof(float), PanoramaLevels(d),
                         batch_size};
}

void RotateQuery(const irs::byte_type* rotation, const float* q, float* out,
                 uint32_t d) {
  const auto* qb = reinterpret_cast<const irs::byte_type*>(q);
  const auto width = static_cast<uint16_t>(d);
  const size_t stride = size_t{d} * sizeof(float);
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = irs::vector::DotProductImpl<float, float>::Compute(
      rotation + i * stride, qb, width);
  }
}

std::vector<float> MakePanoramaData(uint32_t d, size_t n, uint32_t seed) {
  auto basis = irs::MakeRotation(d, seed);
  std::mt19937 rng{seed};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> coef(d);
  std::vector<float> out(n * d, 0.f);
  for (size_t i = 0; i < n; ++i) {
    const float shift = static_cast<float>(i % kGroupRows) * 0.25f;
    for (uint32_t j = 0; j < d; ++j) {
      coef[j] = (nd(rng) + shift) / static_cast<float>(j + 1);
    }
    float* row = out.data() + i * d;
    for (uint32_t j = 0; j < d; ++j) {
      const float* b = basis.data() + size_t{j} * d;
      for (uint32_t t = 0; t < d; ++t) {
        row[t] += coef[j] * b[t];
      }
    }
  }
  return out;
}

std::vector<float> MakeRotatedAnisotropic(uint32_t d, size_t n, uint32_t seed) {
  std::mt19937 rng{seed};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> out(n * d);
  for (size_t i = 0; i < n; ++i) {
    const float shift = static_cast<float>(i % kGroupRows) * 0.25f;
    float* row = out.data() + i * d;
    for (uint32_t j = 0; j < d; ++j) {
      row[j] = (nd(rng) + shift) / static_cast<float>(j + 1);
    }
  }
  return out;
}

std::vector<float> MakeCentroidLike(uint32_t d, size_t n, uint32_t seed) {
  const auto fine = MakeRotatedAnisotropic(d, n * kGroupRows, seed);
  std::vector<float> out(n * d, 0.f);
  const float scale = 1.f / static_cast<float>(kGroupRows);
  for (size_t i = 0; i < n; ++i) {
    float* row = out.data() + i * d;
    for (size_t g = 0; g < kGroupRows; ++g) {
      const float* src = fine.data() + (i * kGroupRows + g) * d;
      for (uint32_t j = 0; j < d; ++j) {
        row[j] += src[j] * scale;
      }
    }
  }
  return out;
}

struct Dataset {
  std::vector<float> rows;
  std::vector<float> query;
  size_t n = 0;
};

void FillDataset(Dataset& ds, int kind, uint32_t d, size_t n) {
  const size_t total = n + 1;
  std::vector<float> raw;
  switch (kind) {
    case 1: {
      const size_t train = std::max(
        total, std::max<size_t>(kPcaTrainRows, size_t{8} * d));
      raw = MakePanoramaData(d, train, kSeed);
      auto pca = irs::TrainPcaRotation(raw.data(), train, d);
      std::vector<float> rotated(total * d);
      pca.apply_noalloc(static_cast<faiss::idx_t>(total), raw.data(),
                        rotated.data());
      raw = std::move(rotated);
      break;
    }
    case 2:
      raw = MakeRotatedAnisotropic(d, total, kSeed);
      irs::NormalizeRows(raw.data(), total, d);
      break;
    case 3:
      raw = MakeCentroidLike(d, total, kSeed);
      break;
    case 4:
      raw = MakePanoramaData(d, total, kSeed);
      break;
    default:
      raw = MakeRotatedAnisotropic(d, total, kSeed);
      break;
  }
  ds.query.assign(raw.begin(), raw.begin() + d);
  ds.rows.assign(raw.begin() + d, raw.begin() + total * d);
  ds.n = n;
}

const Dataset& Data(int kind, uint32_t d, size_t n) {
  static std::map<std::pair<int, uint32_t>, Dataset> gCache;
  auto& ds = gCache[{kind, d}];
  if (ds.n < n) {
    FillDataset(ds, kind, d, n);
  }
  return ds;
}

const std::vector<float>& Rotation(uint32_t d) {
  static std::map<uint32_t, std::vector<float>> gCache;
  auto& rot = gCache[d];
  if (rot.empty()) {
    rot = irs::MakeRotation(d, kSeed);
  }
  return rot;
}

class TopK {
 public:
  explicit TopK(size_t k) : _k{std::max<size_t>(1, k)} { _heap.reserve(_k); }

  void Push(float s) {
    _best = std::max(_best, s);
    if (_heap.size() < _k) {
      _heap.push_back(s);
      std::push_heap(_heap.begin(), _heap.end(), std::greater{});
    } else if (s > _heap.front()) {
      std::pop_heap(_heap.begin(), _heap.end(), std::greater{});
      _heap.back() = s;
      std::push_heap(_heap.begin(), _heap.end(), std::greater{});
    }
  }

  bool Full() const noexcept { return _heap.size() >= _k; }
  float Worst() const noexcept { return _heap.front(); }
  float Best() const noexcept { return _best; }
  void Reset() noexcept { _heap.clear(); }

 private:
  size_t _k;
  std::vector<float> _heap;
  float _best = std::numeric_limits<float>::lowest();
};

template<irs::VectorMetric M>
constexpr faiss::MetricType kFaissMetric =
  M == irs::VectorMetric::L2Sqr ? faiss::METRIC_L2
                                : faiss::METRIC_INNER_PRODUCT;

template<irs::VectorMetric M>
using FaissC = std::conditional_t<kFaissMetric<M> == faiss::METRIC_L2,
                                  faiss::CMax<float, int64_t>,
                                  faiss::CMin<float, int64_t>>;

template<irs::VectorMetric M>
constexpr float NoPruneFaiss() noexcept {
  if constexpr (kFaissMetric<M> == faiss::METRIC_L2) {
    return std::numeric_limits<float>::max();
  } else {
    return std::numeric_limits<float>::lowest();
  }
}

template<irs::VectorMetric M>
float ScoreToFaiss(float score) noexcept {
  if constexpr (kFaissMetric<M> == faiss::METRIC_L2) {
    return std::nextafter(-score, std::numeric_limits<float>::max());
  } else {
    return std::nextafter(score, std::numeric_limits<float>::lowest());
  }
}

template<irs::VectorMetric M>
float FaissThreshold(const TopK& gate) noexcept {
  return gate.Full() ? ScoreToFaiss<M>(gate.Worst()) : NoPruneFaiss<M>();
}

template<irs::VectorMetric M>
float ScoreThreshold(const TopK& gate) noexcept {
  return gate.Full() ? gate.Worst() : std::numeric_limits<float>::lowest();
}

struct BatchDesc {
  size_t off;
  size_t len;
  size_t base;
};

struct LayoutA {
  std::vector<float> buf;
  std::vector<std::vector<BatchDesc>> nodes;
  size_t max_len = 0;
};

LayoutA BuildLayoutA(const float* rows, size_t nodes, size_t n, uint32_t d,
                     size_t bs, bool pad) {
  const uint32_t levels = PanoramaLevels(d);
  const size_t stride = size_t{d} + levels + 1;
  LayoutA out;
  out.nodes.resize(nodes);
  size_t batches = 0;
  for (size_t off = 0; off < n; off += bs) {
    ++batches;
  }
  out.buf.assign(nodes * batches * bs * stride, 0.f);
  std::vector<float> stage(bs * size_t{d}, 0.f);
  size_t cursor = 0;
  for (size_t node = 0; node < nodes; ++node) {
    const float* src = rows + node * n * d;
    for (size_t off = 0; off < n; off += bs) {
      const size_t live = std::min(bs, n - off);
      const size_t len = pad ? bs : live;
      faiss::Panorama pano{size_t{d} * sizeof(float), levels, len};
      std::fill(stage.begin(), stage.end(), 0.f);
      std::memcpy(stage.data(), src + off * d, live * size_t{d} * sizeof(float));
      float* cums = out.buf.data() + cursor;
      float* codes = cums + len * (levels + 1);
      pano.compute_cumulative_sums(cums, 0, len, stage.data());
      pano.copy_codes_to_level_layout(reinterpret_cast<uint8_t*>(codes), 0, len,
                                      reinterpret_cast<const uint8_t*>(
                                        stage.data()));
      out.nodes[node].push_back({cursor, len, off});
      out.max_len = std::max(out.max_len, len);
      cursor += len * stride;
    }
  }
  out.buf.resize(cursor);
  return out;
}

std::vector<float> BuildSuffixNorms(const float* rows, size_t n, uint32_t d) {
  const uint32_t levels = PanoramaLevels(d);
  std::vector<float> cums(n * (levels + 1), 0.f);
  const auto pano = MakePanorama(d, 1);
  pano.compute_cumulative_sums(cums.data(), 0, n, rows);
  return cums;
}

enum class Gate { None, Warm, Oracle };

class PanoramaFixture : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State& state) override {
    d = static_cast<uint32_t>(state.range(0));
    n = static_cast<size_t>(state.range(1));
    beam = static_cast<size_t>(state.range(3));
    kind = static_cast<int>(state.range(4));
    nodes = static_cast<size_t>(state.range(5));
    levels = PanoramaLevels(d);
    const auto arg_bs = static_cast<size_t>(state.range(2));
    bs = arg_bs != 0 ? arg_bs : std::max<size_t>(1, n);

    rot = &Rotation(d);
    rot_scratch.assign(d, 0.f);
    rot_cums.assign(levels + 1, 0.f);
    ds = &Data(kind, d, std::max<size_t>(1, nodes * n));
    query_cums.assign(levels + 1, 0.f);
    MakePanorama(d, bs).compute_query_cum_sums(ds->query.data(),
                                               query_cums.data());
    if (n == 0) {
      return;
    }

    layout = BuildLayoutA(ds->rows.data(), nodes, n, d, bs, /*pad=*/false);
    padded = n % bs != 0
               ? BuildLayoutA(ds->rows.data(), nodes, n, d, bs, /*pad=*/true)
               : LayoutA{};
    suffix = BuildSuffixNorms(ds->rows.data(), nodes * n, d);

    const size_t scratch =
      std::max({bs, layout.max_len, padded.max_len});
    active.assign(scratch, 0);
    byteset.assign(scratch, 0);
    exact.assign(scratch, 0.f);
    dots.assign(scratch, 0.f);

    oracle_l2 = OracleScore<irs::VectorMetric::L2Sqr>();
    oracle_ip = OracleScore<irs::VectorMetric::InnerProduct>();

    Warmup();
  }

  template<irs::VectorMetric M>
  float Oracle() const noexcept {
    return M == irs::VectorMetric::L2Sqr ? oracle_l2 : oracle_ip;
  }

  const LayoutA& Layout(bool pad) const noexcept {
    return pad && !padded.buf.empty() ? padded : layout;
  }

  uint32_t d = 0;
  uint32_t levels = 0;
  size_t n = 0;
  size_t bs = 1;
  size_t beam = 1;
  size_t nodes = 1;
  int kind = 0;
  const Dataset* ds = nullptr;
  const std::vector<float>* rot = nullptr;
  std::vector<float> rot_scratch;
  std::vector<float> rot_cums;
  std::vector<float> query_cums;
  std::vector<float> suffix;
  LayoutA layout;
  LayoutA padded;
  std::vector<uint32_t> active;
  std::vector<uint8_t> byteset;
  std::vector<float> exact;
  std::vector<float> dots;
  float oracle_l2 = 0.f;
  float oracle_ip = 0.f;

 private:
  template<irs::VectorMetric M>
  float OracleScore() const {
    std::vector<float> all(n);
    const auto width = static_cast<uint16_t>(d);
    for (size_t i = 0; i < n; ++i) {
      all[i] = irs::ComputeDistance<M>(ds->query.data(),
                                       ds->rows.data() + i * d, width);
    }
    const size_t k = std::min<size_t>(std::max<size_t>(1, beam), n);
    std::ranges::nth_element(all, all.begin() + (k - 1), std::greater{});
    return all[k - 1];
  }

  void Warmup() {
    const auto width = static_cast<uint16_t>(d);
    float acc = 0.f;
    for (size_t i = 0; i < nodes * n; ++i) {
      acc += irs::ComputeDistance<irs::VectorMetric::L2Sqr>(
        ds->query.data(), ds->rows.data() + i * d, width);
    }
    benchmark::DoNotOptimize(acc);
    RotateQuery(reinterpret_cast<const irs::byte_type*>(rot->data()),
                ds->query.data(), rot_scratch.data(), d);
    benchmark::DoNotOptimize(rot_scratch.data());
    benchmark::DoNotOptimize(suffix.data());
    benchmark::DoNotOptimize(layout.buf.data());
  }
};

void ReportScan(benchmark::State& state, const PanoramaFixture& fx,
                uint64_t total, uint64_t scanned, uint64_t survivors,
                float best) {
  state.counters["dims_frac"] =
    static_cast<double>(scanned) / static_cast<double>(std::max<uint64_t>(1, total));
  state.counters["vec_s"] = benchmark::Counter(
    static_cast<double>(fx.nodes * fx.n),
    benchmark::Counter::kIsIterationInvariantRate);
  state.counters["survivors"] =
    static_cast<double>(survivors) /
    static_cast<double>(std::max<uint64_t>(1, state.iterations() * fx.nodes * fx.n));
  state.counters["levels"] = fx.levels;
  state.counters["best"] = best;
}

template<irs::VectorMetric M, bool WithGate>
void RunScalar(benchmark::State& state, PanoramaFixture& fx) {
  const auto width = static_cast<uint16_t>(fx.d);
  const float* q = fx.ds->query.data();
  TopK gate{std::min<size_t>(fx.n, fx.beam)};
  for (auto _ : state) {
    gate.Reset();
    for (size_t node = 0; node < fx.nodes; ++node) {
      const float* rows = fx.ds->rows.data() + node * fx.n * fx.d;
      for (size_t i = 0; i < fx.n; ++i) {
        const float s = irs::ComputeDistance<M>(q, rows + i * fx.d, width);
        if constexpr (WithGate) {
          gate.Push(s);
        } else {
          benchmark::DoNotOptimize(s);
        }
      }
    }
    float best = gate.Best();
    benchmark::DoNotOptimize(best);
    benchmark::ClobberMemory();
  }
  const uint64_t all = state.iterations() * fx.nodes * fx.n * fx.levels;
  ReportScan(state, fx, all, all, state.iterations() * fx.nodes * fx.n,
             gate.Best());
}

template<irs::VectorMetric M, Gate G, bool Padded, bool Rotate, bool Carry>
void RunPanoA(benchmark::State& state, PanoramaFixture& fx) {
  const auto& lay = fx.Layout(Padded);
  const size_t code_size = size_t{fx.d} * sizeof(float);
  const auto* rot =
    reinterpret_cast<const irs::byte_type*>(fx.rot->data());
  const auto rot_pano = MakePanorama(fx.d, fx.bs);
  TopK gate{std::min<size_t>(fx.n, fx.beam)};
  faiss::PanoramaStats stats;
  uint64_t survivors = 0;
  for (auto _ : state) {
    if constexpr (Rotate) {
      RotateQuery(rot, fx.ds->query.data(), fx.rot_scratch.data(), fx.d);
      rot_pano.compute_query_cum_sums(fx.rot_scratch.data(),
                                      fx.rot_cums.data());
      benchmark::DoNotOptimize(fx.rot_scratch.data());
      benchmark::DoNotOptimize(fx.rot_cums.data());
    }
    gate.Reset();
    for (size_t node = 0; node < fx.nodes; ++node) {
      if constexpr (!Carry) {
        gate.Reset();
      }
      for (const auto& bd : lay.nodes[node]) {
        float threshold;
        if constexpr (G == Gate::None) {
          threshold = NoPruneFaiss<M>();
        } else if constexpr (G == Gate::Oracle) {
          threshold = ScoreToFaiss<M>(fx.Oracle<M>());
        } else {
          threshold = FaissThreshold<M>(gate);
        }
        const faiss::Panorama pano{code_size, fx.levels, bd.len};
        const float* cums = lay.buf.data() + bd.off;
        const auto* codes = reinterpret_cast<const uint8_t*>(
          cums + bd.len * (fx.levels + 1));
        const size_t alive =
          pano.template progressive_filter_batch<FaissC<M>, kFaissMetric<M>>(
            codes, cums, fx.ds->query.data(), fx.query_cums.data(), 0, bd.len,
            /*sel=*/nullptr, /*ids=*/nullptr, /*use_sel=*/false, fx.active,
            fx.byteset, fx.exact, fx.dots, threshold, stats);
        for (size_t i = 0; i < alive; ++i) {
          const uint32_t idx = fx.active[i];
          if (bd.base + idx >= fx.n) {
            continue;
          }
          gate.Push(kFaissMetric<M> == faiss::METRIC_L2 ? -fx.exact[idx]
                                                        : fx.exact[idx]);
          ++survivors;
        }
        benchmark::DoNotOptimize(alive);
      }
    }
    float best = gate.Best();
    benchmark::DoNotOptimize(best);
    benchmark::ClobberMemory();
  }
  ReportScan(state, fx, stats.total_dims, stats.total_dims_scanned, survivors,
             gate.Best());
}

template<irs::VectorMetric M, Gate G, bool Rotate, bool Carry>
void RunPanoB(benchmark::State& state, PanoramaFixture& fx) {
  const uint32_t levels = fx.levels;
  const uint32_t width = MakePanorama(fx.d, 1).level_width_floats;
  const float* q = fx.ds->query.data();
  const float* qc = fx.query_cums.data();
  const auto* rot =
    reinterpret_cast<const irs::byte_type*>(fx.rot->data());
  const auto rot_pano = MakePanorama(fx.d, fx.bs);
  TopK gate{std::min<size_t>(fx.n, fx.beam)};
  uint64_t scanned = 0;
  uint64_t survivors = 0;
  for (auto _ : state) {
    if constexpr (Rotate) {
      RotateQuery(rot, q, fx.rot_scratch.data(), fx.d);
      rot_pano.compute_query_cum_sums(fx.rot_scratch.data(),
                                      fx.rot_cums.data());
      benchmark::DoNotOptimize(fx.rot_scratch.data());
      benchmark::DoNotOptimize(fx.rot_cums.data());
    }
    gate.Reset();
    for (size_t node = 0; node < fx.nodes; ++node) {
      if constexpr (!Carry) {
        gate.Reset();
      }
      const float* rows = fx.ds->rows.data() + node * fx.n * fx.d;
      const float* cums = fx.suffix.data() + node * fx.n * (levels + 1);
      for (size_t i = 0; i < fx.n; ++i) {
        float threshold;
        if constexpr (G == Gate::None) {
          threshold = std::numeric_limits<float>::lowest();
        } else if constexpr (G == Gate::Oracle) {
          threshold = fx.Oracle<M>();
        } else {
          threshold = ScoreThreshold<M>(gate);
        }
        const float* x = rows + i * fx.d;
        const float* xc = cums + i * (levels + 1);
        float acc = 0.f;
        bool pruned = false;
        for (uint32_t l = 0; l < levels; ++l) {
          const uint32_t lo = l * width;
          const uint32_t hi = std::min<uint32_t>(lo + width, fx.d);
          ++scanned;
          if constexpr (M == irs::VectorMetric::L2Sqr) {
            for (uint32_t j = lo; j < hi; ++j) {
              const float t = q[j] - x[j];
              acc += t * t;
            }
            const float delta = qc[l + 1] - xc[l + 1];
            if (-acc - delta * delta < threshold) {
              pruned = true;
              break;
            }
          } else {
            for (uint32_t j = lo; j < hi; ++j) {
              acc += q[j] * x[j];
            }
            if (acc + qc[l + 1] * xc[l + 1] < threshold) {
              pruned = true;
              break;
            }
          }
        }
        if (!pruned) {
          gate.Push(M == irs::VectorMetric::L2Sqr ? -acc : acc);
          ++survivors;
        }
      }
    }
    float best = gate.Best();
    benchmark::DoNotOptimize(best);
    benchmark::ClobberMemory();
  }
  ReportScan(state, fx, state.iterations() * fx.nodes * fx.n * levels, scanned,
             survivors, gate.Best());
}

// Design C: the level-major layout scored densely, with none of the active-set
// machinery -- no prune_kernel, no byteset, no compaction. A full distance is
// n_levels contiguous dot products, so this is the cheapest correct way to scan a
// node too small for pruning to pay, and it reuses faiss's own dot kernel so the
// accumulation is bit-identical to progressive_filter_batch.
template<irs::VectorMetric M, bool Rotate>
void RunPanoC(benchmark::State& state, PanoramaFixture& fx) {
  const auto& lay = fx.Layout(false);
  const auto ref = MakePanorama(fx.d, fx.bs);
  const size_t lw = ref.level_width_floats;
  const auto* rot = reinterpret_cast<const irs::byte_type*>(fx.rot->data());
  TopK gate{std::min<size_t>(fx.n, fx.beam)};
  uint64_t survivors = 0;
  for (auto _ : state) {
    if constexpr (Rotate) {
      RotateQuery(rot, fx.ds->query.data(), fx.rot_scratch.data(), fx.d);
      ref.compute_query_cum_sums(fx.rot_scratch.data(), fx.rot_cums.data());
      benchmark::DoNotOptimize(fx.rot_scratch.data());
      benchmark::DoNotOptimize(fx.rot_cums.data());
    }
    gate.Reset();
    const float* q = fx.ds->query.data();
    const float q_norm = fx.query_cums[0] * fx.query_cums[0];
    for (size_t node = 0; node < fx.nodes; ++node) {
      for (const auto& bd : lay.nodes[node]) {
        const float* cums = lay.buf.data() + bd.off;
        const float* codes = cums + bd.len * (fx.levels + 1);
        for (size_t i = 0; i < bd.len; ++i) {
          fx.exact[i] = M == irs::VectorMetric::L2Sqr
                          ? cums[i] * cums[i] + q_norm
                          : 0.f;
        }
        for (size_t l = 0; l < fx.levels; ++l) {
          const size_t w = std::min(lw, size_t{fx.d} - l * lw);
          faiss::with_level_width(w, [&]<size_t LevelWidth>() {
            faiss::compute_level_dot_kernel<true, LevelWidth>(
              q + l * lw, codes + l * lw * bd.len, nullptr, bd.len, w,
              fx.dots.data());
          });
          for (size_t i = 0; i < bd.len; ++i) {
            fx.exact[i] += M == irs::VectorMetric::L2Sqr ? -2.f * fx.dots[i]
                                                         : fx.dots[i];
          }
        }
        for (size_t i = 0; i < bd.len; ++i) {
          if (bd.base + i >= fx.n) {
            continue;
          }
          gate.Push(M == irs::VectorMetric::L2Sqr ? -fx.exact[i] : fx.exact[i]);
          ++survivors;
        }
      }
    }
    float best = gate.Best();
    benchmark::DoNotOptimize(best);
    benchmark::ClobberMemory();
  }
  const uint64_t all = state.iterations() * fx.nodes * fx.n * fx.levels;
  ReportScan(state, fx, all, all, survivors, gate.Best());
}

void RunRotate(benchmark::State& state, PanoramaFixture& fx, bool with_cums) {
  const auto* rot =
    reinterpret_cast<const irs::byte_type*>(fx.rot->data());
  const float* q = fx.ds->query.data();
  const auto pano = MakePanorama(fx.d, faiss::Panorama::kDefaultBatchSize);
  for (auto _ : state) {
    RotateQuery(rot, q, fx.rot_scratch.data(), fx.d);
    if (with_cums) {
      pano.compute_query_cum_sums(fx.rot_scratch.data(), fx.rot_cums.data());
      benchmark::DoNotOptimize(fx.rot_cums.data());
    }
    benchmark::DoNotOptimize(fx.rot_scratch.data());
    benchmark::ClobberMemory();
  }
  state.counters["d2"] = static_cast<double>(size_t{fx.d} * fx.d);
  state.counters["levels"] = fx.levels;
}

#define PANO_SCALAR(Name, WithGate)                                      \
  BENCHMARK_DEFINE_F(PanoramaFixture, L2_##Name)(benchmark::State& s) {    \
    RunScalar<irs::VectorMetric::L2Sqr, WithGate>(s, *this);               \
  }                                                                        \
  BENCHMARK_DEFINE_F(PanoramaFixture, IP_##Name)(benchmark::State& s) {    \
    RunScalar<irs::VectorMetric::InnerProduct, WithGate>(s, *this);        \
  }

#define PANO_A(Name, G, Padded, Rotate, Carry)                             \
  BENCHMARK_DEFINE_F(PanoramaFixture, L2_##Name)(benchmark::State& s) {    \
    RunPanoA<irs::VectorMetric::L2Sqr, G, Padded, Rotate, Carry>(s, *this); \
  }                                                                        \
  BENCHMARK_DEFINE_F(PanoramaFixture, IP_##Name)(benchmark::State& s) {    \
    RunPanoA<irs::VectorMetric::InnerProduct, G, Padded, Rotate, Carry>(   \
      s, *this);                                                           \
  }

#define PANO_B(Name, G, Rotate, Carry)                                     \
  BENCHMARK_DEFINE_F(PanoramaFixture, L2_##Name)(benchmark::State& s) {    \
    RunPanoB<irs::VectorMetric::L2Sqr, G, Rotate, Carry>(s, *this);        \
  }                                                                        \
  BENCHMARK_DEFINE_F(PanoramaFixture, IP_##Name)(benchmark::State& s) {    \
    RunPanoB<irs::VectorMetric::InnerProduct, G, Rotate, Carry>(s, *this); \
  }

#define PANO_C(Name, Rotate)                                            \
  BENCHMARK_DEFINE_F(PanoramaFixture, L2_##Name)(benchmark::State& s) {  \
    RunPanoC<irs::VectorMetric::L2Sqr, Rotate>(s, *this);                \
  }                                                                     \
  BENCHMARK_DEFINE_F(PanoramaFixture, IP_##Name)(benchmark::State& s) {  \
    RunPanoC<irs::VectorMetric::InnerProduct, Rotate>(s, *this);         \
  }

PANO_SCALAR(ScalarNoGate, false)
PANO_SCALAR(ScalarGate, true)
PANO_A(PanoA_NoGate, Gate::None, false, false, true)
PANO_A(PanoA_Warm, Gate::Warm, false, false, true)
PANO_A(PanoA_Oracle, Gate::Oracle, false, false, true)
PANO_A(PanoA_WarmPadded, Gate::Warm, true, false, true)
PANO_A(PanoA_WarmRotated, Gate::Warm, false, true, true)
PANO_B(PanoB_NoGate, Gate::None, false, true)
PANO_B(PanoB_Warm, Gate::Warm, false, true)
PANO_B(PanoB_WarmRotated, Gate::Warm, true, true)
PANO_C(PanoC_Dense, false)
PANO_C(TreeC, true)

PANO_SCALAR(TreeScalar, true)
PANO_A(TreeA, Gate::Warm, false, true, false)
PANO_A(TreeA_Carry, Gate::Warm, false, true, true)
PANO_B(TreeB, Gate::Warm, true, false)
PANO_B(TreeB_Carry, Gate::Warm, true, true)

BENCHMARK_DEFINE_F(PanoramaFixture, QueryRotate)(benchmark::State& state) {
  RunRotate(state, *this, /*with_cums=*/false);
}
BENCHMARK_DEFINE_F(PanoramaFixture, QueryPrep)(benchmark::State& state) {
  RunRotate(state, *this, /*with_cums=*/true);
}

constexpr int64_t kDims[] = {64, 128, 256, 512, 768, 1024, 1536, 2048};

void RotGrid(benchmark::internal::Benchmark* b) {
  for (const int64_t d : kDims) {
    b->Args({d, 0, 0, 0, 0, 1});
  }
}

void CoreGrid(benchmark::internal::Benchmark* b) {
  for (const int64_t d : kDims) {
    for (const int64_t n : {2, 8, 32, 64, 128, 256, 1024}) {
      b->Args({d, n, 128, 32, 0, 1});
    }
  }
}

void BigNGrid(benchmark::internal::Benchmark* b) {
  for (const int64_t d : {128, 768}) {
    for (const int64_t n : {4096, 16384}) {
      b->Args({d, n, 128, 32, 0, 1});
    }
  }
}

// A wide tree: max_fanout raised until a layer is one enormous node, so the whole
// scan is one node's batches and the gate tightens across them. beam is nprobe
// here, and the nprobe/n ratio -- not n alone -- is what sets pruning power.
void WideGrid(benchmark::internal::Benchmark* b) {
  for (const int64_t d : {128, 768, 1536}) {
    for (const int64_t n : {1024, 4096, 16384, 65536}) {
      for (const int64_t beam : {8, 64}) {
        b->Args({d, n, 128, beam, 0, 1});
      }
    }
  }
}

void BatchGrid(benchmark::internal::Benchmark* b) {
  for (const int64_t d : {128, 768, 1536}) {
    for (const int64_t n : {128, 512, 1024, 4096}) {
      for (const int64_t bs : {32, 128, 512, 0}) {
        if (bs >= n) {
          continue;
        }
        b->Args({d, n, bs, 32, 0, 1});
      }
    }
  }
}

void BeamGrid(benchmark::internal::Benchmark* b) {
  for (const int64_t n : {256, 1024, 4096}) {
    for (const int64_t beam : {1, 8, 32, 128}) {
      b->Args({768, n, 128, beam, 0, 1});
    }
  }
}

void DataGrid(benchmark::internal::Benchmark* b) {
  for (const int64_t d : {64, 128, 256}) {
    for (const int64_t n : {128, 1024}) {
      for (const int64_t kind : {0, 1, 2, 3, 4}) {
        b->Args({d, n, 128, 32, kind, 1});
      }
    }
  }
}

void RawGrid(benchmark::internal::Benchmark* b) {
  for (const int64_t d : {64, 128, 256, 768, 1536}) {
    for (const int64_t n : {128, 1024}) {
      b->Args({d, n, 128, 32, 4, 1});
    }
  }
}

void TreeGrid(benchmark::internal::Benchmark* b) {
  for (const int64_t d : {128, 768, 1536}) {
    for (const int64_t nodes : {1, 8, 64}) {
      for (const int64_t n : {16, 128, 256}) {
        b->Args({d, n, 0, 32, 0, nodes});
      }
    }
    b->Args({d, 976, 0, 32, 0, 1});
    b->Args({d, 99, 0, 32, 0, 13});
  }
}

#define PANO_REGISTER(Method, Grid)               \
  BENCHMARK_REGISTER_F(PanoramaFixture, Method)   \
    ->Apply(Grid)                                 \
    ->ArgNames({"d", "n", "bs", "beam", "data", "nodes"}) \
    ->Unit(benchmark::kNanosecond)                \
    ->MinTime(0.1)

#define PANO_REGISTER_BOTH(Method, Grid) \
  PANO_REGISTER(L2_##Method, Grid);      \
  PANO_REGISTER(IP_##Method, Grid)

#define PANO_REGISTER_SCAN(Grid)              \
  PANO_REGISTER_BOTH(ScalarNoGate, Grid);     \
  PANO_REGISTER_BOTH(ScalarGate, Grid);       \
  PANO_REGISTER_BOTH(PanoA_NoGate, Grid);     \
  PANO_REGISTER_BOTH(PanoA_Warm, Grid);       \
  PANO_REGISTER_BOTH(PanoA_WarmRotated, Grid); \
  PANO_REGISTER_BOTH(PanoB_Warm, Grid);       \
  PANO_REGISTER_BOTH(PanoB_WarmRotated, Grid)

PANO_REGISTER_SCAN(CoreGrid);
PANO_REGISTER_BOTH(PanoA_Oracle, CoreGrid);
PANO_REGISTER_BOTH(PanoA_WarmPadded, CoreGrid);
PANO_REGISTER_BOTH(PanoB_NoGate, CoreGrid);
PANO_REGISTER_BOTH(PanoC_Dense, CoreGrid);

PANO_REGISTER_BOTH(ScalarGate, BigNGrid);
PANO_REGISTER_BOTH(PanoA_Warm, BigNGrid);
PANO_REGISTER_BOTH(PanoA_NoGate, BigNGrid);
PANO_REGISTER_BOTH(PanoB_Warm, BigNGrid);

PANO_REGISTER_BOTH(ScalarGate, WideGrid);
PANO_REGISTER_BOTH(PanoA_Warm, WideGrid);
PANO_REGISTER_BOTH(PanoA_WarmRotated, WideGrid);
PANO_REGISTER_BOTH(PanoC_Dense, WideGrid);

PANO_REGISTER_BOTH(PanoA_NoGate, BatchGrid);
PANO_REGISTER_BOTH(PanoA_Warm, BatchGrid);

PANO_REGISTER_BOTH(PanoA_Warm, BeamGrid);
PANO_REGISTER_BOTH(PanoB_Warm, BeamGrid);

PANO_REGISTER_BOTH(ScalarGate, DataGrid);
PANO_REGISTER_BOTH(PanoA_NoGate, DataGrid);
PANO_REGISTER_BOTH(PanoA_Warm, DataGrid);
PANO_REGISTER_BOTH(PanoB_Warm, DataGrid);

PANO_REGISTER_BOTH(ScalarGate, RawGrid);
PANO_REGISTER_BOTH(PanoA_Warm, RawGrid);
PANO_REGISTER_BOTH(PanoB_Warm, RawGrid);

PANO_REGISTER_BOTH(TreeScalar, TreeGrid);
PANO_REGISTER_BOTH(TreeA, TreeGrid);
PANO_REGISTER_BOTH(TreeA_Carry, TreeGrid);
PANO_REGISTER_BOTH(TreeB, TreeGrid);
PANO_REGISTER_BOTH(TreeC, TreeGrid);
PANO_REGISTER_BOTH(TreeB_Carry, TreeGrid);

PANO_REGISTER(QueryRotate, RotGrid);
PANO_REGISTER(QueryPrep, RotGrid);

}  // namespace

BENCHMARK_MAIN();