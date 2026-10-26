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

#include <absl/random/distributions.h>

#include <limits>
#include <pcg_random.hpp>

#include "iresearch/formats/ivf/quantizers/common.hpp"

namespace irs::quantizers {
namespace {

struct BqStatsHeader {
  uint8_t layout;
  uint8_t bits;
  uint8_t half;
  uint8_t query_bits;
  uint8_t normalized;
  uint8_t reserved[3];
  uint32_t d;
};

static_assert(sizeof(BqStatsHeader) == 12);
static_assert(std::has_unique_object_representations_v<BqStatsHeader>);

constexpr uint8_t kBqLayout = 1;
constexpr float kBqSigmas = 2.f / 3.f;
constexpr float kBqOneBitMagnitude = 0.79788456f;
constexpr float kBqTwoBitMagnitude = 1.2651580f;
constexpr double kBqTwoCellSpread = 0.14269;
constexpr double kBqThreeCellSpread = 0.031738;

constexpr size_t kBqCut0 = 0;
constexpr size_t kBqCut1 = 1;
constexpr size_t kBqBase = 2;
constexpr size_t kBqStep0 = 3;
constexpr size_t kBqStep1 = 4;
constexpr size_t kBqJoint = 5;
constexpr size_t kBqModelFloats = 6;
constexpr size_t kBqCells = 4;
constexpr size_t kBqSampleRows = 16384;
constexpr size_t kBqSampleFloats = size_t{1} << 24;
constexpr uint32_t kBqLloydIters = 64;
constexpr int kBqQueryTop = 127;
constexpr uint32_t kBqQueryBins = 2 * kBqQueryTop + 1;
constexpr uint32_t kBqMaxQueryLevels = 16;
constexpr uint64_t kBqSampleSeed = 0x6271b5a3c0ffee11ULL;
constexpr double kBqUnitNormSlack = 1e-3;

struct BqLayout {
  uint32_t d = 0;
  uint32_t bits = 1;
  bool half = false;
  uint32_t plane0_bytes = 0;
  uint32_t plane1_bytes = 0;
  uint32_t record_size = 0;
};

BqLayout MakeBqLayout(uint32_t d, uint32_t bits, bool half) noexcept {
  BqLayout l;
  l.d = d;
  l.bits = bits;
  l.half = half;
  l.plane0_bytes = (d + 7) / 8;
  if (half) {
    l.plane1_bytes = ((d + 1) / 2 + 7) / 8;
  } else if (bits == 2) {
    l.plane1_bytes = l.plane0_bytes;
  }
  l.record_size = l.plane0_bytes + l.plane1_bytes;
  return l;
}

uint32_t BqPlane1Index(const BqLayout& lay, uint32_t i) noexcept {
  return lay.half ? i / 2 : i;
}

void BqEncode(const BqLayout& lay, const float* model, const float* x,
              uint8_t* out) noexcept {
  const float* cut0 = model + kBqCut0 * lay.d;
  const float* cut1 = model + kBqCut1 * lay.d;
  std::fill_n(out, lay.record_size, uint8_t{0});
  uint8_t* plane1 = out + lay.plane0_bytes;
  const bool two_planes = lay.plane1_bytes != 0;
  for (uint32_t i = 0; i < lay.d; ++i) {
    if (x[i] > cut0[i]) {
      out[i / 8] = static_cast<uint8_t>(out[i / 8] | (1U << (i % 8)));
    }
    if (two_planes && x[i] >= cut1[i]) {
      const uint32_t j = BqPlane1Index(lay, i);
      plane1[j / 8] = static_cast<uint8_t>(plane1[j / 8] | (1U << (j % 8)));
    }
  }
}

void BqEncodeSigns(const BqLayout& lay, const float* model, const float* q,
                   uint8_t* out) noexcept {
  const float* step0 = model + kBqStep0 * lay.d;
  const float* step1 = model + kBqStep1 * lay.d;
  std::fill_n(out, lay.record_size, uint8_t{0});
  uint8_t* plane1 = out + lay.plane0_bytes;
  float w1 = 0.f;
  for (uint32_t i = 0; i < lay.d; ++i) {
    if (q[i] * step0[i] > 0.f) {
      out[i / 8] = static_cast<uint8_t>(out[i / 8] | (1U << (i % 8)));
    }
    if (lay.plane1_bytes == 0) {
      continue;
    }
    w1 += q[i] * step1[i];
    if (!lay.half || i % 2 == 1 || i + 1 == lay.d) {
      const uint32_t j = BqPlane1Index(lay, i);
      if (w1 > 0.f) {
        plane1[j / 8] = static_cast<uint8_t>(plane1[j / 8] | (1U << (j % 8)));
      }
      w1 = 0.f;
    }
  }
}

uint32_t BqCell(const BqLayout& lay, const uint8_t* code, uint32_t i) noexcept {
  uint32_t cell = BqBit(code, i) ? 1U : 0U;
  if (lay.plane1_bytes != 0 &&
      BqBit(code + lay.plane0_bytes, BqPlane1Index(lay, i))) {
    cell |= 2U;
  }
  return cell;
}

void BqGaussianModel(const BqLayout& lay, const float* mean, const float* sd,
                     float* model) noexcept {
  const size_t d = lay.d;
  float* cut0 = model + kBqCut0 * d;
  float* cut1 = model + kBqCut1 * d;
  float* base = model + kBqBase * d;
  float* step0 = model + kBqStep0 * d;
  float* step1 = model + kBqStep1 * d;
  float* joint = model + kBqJoint * d;
  const bool one_bit = lay.plane1_bytes == 0;
  for (size_t i = 0; i < d; ++i) {
    const bool spread = sd[i] > std::numeric_limits<float>::epsilon();
    cut0[i] = one_bit || !spread ? mean[i] : mean[i] - kBqSigmas * sd[i];
    cut1[i] = spread ? mean[i] + kBqSigmas * sd[i]
                     : std::numeric_limits<float>::infinity();
    const float mag =
      (one_bit ? kBqOneBitMagnitude : kBqTwoBitMagnitude) * sd[i];
    base[i] = mean[i] - mag;
    step0[i] = one_bit ? 2.f * mag : mag;
    step1[i] = one_bit ? 0.f : mag;
    joint[i] = 1.f;
  }
}

struct BqLloyd {
  std::array<float, 2> cuts{};
  std::array<float, 3> means{};
};

BqLloyd BqLloydCuts(std::vector<float>& col, uint32_t levels) {
  std::sort(col.begin(), col.end());
  const size_t n = col.size();
  std::vector<double> prefix(n + 1, 0.0);
  for (size_t i = 0; i < n; ++i) {
    prefix[i + 1] = prefix[i] + col[i];
  }
  std::array<size_t, 4> edge{};
  for (uint32_t k = 0; k <= levels; ++k) {
    edge[k] = n * k / levels;
  }
  std::array<double, 3> mean{};
  for (uint32_t k = 0; k < levels; ++k) {
    mean[k] = col[std::min(edge[k], n - 1)];
  }
  BqLloyd out;
  for (uint32_t it = 0; it < kBqLloydIters; ++it) {
    for (uint32_t k = 0; k < levels; ++k) {
      if (edge[k + 1] > edge[k]) {
        mean[k] = (prefix[edge[k + 1]] - prefix[edge[k]]) /
                  static_cast<double>(edge[k + 1] - edge[k]);
      }
    }
    bool moved = false;
    for (uint32_t k = 1; k < levels; ++k) {
      const auto cut = static_cast<float>((mean[k - 1] + mean[k]) / 2.0);
      out.cuts[k - 1] = cut;
      const auto at = k == 1 ? std::upper_bound(col.begin(), col.end(), cut)
                             : std::lower_bound(col.begin(), col.end(), cut);
      const auto e = static_cast<size_t>(at - col.begin());
      moved |= e != edge[k];
      edge[k] = e;
    }
    if (!moved && it != 0) {
      break;
    }
  }
  for (uint32_t k = 0; k < levels; ++k) {
    out.means[k] = static_cast<float>(mean[k]);
  }
  return out;
}

void BqFitQueryLevels(uint32_t levels, std::vector<int8_t>& q0,
                      std::vector<int8_t>& q1) noexcept {
  SDB_ASSERT(levels >= 2 && levels <= kBqMaxQueryLevels);
  std::array<uint32_t, kBqQueryBins> count{};
  for (const int8_t v : q0) {
    ++count[v + kBqQueryTop];
  }
  for (const int8_t v : q1) {
    ++count[v + kBqQueryTop];
  }
  std::array<double, kBqQueryBins + 1> mass{};
  for (uint32_t bin = 0; bin < kBqQueryBins; ++bin) {
    mass[bin + 1] = mass[bin] + std::cbrt(static_cast<double>(count[bin]));
  }
  std::array<double, kBqMaxQueryLevels> level{};
  for (uint32_t c = 0, bin = 0; c < levels; ++c) {
    const double t = (c + 0.5) / levels * mass[kBqQueryBins];
    while (bin + 1 < kBqQueryBins && mass[bin + 1] < t) {
      ++bin;
    }
    level[c] = static_cast<double>(bin) - kBqQueryTop;
    if (c != 0) {
      level[c] = std::max(level[c], level[c - 1] + 1.0);
    }
  }
  std::array<uint8_t, kBqQueryBins> nearest{};
  for (uint32_t it = 0; it < kBqLloydIters; ++it) {
    std::array<double, kBqMaxQueryLevels> sum{};
    std::array<double, kBqMaxQueryLevels> weight{};
    for (uint32_t bin = 0, c = 0; bin < kBqQueryBins; ++bin) {
      const double v = static_cast<double>(bin) - kBqQueryTop;
      while (c + 1 < levels && level[c + 1] - v < v - level[c]) {
        ++c;
      }
      nearest[bin] = static_cast<uint8_t>(c);
      sum[c] += count[bin] * v;
      weight[c] += count[bin];
    }
    bool moved = false;
    for (uint32_t c = 0; c < levels; ++c) {
      if (weight[c] > 0.0 && sum[c] / weight[c] != level[c]) {
        level[c] = sum[c] / weight[c];
        moved = true;
      }
    }
    if (!moved) {
      break;
    }
  }
  std::array<int8_t, kBqQueryBins> snap{};
  for (uint32_t bin = 0; bin < kBqQueryBins; ++bin) {
    snap[bin] = static_cast<int8_t>(std::lround(level[nearest[bin]]));
  }
  for (auto& v : q0) {
    v = snap[v + kBqQueryTop];
  }
  for (auto& v : q1) {
    v = snap[v + kBqQueryTop];
  }
}

bool BqSolveNormal3(std::array<std::array<double, 4>, 3>& g,
                    double scale) noexcept {
  for (size_t c = 0; c < 3; ++c) {
    size_t pivot = c;
    for (size_t r = c + 1; r < 3; ++r) {
      if (std::fabs(g[r][c]) > std::fabs(g[pivot][c])) {
        pivot = r;
      }
    }
    if (std::fabs(g[pivot][c]) <= 1e-9 * scale) {
      return false;
    }
    std::swap(g[c], g[pivot]);
    for (size_t r = 0; r < 3; ++r) {
      if (r == c) {
        continue;
      }
      const double f = g[r][c] / g[c][c];
      for (size_t k = c; k < 4; ++k) {
        g[r][k] -= f * g[c][k];
      }
    }
  }
  return true;
}

void BqFitLevels(const BqLayout& lay, const double* count, const double* sum,
                 float* model) noexcept {
  const size_t d = lay.d;
  float* base = model + kBqBase * d;
  float* step0 = model + kBqStep0 * d;
  float* step1 = model + kBqStep1 * d;
  float* joint = model + kBqJoint * d;
  for (size_t i = 0; i < d; ++i) {
    const double* n = count + kBqCells * i;
    const double* s = sum + kBqCells * i;
    const double total = n[0] + n[1] + n[2] + n[3];
    if (total == 0) {
      continue;
    }
    const double on0 = n[1] + n[3];
    const double on1 = n[2] + n[3];
    const double all = s[0] + s[1] + s[2] + s[3];
    const double sum0 = s[1] + s[3];
    const double sum1 = s[2] + s[3];
    joint[i] = on1 > 0 ? static_cast<float>(n[3] / on1) : 1.f;
    const double ridge = 1e-6 * total;
    std::array<std::array<double, 4>, 3> g{
      {{total + ridge, on0, on1, all + ridge * base[i]},
       {on0, on0 + ridge, n[3], sum0 + ridge * step0[i]},
       {on1, n[3], on1 + ridge, sum1 + ridge * step1[i]}}};
    if (BqSolveNormal3(g, total)) {
      base[i] = static_cast<float>(g[0][3] / g[0][0]);
      step0[i] = static_cast<float>(g[1][3] / g[1][1]);
      step1[i] = static_cast<float>(g[2][3] / g[2][2]);
    }
  }
}

template<VectorMetric M>
class BinaryQuantizerWriter final : public QuantizerWriter {
 public:
  BinaryQuantizerWriter(uint32_t d, uint32_t bits, bool half,
                        uint32_t query_bits)
    : _lay{MakeBqLayout(d, bits, half)},
      _query_bits{query_bits == 0 ? kBQDefaultQueryBits : query_bits},
      _model(kBqModelFloats * size_t{d}, 0.f),
      _sum(d, 0.0),
      _sum2(d, 0.0) {}

  size_t TrainSamples(size_t /*rows*/) const noexcept final {
    return kTrainStreaming;
  }

  void Train(const float* vecs, size_t n) final {
    const size_t d = _lay.d;
    const size_t cap = SampleCap();
    for (size_t r = 0; r < n; ++r) {
      const float* x = vecs + r * d;
      double norm2 = 0.0;
      for (size_t i = 0; i < d; ++i) {
        _sum[i] += x[i];
        _sum2[i] += double{x[i]} * x[i];
        norm2 += double{x[i]} * x[i];
      }
      _normalized = _normalized && std::fabs(norm2 - 1.0) <= kBqUnitNormSlack;
      if (_seen < cap) {
        _sample.insert(_sample.end(), x, x + d);
      } else if (const uint64_t slot =
                   absl::Uniform<uint64_t>(_rng, 0, _seen + 1);
                 slot < cap) {
        std::copy_n(x, d, _sample.data() + slot * d);
      }
      ++_seen;
    }
    _count += n;
    _ready = false;
  }

  size_t RefineSamples(size_t rows) const noexcept final { return rows; }

  void Refine(const float* vecs, size_t n) final {
    Finalize();
    const size_t d = _lay.d;
    if (_cell_count.empty()) {
      _cell_count.assign(kBqCells * d, 0.0);
      _cell_sum.assign(kBqCells * d, 0.0);
    }
    _row.resize(_lay.record_size);
    for (size_t r = 0; r < n; ++r) {
      const float* x = vecs + r * d;
      BqEncode(_lay, _model.data(), x, _row.data());
      for (uint32_t i = 0; i < _lay.d; ++i) {
        const size_t cell = kBqCells * i + BqCell(_lay, _row.data(), i);
        _cell_count[cell] += 1.0;
        _cell_sum[cell] += x[i];
      }
    }
  }

  void RefineDone() final {
    if (!_cell_count.empty()) {
      BqFitLevels(_lay, _cell_count.data(), _cell_sum.data(), _model.data());
    }
    _cell_count.clear();
    _cell_count.shrink_to_fit();
    _cell_sum.clear();
    _cell_sum.shrink_to_fit();
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = 1, .record_size = _lay.record_size};
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    Finalize();
    _row.resize(_lay.record_size);
    for (size_t r = 0; r < n; ++r) {
      BqEncode(_lay, _model.data(), vecs + r * _lay.d, _row.data());
      out.WriteData(_row.data(), _row.size());
    }
  }

  bool EncodeInto(byte_type* dst, const float* vecs, size_t n) final {
    Finalize();
    for (size_t r = 0; r < n; ++r) {
      BqEncode(_lay, _model.data(), vecs + r * _lay.d,
               reinterpret_cast<uint8_t*>(dst) + r * _lay.record_size);
    }
    return true;
  }

  std::unique_ptr<QuantizerWriter> CloneForEncode() const final {
    auto out = std::make_unique<BinaryQuantizerWriter>(_lay.d, _lay.bits,
                                                       _lay.half, _query_bits);
    out->_model = _model;
    out->_ready = true;
    return out;
  }

  void Finish(IndexOutput& /*out*/) final {}

  void Serialize(DataOutput& out) const final {
    out.WriteU64(sizeof(BqStatsHeader) + _model.size() * sizeof(float));
    WritePod(out, BqStatsHeader{
                    .layout = kBqLayout,
                    .bits = static_cast<uint8_t>(_lay.bits),
                    .half = static_cast<uint8_t>(_lay.half ? 1 : 0),
                    .query_bits = static_cast<uint8_t>(_query_bits),
                    .normalized = static_cast<uint8_t>(_normalized ? 1 : 0),
                    .reserved = {},
                    .d = _lay.d});
    out.WriteData(reinterpret_cast<const byte_type*>(_model.data()),
                  _model.size() * sizeof(float));
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::BQ;
  }

 private:
  size_t SampleCap() const noexcept {
    return std::min(kBqSampleRows,
                    std::max<size_t>(1, kBqSampleFloats / _lay.d));
  }

  void Finalize() {
    if (_ready) {
      return;
    }
    _ready = true;
    if (_count == 0) {
      return;
    }
    const size_t d = _lay.d;
    const double inv = 1.0 / static_cast<double>(_count);
    std::vector<float> mean(d);
    std::vector<float> sd(d);
    for (size_t i = 0; i < d; ++i) {
      const double m = _sum[i] * inv;
      mean[i] = static_cast<float>(m);
      sd[i] =
        static_cast<float>(std::sqrt(std::max(_sum2[i] * inv - m * m, 0.0)));
    }
    BqGaussianModel(_lay, mean.data(), sd.data(), _model.data());
    const size_t rows = _sample.size() / d;
    if (rows == 0) {
      return;
    }
    const bool one_bit = _lay.plane1_bytes == 0;
    float* cut0 = _model.data() + kBqCut0 * d;
    float* cut1 = _model.data() + kBqCut1 * d;
    float* base = _model.data() + kBqBase * d;
    float* step0 = _model.data() + kBqStep0 * d;
    float* step1 = _model.data() + kBqStep1 * d;
    std::vector<float> col(rows);
    for (size_t i = 0; i < d; ++i) {
      for (size_t r = 0; r < rows; ++r) {
        col[r] = _sample[r * d + i];
      }
      const BqLloyd fit = BqLloydCuts(col, one_bit ? 2 : 3);
      cut0[i] = fit.cuts[0];
      base[i] = fit.means[0];
      step0[i] = fit.means[1] - fit.means[0];
      if (!one_bit) {
        cut1[i] = fit.cuts[1] > fit.cuts[0]
                    ? fit.cuts[1]
                    : std::numeric_limits<float>::infinity();
        step1[i] = fit.means[2] - fit.means[1];
      }
    }
    _sample.clear();
    _sample.shrink_to_fit();
  }

  BqLayout _lay;
  uint32_t _query_bits;
  std::vector<float> _model;
  std::vector<double> _sum;
  std::vector<double> _sum2;
  uint64_t _count = 0;
  std::vector<float> _sample;
  uint64_t _seen = 0;
  pcg32 _rng{kBqSampleSeed};
  bool _ready = false;
  bool _normalized = true;
  std::vector<double> _cell_count;
  std::vector<double> _cell_sum;
  std::vector<uint8_t> _row;
};

template<VectorMetric M>
class BinaryQuantizerStats final : public QuantizerStats {
 public:
  BinaryQuantizerStats(uint32_t d, std::span<const byte_type> stats) {
    const auto hdr = ReadPodHeader<BqStatsHeader>(stats);
    SDB_ASSERT(hdr.layout == kBqLayout);
    SDB_ASSERT(hdr.d == d);
    _lay = MakeBqLayout(d, hdr.bits, hdr.half != 0);
    _query_bits = hdr.query_bits;
    _normalized = hdr.normalized != 0;
    _model.resize(kBqModelFloats * size_t{d});
    SDB_ASSERT(stats.size() >=
               sizeof(BqStatsHeader) + _model.size() * sizeof(float));
    std::memcpy(_model.data(), stats.data() + sizeof(BqStatsHeader),
                _model.size() * sizeof(float));
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::BQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  std::shared_ptr<const QuantizerCodebook> MakeBuildCodebook(
    std::span<const float> query) const final;

  std::shared_ptr<const QuantizerCodebook> MakeRangeCodebook(
    std::span<const float> query) const final;

  const BqLayout& Layout() const noexcept { return _lay; }
  uint32_t QueryBits() const noexcept { return _query_bits; }
  bool Normalized() const noexcept { return _normalized; }
  const float* Model(size_t slot) const noexcept {
    return _model.data() + slot * _lay.d;
  }

 private:
  BqLayout _lay;
  uint32_t _query_bits = kBQDefaultQueryBits;
  bool _normalized = false;
  std::vector<float> _model;
};

template<VectorMetric M>
class BinaryQuantizerCodebook final : public QuantizerCodebook {
 public:
  BinaryQuantizerCodebook(std::shared_ptr<const BinaryQuantizerStats<M>> stats,
                          std::span<const float> query)
    : BinaryQuantizerCodebook{stats, query, stats->QueryBits(), false} {}

  BinaryQuantizerCodebook(std::shared_ptr<const BinaryQuantizerStats<M>> stats,
                          std::span<const float> query, uint32_t query_bits,
                          bool symmetric)
    : _stats{std::move(stats)}, _query_bits{query_bits}, _symmetric{symmetric} {
    const BqLayout& lay = _stats->Layout();
    if (Binary()) {
      _bits.resize(lay.record_size);
    } else {
      _w0.resize(size_t{lay.plane0_bytes} * 8);
      _w1.resize(size_t{lay.plane1_bytes} * 8);
      _q0.resize(_w0.size());
      _q1.resize(_w1.size());
    }
    Rekey(query);
  }

  void Rekey(std::span<const float> query) {
    const BqLayout& lay = _stats->Layout();
    _query.assign(query.begin(), query.end());
    if (Binary()) {
      if (_symmetric || M == VectorMetric::L2Sqr || _stats->Normalized()) {
        BqEncode(lay, _stats->Model(kBqCut0), query.data(), _bits.data());
      } else {
        BqEncodeSigns(lay, _stats->Model(kBqCut0), query.data(), _bits.data());
      }
      return;
    }
    const float* base = _stats->Model(kBqBase);
    const float* step0 = _stats->Model(kBqStep0);
    const float* step1 = _stats->Model(kBqStep1);
    const float* joint = _stats->Model(kBqJoint);
    const bool two_planes = lay.plane1_bytes != 0;
    std::fill(_w1.begin(), _w1.end(), 0.f);
    double bias = 0.0;
    for (uint32_t i = 0; i < lay.d; ++i) {
      const uint32_t j = BqPlane1Index(lay, i);
      if constexpr (M == VectorMetric::L2Sqr) {
        const float u = query[i] - base[i];
        bias -= double{u} * u;
        _w0[i] = step0[i] * (2.f * u - step0[i]);
        if (two_planes) {
          _w1[j] += step1[i] * (2.f * u - step1[i] - 2.f * joint[i] * step0[i]);
        }
      } else {
        bias += double{query[i]} * base[i];
        _w0[i] = query[i] * step0[i];
        if (two_planes) {
          _w1[j] += query[i] * step1[i];
        }
      }
    }
    _bias = static_cast<float>(bias);
    float top_w = 0.f;
    for (const float w : _w0) {
      top_w = std::max(top_w, std::fabs(w));
    }
    for (const float w : _w1) {
      top_w = std::max(top_w, std::fabs(w));
    }
    const float scale =
      top_w > 0.f ? static_cast<float>(kBqQueryTop) / top_w : 0.f;
    _inv_scale = scale > 0.f ? 1.f / scale : 0.f;
    Quantize(_w0, scale, _q0);
    Quantize(_w1, scale, _q1);
    if (_query_bits < 8) {
      BqFitQueryLevels(1U << _query_bits, _q0, _q1);
    }
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const std::shared_ptr<const BinaryQuantizerStats<M>>& StatsPtr()
    const noexcept {
    return _stats;
  }
  const BinaryQuantizerStats<M>& Stats() const noexcept { return *_stats; }
  uint32_t QueryBits() const noexcept { return _query_bits; }
  bool Binary() const noexcept { return _query_bits == 1; }
  bool Symmetric() const noexcept { return _symmetric; }
  std::span<const float> Query() const noexcept { return _query; }

  double WeightRounding() const noexcept {
    double var = 0.0;
    double mean = 0.0;
    const auto add = [&](std::span<const float> w, std::span<const int8_t> q) {
      for (size_t k = 0; k < w.size(); ++k) {
        const double e = q[k] * double{_inv_scale} - w[k];
        var += e * e / 4.0;
        mean += e / 2.0;
      }
    };
    add(_w0, _q0);
    add(_w1, _q1);
    return var + mean * mean;
  }

  score_t Score(const uint8_t* code) const noexcept {
    const BqLayout& lay = _stats->Layout();
    if (Binary()) {
      return -static_cast<score_t>(
        BqXorPopcount(code, _bits.data(), lay.record_size));
    }
    int32_t dot = BitPlaneSum(code, lay.plane0_bytes, _q0.data());
    if (lay.plane1_bytes != 0) {
      dot += BitPlaneSum(code + lay.plane0_bytes, lay.plane1_bytes, _q1.data());
    }
    return _bias + static_cast<score_t>(dot) * _inv_scale;
  }

 private:
  static void Quantize(std::span<const float> w, float scale,
                       std::vector<int8_t>& q) noexcept {
    constexpr auto kTop = static_cast<float>(kBqQueryTop);
    for (size_t k = 0; k < w.size(); ++k) {
      q[k] = static_cast<int8_t>(
        std::clamp(std::nearbyint(w[k] * scale), -kTop, kTop));
    }
  }

  std::shared_ptr<const BinaryQuantizerStats<M>> _stats;
  uint32_t _query_bits;
  bool _symmetric;
  std::vector<float> _query;
  std::vector<float> _w0;
  std::vector<float> _w1;
  std::vector<uint8_t> _bits;
  std::vector<int8_t> _q0;
  std::vector<int8_t> _q1;
  float _bias = 0.f;
  float _inv_scale = 0.f;
};

template<VectorMetric M>
class BinaryQuantizerReader final : public QuantizerReader {
 public:
  explicit BinaryQuantizerReader(
    std::shared_ptr<const BinaryQuantizerCodebook<M>> cb)
    : _cb{std::move(cb)}, _cur{_cb.get()}, _lay{_cb->Stats().Layout()} {}

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = 1, .record_size = _lay.record_size};
  }

  void StartCluster(const float* /*centroid*/) final {}

  void ComputeBlock(std::span<const byte_type> block, score_t /*threshold*/,
                    score_t* out) final {
    SDB_ASSERT(block.size() % _lay.record_size == 0);
    const size_t rows = block.size() / _lay.record_size;
    const auto* codes = reinterpret_cast<const uint8_t*>(block.data());
    for (size_t i = 0; i < rows; ++i) {
      out[i] = _cur->Score(codes + i * _lay.record_size);
    }
  }

  void ComputeGathered(const byte_type* base, uint32_t record_size,
                       std::span<const uint32_t> ids, score_t /*threshold*/,
                       score_t* out) final {
    ScoreGatheredRows(base, record_size, ids, out, [this](const byte_type* r) {
      return _cur->Score(reinterpret_cast<const uint8_t*>(r));
    });
  }

  void ScoreErrors(const byte_type* base, uint32_t record_size,
                   std::span<const uint32_t> ids, std::span<score_t> scores,
                   score_t* errors) final {
    Errors(
      [&](size_t i) {
        return reinterpret_cast<const uint8_t*>(
          base + static_cast<size_t>(ids[i]) * record_size);
      },
      scores.first(ids.size()), errors);
  }

  void BlockErrors(std::span<const byte_type> block, std::span<score_t> scores,
                   score_t* errors) final {
    Errors(
      [&](size_t i) {
        return reinterpret_cast<const uint8_t*>(block.data() +
                                                i * _lay.record_size);
      },
      scores, errors);
  }

  bool Decode(const byte_type* code, float* out) const final {
    const auto& stats = _cb->Stats();
    const float* base = stats.Model(kBqBase);
    const float* step0 = stats.Model(kBqStep0);
    const float* step1 = stats.Model(kBqStep1);
    const auto* c = reinterpret_cast<const uint8_t*>(code);
    for (uint32_t i = 0; i < _lay.d; ++i) {
      const uint32_t cell = BqCell(_lay, c, i);
      out[i] = base[i] + ((cell & 1U) != 0 ? step0[i] : 0.f) +
               ((cell & 2U) != 0 ? step1[i] : 0.f);
    }
    return true;
  }

  bool SetQuery(std::span<const float> query) final {
    if (!_own) {
      _own = std::make_unique<BinaryQuantizerCodebook<M>>(
        _cb->StatsPtr(), query, _cb->QueryBits(), _cb->Symmetric());
      _cur = _own.get();
    } else {
      _own->Rekey(query);
    }
    _errors_ready = false;
    return true;
  }

  bool SupportsPairScores() const noexcept final { return _cb->Binary(); }

  bool PreparePairTerms(const byte_type* /*base*/, uint32_t /*record_size*/,
                        uint64_t rows, std::vector<float>& terms) final {
    terms.assign(rows, 0.f);
    return rows != 0;
  }

  void ScorePairBatch(const byte_type* base, uint32_t record_size,
                      std::span<const float> /*terms*/, uint32_t from,
                      std::span<const uint32_t> ids, score_t* out) final {
    const auto* a =
      reinterpret_cast<const uint8_t*>(base + size_t{from} * record_size);
    for (size_t i = 0; i < ids.size(); ++i) {
      const auto* b = reinterpret_cast<const uint8_t*>(
        base + static_cast<size_t>(ids[i]) * record_size);
      out[i] = -static_cast<score_t>(BqXorPopcount(a, b, _lay.record_size));
    }
  }

 private:
  template<typename Row>
  void Errors(Row row, std::span<score_t> scores, score_t* errors) {
    if (!_errors_ready) {
      PrepareErrors();
    }
    if (_cur->Binary()) {
      for (size_t i = 0; i < scores.size(); ++i) {
        scores[i] = _fine->Score(row(i));
      }
    }
    if constexpr (M == VectorMetric::L2Sqr) {
      for (size_t i = 0; i < scores.size(); ++i) {
        errors[i] =
          std::sqrt(std::max(0.f, -scores[i]) * _error_slope + _error_var);
      }
    } else {
      std::fill_n(errors, scores.size(), std::sqrt(_error_var));
    }
  }

  void PrepareErrors() {
    const BinaryQuantizerCodebook<M>* est = _cur;
    if (_cur->Binary()) {
      if (!_fine) {
        _fine = std::make_unique<BinaryQuantizerCodebook<M>>(
          _cb->StatsPtr(), _cur->Query(), 8U, false);
      } else {
        _fine->Rekey(_cur->Query());
      }
      est = _fine.get();
    }
    const auto& stats = _cb->Stats();
    const float* step0 = stats.Model(kBqStep0);
    const float* step1 = stats.Model(kBqStep1);
    const auto query = est->Query();
    const bool three = _lay.bits == 2 && !_lay.half;
    double weighted = 0.0;
    double total = 0.0;
    for (uint32_t j = 0; j < _lay.d; ++j) {
      const double s0 = step0[j];
      const double s1 = step1[j];
      const double v = three ? kBqThreeCellSpread * (s0 + s1) * (s0 + s1)
                             : kBqTwoCellSpread * s0 * s0;
      total += v;
      weighted += double{query[j]} * query[j] * v;
    }
    const double rounding = est->WeightRounding();
    if constexpr (M == VectorMetric::L2Sqr) {
      _error_slope =
        static_cast<float>(4.0 * total / static_cast<double>(_lay.d));
      _error_var = static_cast<float>(rounding);
    } else {
      _error_var = static_cast<float>(weighted + rounding);
    }
    _errors_ready = true;
  }

  std::shared_ptr<const BinaryQuantizerCodebook<M>> _cb;
  const BinaryQuantizerCodebook<M>* _cur;
  std::unique_ptr<BinaryQuantizerCodebook<M>> _own;
  std::unique_ptr<BinaryQuantizerCodebook<M>> _fine;
  BqLayout _lay;
  float _error_slope = 0.f;
  float _error_var = 0.f;
  bool _errors_ready = false;
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> BinaryQuantizerCodebook<M>::MakeReader()
  const {
  return MakeReaderT<BinaryQuantizerCodebook<M>, BinaryQuantizerReader<M>>(
    this);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> BinaryQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<BinaryQuantizerStats<M>, BinaryQuantizerCodebook<M>>(
    this, query);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook>
BinaryQuantizerStats<M>::MakeBuildCodebook(std::span<const float> query) const {
  return std::make_shared<const BinaryQuantizerCodebook<M>>(
    std::static_pointer_cast<const BinaryQuantizerStats<M>>(shared_from_this()),
    query, 1U, true);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook>
BinaryQuantizerStats<M>::MakeRangeCodebook(std::span<const float> query) const {
  return std::make_shared<const BinaryQuantizerCodebook<M>>(
    std::static_pointer_cast<const BinaryQuantizerStats<M>>(shared_from_this()),
    query, _query_bits == 1 ? 8U : _query_bits, false);
}

}  // namespace

std::unique_ptr<QuantizerWriter> MakeBinaryWriter(VectorMetric metric,
                                                  uint32_t d, uint32_t nb_bits,
                                                  bool half_bit,
                                                  uint32_t query_bits) {
  return MakeWriterWithMetric<BinaryQuantizerWriter>(metric, d, nb_bits,
                                                     half_bit, query_bits);
}

std::shared_ptr<const QuantizerStats> MakeBinaryStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> stats) {
  return MakeStatsWithMetric<BinaryQuantizerStats>(metric, d, stats);
}

}  // namespace irs::quantizers
