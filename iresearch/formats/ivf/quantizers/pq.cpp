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
#include <absl/random/distributions.h>
#include <faiss/impl/ProductQuantizer.h>
#include <faiss/utils/fp16.h>

#include <core_functions/array_kernels.hpp>
#include <limits>
#include <pcg_random.hpp>

#include "iresearch/formats/ivf/quantizers/common.hpp"
#include "iresearch/index/vector_distance.hpp"
#include "iresearch/utils/empty.hpp"

namespace irs::quantizers {
namespace {

constexpr uint32_t kPqNbits = 4;
constexpr uint32_t kPq8Nbits = 8;
constexpr size_t kPq8Ksub = size_t{1} << kPq8Nbits;
constexpr size_t kPq8TrainRows = 32768;

constexpr size_t kPq8LutBytes = size_t{256} << 10;

constexpr bool Pq8Direct(size_t dsub, size_t m) noexcept {
  return dsub == 2 ||
         (dsub == 1 && m * kPq8Ksub * sizeof(float) > kPq8LutBytes);
}

constexpr size_t kPqTrainResiduals = kFastScanKsub * 256;

struct PqStatsHeader {
  uint32_t m;
  uint32_t ksub;
};

std::vector<float> SubspaceErrors(const faiss::ProductQuantizer& pq,
                                  const float* vecs, size_t n) {
  std::vector<float> out(pq.M, 0.f);
  if (n == 0) {
    return out;
  }
  std::vector<uint8_t> codes(n * pq.code_size);
  pq.compute_codes(vecs, codes.data(), n);
  std::vector<float> decoded(n * pq.d);
  pq.decode(codes.data(), decoded.data(), n);
  std::vector<double> sum(pq.M, 0.0);
  for (size_t i = 0; i < n; ++i) {
    for (size_t m = 0; m < pq.M; ++m) {
      for (size_t j = 0; j < pq.dsub; ++j) {
        const size_t at = i * pq.d + m * pq.dsub + j;
        const double e = double{vecs[at]} - decoded[at];
        sum[m] += e * e;
      }
    }
  }
  for (size_t m = 0; m < pq.M; ++m) {
    out[m] = static_cast<float>(sum[m] / static_cast<double>(n));
  }
  return out;
}

std::vector<float> ReadSubspaceErrors(std::span<const byte_type> stats,
                                      size_t offset, size_t m) {
  if (stats.size() < offset + m * sizeof(float)) {
    return {};
  }
  std::vector<float> out(m);
  std::memcpy(out.data(), stats.data() + offset, m * sizeof(float));
  return out;
}

struct PqErrorTerms {
  float per = 0.f;
  float weighted = 0.f;
  bool known = false;
};

PqErrorTerms MakePqErrorTerms(std::span<const float> sub_errors,
                              std::span<const float> query) {
  if (sub_errors.empty()) {
    return {};
  }
  const size_t m = sub_errors.size();
  const size_t dsub = query.size() / m;
  double total = 0.0;
  double weighted = 0.0;
  for (size_t s = 0; s < m; ++s) {
    double norm2 = 0.0;
    for (size_t j = 0; j < dsub; ++j) {
      const double v = query[s * dsub + j];
      norm2 += v * v;
    }
    total += sub_errors[s];
    weighted += norm2 * sub_errors[s] / static_cast<double>(dsub);
  }
  return {
    .per = static_cast<float>(4.0 * total / static_cast<double>(query.size())),
    .weighted = static_cast<float>(std::sqrt(weighted)),
    .known = true};
}

template<VectorMetric M>
void PqScoreErrors(const PqErrorTerms& terms, std::span<const score_t> scores,
                   score_t* errors) {
  const size_t n = scores.size();
  if (!terms.known) {
    std::fill_n(errors, n, std::numeric_limits<score_t>::infinity());
    return;
  }
  if constexpr (M == VectorMetric::L2Sqr) {
    for (size_t i = 0; i < n; ++i) {
      errors[i] = std::sqrt(std::max(0.f, -scores[i]) * terms.per);
    }
  } else {
    std::fill_n(errors, n, terms.weighted);
  }
}

template<VectorMetric M>
class ProductQuantizerWriter final : public QuantizerWriter {
 public:
  ProductQuantizerWriter(uint32_t d, uint32_t m, uint32_t niter)
    : _d{d}, _pq{d, m == 0 ? 1 : m, kPqNbits} {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    if (niter != 0) {
      _pq.cp.niter = static_cast<int>(niter);
    }
    _packed.resize(kFastScanBbs * FastScanNsq(_pq.M) / 2);
    _res_tile.resize(kFastScanBbs * size_t{_d});
    _codes_tile.resize(kFastScanBbs * _pq.code_size);
    if constexpr (M == VectorMetric::L2Sqr) {
      _dec.resize(_d);
    }
  }

  size_t TrainSamples(size_t rows) const noexcept final {
    return std::min<size_t>(rows, kPqTrainResiduals);
  }

  void Train(const float* vecs, size_t n) final {
    if (n == 0) {
      return;
    }
    const size_t ksub = _pq.ksub;
    if (n >= ksub) {
      _pq.train(n, vecs);
    } else {
      std::vector<float> padded(ksub * _d);
      for (size_t i = 0; i < ksub; ++i) {
        std::memcpy(padded.data() + i * _d, vecs + (i % n) * _d,
                    _d * sizeof(float));
      }
      _pq.train(ksub, padded.data());
    }
    _errors = SubspaceErrors(_pq, vecs, n);
    _trained = true;
  }

  void SetClusterCentroid(const float* centroid) final {
    CodePending();
    _centroid.assign(centroid, centroid + _d);
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {
      .group_size = kFastScanBbs,
      .record_size =
        static_cast<uint32_t>(FastScanNsq(_pq.M) / 2 +
                              (M == VectorMetric::L2Sqr ? sizeof(float) : 0)),
      .alignment = M == VectorMetric::L2Sqr ? alignof(float) : 1U,
    };
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    SDB_ASSERT(_trained);
    SDB_ASSERT(_centroid.size() == _d);
    for (size_t i = 0; i < n; ++i) {
      const float* vec = vecs + i * _d;
      float* res = _res_tile.data() + _lane * size_t{_d};
      for (uint32_t j = 0; j < _d; ++j) {
        res[j] = vec[j] - _centroid[j];
      }
      if (++_lane == kFastScanBbs) {
        WriteGroup(out, kFastScanBbs);
      }
    }
  }

  void Finish(IndexOutput& out) final {
    if (_lane != 0) {
      WriteGroup(out, _lane);
    }
  }

  uint32_t PendingLanes() const noexcept final {
    return static_cast<uint32_t>(_lane);
  }

  void Serialize(DataOutput& out) const final {
    if (!_trained) {
      out.WriteU64(0);
      return;
    }
    const auto cents = FloatSpan(_pq.centroids);
    const auto errors = FloatSpan(_errors);
    out.WriteU64(sizeof(PqStatsHeader) + cents.size() + errors.size());
    WritePod(out, PqStatsHeader{static_cast<uint32_t>(_pq.M),
                                static_cast<uint32_t>(_pq.ksub)});
    out.WriteData(cents.data(), cents.size());
    out.WriteData(errors.data(), errors.size());
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::PQ;
  }

 private:
  void CodePending() {
    if (_lane == _coded) {
      return;
    }
    const size_t count = _lane - _coded;
    uint8_t* codes = _codes_tile.data() + _coded * _pq.code_size;
    _pq.compute_codes(_res_tile.data() + _coded * size_t{_d}, codes, count);
    if constexpr (M == VectorMetric::L2Sqr) {
      for (size_t i = 0; i < count; ++i) {
        _pq.decode(codes + i * _pq.code_size, _dec.data());
        for (uint32_t j = 0; j < _d; ++j) {
          _dec[j] += _centroid[j];
        }
        _norms[_coded + i] = duckdb::NormSquaredOp::Operation(_dec.data(), _d);
      }
    }
    _coded = _lane;
  }

  void WriteGroup(IndexOutput& out, size_t count) {
    CodePending();
    faiss::pq4_pack_codes(_codes_tile.data(), count, _pq.M, kFastScanBbs,
                          kFastScanBbs, FastScanNsq(_pq.M), _packed.data(),
                          _pq.code_size);
    out.WriteData(_packed.data(), _packed.size());
    if constexpr (M == VectorMetric::L2Sqr) {
      std::fill_n(_norms.data() + count, kFastScanBbs - count, 0.f);
      out.WriteData(reinterpret_cast<const byte_type*>(_norms.data()),
                    kFastScanBbs * sizeof(float));
    }
    _lane = 0;
    _coded = 0;
  }

  uint32_t _d;
  faiss::ProductQuantizer _pq;
  bool _trained = false;
  size_t _lane = 0;
  size_t _coded = 0;
  std::vector<float> _centroid;
  std::vector<float> _res_tile;
  std::vector<uint8_t> _codes_tile;
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr,
                                    std::array<float, kFastScanBbs>> _norms;
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr,
                                    std::vector<float>> _dec;
  std::vector<uint8_t> _packed;
  std::vector<float> _errors;
};

template<VectorMetric M>
class ProductQuantizerStats final : public QuantizerStats {
 public:
  ProductQuantizerStats(uint32_t d, std::span<const byte_type> stats) {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    const PqStatsHeader hdr = ReadPodHeader<PqStatsHeader>(stats);
    SDB_ASSERT(hdr.m != 0);
    SDB_ASSERT(d % hdr.m == 0);
    SDB_ASSERT(hdr.ksub != 0);
    _pq.d = d;
    _pq.M = hdr.m;
    _pq.nbits = kPqNbits;
    _pq.set_derived_values();
    const size_t want = _pq.centroids.size() * sizeof(float);
    SDB_ASSERT(hdr.ksub == static_cast<uint32_t>(_pq.ksub));
    SDB_ASSERT(stats.size() >= sizeof(PqStatsHeader) + want);
    std::memcpy(_pq.centroids.data(), stats.data() + sizeof(PqStatsHeader),
                want);
    _errors = ReadSubspaceErrors(stats, sizeof(PqStatsHeader) + want, _pq.M);
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::PQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const faiss::ProductQuantizer& Pq() const noexcept { return _pq; }
  std::span<const float> Errors() const noexcept { return _errors; }

 private:
  faiss::ProductQuantizer _pq;
  std::vector<float> _errors;
};

template<VectorMetric M>
class ProductQuantizerCodebook final : public QuantizerCodebook {
 public:
  ProductQuantizerCodebook(
    std::shared_ptr<const ProductQuantizerStats<M>> stats,
    std::span<const float> query)
    : _stats{std::move(stats)}, _query(query.begin(), query.end()) {
    // IP(q, c + r) = IP(q, c) + IP(q, r); the packed LUT for IP(q, r) is
    // query-only and precomputed once per query here.
    const faiss::ProductQuantizer& pq = _stats->Pq();
    const size_t ksub = pq.ksub;
    std::vector<float> ip_table(static_cast<size_t>(pq.M) * ksub);
    pq.compute_inner_prod_table(_query.data(), ip_table.data());
    _lut.Build(ip_table.data(), pq.M, ksub);
    _error_terms = MakePqErrorTerms(_stats->Errors(), _query);
    if constexpr (M == VectorMetric::L2Sqr) {
      _query_norm2 =
        duckdb::NormSquaredOp::Operation(_query.data(), _query.size());
    }
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const faiss::ProductQuantizer& Pq() const noexcept { return _stats->Pq(); }
  const PqErrorTerms& ErrorTerms() const noexcept { return _error_terms; }
  std::span<const float> Query() const noexcept { return _query; }
  const FastScanLut& Lut() const noexcept { return _lut; }
  float QueryNorm() const noexcept
    requires(M == VectorMetric::L2Sqr)
  {
    return _query_norm2;
  }

 private:
  std::shared_ptr<const ProductQuantizerStats<M>> _stats;
  std::vector<float> _query;
  PqErrorTerms _error_terms;
  FastScanLut _lut;
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr, float>
    _query_norm2;
};

template<VectorMetric M>
class ProductQuantizerReader final : public QuantizerReader {
 public:
  explicit ProductQuantizerReader(
    std::shared_ptr<const ProductQuantizerCodebook<M>> cb)
    : _cb{std::move(cb)},
      _nsq{FastScanNsq(_cb->Pq().M)},
      _code_bytes{kFastScanBbs * _nsq / 2},
      _group_bytes{_code_bytes + (M == VectorMetric::L2Sqr
                                    ? kFastScanBbs * sizeof(float)
                                    : 0)} {}

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {
      .group_size = kFastScanBbs,
      .record_size = static_cast<uint32_t>(_group_bytes / kFastScanBbs),
      .alignment = M == VectorMetric::L2Sqr ? alignof(float) : 1U,
    };
  }

  void StartCluster(const float* centroid) final {
    SDB_ASSERT(centroid);
    const std::span<const float> query = _cb->Query();
    // IP(q, c + r) = IP(q, c) + IP(q, r); the packed LUT for IP(q, r) is
    // query-only and precomputed once per query in the codebook.
    _qc = ComputeDistance<VectorMetric::InnerProduct>(
      query.data(), centroid, static_cast<uint16_t>(query.size()));
  }

  void ComputeBlock(std::span<const byte_type> block, score_t /*threshold*/,
                    score_t* out) final {
    SDB_ASSERT(block.size() % _group_bytes == 0);
    const FastScanLut& lut = _cb->Lut();
    for (size_t off = 0; off < block.size();
         off += _group_bytes, out += kFastScanBbs) {
      const byte_type* codes = block.data() + off;
      faiss::accumulate_to_mem(1, kFastScanBbs, static_cast<int>(_nsq), codes,
                               lut.Packed(), _accu.data());
      if constexpr (M == VectorMetric::L2Sqr) {
        const float* norms =
          reinterpret_cast<const float*>(codes + _code_bytes);
        const float q2 = _cb->QueryNorm();
        for (size_t i = 0; i < kFastScanBbs; ++i) {
          const float ip = lut.Decode(_accu[i]);
          out[i] = -(q2 - 2.f * _qc - 2.f * ip + norms[i]);
        }
      } else {
        for (size_t i = 0; i < kFastScanBbs; ++i) {
          out[i] = lut.Decode(_accu[i]) + _qc;
        }
      }
    }
  }

  void ScoreErrors(const byte_type* /*base*/, uint32_t /*record_size*/,
                   std::span<const uint32_t> ids, std::span<score_t> scores,
                   score_t* errors) final {
    Errors(scores.first(ids.size()), errors);
  }

  void BlockErrors(std::span<const byte_type> /*block*/,
                   std::span<score_t> scores, score_t* errors) final {
    Errors(scores, errors);
  }

 private:
  void Errors(std::span<const score_t> scores, score_t* errors) const {
    PqScoreErrors<VectorMetric::InnerProduct>(_cb->ErrorTerms(), scores,
                                              errors);
    if constexpr (M == VectorMetric::L2Sqr) {
      for (size_t i = 0; i < scores.size(); ++i) {
        errors[i] *= 2.f;
      }
    }
  }

  std::shared_ptr<const ProductQuantizerCodebook<M>> _cb;
  size_t _nsq;
  size_t _code_bytes;
  size_t _group_bytes;
  std::array<uint16_t, kFastScanBbs> _accu;
  float _qc = 0.f;
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> ProductQuantizerCodebook<M>::MakeReader()
  const {
  return MakeReaderT<ProductQuantizerCodebook<M>, ProductQuantizerReader<M>>(
    this);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> ProductQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<ProductQuantizerStats<M>, ProductQuantizerCodebook<M>>(
    this, query);
}

uint32_t NearestCentroid(const float* __restrict x, uint32_t dsub,
                         const float* __restrict cent_t,
                         const float* __restrict cnorm,
                         float* __restrict dist) {
  float x2 = 0.f;
  for (uint32_t j = 0; j < dsub; ++j) {
    x2 += x[j] * x[j];
  }
  for (size_t c = 0; c < kPq8Ksub; ++c) {
    dist[c] = cnorm[c] + x2;
  }
  for (uint32_t j = 0; j < dsub; ++j) {
    const float xj = -2.f * x[j];
    const float* __restrict cj = cent_t + j * kPq8Ksub;
    for (size_t c = 0; c < kPq8Ksub; ++c) {
      dist[c] += xj * cj[c];
    }
  }
  int32_t best = std::numeric_limits<int32_t>::max();
  for (size_t c = 0; c < kPq8Ksub; ++c) {
    best = std::min(best, std::bit_cast<int32_t>(dist[c]));
  }
  constexpr auto kNone = static_cast<uint32_t>(kPq8Ksub);
  uint32_t arg = kNone;
  for (uint32_t c = 0; c < kNone; ++c) {
    arg = std::min(arg, std::bit_cast<int32_t>(dist[c]) == best ? c : kNone);
  }
  return arg;
}

void InitLine(std::span<const float> sorted, uint32_t k, float* cent) {
  const size_t n = sorted.size();
  const double lo = sorted.front();
  const double hi = sorted.back();
  if (!(hi > lo)) {
    std::fill_n(cent, k, sorted.front());
    return;
  }
  const size_t bins = size_t{4} * k;
  const double width = (hi - lo) / static_cast<double>(bins);
  std::vector<double> mass(bins + 1, 0.0);
  size_t i = 0;
  for (size_t b = 0; b < bins; ++b) {
    const double edge = lo + width * static_cast<double>(b + 1);
    size_t j = i;
    while (j < n && (b + 1 == bins || sorted[j] < edge)) {
      ++j;
    }
    mass[b + 1] = mass[b] + std::cbrt(static_cast<double>(j - i));
    i = j;
  }
  size_t b = 0;
  for (uint32_t c = 0; c < k; ++c) {
    const double t = (c + 0.5) / k * mass[bins];
    while (b + 1 < bins && mass[b + 1] < t) {
      ++b;
    }
    const double span = mass[b + 1] - mass[b];
    const double f = span > 0.0 ? (t - mass[b]) / span : 0.5;
    cent[c] = static_cast<float>(lo + width * (static_cast<double>(b) + f));
  }
}

void TrainLine(const float* x, size_t n, uint32_t k, uint32_t niter,
               float* cent) {
  std::vector<float> sorted(x, x + n);
  absl::c_sort(sorted);
  InitLine(sorted, k, cent);
  std::vector<double> prefix(n + 1);
  for (size_t i = 0; i < n; ++i) {
    prefix[i + 1] = prefix[i] + sorted[i];
  }
  std::vector<uint32_t> by_value(k);
  std::vector<size_t> lo(k, 0);
  std::vector<size_t> hi(k, 0);
  std::vector<size_t> count(k);
  for (uint32_t it = 0; it < niter; ++it) {
    std::iota(by_value.begin(), by_value.end(), 0U);
    absl::c_stable_sort(
      by_value, [&](uint32_t l, uint32_t r) { return cent[l] < cent[r]; });
    size_t moved = n;
    size_t begin = 0;
    for (uint32_t p = 0; p < k; ++p) {
      const uint32_t c = by_value[p];
      size_t end = n;
      if (p + 1 < k) {
        const float mid = 0.5f * (cent[c] + cent[by_value[p + 1]]);
        end = static_cast<size_t>(
          std::lower_bound(sorted.begin() + begin, sorted.end(), mid) -
          sorted.begin());
      }
      const size_t kept_lo = std::max(begin, lo[c]);
      const size_t kept_hi = std::min(end, hi[c]);
      moved -= kept_hi > kept_lo ? kept_hi - kept_lo : 0;
      lo[c] = begin;
      hi[c] = end;
      count[c] = end - begin;
      begin = end;
    }
    if (moved * 1000 < n) {
      break;
    }
    for (uint32_t c = 0; c < k; ++c) {
      if (count[c] != 0) {
        cent[c] = static_cast<float>((prefix[hi[c]] - prefix[lo[c]]) /
                                     static_cast<double>(count[c]));
      }
    }
    constexpr float kSplit = 1.f / 1024.f;
    for (uint32_t c = 0; c < k; ++c) {
      if (count[c] != 0) {
        continue;
      }
      const auto big = static_cast<uint32_t>(
        std::max_element(count.begin(), count.end()) - count.begin());
      const float v = cent[big];
      cent[c] = v * (1.f + kSplit);
      cent[big] = v * (1.f - kSplit);
      count[c] = count[big] / 2;
      count[big] -= count[c];
    }
  }
}

float LaneSum(const float* v, size_t n) noexcept {
  float a[8] = {};
  size_t i = 0;
  for (; i + 8 <= n; i += 8) {
    for (size_t g = 0; g < 8; ++g) {
      a[g] += v[i + g];
    }
  }
  for (; i < n; ++i) {
    a[0] += v[i];
  }
  return ((a[0] + a[1]) + (a[2] + a[3])) + ((a[4] + a[5]) + (a[6] + a[7]));
}

void InitSpread(const float* x, size_t n, uint32_t dsub, uint32_t k, pcg32& rng,
                float* cent) {
  constexpr size_t kBlock = 256;
  const size_t blocks = (n + kBlock - 1) / kBlock;
  std::vector<float> xt(n * dsub);
  for (size_t r = 0; r < n; ++r) {
    for (uint32_t j = 0; j < dsub; ++j) {
      xt[j * n + r] = x[r * dsub + j];
    }
  }
  std::vector<float> near(n, std::numeric_limits<float>::max());
  std::vector<float> dist(n);
  std::vector<double> mass(blocks);
  const auto pick = [&](uint32_t c, size_t i) {
    const float* p = x + i * dsub;
    std::copy_n(p, dsub, cent + size_t{c} * dsub);
    std::fill(dist.begin(), dist.end(), 0.f);
    for (uint32_t j = 0; j < dsub; ++j) {
      const float pj = p[j];
      const float* __restrict col = xt.data() + j * n;
      float* __restrict d = dist.data();
      for (size_t r = 0; r < n; ++r) {
        const float t = col[r] - pj;
        d[r] += t * t;
      }
    }
    for (size_t r = 0; r < n; ++r) {
      near[r] = std::min(near[r], dist[r]);
    }
    for (size_t b = 0; b < blocks; ++b) {
      const size_t from = b * kBlock;
      mass[b] = LaneSum(near.data() + from, std::min(kBlock, n - from));
    }
  };
  pick(0, absl::Uniform<size_t>(rng, 0, n));
  for (uint32_t c = 1; c < k; ++c) {
    const double total = std::accumulate(mass.begin(), mass.end(), 0.0);
    size_t i = 0;
    if (total > 0.0) {
      double r = absl::Uniform<double>(rng, 0.0, total);
      size_t b = 0;
      for (; b + 1 < blocks && r >= mass[b]; ++b) {
        r -= mass[b];
      }
      i = b * kBlock;
      const size_t end = std::min(n, i + kBlock);
      for (; i + 1 < end; ++i) {
        r -= near[i];
        if (r < 0.0) {
          break;
        }
      }
    } else {
      i = absl::Uniform<size_t>(rng, 0, n);
    }
    pick(c, i);
  }
}

void TrainSubspace(const float* x, size_t n, uint32_t dsub, uint32_t k,
                   uint32_t niter, uint64_t seed, float* cent) {
  SDB_ASSERT(n >= k);
  SDB_ASSERT(k % 8 == 0);
  if (dsub == 1) {
    TrainLine(x, n, k, niter, cent);
    return;
  }
  pcg32 rng{seed};
  InitSpread(x, n, dsub, k, rng, cent);
  SDB_ASSERT(k == kPq8Ksub);
  std::vector<float> cent_t(size_t{k} * dsub);
  std::vector<float> cnorm(k);
  std::vector<float> dist(k);
  std::vector<uint32_t> assign(n, k);
  std::vector<double> sum(size_t{k} * dsub);
  std::vector<size_t> count(k);
  for (uint32_t it = 0; it < niter; ++it) {
    for (uint32_t c = 0; c < k; ++c) {
      float s = 0.f;
      for (uint32_t j = 0; j < dsub; ++j) {
        const float v = cent[size_t{c} * dsub + j];
        cent_t[size_t{j} * k + c] = v;
        s += v * v;
      }
      cnorm[c] = s;
    }
    size_t moved = 0;
    for (size_t i = 0; i < n; ++i) {
      const uint32_t c = NearestCentroid(x + i * dsub, dsub, cent_t.data(),
                                         cnorm.data(), dist.data());
      moved += c != assign[i] ? 1 : 0;
      assign[i] = c;
    }
    if (moved * 1000 < n) {
      break;
    }
    std::fill(sum.begin(), sum.end(), 0.0);
    std::fill(count.begin(), count.end(), size_t{0});
    for (size_t i = 0; i < n; ++i) {
      const uint32_t c = assign[i];
      ++count[c];
      for (uint32_t j = 0; j < dsub; ++j) {
        sum[size_t{c} * dsub + j] += x[i * dsub + j];
      }
    }
    for (uint32_t c = 0; c < k; ++c) {
      if (count[c] == 0) {
        continue;
      }
      const double inv = 1.0 / static_cast<double>(count[c]);
      for (uint32_t j = 0; j < dsub; ++j) {
        cent[size_t{c} * dsub + j] =
          static_cast<float>(sum[size_t{c} * dsub + j] * inv);
      }
    }
    constexpr float kSplit = 1.f / 1024.f;
    for (uint32_t c = 0; c < k; ++c) {
      if (count[c] != 0) {
        continue;
      }
      const auto big = static_cast<uint32_t>(
        std::max_element(count.begin(), count.end()) - count.begin());
      for (uint32_t j = 0; j < dsub; ++j) {
        const float v = cent[size_t{big} * dsub + j];
        const float sign = j % 2 == 0 ? 1.f : -1.f;
        cent[size_t{c} * dsub + j] = v * (1.f + sign * kSplit);
        cent[size_t{big} * dsub + j] = v * (1.f - sign * kSplit);
      }
      count[c] = count[big] / 2;
      count[big] -= count[c];
    }
  }
}

float SubspaceMse(const float* x, size_t n, uint32_t dsub, const float* cent) {
  std::vector<float> cent_t(kPq8Ksub * dsub);
  std::vector<float> cnorm(kPq8Ksub);
  for (size_t c = 0; c < kPq8Ksub; ++c) {
    float s = 0.f;
    for (uint32_t j = 0; j < dsub; ++j) {
      const float v = cent[c * dsub + j];
      cent_t[j * kPq8Ksub + c] = v;
      s += v * v;
    }
    cnorm[c] = s;
  }
  std::vector<float> dist(kPq8Ksub);
  double sum = 0.0;
  for (size_t i = 0; i < n; ++i) {
    const float* xi = x + i * dsub;
    const uint32_t c =
      NearestCentroid(xi, dsub, cent_t.data(), cnorm.data(), dist.data());
    for (uint32_t j = 0; j < dsub; ++j) {
      const double e = double{xi[j]} - cent[size_t{c} * dsub + j];
      sum += e * e;
    }
  }
  return n == 0 ? 0.f : static_cast<float>(sum / static_cast<double>(n));
}

template<VectorMetric M>
class ProductQuantizer8Writer final : public QuantizerWriter {
 public:
  ProductQuantizer8Writer(uint32_t d, uint32_t m, uint32_t niter)
    : _d{d}, _pq{d, m == 0 ? 1 : m, kPq8Nbits} {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    if (niter != 0) {
      _pq.cp.niter = static_cast<int>(niter);
    }
  }

  size_t TrainSamples(size_t rows) const noexcept final {
    return std::min<size_t>(rows, kPq8TrainRows);
  }

  void Train(const float* vecs, size_t n) final {
    TrainParallel(vecs, n,
                  [](size_t tasks, absl::FunctionRef<void(size_t)> task) {
                    for (size_t t = 0; t < tasks; ++t) {
                      task(t);
                    }
                  });
  }

  void TrainParallel(const float* vecs, size_t n, TrainFanout fanout) final {
    if (n == 0) {
      return;
    }
    const size_t ksub = _pq.ksub;
    std::vector<float> padded;
    if (n < ksub) {
      padded.resize(ksub * _d);
      for (size_t i = 0; i < ksub; ++i) {
        std::memcpy(padded.data() + i * _d, vecs + (i % n) * _d,
                    _d * sizeof(float));
      }
      vecs = padded.data();
      n = ksub;
    }
    const size_t dsub = _pq.dsub;
    _errors.assign(_pq.M, 0.f);
    fanout(_pq.M, [&](size_t m) {
      std::vector<float> sub(n * dsub);
      for (size_t i = 0; i < n; ++i) {
        std::copy_n(vecs + i * _d + m * dsub, dsub, sub.data() + i * dsub);
      }
      float* cent = _pq.centroids.data() + m * ksub * dsub;
      TrainSubspace(
        sub.data(), n, static_cast<uint32_t>(dsub), static_cast<uint32_t>(ksub),
        static_cast<uint32_t>(_pq.cp.niter), kIvfRotationSeed + m, cent);
      _errors[m] =
        SubspaceMse(sub.data(), n, static_cast<uint32_t>(dsub), cent);
    });
    PrepareEncode();
    _trained = true;
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = 1,
            .record_size = static_cast<uint32_t>(_pq.code_size)};
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    SDB_ASSERT(_trained);
    _codes.resize(n * _pq.code_size);
    EncodeRows(vecs, n, _codes.data());
    out.WriteData(_codes.data(), _codes.size());
  }

  bool EncodeInto(byte_type* dst, const float* vecs, size_t n) final {
    SDB_ASSERT(_trained);
    EncodeRows(vecs, n, reinterpret_cast<uint8_t*>(dst));
    return true;
  }

  std::unique_ptr<QuantizerWriter> CloneForEncode() const final {
    auto out = std::make_unique<ProductQuantizer8Writer>(_d, 0, 0);
    out->_pq = _pq;
    out->_cent_t = _cent_t;
    out->_cnorm = _cnorm;
    out->_mid = _mid;
    out->_order = _order;
    out->_trained = _trained;
    return out;
  }

  uint32_t EncodeWidth() const noexcept final {
    return _pq.dsub == 1 ? 1U : static_cast<uint32_t>(kPq8Ksub);
  }

  void Finish(IndexOutput& /*out*/) final {}

  void Serialize(DataOutput& out) const final {
    if (!_trained) {
      out.WriteU64(0);
      return;
    }
    const auto cents = FloatSpan(_pq.centroids);
    const auto errors = FloatSpan(_errors);
    out.WriteU64(sizeof(PqStatsHeader) + cents.size() + errors.size());
    WritePod(out, PqStatsHeader{static_cast<uint32_t>(_pq.M),
                                static_cast<uint32_t>(_pq.ksub)});
    out.WriteData(cents.data(), cents.size());
    out.WriteData(errors.data(), errors.size());
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::PQ;
  }

 private:
  void PrepareEncode() {
    const size_t dsub = _pq.dsub;
    _cent_t.resize(_pq.M * kPq8Ksub * dsub);
    _cnorm.resize(_pq.M * kPq8Ksub);
    for (size_t m = 0; m < _pq.M; ++m) {
      const float* cent = _pq.centroids.data() + m * kPq8Ksub * dsub;
      float* cent_t = _cent_t.data() + m * kPq8Ksub * dsub;
      for (size_t c = 0; c < kPq8Ksub; ++c) {
        float s = 0.f;
        for (size_t j = 0; j < dsub; ++j) {
          const float v = cent[c * dsub + j];
          cent_t[j * kPq8Ksub + c] = v;
          s += v * v;
        }
        _cnorm[m * kPq8Ksub + c] = s;
      }
    }
    if (dsub != 1) {
      return;
    }
    _mid.resize(_pq.M * kPq8Ksub);
    _order.resize(_pq.M * kPq8Ksub);
    std::array<uint32_t, kPq8Ksub> by_value;
    for (size_t m = 0; m < _pq.M; ++m) {
      const float* cent = _pq.centroids.data() + m * kPq8Ksub;
      std::iota(by_value.begin(), by_value.end(), 0U);
      absl::c_stable_sort(
        by_value, [&](uint32_t l, uint32_t r) { return cent[l] < cent[r]; });
      float* mid = _mid.data() + m * kPq8Ksub;
      uint8_t* order = _order.data() + m * kPq8Ksub;
      for (size_t p = 0; p < kPq8Ksub; ++p) {
        order[p] = static_cast<uint8_t>(by_value[p]);
        mid[p] = p + 1 < kPq8Ksub
                   ? 0.5f * (cent[by_value[p]] + cent[by_value[p + 1]])
                   : std::numeric_limits<float>::infinity();
      }
    }
  }

  void EncodeLines(const float* vecs, size_t n, uint8_t* codes) const {
    for (size_t i = 0; i < n; ++i) {
      const float* x = vecs + i * _d;
      uint8_t* code = codes + i * _pq.M;
      for (size_t m = 0; m < _pq.M; ++m) {
        const float* mid = _mid.data() + m * kPq8Ksub;
        const float v = x[m];
        uint32_t pos = 0;
        for (uint32_t step = kPq8Ksub / 2; step != 0; step >>= 1) {
          pos += mid[pos + step - 1] < v ? step : 0;
        }
        code[m] = _order[m * kPq8Ksub + pos];
      }
    }
  }

  void EncodeRows(const float* vecs, size_t n, uint8_t* codes) {
    const size_t dsub = _pq.dsub;
    if (dsub == 1) {
      EncodeLines(vecs, n, codes);
      return;
    }
    _dist.resize(kPq8Ksub);
    for (size_t i = 0; i < n; ++i) {
      const float* x = vecs + i * _d;
      uint8_t* code = codes + i * _pq.M;
      for (size_t m = 0; m < _pq.M; ++m) {
        code[m] = static_cast<uint8_t>(
          NearestCentroid(x + m * dsub, static_cast<uint32_t>(dsub),
                          _cent_t.data() + m * kPq8Ksub * dsub,
                          _cnorm.data() + m * kPq8Ksub, _dist.data()));
      }
    }
  }

  uint32_t _d;
  faiss::ProductQuantizer _pq;
  bool _trained = false;
  std::vector<float> _cent_t;
  std::vector<float> _cnorm;
  std::vector<float> _mid;
  std::vector<uint8_t> _order;
  std::vector<float> _dist;
  std::vector<uint8_t> _codes;
  std::vector<float> _errors;
};

template<VectorMetric M>
class ProductQuantizer8Stats final : public QuantizerStats {
 public:
  ProductQuantizer8Stats(uint32_t d, std::span<const byte_type> stats) {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    const PqStatsHeader hdr = ReadPodHeader<PqStatsHeader>(stats);
    SDB_ASSERT(hdr.m != 0);
    SDB_ASSERT(d % hdr.m == 0);
    _pq.d = d;
    _pq.M = hdr.m;
    _pq.nbits = kPq8Nbits;
    _pq.set_derived_values();
    const size_t want = _pq.centroids.size() * sizeof(float);
    SDB_ASSERT(hdr.ksub == static_cast<uint32_t>(_pq.ksub));
    SDB_ASSERT(stats.size() >= sizeof(PqStatsHeader) + want);
    std::memcpy(_pq.centroids.data(), stats.data() + sizeof(PqStatsHeader),
                want);
    _norms.resize(_pq.M * _pq.ksub);
    for (size_t i = 0; i < _norms.size(); ++i) {
      _norms[i] = duckdb::NormSquaredOp::Operation(
        _pq.centroids.data() + i * _pq.dsub, _pq.dsub);
    }
    _cent_t.resize(_pq.M * _pq.dsub * kPq8Ksub);
    for (size_t m = 0; m < _pq.M; ++m) {
      const float* cent = _pq.centroids.data() + m * kPq8Ksub * _pq.dsub;
      float* cent_t = _cent_t.data() + m * _pq.dsub * kPq8Ksub;
      for (size_t c = 0; c < kPq8Ksub; ++c) {
        for (size_t j = 0; j < _pq.dsub; ++j) {
          cent_t[j * kPq8Ksub + c] = cent[c * _pq.dsub + j];
        }
      }
    }
    if (Pq8Direct(_pq.dsub, _pq.M)) {
      _cent_h.resize(_pq.centroids.size());
      for (size_t i = 0; i < _cent_h.size(); ++i) {
        _cent_h[i] = faiss::encode_fp16(_pq.centroids[i]);
      }
    }
    _errors = ReadSubspaceErrors(stats, sizeof(PqStatsHeader) + want, _pq.M);
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::PQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const faiss::ProductQuantizer& Pq() const noexcept { return _pq; }
  const float* CentroidNorms() const noexcept { return _norms.data(); }
  const float* TransposedCentroids() const noexcept { return _cent_t.data(); }
  const uint16_t* HalfCentroids() const noexcept { return _cent_h.data(); }
  std::span<const float> Errors() const noexcept { return _errors; }

 private:
  faiss::ProductQuantizer _pq;
  std::vector<float> _norms;
  std::vector<float> _cent_t;
  std::vector<uint16_t> _cent_h;
  std::vector<float> _errors;
};

template<VectorMetric M>
class ProductQuantizer8Codebook final : public QuantizerCodebook {
 public:
  ProductQuantizer8Codebook(
    std::shared_ptr<const ProductQuantizer8Stats<M>> stats,
    std::span<const float> query)
    : _stats{std::move(stats)} {
    Rekey(query);
  }

  void Rekey(std::span<const float> query) {
    const faiss::ProductQuantizer& pq = _stats->Pq();
    const size_t dsub = pq.dsub;
    _error_terms = MakePqErrorTerms(_stats->Errors(), query);
    if (Pq8Direct(dsub, pq.M)) {
      _query.assign(query.begin(), query.end());
      return;
    }
    _lut.resize(pq.M * kPq8Ksub);
    const float* cent_t = _stats->TransposedCentroids();
    const float* norms = _stats->CentroidNorms();
    for (size_t m = 0; m < pq.M; ++m) {
      const float* q = query.data() + m * dsub;
      const float* __restrict ct = cent_t + m * dsub * kPq8Ksub;
      float* __restrict out = _lut.data() + m * kPq8Ksub;
      if constexpr (M == VectorMetric::L2Sqr) {
        float q2 = 0.f;
        for (size_t j = 0; j < dsub; ++j) {
          q2 += q[j] * q[j];
        }
        const float* __restrict cn = norms + m * kPq8Ksub;
        for (size_t c = 0; c < kPq8Ksub; ++c) {
          out[c] = cn[c] + q2;
        }
        for (size_t j = 0; j < dsub; ++j) {
          const float w = -2.f * q[j];
          for (size_t c = 0; c < kPq8Ksub; ++c) {
            out[c] += w * ct[j * kPq8Ksub + c];
          }
        }
      } else {
        std::fill_n(out, kPq8Ksub, 0.f);
        for (size_t j = 0; j < dsub; ++j) {
          const float w = q[j];
          for (size_t c = 0; c < kPq8Ksub; ++c) {
            out[c] += w * ct[j * kPq8Ksub + c];
          }
        }
      }
    }
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const std::shared_ptr<const ProductQuantizer8Stats<M>>& StatsPtr()
    const noexcept {
    return _stats;
  }
  const faiss::ProductQuantizer& Pq() const noexcept { return _stats->Pq(); }
  const float* Lut() const noexcept { return _lut.data(); }
  const float* Query() const noexcept { return _query.data(); }
  const uint16_t* HalfCentroids() const noexcept {
    return _stats->HalfCentroids();
  }
  const PqErrorTerms& ErrorTerms() const noexcept { return _error_terms; }

 private:
  std::shared_ptr<const ProductQuantizer8Stats<M>> _stats;
  std::vector<float> _lut;
  std::vector<float> _query;
  PqErrorTerms _error_terms;
};

template<VectorMetric M>
class ProductQuantizer8Reader final : public QuantizerReader {
 public:
  explicit ProductQuantizer8Reader(
    std::shared_ptr<const ProductQuantizer8Codebook<M>> cb)
    : _cb{std::move(cb)},
      _cur{_cb.get()},
      _m{_cb->Pq().M},
      _dsub{_cb->Pq().dsub},
      _kernel{Pq8Direct(_dsub, _m) ? _dsub : 0} {}

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = 1, .record_size = static_cast<uint32_t>(_m)};
  }

  void StartCluster(const float* /*centroid*/) final {}

  void ComputeBlock(std::span<const byte_type> block, score_t /*threshold*/,
                    score_t* out) final {
    SDB_ASSERT(block.size() % _m == 0);
    const size_t rows = block.size() / _m;
    const auto* codes = reinterpret_cast<const uint8_t*>(block.data());
    for (size_t i = 0; i < rows; ++i) {
      out[i] = Score(codes + i * _m);
    }
  }

  void ComputeGathered(const byte_type* base, uint32_t record_size,
                       std::span<const uint32_t> ids, score_t /*threshold*/,
                       score_t* out) final {
    for (size_t s = 0; s < ids.size(); s += kSlice) {
      const auto slice = ids.subspan(s, std::min(kSlice, ids.size() - s));
      const size_t grouped = Grouped(slice.size());
      if (grouped != 0) {
        ScoreGrouped(base, record_size, slice.first(grouped), out + s);
      }
      for (size_t k = grouped; k < slice.size(); ++k) {
        out[s + k] = Score(reinterpret_cast<const uint8_t*>(
          base + static_cast<size_t>(slice[k]) * record_size));
      }
    }
  }

  void ScoreErrors(const byte_type* /*base*/, uint32_t /*record_size*/,
                   std::span<const uint32_t> ids, std::span<score_t> scores,
                   score_t* errors) final {
    PqScoreErrors<M>(_cur->ErrorTerms(), scores.first(ids.size()), errors);
  }

  void BlockErrors(std::span<const byte_type> /*block*/,
                   std::span<score_t> scores, score_t* errors) final {
    PqScoreErrors<M>(_cur->ErrorTerms(), scores, errors);
  }

  bool Decode(const byte_type* code, float* out) const final {
    const auto* c = reinterpret_cast<const uint8_t*>(code);
    switch (_dsub) {
      case 1:
        DecodeRow<1>(c, out);
        break;
      case 2:
        DecodeRow<2>(c, out);
        break;
      case 4:
        DecodeRow<4>(c, out);
        break;
      case 8:
        DecodeRow<8>(c, out);
        break;
      case 16:
        DecodeRow<16>(c, out);
        break;
      default:
        DecodeRow<0>(c, out);
        break;
    }
    return true;
  }

  bool SetQuery(std::span<const float> query) final {
    if (!_own) {
      _own =
        std::make_unique<ProductQuantizer8Codebook<M>>(_cb->StatsPtr(), query);
      _cur = _own.get();
    } else {
      _own->Rekey(query);
    }
    return true;
  }

  bool SupportsPairScores() const noexcept final { return true; }

  bool PreparePairTerms(const byte_type* base, uint32_t record_size,
                        uint64_t rows, std::vector<float>& terms) final {
    const float* norms = _cb->StatsPtr()->CentroidNorms();
    terms.assign(rows, 0.f);
    for (uint64_t row = 0; row < rows; ++row) {
      const auto* code =
        reinterpret_cast<const uint8_t*>(base + row * size_t{record_size});
      float s = 0.f;
      for (size_t j = 0; j < _m; ++j) {
        s += norms[j * kPq8Ksub + code[j]];
      }
      terms[row] = s;
    }
    return true;
  }

  void ScorePairBatch(const byte_type* base, uint32_t record_size,
                      std::span<const float> terms, uint32_t from,
                      std::span<const uint32_t> ids, score_t* out) final {
    switch (_dsub) {
      case 1:
        ScorePairs<1>(base, record_size, terms, from, ids, out);
        return;
      case 2:
        ScorePairs<2>(base, record_size, terms, from, ids, out);
        return;
      case 4:
        ScorePairs<4>(base, record_size, terms, from, ids, out);
        return;
      case 8:
        ScorePairs<8>(base, record_size, terms, from, ids, out);
        return;
      case 16:
        ScorePairs<16>(base, record_size, terms, from, ids, out);
        return;
      default:
        ScorePairs<0>(base, record_size, terms, from, ids, out);
        return;
    }
  }

 private:
  template<size_t Dsub>
  void DecodeRow(const uint8_t* code, float* out) const noexcept {
    const size_t dsub = Dsub == 0 ? _dsub : Dsub;
    const float* cents = _cb->Pq().centroids.data();
    for (size_t j = 0; j < _m; ++j) {
      std::copy_n(cents + (j * kPq8Ksub + code[j]) * dsub, dsub,
                  out + j * dsub);
    }
  }

  template<size_t Dsub>
  void ScorePairs(const byte_type* base, uint32_t record_size,
                  std::span<const float> terms, uint32_t from,
                  std::span<const uint32_t> ids, score_t* out) {
    const auto& pq = _cb->Pq();
    const size_t dsub = Dsub == 0 ? _dsub : Dsub;
    const auto* code =
      reinterpret_cast<const uint8_t*>(base + size_t{from} * record_size);
    if (code != _pair_code) {
      _pair_a.resize(pq.d);
      DecodeRow<Dsub>(code, _pair_a.data());
      _pair_code = code;
    }
    const float* cents = pq.centroids.data();
    const float* xa = _pair_a.data();
    for (size_t i = 0; i < ids.size(); ++i) {
      const auto* b = reinterpret_cast<const uint8_t*>(
        base + static_cast<size_t>(ids[i]) * record_size);
      float acc[4] = {};
      for (size_t j = 0; j < _m; ++j) {
        const float* cb = cents + (j * kPq8Ksub + b[j]) * dsub;
        const float* aj = xa + j * dsub;
        float s = 0.f;
        for (size_t k = 0; k < dsub; ++k) {
          s += aj[k] * cb[k];
        }
        acc[j & 3] += s;
      }
      const float dot = (acc[0] + acc[1]) + (acc[2] + acc[3]);
      if constexpr (M == VectorMetric::L2Sqr) {
        out[i] = -(terms[from] + terms[ids[i]] - 2.f * dot);
      } else {
        out[i] = dot;
      }
    }
  }

  static constexpr size_t kGroup = 8;
  static constexpr size_t kHalfGroup = kGroup / 2;
  static constexpr size_t kSlice = 64;
  static constexpr size_t kChunk = 32;
  static constexpr size_t kLine = 64;

  static size_t Grouped(size_t n) noexcept {
    return n / kGroup * kGroup + (n % kGroup >= kHalfGroup ? kHalfGroup : 0);
  }

  template<size_t Dsub>
  static float SubspaceScore(const float* q, const uint16_t* c) noexcept {
    float s = 0.f;
    for (size_t t = 0; t < Dsub; ++t) {
      const float v = faiss::decode_fp16(c[t]);
      if constexpr (M == VectorMetric::L2Sqr) {
        const float e = q[t] - v;
        s += e * e;
      } else {
        s += q[t] * v;
      }
    }
    return s;
  }

  template<size_t G, size_t Dsub>
  static void ScoreRows(const uint8_t* const* rows, float* acc,
                        const float* lut, const uint16_t* half, const float* q,
                        size_t m0, size_t m1) noexcept {
    const uint8_t* r[G];
    float a[G];
    for (size_t g = 0; g < G; ++g) {
      r[g] = rows[g];
      a[g] = acc[g];
    }
    for (size_t m = m0; m < m1; ++m) {
      if constexpr (Dsub == 0) {
        for (size_t g = 0; g < G; ++g) {
          a[g] += lut[r[g][m]];
        }
        lut += kPq8Ksub;
      } else {
        const float* qm = q + m * Dsub;
        for (size_t g = 0; g < G; ++g) {
          a[g] += SubspaceScore<Dsub>(qm, half + size_t{r[g][m]} * Dsub);
        }
        half += kPq8Ksub * Dsub;
      }
    }
    for (size_t g = 0; g < G; ++g) {
      acc[g] = a[g];
    }
  }

  void ScoreGrouped(const byte_type* base, uint32_t record_size,
                    std::span<const uint32_t> ids, score_t* out) const {
    switch (_kernel) {
      case 1:
        return ScoreGrouped<1>(base, record_size, ids, out);
      case 2:
        return ScoreGrouped<2>(base, record_size, ids, out);
      default:
        return ScoreGrouped<0>(base, record_size, ids, out);
    }
  }

#ifdef __AVX2__
  float GatherPairs(const uint8_t* code) const noexcept {
    const auto* table = reinterpret_cast<const int*>(_cur->HalfCentroids());
    const float* q = _cur->Query();
    const __m256i step = _mm256_set1_epi32(static_cast<int>(8 * kPq8Ksub));
    __m256i base =
      _mm256_mullo_epi32(_mm256_setr_epi32(0, 1, 2, 3, 4, 5, 6, 7),
                         _mm256_set1_epi32(static_cast<int>(kPq8Ksub)));
    __m256 acc0 = _mm256_setzero_ps();
    __m256 acc1 = _mm256_setzero_ps();
    size_t j = 0;
    for (; j + 8 <= _m; j += 8) {
      const __m256i idx = _mm256_add_epi32(
        base, _mm256_cvtepu8_epi32(
                _mm_loadl_epi64(reinterpret_cast<const __m128i*>(code + j))));
      const __m256i g = _mm256_i32gather_epi32(table, idx, 4);
      const __m256 lo = _mm256_cvtph_ps(_mm256_castsi256_si128(g));
      const __m256 hi = _mm256_cvtph_ps(_mm256_extracti128_si256(g, 1));
      const __m256 q0 = _mm256_loadu_ps(q + 2 * j);
      const __m256 q1 = _mm256_loadu_ps(q + 2 * j + 8);
      if constexpr (M == VectorMetric::L2Sqr) {
        const __m256 d0 = _mm256_sub_ps(q0, lo);
        const __m256 d1 = _mm256_sub_ps(q1, hi);
        acc0 = _mm256_fmadd_ps(d0, d0, acc0);
        acc1 = _mm256_fmadd_ps(d1, d1, acc1);
      } else {
        acc0 = _mm256_fmadd_ps(q0, lo, acc0);
        acc1 = _mm256_fmadd_ps(q1, hi, acc1);
      }
      base = _mm256_add_epi32(base, step);
    }
    const __m256 v = _mm256_add_ps(acc0, acc1);
    __m128 h =
      _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
    h = _mm_add_ps(h, _mm_movehl_ps(h, h));
    h = _mm_add_ss(h, _mm_movehdup_ps(h));
    float s = _mm_cvtss_f32(h);
    const uint16_t* half = _cur->HalfCentroids();
    for (; j < _m; ++j) {
      s += SubspaceScore<2>(q + 2 * j,
                            half + (j * kPq8Ksub + size_t{code[j]}) * 2);
    }
    if constexpr (M == VectorMetric::L2Sqr) {
      return -s;
    } else {
      return s;
    }
  }
#endif

  template<size_t Dsub>
  void ScoreGrouped(const byte_type* base, uint32_t record_size,
                    std::span<const uint32_t> ids, score_t* out) const {
    const uint8_t* rows[kSlice];
    float acc[kSlice];
    const size_t n = ids.size();
    for (size_t k = 0; k < n; ++k) {
      rows[k] = reinterpret_cast<const uint8_t*>(
        base + static_cast<size_t>(ids[k]) * record_size);
      acc[k] = 0.f;
      __builtin_prefetch(rows[k], 0, 3);
    }
#ifdef __AVX2__
    if constexpr (Dsub == 2) {
      for (size_t k = 0; k < n; ++k) {
        out[k] = GatherPairs(rows[k]);
      }
      return;
    }
#endif
    const float* lut = _cur->Lut();
    const uint16_t* half = _cur->HalfCentroids();
    const float* q = _cur->Query();
    for (size_t m0 = 0; m0 < _m; m0 += kChunk) {
      const size_t m1 = std::min(_m, m0 + kChunk);
      if (m0 % kLine == 0 && m0 + kLine < _m) {
        for (size_t k = 0; k < n; ++k) {
          __builtin_prefetch(rows[k] + m0 + kLine, 0, 3);
        }
      }
      const float* l = Dsub == 0 ? lut + m0 * kPq8Ksub : nullptr;
      const uint16_t* h = Dsub == 0 ? nullptr : half + m0 * kPq8Ksub * Dsub;
      size_t k = 0;
      for (; k + kGroup <= n; k += kGroup) {
        ScoreRows<kGroup, Dsub>(rows + k, acc + k, l, h, q, m0, m1);
      }
      if (k < n) {
        ScoreRows<kHalfGroup, Dsub>(rows + k, acc + k, l, h, q, m0, m1);
      }
    }
    for (size_t k = 0; k < n; ++k) {
      if constexpr (M == VectorMetric::L2Sqr) {
        out[k] = -acc[k];
      } else {
        out[k] = acc[k];
      }
    }
  }

  template<size_t Dsub>
  float ScoreDirect(const uint8_t* code) const noexcept {
#ifdef __AVX2__
    if constexpr (Dsub == 2) {
      return GatherPairs(code);
    }
#endif
    const uint16_t* half = _cur->HalfCentroids();
    const float* q = _cur->Query();
    float a[4] = {};
    for (size_t j = 0; j < _m; ++j, half += kPq8Ksub * Dsub) {
      a[j & 3] +=
        SubspaceScore<Dsub>(q + j * Dsub, half + size_t{code[j]} * Dsub);
    }
    const float s = (a[0] + a[1]) + (a[2] + a[3]);
    if constexpr (M == VectorMetric::L2Sqr) {
      return -s;
    } else {
      return s;
    }
  }

  float Score(const uint8_t* code) const noexcept {
    switch (_kernel) {
      case 1:
        return ScoreDirect<1>(code);
      case 2:
        return ScoreDirect<2>(code);
      default:
        break;
    }
    const float* lut = _cur->Lut();
    float a0 = 0.f;
    float a1 = 0.f;
    float a2 = 0.f;
    float a3 = 0.f;
    size_t j = 0;
    for (; j + 4 <= _m; j += 4, lut += 4 * kPq8Ksub) {
      a0 += lut[code[j]];
      a1 += lut[kPq8Ksub + code[j + 1]];
      a2 += lut[2 * kPq8Ksub + code[j + 2]];
      a3 += lut[3 * kPq8Ksub + code[j + 3]];
    }
    for (; j < _m; ++j, lut += kPq8Ksub) {
      a0 += lut[code[j]];
    }
    const float s = (a0 + a1) + (a2 + a3);
    if constexpr (M == VectorMetric::L2Sqr) {
      return -s;
    } else {
      return s;
    }
  }

  std::shared_ptr<const ProductQuantizer8Codebook<M>> _cb;
  const ProductQuantizer8Codebook<M>* _cur;
  std::unique_ptr<ProductQuantizer8Codebook<M>> _own;
  size_t _m;
  size_t _dsub;
  size_t _kernel;
  std::vector<float> _pair_a;
  const uint8_t* _pair_code = nullptr;
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> ProductQuantizer8Codebook<M>::MakeReader()
  const {
  return MakeReaderT<ProductQuantizer8Codebook<M>, ProductQuantizer8Reader<M>>(
    this);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook>
ProductQuantizer8Stats<M>::MakeCodebook(std::span<const float> query) const {
  return MakeCodebookT<ProductQuantizer8Stats<M>, ProductQuantizer8Codebook<M>>(
    this, query);
}

}  // namespace

std::unique_ptr<QuantizerWriter> MakePqWriter(VectorMetric metric, uint32_t d,
                                              uint32_t pq_m, uint32_t pq_niter,
                                              bool row_major) {
  if (row_major) {
    return MakeWriterWithMetric<ProductQuantizer8Writer>(metric, d, pq_m,
                                                         pq_niter);
  }
  return MakeWriterWithMetric<ProductQuantizerWriter>(metric, d, pq_m,
                                                      pq_niter);
}

std::shared_ptr<const QuantizerStats> MakePqStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> stats,
  bool row_major) {
  if (row_major) {
    return MakeStatsWithMetric<ProductQuantizer8Stats>(metric, d, stats);
  }
  return MakeStatsWithMetric<ProductQuantizerStats>(metric, d, stats);
}

}  // namespace irs::quantizers
