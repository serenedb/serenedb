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

#include "iresearch/formats/ivf/quantizer.hpp"

#include <faiss/impl/ProductQuantizer.h>
#include <faiss/impl/RaBitQUtils.h>
#include <faiss/impl/RaBitQuantizerMultiBit.h>
#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/impl/fast_scan/fast_scan.h>
#include <faiss/utils/AlignedTable.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/quantize_lut.h>
#include <faiss/utils/random.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <vector>

#include "basics/assert.h"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {
namespace {

faiss::ScalarQuantizer::QuantizerType FaissScalarType(
  VectorQuantization quant) {
  switch (quant) {
    case VectorQuantization::SQ4:
      return faiss::ScalarQuantizer::QuantizerType::QT_4bit;
    default:
      return faiss::ScalarQuantizer::QuantizerType::QT_8bit;
  }
}

std::span<const byte_type> FloatSpan(const std::vector<float>& v) noexcept {
  return {reinterpret_cast<const byte_type*>(v.data()),
          v.size() * sizeof(float)};
}

constexpr size_t kFastScanBbs = 32;
constexpr uint32_t kPqNbits = 4;
constexpr size_t kFastScanBits = 4;
constexpr size_t kFastScanKsub = size_t{1} << kFastScanBits;

size_t FastScanNsq(size_t m) noexcept { return m + (m & 1); }

constexpr int64_t kRaBitQRotationSeed = 0x5a17b17c5eed5eedULL;

uint32_t RotatedDim(uint32_t d) noexcept {
  return std::max<uint32_t>(kFastScanBits, std::bit_ceil(d));
}

void GenerateSigns(uint32_t rotated_d, int64_t seed,
                   std::vector<float>& signs) {
  signs.resize(rotated_d);
  faiss::float_randn(signs.data(), signs.size(), seed);
  for (uint32_t i = 0; i < rotated_d; ++i) {
    signs[i] = signs[i] < 0.f ? -1.f : 1.f;
  }
}

// In-place Fast Walsh-Hadamard transform; len must be a power of two.
void Fwht(float* a, uint32_t len) noexcept {
  for (uint32_t h = 1; h < len; h <<= 1) {
    for (uint32_t i = 0; i < len; i += (h << 1)) {
      for (uint32_t j = i; j < i + h; ++j) {
        const float x = a[j];
        const float y = a[j + h];
        a[j] = x + y;
        a[j + h] = x - y;
      }
    }
  }
}

void RotateInto(const float* signs, const float* in, float* out, uint32_t d,
                uint32_t rotated_d) noexcept {
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = in[i] * signs[i];
  }
  for (uint32_t i = d; i < rotated_d; ++i) {
    out[i] = 0.f;
  }
  Fwht(out, rotated_d);
  const float scale = 1.f / std::sqrt(static_cast<float>(rotated_d));
  for (uint32_t i = 0; i < rotated_d; ++i) {
    out[i] *= scale;
  }
}

template<typename H>
void WritePodHeader(const H& h, byte_type* out) noexcept {
  std::memcpy(out, &h, sizeof(H));
}

template<typename H>
H ReadPodHeader(std::span<const byte_type> in) noexcept {
  H h{};
  if (in.size() >= sizeof(H)) {
    std::memcpy(&h, in.data(), sizeof(H));
  }
  return h;
}

struct PqStatsHeader {
  uint32_t m;
  uint32_t ksub;
};

struct RaBitQStatsHeader {
  uint32_t nb_bits;
  uint32_t d;
};

constexpr uint8_t kRaBitQQueryBits = 8;
constexpr bool kRaBitQCentered = false;
constexpr size_t kRaBitQRefinePool = 64;

class ScalarQuantizerWriter final : public QuantizerWriter {
 public:
  ScalarQuantizerWriter(uint32_t d, VectorQuantization quant)
    : _d{d},
      _quant{quant},
      _sq{d, FaissScalarType(quant)},
      _vmin(d, std::numeric_limits<float>::max()),
      _vmax(d, std::numeric_limits<float>::lowest()) {
    _sq.trained.assign(2 * static_cast<size_t>(_d), 0.f);
  }

  void Train(const float* vecs, size_t n) final {
    for (size_t i = 0; i < n; ++i) {
      const float* v = vecs + i * _d;
      for (uint32_t j = 0; j < _d; ++j) {
        _vmin[j] = std::min(_vmin[j], v[j]);
        _vmax[j] = std::max(_vmax[j], v[j]);
      }
    }
    for (uint32_t j = 0; j < _d; ++j) {
      const bool seen = _vmin[j] <= _vmax[j];
      _sq.trained[j] = seen ? _vmin[j] : 0.f;
      _sq.trained[_d + j] = seen ? (_vmax[j] - _vmin[j]) : 0.f;
    }
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    if (n == 0) {
      return;
    }
    _code.resize(n * _sq.code_size);
    _sq.compute_codes(vecs, _code.data(), n);
    out.WriteData(_code.data(), _code.size());
  }

  std::span<const byte_type> StatsBytes() const final {
    return FloatSpan(_sq.trained);
  }

  VectorQuantization Kind() const noexcept final { return _quant; }

  uint32_t CodeSize() const noexcept final {
    return static_cast<uint32_t>(_sq.code_size);
  }

 private:
  uint32_t _d;
  VectorQuantization _quant;
  faiss::ScalarQuantizer _sq;
  std::vector<float> _vmin;
  std::vector<float> _vmax;
  std::vector<uint8_t> _code;
};

template<VectorMetric M>
class ScalarQuantizerStats final : public QuantizerStats {
 public:
  ScalarQuantizerStats(uint32_t d, VectorQuantization quant,
                       std::span<const byte_type> stats)
    : _sq{d, FaissScalarType(quant)}, _quant{quant} {
    _sq.trained.assign(2 * static_cast<size_t>(d), 0.f);
    const size_t want = _sq.trained.size() * sizeof(float);
    if (stats.size() >= want) {
      std::memcpy(_sq.trained.data(), stats.data(), want);
    }
  }

  VectorQuantization Kind() const noexcept final { return _quant; }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const faiss::ScalarQuantizer& Sq() const noexcept { return _sq; }

 private:
  faiss::ScalarQuantizer _sq;
  VectorQuantization _quant;
};

template<VectorMetric M>
class ScalarQuantizerCodebook final : public QuantizerCodebook {
 public:
  ScalarQuantizerCodebook(std::shared_ptr<const ScalarQuantizerStats<M>> stats,
                          std::span<const float> query)
    : _stats{std::move(stats)}, _query(query.begin(), query.end()) {}

  std::unique_ptr<QuantizerReader> MakeReader(
    std::unique_ptr<IndexInput> pay_in, uint64_t pay_base) const final;

  const faiss::ScalarQuantizer& Sq() const noexcept { return _stats->Sq(); }
  std::span<const float> Query() const noexcept { return _query; }

 private:
  std::shared_ptr<const ScalarQuantizerStats<M>> _stats;
  std::vector<float> _query;
};

template<VectorMetric M>
class ScalarQuantizerReader final : public QuantizerReader {
 public:
  ScalarQuantizerReader(std::shared_ptr<const ScalarQuantizerCodebook<M>> cb,
                        std::unique_ptr<IndexInput> pay_in, uint64_t pay_base)
    : _cb{std::move(cb)},
      _pay_in{std::move(pay_in)},
      _codes_reader{*_pay_in, static_cast<uint32_t>(_cb->Sq().code_size)} {
    _codes_reader.Reset(pay_base);
    _dc.reset(_cb->Sq().get_distance_computer(
      M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                               : faiss::MetricType::METRIC_INNER_PRODUCT));
    _dc->code_size = _cb->Sq().code_size;
    _dc->set_query(_cb->Query().data());
  }

  // Scalar codes are one fixed-width record per lane, so a cluster needs no
  // more than where its lanes begin.
  void StartCluster(uint64_t first_lane, size_t num_docs,
                    const float* /*centroid*/) final {
    _first_lane = first_lane;
    _n = num_docs;
  }

  void ComputeBlock(size_t offset, size_t count, score_t* out) final {
    SDB_ASSERT(_dc);
    SDB_ASSERT(offset + count <= _n);
    if (count == 0) {
      return;
    }
    const byte_type* block = _codes_reader.Read(_first_lane + offset, count);
    _dc->codes = block;
    const size_t cs = _cb->Sq().code_size;
    size_t i = 0;
    for (; i + 3 < count; i += 4) {
      _dc->distances_batch_4(i, i + 1, i + 2, i + 3, out[i], out[i + 1],
                             out[i + 2], out[i + 3]);
      if constexpr (M == VectorMetric::L2Sqr) {
        out[i] = -out[i];
        out[i + 1] = -out[i + 1];
        out[i + 2] = -out[i + 2];
        out[i + 3] = -out[i + 3];
      }
    }
    for (; i < count; i++) {
      const auto d = _dc->distance_to_code(block + i * cs);
      out[i] = M == VectorMetric::L2Sqr ? -d : d;
    }
  }

 private:
  std::shared_ptr<const ScalarQuantizerCodebook<M>> _cb;
  std::unique_ptr<IndexInput> _pay_in;
  std::unique_ptr<faiss::ScalarQuantizer::SQDistanceComputer> _dc;
  VectorBlockReader _codes_reader;
  uint64_t _first_lane = 0;
  size_t _n = 0;
};

template<class Codebook, class Reader>
std::unique_ptr<QuantizerReader> MakeReaderT(const Codebook* self,
                                             std::unique_ptr<IndexInput> in,
                                             uint64_t pay_base) {
  return std::make_unique<Reader>(
    std::static_pointer_cast<const Codebook>(self->shared_from_this()),
    std::move(in), pay_base);
}

template<class Stats, class Codebook>
std::shared_ptr<const QuantizerCodebook> MakeCodebookT(
  const Stats* self, std::span<const float> query) {
  return std::make_shared<const Codebook>(
    std::static_pointer_cast<const Stats>(self->shared_from_this()), query);
}

template<VectorMetric M>
std::unique_ptr<QuantizerReader> ScalarQuantizerCodebook<M>::MakeReader(
  std::unique_ptr<IndexInput> pay_in, uint64_t pay_base) const {
  return MakeReaderT<ScalarQuantizerCodebook<M>, ScalarQuantizerReader<M>>(
    this, std::move(pay_in), pay_base);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> ScalarQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<ScalarQuantizerStats<M>, ScalarQuantizerCodebook<M>>(
    this, query);
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
    BuildStats();
    _trained = true;
  }

  void SetClusterCentroid(const float* centroid) final {
    _centroid.assign(centroid, centroid + _d);
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    if (n == 0) {
      return;
    }
    SDB_ASSERT(_trained);
    SDB_ASSERT(_centroid.size() == _d);
    const size_t cs = _pq.code_size;
    if (_lane_codes.empty()) {
      _lane_codes.assign(kFastScanBbs * cs, 0);
      if constexpr (M == VectorMetric::L2Sqr) {
        _lane_norms.assign(kFastScanBbs, 0.f);
      }
    }
    _res.resize(_d);
    if constexpr (M == VectorMetric::L2Sqr) {
      _dec.resize(_d);
    }
    for (size_t i = 0; i < n; ++i) {
      const float* v = vecs + i * _d;
      for (uint32_t j = 0; j < _d; ++j) {
        _res[j] = v[j] - _centroid[j];
      }
      uint8_t* code = _lane_codes.data() + _filled * cs;
      _pq.compute_code(_res.data(), code);
      if constexpr (M == VectorMetric::L2Sqr) {
        _pq.decode(code, _dec.data());
        for (uint32_t j = 0; j < _d; ++j) {
          _dec[j] += _centroid[j];
        }
        _lane_norms[_filled] = vector::L2Space<float, float, float>::Norm(
          reinterpret_cast<const byte_type*>(_dec.data()),
          static_cast<uint16_t>(_d));
      }
      if (++_filled == kFastScanBbs) {
        FlushPack(out);
      }
    }
  }

  void Finish(IndexOutput& out) final {
    if (_filled != 0) {
      FlushPack(out);
    }
  }

  std::span<const byte_type> StatsBytes() const final {
    return {_stats.data(), _stats.size()};
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::PQ;
  }

  uint32_t CodeSize() const noexcept final {
    return static_cast<uint32_t>(_pq.code_size);
  }

 private:
  // A pack is the 32 lanes' packed codes followed, for L2, by their
  // reconstructed norms. The stream's last pack pads its codes -- the scan
  // reads all 32 lanes of them -- but not its norms: those are addressed one
  // lane at a time and a lane that holds no document is never read.
  void FlushPack(IndexOutput& out) {
    const size_t m = _pq.M;
    const size_t nsq = FastScanNsq(m);
    _packed.resize(kFastScanBbs * nsq / 2);
    faiss::pq4_pack_codes(_lane_codes.data(), _filled, m, kFastScanBbs,
                          kFastScanBbs, nsq, _packed.data());
    out.WriteData(_packed.data(), _packed.size());
    if constexpr (M == VectorMetric::L2Sqr) {
      out.WriteData(reinterpret_cast<const byte_type*>(_lane_norms.data()),
                    _filled * sizeof(float));
    }
    _filled = 0;
  }

  void BuildStats() {
    const uint32_t m = static_cast<uint32_t>(_pq.M);
    const uint32_t ksub = static_cast<uint32_t>(_pq.ksub);
    _stats.resize(sizeof(PqStatsHeader) + _pq.centroids.size() * sizeof(float));
    WritePodHeader(PqStatsHeader{m, ksub}, _stats.data());
    std::memcpy(_stats.data() + sizeof(PqStatsHeader), _pq.centroids.data(),
                _pq.centroids.size() * sizeof(float));
  }

  uint32_t _d;
  faiss::ProductQuantizer _pq;
  bool _trained = false;
  std::vector<byte_type> _stats;
  std::vector<float> _centroid;
  // The stream's open pack: up to 32 unpacked codes, flushed when full and
  // once, padded, at `Finish`.
  std::vector<uint8_t> _lane_codes;
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr,
                                    std::vector<float>> _lane_norms;
  size_t _filled = 0;
  std::vector<float> _res;
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr,
                                    std::vector<float>> _dec;
  std::vector<uint8_t> _packed;
};

template<VectorMetric M>
class ProductQuantizerStats final : public QuantizerStats {
 public:
  ProductQuantizerStats(uint32_t d, std::span<const byte_type> stats) {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    const PqStatsHeader hdr = ReadPodHeader<PqStatsHeader>(stats);
    if (hdr.m != 0 && d % hdr.m == 0 && hdr.ksub != 0) {
      _pq.d = d;
      _pq.M = hdr.m;
      _pq.nbits = kPqNbits;
      _pq.set_derived_values();
      const size_t want = _pq.centroids.size() * sizeof(float);
      if (hdr.ksub == static_cast<uint32_t>(_pq.ksub) &&
          stats.size() >= sizeof(PqStatsHeader) + want) {
        std::memcpy(_pq.centroids.data(), stats.data() + sizeof(PqStatsHeader),
                    want);
        _valid = true;
      }
    }
  }

  bool Valid() const noexcept { return _valid; }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::PQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const faiss::ProductQuantizer& Pq() const noexcept { return _pq; }

 private:
  faiss::ProductQuantizer _pq;
  bool _valid = false;
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
    const size_t nsq = FastScanNsq(pq.M);
    std::vector<float> ip_table(static_cast<size_t>(pq.M) * ksub);
    pq.compute_inner_prod_table(_query.data(), ip_table.data());
    std::vector<byte_type> lutq(nsq * ksub);
    faiss::quantize_lut::quantize_LUT_and_bias(
      1, pq.M, ksub, false, ip_table.data(), nullptr, lutq.data(), nsq, nullptr,
      &_ip_a, &_ip_b);
    _packed_ip_lut.resize(nsq * ksub);
    faiss::pq4_pack_LUT(1, static_cast<int>(nsq), lutq.data(),
                        _packed_ip_lut.data());
    if constexpr (M == VectorMetric::L2Sqr) {
      _query_norm2 = vector::L2Space<float, float, float>::Norm(
        reinterpret_cast<const byte_type*>(_query.data()),
        static_cast<uint16_t>(_query.size()));
    }
  }

  std::unique_ptr<QuantizerReader> MakeReader(
    std::unique_ptr<IndexInput> pay_in, uint64_t pay_base) const final;

  const faiss::ProductQuantizer& Pq() const noexcept { return _stats->Pq(); }
  std::span<const float> Query() const noexcept { return _query; }
  const uint8_t* PackedIpLut() const noexcept { return _packed_ip_lut.data(); }
  float IpA() const noexcept { return _ip_a; }
  float IpB() const noexcept { return _ip_b; }
  float QueryNorm() const noexcept
    requires(M == VectorMetric::L2Sqr)
  {
    return _query_norm2;
  }

 private:
  std::shared_ptr<const ProductQuantizerStats<M>> _stats;
  std::vector<float> _query;
  faiss::AlignedTable<uint8_t> _packed_ip_lut;
  float _ip_a = 1.f;
  float _ip_b = 0.f;
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr, float>
    _query_norm2;
};

template<VectorMetric M>
class ProductQuantizerReader final : public QuantizerReader {
 public:
  ProductQuantizerReader(std::shared_ptr<const ProductQuantizerCodebook<M>> cb,
                         std::unique_ptr<IndexInput> pay_in, uint64_t pay_base)
    : _cb{std::move(cb)}, _pay_in{std::move(pay_in)}, _pay_base{pay_base} {}

  void StartCluster(uint64_t first_lane, size_t num_docs,
                    const float* centroid) final {
    _n = num_docs;
    if (_n == 0) {
      return;
    }
    SDB_ASSERT(centroid != nullptr);
    const faiss::ProductQuantizer& pq = _cb->Pq();
    const size_t nsq = FastScanNsq(pq.M);
    const std::span<const float> query = _cb->Query();

    // IP(q, c + r) = IP(q, c) + IP(q, r); the packed LUT for IP(q, r) is
    // query-only and precomputed once per query in the codebook.
    const float qc = ComputeDistance<VectorMetric::InnerProduct>(
      query.data(), centroid, static_cast<uint16_t>(query.size()));

    // The cluster's lanes span packs it may share with its neighbours: the
    // whole span is one contiguous read, every lane is accumulated, and the
    // out-of-cluster lanes at both edges are skipped by indexing -- a code's
    // accumulation depends on nothing outside its own lane. The last pack is
    // read only as far as the cluster's last lane, since norms past it may not
    // have been written.
    const size_t pack_bytes = kFastScanBbs * nsq / 2;
    const size_t norm_size = M == VectorMetric::L2Sqr ? sizeof(float) : 0;
    const size_t stride = pack_bytes + kFastScanBbs * norm_size;
    const uint64_t last_lane = first_lane + _n - 1;
    const uint64_t p0 = first_lane / kFastScanBbs;
    const size_t packs = static_cast<size_t>(last_lane / kFastScanBbs - p0 + 1);
    const size_t bytes = (packs - 1) * stride + pack_bytes +
                         (last_lane % kFastScanBbs + 1) * norm_size;
    const uint64_t at = _pay_base + p0 * stride;
    const byte_type* base = _pay_in->ReadVolatile(at, bytes);
    if (!base) {
      _codes_buf.resize(bytes);
      _pay_in->ReadData(at, _codes_buf.data(), bytes);
      base = _codes_buf.data();
    }
    _accu.resize(packs * kFastScanBbs);
    for (size_t p = 0; p != packs; ++p) {
      faiss::accumulate_to_mem(1, kFastScanBbs, static_cast<int>(nsq),
                               base + p * stride, _cb->PackedIpLut(),
                               _accu.data() + p * kFastScanBbs);
    }

    const size_t lane0 = static_cast<size_t>(first_lane - p0 * kFastScanBbs);
    _scores.resize(_n);
    const float inv_a = 1.f / _cb->IpA();
    const float b = _cb->IpB();
    if constexpr (M == VectorMetric::L2Sqr) {
      const float q2 = _cb->QueryNorm();
      for (size_t i = 0; i < _n; ++i) {
        const size_t lane = lane0 + i;
        const float ip = static_cast<float>(_accu[lane]) * inv_a + b;
        float norm;
        std::memcpy(&norm,
                    base + (lane / kFastScanBbs) * stride + pack_bytes +
                      (lane % kFastScanBbs) * sizeof(float),
                    sizeof(float));
        _scores[i] = -(q2 - 2.f * qc - 2.f * ip + norm);
      }
    } else {
      for (size_t i = 0; i < _n; ++i) {
        _scores[i] = static_cast<float>(_accu[lane0 + i]) * inv_a + b + qc;
      }
    }
  }

  void ComputeBlock(size_t offset, size_t count, score_t* out) final {
    SDB_ASSERT(offset + count <= _n);
    std::memcpy(out, _scores.data() + offset, count * sizeof(score_t));
  }

 private:
  std::shared_ptr<const ProductQuantizerCodebook<M>> _cb;
  std::unique_ptr<IndexInput> _pay_in;
  uint64_t _pay_base;
  std::vector<byte_type> _codes_buf;
  faiss::AlignedTable<uint16_t> _accu;
  std::vector<score_t> _scores;
  size_t _n = 0;
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> ProductQuantizerCodebook<M>::MakeReader(
  std::unique_ptr<IndexInput> pay_in, uint64_t pay_base) const {
  return MakeReaderT<ProductQuantizerCodebook<M>, ProductQuantizerReader<M>>(
    this, std::move(pay_in), pay_base);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> ProductQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<ProductQuantizerStats<M>, ProductQuantizerCodebook<M>>(
    this, query);
}

template<VectorMetric M>
class RaBitQuantizerWriter final : public QuantizerWriter {
 public:
  RaBitQuantizerWriter(uint32_t d, uint32_t nb_bits)
    : _d{d},
      _rd{RotatedDim(d)},
      _nb_bits{nb_bits},
      _ex_bits{nb_bits - 1},
      _storage{
        faiss::rabitq_utils::compute_per_vector_storage_size(nb_bits, _rd)},
      _ex_code_size{(static_cast<size_t>(_rd) * _ex_bits + 7) / 8},
      _record{_storage + sizeof(float)},
      _m{_rd / kFastScanBits},
      _nsq{FastScanNsq(_rd / kFastScanBits)},
      _sign_stride{FastScanNsq(_rd / kFastScanBits) / 2} {
    GenerateSigns(_rd, kRaBitQRotationSeed, _signs);
    _stats.resize(sizeof(RaBitQStatsHeader));
    WritePodHeader(RaBitQStatsHeader{nb_bits, d}, _stats.data());
  }

  void Train(const float* /*vecs*/, size_t /*n*/) final {}

  void SetClusterCentroid(const float* centroid) final {
    _centroid.resize(_rd);
    RotateInto(_signs.data(), centroid, _centroid.data(), _d, _rd);
    _centroid_sum = 0.f;
    for (uint32_t j = 0; j < _rd; ++j) {
      _centroid_sum += _centroid[j];
    }
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    if (n == 0) {
      return;
    }
    SDB_ASSERT(_centroid.size() == _rd);
    if (_lane_signs.empty()) {
      _lane_signs.resize(kFastScanBbs * _sign_stride);
      _lane_aux.resize(kFastScanBbs * _record);
    }
    const float inv_rd_sqrt = 1.f / std::sqrt(static_cast<float>(_rd));
    _rotated.resize(_rd);
    _residual.resize(_rd);
    for (size_t i = 0; i < n; ++i) {
      RotateInto(_signs.data(), vecs + i * _d, _rotated.data(), _d, _rd);
      uint8_t* sign = _lane_signs.data() + _filled * _sign_stride;
      std::memset(sign, 0, _sign_stride);
      float cs_sum = 0.f;
      for (uint32_t j = 0; j < _rd; ++j) {
        _residual[j] = _rotated[j] - _centroid[j];
        if (_residual[j] > 0.f) {
          faiss::rabitq_utils::set_bit_fastscan(sign, j);
          cs_sum += _centroid[j];
        }
      }
      uint8_t* aux = _lane_aux.data() + _filled * _record;
      const faiss::rabitq_utils::SignBitFactorsWithError f =
        faiss::rabitq_utils::compute_vector_factors(
          _rotated.data(), _rd, _centroid.data(),
          M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                                   : faiss::MetricType::METRIC_INNER_PRODUCT,
          /*compute_error=*/_ex_bits > 0);
      if (_ex_bits == 0) {
        std::memcpy(aux, &f, sizeof(faiss::rabitq_utils::SignBitFactors));
      } else {
        std::memcpy(aux, &f,
                    sizeof(faiss::rabitq_utils::SignBitFactorsWithError));
        uint8_t* ex_code =
          aux + sizeof(faiss::rabitq_utils::SignBitFactorsWithError);
        faiss::rabitq_utils::ExtraBitsFactors ex;
        faiss::rabitq_multibit::quantize_ex_bits(
          _residual.data(), _rd, _nb_bits, ex_code, ex,
          M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                                   : faiss::MetricType::METRIC_INNER_PRODUCT,
          _centroid.data());
        std::memcpy(ex_code + _ex_code_size, &ex,
                    sizeof(faiss::rabitq_utils::ExtraBitsFactors));
      }
      const float cs = (2.f * cs_sum - _centroid_sum) * inv_rd_sqrt;
      std::memcpy(aux + _storage, &cs, sizeof(float));
      if (++_filled == kFastScanBbs) {
        FlushPack(out);
      }
    }
  }

  void Finish(IndexOutput& out) final {
    if (_filled != 0) {
      FlushPack(out);
    }
  }

  std::span<const byte_type> StatsBytes() const final {
    return {_stats.data(), _stats.size()};
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::RaBitQ;
  }

  uint32_t CodeSize() const noexcept final {
    return static_cast<uint32_t>(_storage);
  }

 private:
  // A pack is the 32 lanes' packed sign codes followed by their per-lane
  // records -- the faiss factors plus the centroid-sum term -- which is the
  // block layout `get_block_aux_ptr` addresses. The stream's last pack pads
  // its codes -- the scan reads all 32 lanes of them -- but not its records:
  // those are addressed one lane at a time and a lane that holds no document
  // is never read.
  void FlushPack(IndexOutput& out) {
    _packed.resize(kFastScanBbs * _nsq / 2);
    faiss::pq4_pack_codes(_lane_signs.data(), _filled, _m, kFastScanBbs,
                          kFastScanBbs, _nsq, _packed.data());
    out.WriteData(_packed.data(), _packed.size());
    out.WriteData(_lane_aux.data(), _filled * _record);
    _filled = 0;
  }

  uint32_t _d;
  uint32_t _rd;
  uint32_t _nb_bits;
  uint32_t _ex_bits;
  size_t _storage;
  size_t _ex_code_size;
  size_t _record;
  size_t _m;
  size_t _nsq;
  size_t _sign_stride;
  std::vector<float> _signs;
  std::vector<float> _centroid;
  float _centroid_sum = 0.f;
  std::vector<byte_type> _stats;
  std::vector<float> _rotated;
  std::vector<float> _residual;
  // The stream's open pack: up to 32 unpacked sign codes and their records,
  // flushed when full and once, partly padded, at `Finish`.
  std::vector<uint8_t> _lane_signs;
  std::vector<uint8_t> _lane_aux;
  std::vector<uint8_t> _packed;
  size_t _filled = 0;
};

template<VectorMetric M>
class RaBitQuantizerStats final : public QuantizerStats {
 public:
  RaBitQuantizerStats(uint32_t d, std::span<const byte_type> stats) {
    const RaBitQStatsHeader hdr = ReadPodHeader<RaBitQStatsHeader>(stats);
    if (hdr.nb_bits >= kRaBitQMinBits && hdr.nb_bits <= kRaBitQMaxBits &&
        hdr.d == d && stats.size() >= sizeof(RaBitQStatsHeader)) {
      _d = d;
      _rd = RotatedDim(d);
      _nb_bits = hdr.nb_bits;
      GenerateSigns(_rd, kRaBitQRotationSeed, _signs);
      _valid = true;
    }
  }

  bool Valid() const noexcept { return _valid; }
  uint32_t NbBits() const noexcept { return _nb_bits; }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::RaBitQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const std::vector<float>& Signs() const noexcept { return _signs; }
  uint32_t SrcDim() const noexcept { return _d; }
  uint32_t RotDim() const noexcept { return _rd; }

 private:
  uint32_t _d = 0;
  uint32_t _rd = 0;
  uint32_t _nb_bits = 0;
  std::vector<float> _signs;
  bool _valid = false;
};

template<VectorMetric M>
class RaBitQuantizerCodebook final : public QuantizerCodebook {
 public:
  RaBitQuantizerCodebook(std::shared_ptr<const RaBitQuantizerStats<M>> stats,
                         std::span<const float> query)
    : _stats{std::move(stats)}, _query(query.begin(), query.end()) {
    static_assert(!kRaBitQCentered);
    const size_t rd = _stats->RotDim();
    _rotated_query.resize(rd);
    RotateInto(_stats->Signs().data(), _query.data(), _rotated_query.data(),
               _stats->SrcDim(), static_cast<uint32_t>(rd));

    std::vector<float> tmp_q;
    std::vector<uint8_t> qq;
    _qf = faiss::rabitq_utils::compute_query_factors(
      _rotated_query.data(), rd, /*centroid=*/nullptr, kRaBitQQueryBits,
      kRaBitQCentered,
      M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                               : faiss::MetricType::METRIC_INNER_PRODUCT,
      tmp_q, qq);

    const size_t m = rd / kFastScanBits;
    const size_t nsq = FastScanNsq(m);
    std::vector<float> lut(m * kFastScanKsub);
    for (size_t mi = 0; mi < m; ++mi) {
      const size_t dim_start = mi * kFastScanBits;
      for (size_t code = 0; code < kFastScanKsub; ++code) {
        float ip = 0.f;
        int pc = 0;
        for (size_t off = 0; off < kFastScanBits; ++off) {
          if ((code >> off) & 1) {
            ip += qq[dim_start + off];
            ++pc;
          }
        }
        lut[mi * kFastScanKsub + code] =
          _qf.c1 * ip + _qf.c2 * static_cast<float>(pc);
      }
    }
    std::vector<uint8_t> lutq(nsq * kFastScanKsub);
    faiss::quantize_lut::quantize_LUT_and_bias(1, m, kFastScanKsub, false,
                                               lut.data(), nullptr, lutq.data(),
                                               nsq, nullptr, &_a, &_b);
    _packed_lut.resize(nsq * kFastScanKsub);
    faiss::pq4_pack_LUT(1, static_cast<int>(nsq), lutq.data(),
                        _packed_lut.data());
  }

  std::unique_ptr<QuantizerReader> MakeReader(
    std::unique_ptr<IndexInput> pay_in, uint64_t pay_base) const final;

  const std::vector<float>& Signs() const noexcept { return _stats->Signs(); }
  const std::vector<float>& RotatedQuery() const noexcept {
    return _rotated_query;
  }
  std::span<const float> Query() const noexcept { return _query; }
  uint32_t SrcDim() const noexcept { return _stats->SrcDim(); }
  uint32_t RotDim() const noexcept { return _stats->RotDim(); }
  uint32_t NbBits() const noexcept { return _stats->NbBits(); }
  const faiss::rabitq_utils::QueryFactorsData& QueryFactors() const noexcept {
    return _qf;
  }
  const uint8_t* PackedLut() const noexcept { return _packed_lut.data(); }
  float A() const noexcept { return _a; }
  float B() const noexcept { return _b; }

 private:
  std::shared_ptr<const RaBitQuantizerStats<M>> _stats;
  std::vector<float> _query;
  std::vector<float> _rotated_query;
  faiss::rabitq_utils::QueryFactorsData _qf;
  faiss::AlignedTable<uint8_t> _packed_lut;
  float _a = 1.f;
  float _b = 0.f;
};

template<VectorMetric M>
class RaBitQuantizerReader final : public QuantizerReader {
 public:
  RaBitQuantizerReader(std::shared_ptr<const RaBitQuantizerCodebook<M>> cb,
                       std::unique_ptr<IndexInput> pay_in, uint64_t pay_base)
    : _cb{std::move(cb)},
      _pay_in{std::move(pay_in)},
      _pay_base{pay_base},
      _rd{_cb->RotDim()},
      _nsq{FastScanNsq(_rd / kFastScanBits)},
      _ex_bits{_cb->NbBits() - 1},
      _storage{faiss::rabitq_utils::compute_per_vector_storage_size(
        _cb->NbBits(), _rd)},
      _ex_code_size{(static_cast<size_t>(_rd) * _ex_bits + 7) / 8},
      _record{_storage + sizeof(float)} {}

  void StartCluster(uint64_t first_lane, size_t num_docs,
                    const float* centroid) final {
    _n = num_docs;
    if (_n == 0) {
      return;
    }
    SDB_ASSERT(centroid);
    const std::span<const float> query = _cb->Query();
    const auto d = static_cast<uint16_t>(query.size());

    faiss::rabitq_utils::QueryFactorsData qf = _cb->QueryFactors();
    qf.qr_to_c_L2sqr =
      -ComputeDistance<VectorMetric::L2Sqr>(query.data(), centroid, d);
    qf.g_error = std::sqrt(qf.qr_to_c_L2sqr);
    if constexpr (M == VectorMetric::InnerProduct) {
      qf.q_dot_c =
        ComputeDistance<VectorMetric::InnerProduct>(query.data(), centroid, d);
    }

    // The cluster's lanes span packs it may share with its neighbours: the
    // whole span is one contiguous read, and the out-of-cluster lanes at both
    // edges are skipped by indexing -- a lane's score depends on nothing
    // outside itself. The last pack is read only as far as the cluster's last
    // lane, since records past it may not have been written.
    const size_t pack_bytes = kFastScanBbs * _nsq / 2;
    const size_t stride = pack_bytes + kFastScanBbs * _record;
    const uint64_t last_lane = first_lane + _n - 1;
    const uint64_t p0 = first_lane / kFastScanBbs;
    const size_t packs = static_cast<size_t>(last_lane / kFastScanBbs - p0 + 1);
    const size_t bytes = (packs - 1) * stride + pack_bytes +
                         (last_lane % kFastScanBbs + 1) * _record;
    const uint64_t at = _pay_base + p0 * stride;
    const byte_type* base = _pay_in->ReadVolatile(at, bytes);
    if (!base) {
      _buf.resize(bytes);
      _pay_in->ReadData(at, _buf.data(), bytes);
      base = _buf.data();
    }

    // Pass 1: the approximate score of a document depends only on its own code
    // and record, so it is the same wherever the stream put it.
    const size_t lane0 = static_cast<size_t>(first_lane - p0 * kFastScanBbs);
    _accu.resize(packs * kFastScanBbs);
    for (size_t p = 0; p != packs; ++p) {
      faiss::accumulate_to_mem(1, kFastScanBbs, static_cast<int>(_nsq),
                               base + p * stride, _cb->PackedLut(),
                               _accu.data() + p * kFastScanBbs);
    }

    _scores.resize(_n);
    const float inv_a = 1.f / _cb->A();
    const float b = _cb->B();
    for (size_t i = 0; i < _n; ++i) {
      const size_t lane = lane0 + i;
      const byte_type* rec = faiss::rabitq_utils::get_block_aux_ptr(
        base, lane, kFastScanBbs, pack_bytes, stride, _record);
      float cs;
      std::memcpy(&cs, rec + _storage, sizeof(float));
      const float normalized = static_cast<float>(_accu[lane]) * inv_a + b - cs;
      const auto* fac =
        reinterpret_cast<const faiss::rabitq_utils::SignBitFactors*>(rec);
      _scores[i] = faiss::rabitq_utils::compute_1bit_adjusted_distance(
        normalized, *fac, qf, kRaBitQCentered, kRaBitQQueryBits, _rd);
    }

    // Pass 2: the refine pool is a property of the CLUSTER -- one threshold
    // over its whole score range, so the refined candidate set is exactly the
    // one an uninterleaved cluster would have picked.
    if (_ex_bits > 0) {
      const size_t pool = kRaBitQRefinePool;
      float threshold;
      if (pool >= _n) {
        threshold = M != VectorMetric::L2Sqr
                      ? std::numeric_limits<float>::lowest()
                      : std::numeric_limits<float>::max();
      } else {
        _threshold_tmp.assign(_scores.begin(), _scores.end());
        auto kth =
          _threshold_tmp.begin() + static_cast<std::ptrdiff_t>(pool - 1);
        if constexpr (M != VectorMetric::L2Sqr) {
          std::nth_element(_threshold_tmp.begin(), kth, _threshold_tmp.end(),
                           std::greater<float>{});
        } else {
          std::nth_element(_threshold_tmp.begin(), kth, _threshold_tmp.end());
        }
        threshold = *kth;
      }
      _sign_bits.resize((_rd + 7) / 8);
      bool residual_ready = false;
      for (size_t i = 0; i < _n; ++i) {
        const size_t lane = lane0 + i;
        const byte_type* rec = faiss::rabitq_utils::get_block_aux_ptr(
          base, lane, kFastScanBbs, pack_bytes, stride, _record);
        const auto* fe =
          reinterpret_cast<const faiss::rabitq_utils::SignBitFactorsWithError*>(
            rec);
        if (!faiss::rabitq_utils::should_refine_candidate(
              _scores[i], fe->f_error, qf.g_error, threshold,
              M != VectorMetric::L2Sqr)) {
          continue;
        }
        if (!residual_ready) {
          _rot_centroid.resize(_rd);
          RotateInto(_cb->Signs().data(), centroid, _rot_centroid.data(),
                     _cb->SrcDim(), static_cast<uint32_t>(_rd));
          const std::vector<float>& rq = _cb->RotatedQuery();
          _q_res.resize(_rd);
          for (size_t j = 0; j < _rd; ++j) {
            _q_res[j] = rq[j] - _rot_centroid[j];
          }
          residual_ready = true;
        }
        faiss::rabitq_utils::unpack_sign_bits_from_packed(
          base, kFastScanBbs, _nsq, lane, stride, _sign_bits.data());
        const uint8_t* ex_code =
          rec + sizeof(faiss::rabitq_utils::SignBitFactorsWithError);
        const auto* ex_fac =
          reinterpret_cast<const faiss::rabitq_utils::ExtraBitsFactors*>(
            ex_code + _ex_code_size);
        const float qr_base =
          M == VectorMetric::L2Sqr ? qf.qr_to_c_L2sqr : qf.q_dot_c;
        _scores[i] = faiss::rabitq_utils::compute_full_multibit_distance(
          _sign_bits.data(), ex_code, *ex_fac, _q_res.data(), qr_base, _rd,
          _ex_bits,
          M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                                   : faiss::MetricType::METRIC_INNER_PRODUCT);
      }
    }

    if constexpr (M == VectorMetric::L2Sqr) {
      for (size_t i = 0; i < _n; ++i) {
        _scores[i] = -_scores[i];
      }
    }
  }

  void ComputeBlock(size_t offset, size_t count, score_t* out) final {
    SDB_ASSERT(offset + count <= _n);
    std::memcpy(out, _scores.data() + offset, count * sizeof(score_t));
  }

 private:
  std::shared_ptr<const RaBitQuantizerCodebook<M>> _cb;
  std::unique_ptr<IndexInput> _pay_in;
  uint64_t _pay_base;
  size_t _rd;
  size_t _nsq;
  uint32_t _ex_bits;
  size_t _storage;
  size_t _ex_code_size;
  size_t _record;
  std::vector<float> _rot_centroid;
  std::vector<float> _q_res;
  std::vector<uint8_t> _sign_bits;
  std::vector<float> _threshold_tmp;
  std::vector<byte_type> _buf;
  faiss::AlignedTable<uint16_t> _accu;
  std::vector<score_t> _scores;
  size_t _n = 0;
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> RaBitQuantizerCodebook<M>::MakeReader(
  std::unique_ptr<IndexInput> pay_in, uint64_t pay_base) const {
  return MakeReaderT<RaBitQuantizerCodebook<M>, RaBitQuantizerReader<M>>(
    this, std::move(pay_in), pay_base);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> RaBitQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<RaBitQuantizerStats<M>, RaBitQuantizerCodebook<M>>(
    this, query);
}

template<template<VectorMetric> class Writer, typename... Args>
std::unique_ptr<QuantizerWriter> MakeWriterWithMetric(VectorMetric metric,
                                                      Args&&... args) {
  switch (EffectiveQuantMetric(metric)) {
    case VectorMetric::L2Sqr:
      return std::make_unique<Writer<VectorMetric::L2Sqr>>(
        std::forward<Args>(args)...);
    case VectorMetric::InnerProduct:
      return std::make_unique<Writer<VectorMetric::InnerProduct>>(
        std::forward<Args>(args)...);
    default:
      SDB_ASSERT(false);
      return nullptr;
  }
}

template<template<VectorMetric> class Stats, typename... Args>
std::shared_ptr<const QuantizerStats> MakeStatsWithMetric(VectorMetric metric,
                                                          Args&&... args) {
  const auto make = [&]<VectorMetric M> {
    auto s = std::make_shared<const Stats<M>>(std::forward<Args>(args)...);
    if constexpr (requires { s->Valid(); }) {
      if (!s->Valid()) {
        return std::shared_ptr<const QuantizerStats>{};
      }
    }
    return std::shared_ptr<const QuantizerStats>{std::move(s)};
  };
  switch (EffectiveQuantMetric(metric)) {
    case VectorMetric::L2Sqr:
      return make.template operator()<VectorMetric::L2Sqr>();
    case VectorMetric::InnerProduct:
      return make.template operator()<VectorMetric::InnerProduct>();
    default:
      SDB_ASSERT(false);
      return nullptr;
  }
}

}  // namespace

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(
  VectorQuantization quant, uint32_t d, VectorMetric metric, uint32_t pq_m,
  uint32_t pq_niter, uint32_t nb_bits) {
  switch (quant) {
    case VectorQuantization::None:
      return nullptr;
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
      return std::make_unique<ScalarQuantizerWriter>(d, quant);
    case VectorQuantization::PQ:
      return MakeWriterWithMetric<ProductQuantizerWriter>(metric, d, pq_m,
                                                          pq_niter);
    case VectorQuantization::RaBitQ:
      return MakeWriterWithMetric<RaBitQuantizerWriter>(metric, d, nb_bits);
  }
  return nullptr;
}

std::shared_ptr<const QuantizerStats> MakeQuantizerStats(
  VectorQuantization quant, uint32_t d, std::span<const byte_type> stats,
  VectorMetric metric) {
  switch (quant) {
    case VectorQuantization::None:
      return nullptr;
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
      return MakeStatsWithMetric<ScalarQuantizerStats>(metric, d, quant, stats);
    case VectorQuantization::PQ:
      return MakeStatsWithMetric<ProductQuantizerStats>(metric, d, stats);
    case VectorQuantization::RaBitQ:
      return MakeStatsWithMetric<RaBitQuantizerStats>(metric, d, stats);
  }
  return nullptr;
}

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook,
  std::unique_ptr<IndexInput> pay_in, uint64_t pay_base) {
  return codebook ? codebook->MakeReader(std::move(pay_in), pay_base) : nullptr;
}

}  // namespace irs
