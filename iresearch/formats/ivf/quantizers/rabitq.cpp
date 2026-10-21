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

#include <faiss/MetricType.h>
#include <faiss/impl/RaBitQUtils.h>
#include <faiss/impl/RaBitQuantizerMultiBit.h>

#include "iresearch/formats/ivf/quantizers/common.hpp"
#include "iresearch/index/vector_distance.hpp"

namespace irs::quantizers {
namespace {

constexpr size_t SignBytes(uint32_t rotated_d) noexcept {
  return (static_cast<size_t>(rotated_d) + 7) / 8;
}

std::vector<byte_type> PackSigns(const std::vector<float>& signs) {
  std::vector<byte_type> out(SignBytes(static_cast<uint32_t>(signs.size())), 0);
  for (size_t i = 0; i < signs.size(); ++i) {
    if (signs[i] > 0.f) {
      out[i / 8] = static_cast<byte_type>(out[i / 8] | (1U << (i % 8)));
    }
  }
  return out;
}

void LoadSigns(std::span<const byte_type> stats, size_t offset, uint32_t rd,
               std::vector<float>& signs) {
  SDB_ASSERT(stats.size() >= offset + SignBytes(rd));
  const byte_type* p = stats.data() + offset;
  signs.resize(rd);
  for (uint32_t i = 0; i < rd; ++i) {
    signs[i] = ((p[i / 8] >> (i % 8)) & 1U) != 0 ? 1.f : -1.f;
  }
}

struct RaBitQStatsHeader {
  uint32_t nb_bits;
  uint32_t d;
};

constexpr uint8_t kRaBitQQueryBits = 8;
constexpr bool kRaBitQCentered = false;

struct RaBitQLayout {
  uint32_t d = 0;
  uint32_t rd = 0;
  uint32_t nb_bits = 0;
  uint32_t ex_bits = 0;
  uint32_t m = 0;
  uint32_t nsq = 0;
  uint32_t row_code_bytes = 0;
  uint32_t group_code_bytes = 0;
  uint32_t storage = 0;
  uint32_t ex_code_size = 0;
  uint32_t record_size = 0;
  bool row_major = false;

  uint32_t RowAuxOffset() const noexcept { return row_code_bytes; }
  uint32_t RowCsOffset() const noexcept { return row_code_bytes + storage; }
  uint32_t RowNormOffset() const noexcept {
    return RowCsOffset() + static_cast<uint32_t>(sizeof(float));
  }

  uint32_t AuxOffset() const noexcept { return group_code_bytes; }
  uint32_t CsOffset() const noexcept {
    return AuxOffset() + static_cast<uint32_t>(kFastScanBbs) * storage;
  }
  uint32_t GroupBytes() const noexcept {
    return static_cast<uint32_t>(kFastScanBbs) * record_size;
  }
};

RaBitQLayout MakeRaBitQLayout(uint32_t d, uint32_t nb_bits,
                              bool row_major) noexcept {
  RaBitQLayout l;
  l.d = d;
  l.rd = RotatedDim(d);
  l.nb_bits = nb_bits;
  l.ex_bits = nb_bits - 1;
  l.m = l.rd / static_cast<uint32_t>(kFastScanBits);
  l.nsq = static_cast<uint32_t>(FastScanNsq(l.m));
  l.row_code_bytes = (l.m + 1) / 2;
  l.group_code_bytes = static_cast<uint32_t>(kFastScanBbs) * l.nsq / 2;
  l.storage = static_cast<uint32_t>(
    faiss::rabitq_utils::compute_per_vector_storage_size(nb_bits, l.rd));
  l.ex_code_size = (l.rd * l.ex_bits + 7) / 8;
  l.row_major = row_major;
  l.record_size = (row_major ? l.row_code_bytes : l.nsq / 2) + l.storage +
                  static_cast<uint32_t>(sizeof(float)) * (row_major ? 2U : 1U);
  return l;
}

template<VectorMetric M>
class RaBitQuantizerWriter final : public QuantizerWriter {
 public:
  RaBitQuantizerWriter(uint32_t d, uint32_t nb_bits, bool row_major)
    : _lay{MakeRaBitQLayout(d, nb_bits, row_major)},
      _inv_rd_sqrt{1.f / std::sqrt(static_cast<float>(_lay.rd))} {
    GenerateSigns(_lay.rd, kIvfRotationSeed, _signs);
    _rotated.resize(_lay.rd);
    _residual.resize(_lay.rd);
    if (_lay.row_major) {
      _row.resize(_lay.record_size);
    } else {
      _packed.resize(_lay.group_code_bytes);
      _aux.resize(kFastScanBbs * size_t{_lay.storage});
    }
    SDB_ASSERT(_lay.rd % kFastScanBits == 0);
  }

  void Train(const float* /*vecs*/, size_t /*n*/) final {}

  void SetClusterCentroid(const float* centroid) final {
    _centroid.resize(_lay.rd);
    RotateInto(_signs.data(), centroid, _centroid.data(), _lay.d, _lay.rd);
    _centroid_sum = 0.f;
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      _centroid_sum += _centroid[j];
    }
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = _lay.row_major ? 1U : uint32_t{kFastScanBbs},
            .record_size = _lay.record_size};
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    SDB_ASSERT(_centroid.size() == _lay.rd);
    for (size_t i = 0; i < n; ++i) {
      const float* vec = vecs + i * size_t{_lay.d};
      if (_lay.row_major) {
        PackRecord(_row.data(), vec);
        out.WriteData(_row.data(), _row.size());
        continue;
      }
      EncodeGrouped(vec, _lane);
      if (++_lane == kFastScanBbs) {
        WriteGroup(out, kFastScanBbs);
      }
    }
  }

  bool EncodeInto(byte_type* dst, const float* vecs, size_t n) final {
    if (!_lay.row_major) {
      return false;
    }
    SDB_ASSERT(_centroid.size() == _lay.rd);
    for (size_t i = 0; i < n; ++i) {
      PackRecord(dst + i * size_t{_lay.record_size}, vecs + i * size_t{_lay.d});
    }
    return true;
  }

  std::unique_ptr<QuantizerWriter> CloneForEncode() const final {
    if (!_lay.row_major) {
      return nullptr;
    }
    auto clone = std::make_unique<RaBitQuantizerWriter>(_lay.d, _lay.nb_bits,
                                                        _lay.row_major);
    clone->_centroid = _centroid;
    clone->_centroid_sum = _centroid_sum;
    return clone;
  }

  void Finish(IndexOutput& out) final {
    if (!_lay.row_major && _lane != 0) {
      WriteGroup(out, _lane);
    }
  }

  uint32_t PendingLanes() const noexcept final {
    return _lay.row_major ? 0U : static_cast<uint32_t>(_lane);
  }

  void Serialize(DataOutput& out) const final {
    const auto packed = PackSigns(_signs);
    out.WriteU64(sizeof(RaBitQStatsHeader) + packed.size());
    WritePod(out, RaBitQStatsHeader{_lay.nb_bits, _lay.d});
    out.WriteData(packed.data(), packed.size());
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::RaBitQ;
  }

 private:
  static constexpr faiss::MetricType kMetric =
    M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                             : faiss::MetricType::METRIC_INNER_PRODUCT;

  template<typename SetNib>
  float EncodeVector(const float* vec, uint8_t* aux, float* norm_out,
                     SetNib&& set) {
    RotateInto(_signs.data(), vec, _rotated.data(), _lay.d, _lay.rd);
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      _residual[j] = _rotated[j] - _centroid[j];
    }
    float cs_sum = 0.f;
    for (uint32_t sq = 0; sq < _lay.m; ++sq) {
      uint8_t nib = 0;
      for (uint32_t b = 0; b < kFastScanBits; ++b) {
        const uint32_t j = sq * static_cast<uint32_t>(kFastScanBits) + b;
        if (_residual[j] > 0.f) {
          nib = static_cast<uint8_t>(nib | (1U << b));
          cs_sum += _centroid[j];
        }
      }
      set(sq, nib);
    }
    const faiss::rabitq_utils::SignBitFactorsWithError f =
      faiss::rabitq_utils::compute_vector_factors(
        _rotated.data(), _lay.rd, _centroid.data(), kMetric,
        /*compute_error=*/_lay.ex_bits > 0);
    if (_lay.ex_bits == 0) {
      std::memcpy(aux, &f, sizeof(faiss::rabitq_utils::SignBitFactors));
    } else {
      std::memcpy(aux, &f,
                  sizeof(faiss::rabitq_utils::SignBitFactorsWithError));
      uint8_t* ex_code =
        aux + sizeof(faiss::rabitq_utils::SignBitFactorsWithError);
      faiss::rabitq_utils::ExtraBitsFactors ex;
      faiss::rabitq_multibit::quantize_ex_bits(_residual.data(), _lay.rd,
                                               _lay.nb_bits, ex_code, ex,
                                               kMetric, _centroid.data());
      std::memcpy(ex_code + _lay.ex_code_size, &ex,
                  sizeof(faiss::rabitq_utils::ExtraBitsFactors));
    }
    if (norm_out) {
      float sum = 0.f;
      for (uint32_t j = 0; j < _lay.rd; ++j) {
        sum += _residual[j] * _residual[j];
      }
      *norm_out = std::sqrt(sum);
    }
    return (2.f * cs_sum - _centroid_sum) * _inv_rd_sqrt;
  }

  void EncodeGrouped(const float* vec, size_t lane) {
    _cs[lane] =
      EncodeVector(vec, _aux.data() + lane * size_t{_lay.storage}, nullptr,
                   [this, lane](uint32_t sq, uint8_t nib) {
                     faiss::pq4_set_packed_element(
                       _packed.data(), nib, kFastScanBbs, _lay.nsq, lane, sq);
                   });
  }

  void PackRecord(byte_type* dst, const float* vec) {
    std::fill_n(dst, _lay.record_size, byte_type{0});
    uint8_t* code = reinterpret_cast<uint8_t*>(dst);
    float norm = 0.f;
    const float cs = EncodeVector(
      vec, code + _lay.RowAuxOffset(), &norm,
      [code](uint32_t sq, uint8_t nib) { SetNibble(code, sq, nib); });
    std::memcpy(dst + _lay.RowCsOffset(), &cs, sizeof(float));
    std::memcpy(dst + _lay.RowNormOffset(), &norm, sizeof(float));
  }

  void WriteGroup(IndexOutput& out, size_t count) {
    const size_t storage = _lay.storage;
    std::memset(_aux.data() + count * storage, 0,
                (kFastScanBbs - count) * storage);
    std::fill_n(_cs.data() + count, kFastScanBbs - count, 0.f);
    out.WriteData(_packed.data(), _packed.size());
    out.WriteData(_aux.data(), _aux.size());
    out.WriteData(reinterpret_cast<const byte_type*>(_cs.data()),
                  kFastScanBbs * sizeof(float));
    std::memset(_packed.data(), 0, _packed.size());
    _lane = 0;
  }

  RaBitQLayout _lay;
  float _inv_rd_sqrt;
  float _centroid_sum = 0.f;
  size_t _lane = 0;
  std::vector<float> _signs;
  std::vector<float> _centroid;
  std::vector<float> _rotated;
  std::vector<float> _residual;
  std::vector<uint8_t> _packed;
  std::vector<uint8_t> _aux;
  std::array<float, kFastScanBbs> _cs{};
  std::vector<byte_type> _row;
};

template<VectorMetric M>
class RaBitQuantizerStats final : public QuantizerStats {
 public:
  RaBitQuantizerStats(uint32_t d, std::span<const byte_type> stats,
                      bool row_major) {
    const RaBitQStatsHeader hdr = ReadPodHeader<RaBitQStatsHeader>(stats);
    SDB_ASSERT(hdr.nb_bits >= kRaBitQMinBits);
    SDB_ASSERT(hdr.nb_bits <= kRaBitQMaxBits);
    SDB_ASSERT(hdr.d == d);
    SDB_ASSERT(stats.size() >=
               sizeof(RaBitQStatsHeader) + SignBytes(RotatedDim(d)));
    _lay = MakeRaBitQLayout(d, hdr.nb_bits, row_major);
    LoadSigns(stats, sizeof(RaBitQStatsHeader), _lay.rd, _signs);
  }
  uint32_t NbBits() const noexcept { return _lay.nb_bits; }
  const RaBitQLayout& Layout() const noexcept { return _lay; }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::RaBitQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const std::vector<float>& Signs() const noexcept { return _signs; }
  uint32_t SrcDim() const noexcept { return _lay.d; }
  uint32_t RotDim() const noexcept { return _lay.rd; }

 private:
  RaBitQLayout _lay;
  std::vector<float> _signs;
};

template<VectorMetric M>
class RaBitQuantizerCodebook final : public QuantizerCodebook {
 public:
  RaBitQuantizerCodebook(std::shared_ptr<const RaBitQuantizerStats<M>> stats,
                         std::span<const float> query)
    : _stats{std::move(stats)} {
    Rekey(query);
  }

  void Rekey(std::span<const float> query) {
    _query.assign(query.begin(), query.end());
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
    _lut.Build(lut.data(), m, kFastScanKsub);
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const std::shared_ptr<const RaBitQuantizerStats<M>>& StatsPtr()
    const noexcept {
    return _stats;
  }
  const std::vector<float>& Signs() const noexcept { return _stats->Signs(); }
  const std::vector<float>& RotatedQuery() const noexcept {
    return _rotated_query;
  }
  std::span<const float> Query() const noexcept { return _query; }
  uint32_t SrcDim() const noexcept { return _stats->SrcDim(); }
  uint32_t RotDim() const noexcept { return _stats->RotDim(); }
  uint32_t NbBits() const noexcept { return _stats->NbBits(); }
  const RaBitQLayout& Layout() const noexcept { return _stats->Layout(); }
  const faiss::rabitq_utils::QueryFactorsData& QueryFactors() const noexcept {
    return _qf;
  }
  const FastScanLut& Lut() const noexcept { return _lut; }

 private:
  std::shared_ptr<const RaBitQuantizerStats<M>> _stats;
  std::vector<float> _query;
  std::vector<float> _rotated_query;
  faiss::rabitq_utils::QueryFactorsData _qf;
  FastScanLut _lut;
};

template<VectorMetric M>
class RaBitQuantizerReader final : public QuantizerReader {
 public:
  explicit RaBitQuantizerReader(
    std::shared_ptr<const RaBitQuantizerCodebook<M>> cb)
    : _cb{std::move(cb)}, _cur{_cb.get()}, _lay{_cb->Layout()}, _rd{_lay.rd} {
    if (_lay.ex_bits > 0) {
      _sign_bits.resize((_rd + 7) / 8);
    }
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = _lay.row_major ? 1U : uint32_t{kFastScanBbs},
            .record_size = _lay.record_size};
  }

  void StartCluster(const float* centroid) final {
    SDB_ASSERT(centroid);
    _cluster = centroid;
    RefreshQuery();
  }

  bool SetQuery(std::span<const float> query) final {
    SDB_ASSERT(query.size() == _lay.d);
    if (!_own) {
      _own =
        std::make_unique<RaBitQuantizerCodebook<M>>(_cb->StatsPtr(), query);
      _cur = _own.get();
    } else {
      _own->Rekey(query);
    }
    RefreshQuery();
    return true;
  }

  bool Decode(const byte_type* code, float* out) const final {
    if (!_lay.row_major || !_cluster) {
      return false;
    }
    float norm = 0.f;
    std::memcpy(&norm, code + _lay.RowNormOffset(), sizeof(float));
    const float step = norm / std::sqrt(static_cast<float>(_rd));
    const auto* bits = reinterpret_cast<const uint8_t*>(code);
    _dec_scratch.resize(_rd);
    for (uint32_t j = 0; j < _rd; ++j) {
      const bool one = ((bits[j >> 3] >> (j & 7)) & 1) != 0;
      _dec_scratch[j] = _rot_centroid[j] + (one ? step : -step);
    }
    RotateBack(_cb->Signs().data(), _dec_scratch.data(), out, _lay.d,
               static_cast<uint32_t>(_rd), _fwht_scratch);
    return true;
  }

 private:
  void RefreshQuery() {
    SDB_ASSERT(_cluster);
    const std::span<const float> query = _cur->Query();
    const auto d = static_cast<uint16_t>(query.size());
    const float* centroid = _cluster;

    _rot_centroid.resize(_rd);
    RotateInto(_cb->Signs().data(), centroid, _rot_centroid.data(), _lay.d,
               static_cast<uint32_t>(_rd));

    _qf = _cur->QueryFactors();
    _qf.qr_to_c_L2sqr =
      -ComputeDistance<VectorMetric::L2Sqr>(query.data(), centroid, d);
    _qf.g_error = std::sqrt(_qf.qr_to_c_L2sqr);
    if constexpr (M == VectorMetric::InnerProduct) {
      _qf.q_dot_c =
        ComputeDistance<VectorMetric::InnerProduct>(query.data(), centroid, d);
    }
    if (_lay.ex_bits == 0) {
      return;
    }
    const std::vector<float>& rq = _cur->RotatedQuery();
    _q_res.resize(_rd);
    for (size_t j = 0; j < _rd; ++j) {
      _q_res[j] = rq[j] - _rot_centroid[j];
    }
  }

  void ComputeGathered(const byte_type* base, uint32_t record_size,
                       std::span<const uint32_t> ids, score_t threshold,
                       score_t* out) final {
    if (!_lay.row_major) {
      QuantizerReader::ComputeGathered(base, record_size, ids, threshold, out);
      return;
    }
    const float mt = MetricThreshold(threshold);
    for (size_t off = 0; off < ids.size(); off += kFastScanBbs) {
      const size_t take = std::min<size_t>(ids.size() - off, kFastScanBbs);
      const auto sub = ids.subspan(off, take);
      ScoreRows(
        [base, record_size, sub](size_t k) {
          return base + static_cast<size_t>(sub[k]) * record_size;
        },
        take, mt, out + off);
    }
  }

  void ComputeBlock(std::span<const byte_type> block, score_t threshold,
                    score_t* out) final {
    const float mt = MetricThreshold(threshold);
    if (_lay.row_major) {
      SDB_ASSERT(block.size() % _lay.record_size == 0);
      size_t left = block.size() / _lay.record_size;
      const byte_type* rec = block.data();
      const size_t stride = _lay.record_size;
      while (left != 0) {
        const size_t take = std::min<size_t>(left, kFastScanBbs);
        ScoreRows([rec, stride](size_t k) { return rec + k * stride; }, take,
                  mt, out);
        rec += take * stride;
        out += take;
        left -= take;
      }
      return;
    }
    const size_t group_bytes = _lay.GroupBytes();
    SDB_ASSERT(block.size() % group_bytes == 0);
    for (size_t off = 0; off < block.size();
         off += group_bytes, out += kFastScanBbs) {
      ScoreGroup(block.data() + off, mt, out);
    }
  }

 private:
  float MetricThreshold(score_t threshold) const noexcept {
    return M == VectorMetric::L2Sqr ? -threshold : threshold;
  }

  void ScoreGroup(const byte_type* codes, float mt, score_t* out) {
    const byte_type* aux = codes + _lay.AuxOffset();
    const auto* cs = reinterpret_cast<const float*>(codes + _lay.CsOffset());
    const size_t storage = _lay.storage;
    ScoreLanes(
      codes, [aux, storage](size_t i) { return aux + i * storage; },
      [this, codes](size_t i) {
        faiss::rabitq_utils::unpack_sign_bits_from_packed(
          codes, kFastScanBbs, _lay.nsq, i, _lay.group_code_bytes,
          _sign_bits.data());
        return const_cast<const uint8_t*>(_sign_bits.data());
      },
      cs, kFastScanBbs, mt, out);
  }

  template<typename Row>
  void ScoreRows(Row row, size_t count, float mt, score_t* out) {
    _packed.resize(_lay.group_code_bytes);
    PackFastScanRows(row, count, 0, _lay.nsq, _packed.data());
    for (size_t i = 0; i < count; ++i) {
      std::memcpy(&_rm_cs[i], row(i) + _lay.RowCsOffset(), sizeof(float));
    }
    const uint32_t aux_off = _lay.RowAuxOffset();
    ScoreLanes(
      _packed.data(), [row, aux_off](size_t i) { return row(i) + aux_off; },
      [row](size_t i) { return reinterpret_cast<const uint8_t*>(row(i)); },
      _rm_cs.data(), count, mt, out);
  }

  template<typename Aux, typename Signs>
  void ScoreLanes(const byte_type* packed, Aux aux, Signs signs,
                  const float* cs, size_t count, float mt, score_t* out) {
    const FastScanLut& lut = _cur->Lut();
    faiss::accumulate_to_mem(1, kFastScanBbs, static_cast<int>(_lay.nsq),
                             packed, lut.Packed(), _accu.data());
    for (size_t i = 0; i < count; ++i) {
      const float normalized = lut.Decode(_accu[i]) - cs[i];
      const auto* fac =
        reinterpret_cast<const faiss::rabitq_utils::SignBitFactors*>(aux(i));
      out[i] = faiss::rabitq_utils::compute_1bit_adjusted_distance(
        normalized, *fac, _qf, kRaBitQCentered, kRaBitQQueryBits, _rd);
    }
    if (_lay.ex_bits > 0) {
      Refine(aux, signs, count, mt, out);
    }
    if constexpr (M == VectorMetric::L2Sqr) {
      for (size_t i = 0; i < count; ++i) {
        out[i] = -out[i];
      }
    }
  }

  template<typename Aux, typename Signs>
  void Refine(Aux aux, Signs signs, size_t count, float threshold,
              score_t* out) {
    const float qr_base =
      M == VectorMetric::L2Sqr ? _qf.qr_to_c_L2sqr : _qf.q_dot_c;
    for (size_t i = 0; i < count; ++i) {
      const byte_type* rec = aux(i);
      const auto* fe =
        reinterpret_cast<const faiss::rabitq_utils::SignBitFactorsWithError*>(
          rec);
      if (!faiss::rabitq_utils::should_refine_candidate(
            out[i], fe->f_error, _qf.g_error, threshold,
            M != VectorMetric::L2Sqr)) {
        continue;
      }
      const uint8_t* ex_code =
        rec + sizeof(faiss::rabitq_utils::SignBitFactorsWithError);
      const auto* ex_fac =
        reinterpret_cast<const faiss::rabitq_utils::ExtraBitsFactors*>(
          ex_code + _lay.ex_code_size);
      out[i] = faiss::rabitq_utils::compute_full_multibit_distance(
        signs(i), ex_code, *ex_fac, _q_res.data(), qr_base, _rd, _lay.ex_bits,
        M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                                 : faiss::MetricType::METRIC_INNER_PRODUCT);
    }
  }

  std::shared_ptr<const RaBitQuantizerCodebook<M>> _cb;
  const RaBitQuantizerCodebook<M>* _cur;
  std::unique_ptr<RaBitQuantizerCodebook<M>> _own;
  RaBitQLayout _lay;
  size_t _rd;
  const float* _cluster = nullptr;
  faiss::rabitq_utils::QueryFactorsData _qf;
  std::vector<float> _rot_centroid;
  std::vector<float> _q_res;
  std::vector<uint8_t> _sign_bits;
  std::vector<uint8_t> _packed;
  mutable std::vector<float> _dec_scratch;
  mutable std::vector<float> _fwht_scratch;
  std::array<float, kFastScanBbs> _rm_cs{};
  std::array<uint16_t, kFastScanBbs> _accu;
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> RaBitQuantizerCodebook<M>::MakeReader() const {
  return MakeReaderT<RaBitQuantizerCodebook<M>, RaBitQuantizerReader<M>>(this);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> RaBitQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<RaBitQuantizerStats<M>, RaBitQuantizerCodebook<M>>(
    this, query);
}

}  // namespace

std::unique_ptr<QuantizerWriter> MakeRaBitQWriter(VectorMetric metric,
                                                  uint32_t d, uint32_t nb_bits,
                                                  bool row_major) {
  return MakeWriterWithMetric<RaBitQuantizerWriter>(metric, d, nb_bits,
                                                    row_major);
}

std::shared_ptr<const QuantizerStats> MakeRaBitQStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> stats,
  bool row_major) {
  return MakeStatsWithMetric<RaBitQuantizerStats>(metric, d, stats, row_major);
}

}  // namespace irs::quantizers
