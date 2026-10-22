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

#include <absl/strings/str_split.h>
#include <faiss/impl/ProductQuantizer.h>
#include <fast_float/fast_float.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iresearch/formats/hnsw/hnsw_graph.hpp>
#include <iresearch/formats/ivf/quantizer.hpp>
#include <iresearch/utils/bytes_output.hpp>
#include <limits>
#include <numbers>
#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace irs;

namespace {

std::vector<float> ReadF32(const std::string& path, size_t count) {
  std::vector<float> out(count);
  std::ifstream in{path, std::ios::binary};
  in.read(reinterpret_cast<char*>(out.data()),
          static_cast<std::streamsize>(count * sizeof(float)));
  if (!in) {
    std::fprintf(stderr, "short read %s\n", path.c_str());
    std::exit(1);
  }
  return out;
}

template<typename F>
void ParallelFor(size_t n, F&& f) {
  const size_t threads =
    std::max<size_t>(1, std::thread::hardware_concurrency());
  std::atomic<size_t> next{0};
  std::vector<std::thread> pool;
  for (size_t t = 0; t < threads; ++t) {
    pool.emplace_back([&] {
      for (size_t i = next++; i < n; i = next++) {
        f(i);
      }
    });
  }
  for (auto& th : pool) {
    th.join();
  }
}

float Exact(const float* q, const float* x, uint32_t d, bool l2) {
  float s = 0.f;
  if (l2) {
    for (uint32_t j = 0; j < d; ++j) {
      const float t = q[j] - x[j];
      s += t * t;
    }
    return -s;
  }
  for (uint32_t j = 0; j < d; ++j) {
    s += q[j] * x[j];
  }
  return s;
}

std::vector<uint32_t> TopIds(const std::vector<float>& scores, size_t k) {
  std::vector<uint32_t> ids(scores.size());
  for (uint32_t i = 0; i < ids.size(); ++i) {
    ids[i] = i;
  }
  k = std::min(k, ids.size());
  std::partial_sort(
    ids.begin(), ids.begin() + k, ids.end(),
    [&](uint32_t a, uint32_t b) { return scores[a] > scores[b]; });
  ids.resize(k);
  return ids;
}

void Wht(float* a, uint32_t len) {
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
  const float s = 1.f / std::sqrt(static_cast<float>(len));
  for (uint32_t i = 0; i < len; ++i) {
    a[i] *= s;
  }
}

struct RefOpts {
  uint32_t bits = 1;
  std::string rot = "hd";
  bool center = true;
  bool ec = true;
  std::string est = "renorm";
  uint32_t qbits = 0;
  bool cq = false;
};

const std::vector<float>& LloydMax(uint32_t bits) {
  static const std::vector<float> b1{-0.7979f, 0.7979f};
  static const std::vector<float> b2{-1.5104f, -0.4528f, 0.4528f, 1.5104f};
  static const std::vector<float> b3{-2.1520f, -1.3439f, -0.7560f, -0.2451f,
                                     0.2451f,  0.7560f,  1.3439f,  2.1520f};
  static const std::vector<float> b4{-2.7326f, -2.0690f, -1.6181f, -1.2562f,
                                     -0.9424f, -0.6568f, -0.3881f, -0.1284f,
                                     0.1284f,  0.3881f,  0.6568f,  0.9424f,
                                     1.2562f,  1.6181f,  2.0690f,  2.7326f};
  switch (bits) {
    case 1:
      return b1;
    case 2:
      return b2;
    case 3:
      return b3;
    default:
      return b4;
  }
}

class RefTq {
 public:
  RefTq(RefOpts o, uint32_t d) : _o{std::move(o)}, _d{d} {
    _rd = std::bit_ceil(d);
    std::mt19937_64 rng{0x5eed};
    _signs.resize(_rd);
    for (auto& s : _signs) {
      s = (rng() & 1) ? 1.f : -1.f;
    }
    for (int p = 0; p < 3; ++p) {
      std::vector<uint32_t> perm(_rd);
      for (uint32_t i = 0; i < _rd; ++i) {
        perm[i] = i;
      }
      std::shuffle(perm.begin(), perm.end(), rng);
      _perms.push_back(std::move(perm));
    }
    _cent = LloydMax(_o.bits);
  }

  void Rotate(const float* x, float* out) const {
    for (uint32_t i = 0; i < _d; ++i) {
      out[i] = x[i];
    }
    for (uint32_t i = _d; i < _rd; ++i) {
      out[i] = 0.f;
    }
    if (_o.rot == "hd") {
      for (uint32_t i = 0; i < _rd; ++i) {
        out[i] *= _signs[i];
      }
      Wht(out, _rd);
      return;
    }
    if (_o.rot == "hd3") {
      std::vector<float> tmp(_rd);
      for (int r = 0; r < 3; ++r) {
        for (uint32_t i = 0; i < _rd; ++i) {
          out[i] *= _signs[(i * (r + 1) * 7919 + r) % _rd];
        }
        Wht(out, _rd);
      }
      return;
    }
    Wht(out, _rd);
    std::vector<float> tmp(_rd);
    for (const auto& perm : _perms) {
      for (uint32_t i = 0; i < _rd; ++i) {
        tmp[i] = out[perm[i]];
      }
      std::copy(tmp.begin(), tmp.end(), out);
      Wht(out, _rd);
    }
  }

  void Train(const float* base, size_t n) {
    _c.assign(_d, 0.f);
    if (_o.center) {
      const size_t sample = std::min<size_t>(n, kHnswTrainSample);
      for (size_t i = 0; i < sample; ++i) {
        for (uint32_t j = 0; j < _d; ++j) {
          _c[j] += base[i * _d + j];
        }
      }
      for (auto& v : _c) {
        v /= static_cast<float>(sample);
      }
    }
    _shift.assign(_rd, 0.f);
    _scale.assign(_rd, 1.f);
    if (!_o.ec) {
      return;
    }
    const size_t rows = std::min<size_t>(n, 65536);
    std::vector<std::vector<float>> cols(_rd);
    std::vector<float> res(_d);
    std::vector<float> rot(_rd);
    for (size_t i = 0; i < rows; i += 8) {
      for (uint32_t j = 0; j < _d; ++j) {
        res[j] = base[i * _d + j] - _c[j];
      }
      Rotate(res.data(), rot.data());
      float nn = 0.f;
      for (auto v : rot) {
        nn += v * v;
      }
      const float k = std::sqrt(static_cast<float>(_rd) / std::max(nn, 1e-30f));
      for (uint32_t j = 0; j < _rd; ++j) {
        cols[j].push_back(rot[j] * k);
      }
    }
    float c_outer = 0.f;
    for (auto v : _cent) {
      c_outer = std::max(c_outer, std::abs(v));
    }
    const double p = 0.5 * std::erfc(-double{c_outer} / std::numbers::sqrt2);
    for (uint32_t j = 0; j < _rd; ++j) {
      auto& col = cols[j];
      const double span = static_cast<double>(col.size() - 1);
      const auto lo = static_cast<size_t>(std::llround((1 - p) * span));
      const auto hi = static_cast<size_t>(std::llround(p * span));
      std::nth_element(col.begin(), col.begin() + lo, col.end());
      const float qlo = col[lo];
      std::nth_element(col.begin(), col.begin() + hi, col.end());
      const float qhi = col[hi];
      if (qhi - qlo > 1e-3f) {
        _shift[j] = -(qlo + qhi) / 2.f;
        _scale[j] = 2.f * c_outer / (qhi - qlo);
      }
    }
  }

  size_t RecordFloats() const { return _rd + 2; }

  bool _l2 = true;

  void Encode(const float* x, float* rec) const {
    std::vector<float> res(_d);
    std::vector<float> rot(_rd);
    for (uint32_t j = 0; j < _d; ++j) {
      res[j] = x[j] - _c[j];
    }
    Rotate(res.data(), rot.data());
    float nn = 0.f;
    for (auto v : rot) {
      nn += v * v;
    }
    const float norm = std::sqrt(std::max(nn, 1e-30f));
    const float k = std::sqrt(static_cast<float>(_rd)) / norm;
    double cn2 = 0.0;
    double cy = 0.0;
    for (uint32_t j = 0; j < _rd; ++j) {
      const float y = rot[j] * k;
      const float yp = (y + _shift[j]) * _scale[j];
      size_t best = 0;
      for (size_t c = 1; c < _cent.size(); ++c) {
        if (std::abs(_cent[c] - yp) < std::abs(_cent[best] - yp)) {
          best = c;
        }
      }
      const float cr = _cent[best] / _scale[j] - _shift[j];
      rec[j] = cr;
      cn2 += double{cr} * cr;
      cy += double{cr} * y;
    }
    const auto cn = static_cast<float>(std::sqrt(cn2));
    if (_o.est == "unbiased") {
      rec[_rd] = norm * static_cast<float>(_rd) / static_cast<float>(cy);
    } else {
      rec[_rd] = norm * std::sqrt(static_cast<float>(_rd)) / cn;
    }
    float xn = 0.f;
    if (_l2) {
      for (uint32_t j = 0; j < _d; ++j) {
        const float v = _o.cq ? x[j] - _c[j] : x[j];
        xn += v * v;
      }
    } else if (_o.cq) {
      for (uint32_t j = 0; j < _d; ++j) {
        xn += _c[j] * (x[j] - _c[j]);
      }
    }
    rec[_rd + 1] = xn;
  }

  void Score(const float* q, const float* recs, size_t n, bool l2,
             float* out) const {
    std::vector<float> qr(_rd);
    std::vector<float> qq(q, q + _d);
    if (_o.cq) {
      for (uint32_t j = 0; j < _d; ++j) {
        qq[j] -= _c[j];
      }
    }
    Rotate(qq.data(), qr.data());
    if (_o.qbits != 0) {
      float mx = 0.f;
      for (auto v : qr) {
        mx = std::max(mx, std::abs(v));
      }
      const float levels = static_cast<float>((1U << (_o.qbits - 1)) - 1);
      const float step = mx / levels;
      for (auto& v : qr) {
        v = std::round(v / step) * step;
      }
    }
    float qc = 0.f;
    float qn = 0.f;
    for (uint32_t j = 0; j < _d; ++j) {
      qc += (_o.cq && l2) ? 0.f : q[j] * _c[j];
      qn += qq[j] * qq[j];
    }
    const float inv = 1.f / std::sqrt(static_cast<float>(_rd));
    const size_t stride = RecordFloats();
    for (size_t i = 0; i < n; ++i) {
      const float* r = recs + i * stride;
      float s = 0.f;
      for (uint32_t j = 0; j < _rd; ++j) {
        s += qr[j] * r[j];
      }
      const float ip = r[_rd] * s * inv + qc;
      out[i] = l2 ? -(qn + r[_rd + 1] - 2.f * ip) : ip + r[_rd + 1];
    }
  }

 private:
  RefOpts _o;
  uint32_t _d;
  uint32_t _rd;
  std::vector<float> _signs;
  std::vector<std::vector<uint32_t>> _perms;
  std::vector<float> _cent;
  std::vector<float> _c;
  std::vector<float> _shift;
  std::vector<float> _scale;
};

struct Config {
  std::string name;
  VectorQuantization quant;
  uint32_t nb_bits;
  bool ref = false;
  RefOpts ref_opts;
  uint32_t pq8_m = 0;
  bool half_bit = false;
  uint32_t query_bits = 0;
};

template<typename T>
T ParseNumber(std::string_view s) {
  T value{};
  fast_float::from_chars(s.data(), s.data() + s.size(), value);
  return value;
}

Config ParseConfig(std::string_view s) {
  std::vector<std::string> parts = absl::StrSplit(s, ':');
  Config c{.name = std::string{s},
           .quant = VectorQuantization::None,
           .nb_bits = parts.size() > 1 ? ParseNumber<uint32_t>(parts[1]) : 0};
  const auto& k = parts[0];
  if (k == "pq8") {
    c.pq8_m = c.nb_bits;
    return c;
  }
  c.half_bit = parts.size() > 1 && parts[1].find(".5") != std::string::npos;
  if (k == "ref") {
    c.ref = true;
    for (size_t i = 1; i < parts.size(); ++i) {
      const auto& p = parts[i];
      if (i == 1) {
        c.ref_opts.bits = ParseNumber<uint32_t>(p);
      } else if (p == "hd" || p == "q4" || p == "hd3") {
        c.ref_opts.rot = p;
      } else if (p == "nocenter") {
        c.ref_opts.center = false;
      } else if (p == "noec") {
        c.ref_opts.ec = false;
      } else if (p == "unbiased" || p == "renorm") {
        c.ref_opts.est = p;
      } else if (p == "cq") {
        c.ref_opts.cq = true;
      } else if (p.starts_with("q")) {
        c.ref_opts.qbits = ParseNumber<uint32_t>(std::string_view{p}.substr(1));
      }
    }
    return c;
  }
  if (k == "sq8") {
    c.quant = VectorQuantization::SQ8;
  } else if (k == "sq4") {
    c.quant = VectorQuantization::SQ4;
  } else if (k == "usq8") {
    c.quant = VectorQuantization::USQ8;
  } else if (k == "usq4") {
    c.quant = VectorQuantization::USQ4;
  } else if (k == "tq") {
    c.quant = VectorQuantization::TQ;
  } else if (k == "rabitq") {
    c.quant = VectorQuantization::RaBitQ;
  } else if (k == "pq") {
    c.quant = VectorQuantization::PQ;
  } else if (k == "fp16") {
    c.quant = VectorQuantization::FP16;
  } else if (k == "bf16") {
    c.quant = VectorQuantization::BF16;
  } else if (k == "bq") {
    c.quant = VectorQuantization::BQ;
    if (parts.size() > 2) {
      c.query_bits = ParseNumber<uint32_t>(parts[2]);
    }
  } else if (k == "osq") {
    c.quant = VectorQuantization::OSQ;
    if (parts.size() > 2) {
      c.query_bits = ParseNumber<uint32_t>(parts[2]);
    }
  }
  return c;
}

int Main(int argc, char** argv) {
  if (argc < 7) {
    std::fprintf(stderr,
                 "usage: quant_quality <base.f32> <query.f32> <d> <l2|ip> <n> "
                 "<nq> cfg...\n");
    return 1;
  }
  const std::string base_path = argv[1];
  const std::string query_path = argv[2];
  const auto d = ParseNumber<uint32_t>(argv[3]);
  const bool l2 = std::string_view{argv[4]} == "l2";
  const auto n = ParseNumber<size_t>(argv[5]);
  const auto nq = ParseNumber<size_t>(argv[6]);
  const VectorMetric metric =
    l2 ? VectorMetric::L2Sqr : VectorMetric::InnerProduct;

  const auto base = ReadF32(base_path, n * d);
  const auto queries = ReadF32(query_path, nq * d);

  constexpr size_t kK = 10;
  const std::string gt_path =
    base_path + ".gt" + std::to_string(n) + "_" + std::to_string(nq);
  std::vector<uint32_t> gt(nq * kK);
  {
    std::ifstream in{gt_path, std::ios::binary};
    if (in.read(reinterpret_cast<char*>(gt.data()),
                static_cast<std::streamsize>(gt.size() * sizeof(uint32_t)))) {
    } else {
      ParallelFor(nq, [&](size_t qi) {
        std::vector<float> s(n);
        for (size_t i = 0; i < n; ++i) {
          s[i] = Exact(queries.data() + qi * d, base.data() + i * d, d, l2);
        }
        auto top = TopIds(s, kK);
        std::copy(top.begin(), top.end(), gt.begin() + qi * kK);
      });
      std::ofstream out{gt_path, std::ios::binary};
      out.write(reinterpret_cast<const char*>(gt.data()),
                static_cast<std::streamsize>(gt.size() * sizeof(uint32_t)));
    }
  }

  const std::vector<size_t> pools{10, 20, 40, 80, 160, 320};
  std::printf("%-28s %6s", "config", "bytes");
  for (auto r : pools) {
    std::printf("   R=%-4zu", r);
  }
  std::printf("  score_ns\n");

  const auto report = [&](const std::string& name, size_t bytes,
                          const std::vector<std::vector<size_t>>& hits,
                          double ns) {
    std::printf("%-28s %6zu", name.c_str(), bytes);
    for (size_t pi = 0; pi < pools.size(); ++pi) {
      size_t h = 0;
      for (size_t qi = 0; qi < nq; ++qi) {
        h += hits[qi][pi];
      }
      std::printf("   %.4f",
                  static_cast<double>(h) / static_cast<double>(nq * kK));
    }
    std::printf("  %8.2f\n", ns);
    std::fflush(stdout);
  };
  const auto count_hits = [&](size_t qi, const std::vector<float>& s,
                              std::vector<size_t>& out) {
    auto top = TopIds(s, pools.back());
    const uint32_t* g = gt.data() + qi * kK;
    for (size_t pi = 0; pi < pools.size(); ++pi) {
      size_t h = 0;
      for (size_t r = 0; r < pools[pi]; ++r) {
        h += std::find(g, g + kK, top[r]) != g + kK ? 1 : 0;
      }
      out[pi] = h;
    }
  };

  for (int ai = 7; ai < argc; ++ai) {
    const Config cfg = ParseConfig(argv[ai]);
    if (cfg.pq8_m != 0) {
      faiss::ProductQuantizer pq{d, cfg.pq8_m, 8};
      const size_t tn = std::min<size_t>(n, 65536);
      pq.train(tn, base.data());
      std::vector<uint8_t> codes(n * pq.code_size);
      pq.compute_codes(base.data(), codes.data(), n);
      std::vector<std::vector<size_t>> hits(nq,
                                            std::vector<size_t>(pools.size()));
      ParallelFor(nq, [&](size_t qi) {
        const float* q = queries.data() + qi * d;
        std::vector<float> lut(pq.M * pq.ksub);
        if (l2) {
          pq.compute_distance_table(q, lut.data());
        } else {
          pq.compute_inner_prod_table(q, lut.data());
        }
        std::vector<float> s(n);
        for (size_t i = 0; i < n; ++i) {
          const uint8_t* c = codes.data() + i * pq.code_size;
          float acc = 0.f;
          for (size_t m = 0; m < pq.M; ++m) {
            acc += lut[m * pq.ksub + c[m]];
          }
          s[i] = l2 ? -acc : acc;
        }
        count_hits(qi, s, hits[qi]);
      });
      report(cfg.name, pq.code_size, hits, 0.0);
      continue;
    }
    if (cfg.ref) {
      RefTq tq{cfg.ref_opts, d};
      tq._l2 = l2;
      tq.Train(base.data(), n);
      const size_t stride = tq.RecordFloats();
      std::vector<float> recs(n * stride);
      ParallelFor(n / 1024 + 1, [&](size_t b) {
        for (size_t i = b * 1024; i < std::min(n, (b + 1) * 1024); ++i) {
          tq.Encode(base.data() + i * d, recs.data() + i * stride);
        }
      });
      std::vector<std::vector<size_t>> hits(nq,
                                            std::vector<size_t>(pools.size()));
      ParallelFor(nq, [&](size_t qi) {
        std::vector<float> s(n);
        tq.Score(queries.data() + qi * d, recs.data(), n, l2, s.data());
        count_hits(qi, s, hits[qi]);
      });
      report(cfg.name, 0, hits, 0.0);
      continue;
    }
    const uint32_t pq_m = cfg.quant == VectorQuantization::PQ ? cfg.nb_bits : 0;
    const std::vector<std::string> cparts = absl::StrSplit(cfg.name, ':');
    const uint32_t pq_niter =
      pq_m != 0 && cparts.size() > 2 ? ParseNumber<uint32_t>(cparts[2]) : 0;
    auto writer = MakeQuantizerWriter(
      AnnInfo::Quantizer{.kind = cfg.quant,
                         .pq_m = pq_m,
                         .nb_bits = pq_m != 0 ? 0 : cfg.nb_bits,
                         .half_bit = cfg.half_bit,
                         .query_bits = cfg.query_bits},
      d, metric, pq_niter, /*row_major=*/true);
    if (!writer) {
      std::printf("%-12s unsupported\n", cfg.name.c_str());
      continue;
    }
    const bool train =
      writer->TrainSamples(n) == QuantizerWriter::kTrainStreaming;
    if (train) {
      writer->Train(base.data(), n);
    } else if (const size_t t = writer->TrainSamples(n); t != 0) {
      const auto t0 = std::chrono::steady_clock::now();
      writer->Train(base.data(), t);
      std::fprintf(
        stderr, "%s train %zu rows: %.2fs\n", cfg.name.c_str(), t,
        std::chrono::duration<double>(std::chrono::steady_clock::now() - t0)
          .count());
    }
    std::vector<float> centroid;
    if (QuantizerNeedsCentroid(cfg.quant)) {
      const size_t sample = train ? n : std::min<size_t>(n, kHnswTrainSample);
      centroid.assign(d, 0.f);
      for (size_t i = 0; i < sample; ++i) {
        for (uint32_t j = 0; j < d; ++j) {
          centroid[j] += base[i * d + j];
        }
      }
      for (auto& v : centroid) {
        v /= static_cast<float>(sample);
      }
      writer->SetClusterCentroid(centroid.data());
    }
    const size_t refine = writer->RefineSamples(n);
    if (refine != 0) {
      writer->Refine(base.data(), std::min(n, refine));
      writer->RefineDone();
    }
    const uint32_t record_size = writer->BlockSetting().record_size;
    std::vector<byte_type> codes(n * record_size);
    if (!writer->EncodeInto(codes.data(), base.data(), n)) {
      std::printf("%-12s no EncodeInto\n", cfg.name.c_str());
      continue;
    }
    bstring framed;
    {
      BytesOutput out{framed};
      writer->Serialize(out);
    }
    const bstring blob = framed.substr(sizeof(uint64_t));
    auto stats = MakeQuantizerStats(cfg.quant, d, blob, metric, true);

    std::vector<std::vector<size_t>> hits(nq,
                                          std::vector<size_t>(pools.size()));
    std::atomic<uint64_t> score_ns{0};
    ParallelFor(nq, [&](size_t qi) {
      const std::span<const float> q{queries.data() + qi * d, d};
      auto cb = stats->MakeCodebook(q);
      auto reader = MakeQuantizerReader(cb);
      reader->StartCluster(centroid.empty() ? nullptr : centroid.data());
      std::vector<float> s(n);
      const auto t0 = std::chrono::steady_clock::now();
      reader->ComputeBlock({codes.data(), codes.size()},
                           std::numeric_limits<score_t>::lowest(), s.data());
      const auto t1 = std::chrono::steady_clock::now();
      score_ns += static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
      count_hits(qi, s, hits[qi]);
    });
    double gather_ns = 0.0;
    {
      std::mt19937 rng{7};
      std::vector<uint32_t> ids(32);
      std::vector<float> out(32);
      const size_t kQ = 200;
      const size_t kBatches = 256;
      uint64_t total = 0;
      uint64_t setup = 0;
      for (size_t qi = 0; qi < kQ; ++qi) {
        const std::span<const float> q{queries.data() + (qi % nq) * d, d};
        const auto s0 = std::chrono::steady_clock::now();
        auto cb = stats->MakeCodebook(q);
        auto reader = MakeQuantizerReader(cb);
        reader->StartCluster(centroid.empty() ? nullptr : centroid.data());
        setup += static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - s0)
            .count());
        std::vector<uint32_t> all(kBatches * 32);
        for (auto& id : all) {
          id = rng() % n;
        }
        const auto t0 = std::chrono::steady_clock::now();
        for (size_t b = 0; b < kBatches; ++b) {
          reader->ComputeGathered(
            codes.data(), record_size, {all.data() + b * 32, 32},
            std::numeric_limits<score_t>::lowest(), out.data());
        }
        const auto t1 = std::chrono::steady_clock::now();
        total += static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0)
            .count());
      }
      gather_ns =
        static_cast<double>(total) / static_cast<double>(kQ * kBatches * 32);
      std::fprintf(
        stderr, "%s setup %.2f us/query\n", cfg.name.c_str(),
        static_cast<double>(setup) / static_cast<double>(kQ) / 1000.0);
    }
    report(cfg.name, record_size, hits, gather_ns);
  }
  return 0;
}

}  // namespace

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
