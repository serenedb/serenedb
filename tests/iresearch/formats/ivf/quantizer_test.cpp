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

#include <algorithm>
#include <bit>
#include <cmath>
#include <core_functions/array_kernels.hpp>
#include <cstring>
#include <functional>
#include <iostream>
#include <iresearch/formats/hnsw/hnsw_bases.hpp>
#include <iresearch/formats/ivf/clustering.hpp>
#include <iresearch/formats/ivf/quantizer.hpp>
#include <iresearch/formats/ivf/quantizers/common.hpp>
#include <iresearch/search/scorers/score_function.hpp>
#include <iresearch/store/data_input.hpp>
#include <iresearch/store/data_output.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/bytes_output.hpp>
#include <iresearch/utils/misc.hpp>
#include <limits>
#include <memory>
#include <numeric>
#include <random>
#include <span>
#include <utility>
#include <vector>

#include "tests_shared.hpp"

using namespace irs;

namespace {

constexpr score_t kNoPrune = std::numeric_limits<score_t>::lowest();

// faiss enables float_control(precise, off) on x86 only, so a survivor's
// accumulated distance depends on which of compute_level_dot_kernel's two loop
// bodies it lands in -- and that depends on the threshold. Scores from a pruned
// and an unpruned scan agree to a few ULP, not bit-for-bit.
constexpr score_t kScoreTol = 1e-6f;

score_t ScoreTol(score_t want) { return kScoreTol * (1.f + std::fabs(want)); }

// The writer emits a u64 length prefix ahead of the blob, exactly as
// IvfWriter::Flush stores it; strip it to get what MakeQuantizerStats sees.
bstring SerializeStats(const QuantizerWriter& writer) {
  bstring framed;
  BytesOutput out{framed};
  writer.Serialize(out);
  return framed.substr(sizeof(uint64_t));
}

constexpr size_t kFastScanBbs = 32;

std::vector<float> MakeSpread(uint32_t d, size_t n, uint32_t seed) {
  std::mt19937 rng{seed};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> out(n * d);
  for (float& v : out) {
    v = nd(rng);
  }
  return out;
}

// Mirrors IvfTermReader's driving loop: rows are encoded against the centroid
// current at that moment, and only whole groups are flushed -- so a group can
// hold lanes from two clusters, and only the stream's final group is padded.
class PayloadStream {
 public:
  struct Cluster {
    uint64_t pay_start = 0;
    uint32_t lane0 = 0;
    size_t count = 0;
  };

  explicit PayloadStream(QuantizerWriter& writer) : _writer{writer} {}

  Cluster Add(IndexOutput& out, const float* vecs, size_t n) {
    const Cluster c{out.Position(), _writer.PendingLanes(), n};
    _writer.Encode(out, vecs, n);
    return c;
  }

  void Finish(IndexOutput& out) { _writer.Finish(out); }

 private:
  QuantizerWriter& _writer;
};

uint64_t EncodeCluster(QuantizerWriter& writer, IndexOutput& out,
                       const float* vecs, size_t n) {
  PayloadStream stream{writer};
  const auto c = stream.Add(out, vecs, n);
  stream.Finish(out);
  return c.pay_start;
}

bstring ReadPayload(MemoryFile& file, uint64_t start, uint64_t end) {
  bstring payload(end - start, 0);
  MemoryIndexInput in{file};
  in.ReadData(start, payload.data(), payload.size());
  return payload;
}

bstring EncodeChunks(QuantizerWriter& writer, const std::vector<float>& points,
                     uint32_t d, std::span<const size_t> chunks) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t start = 0;
  uint64_t end = 0;
  {
    MemoryIndexOutput out{file};
    start = out.Position();
    PayloadStream stream{writer};
    size_t off = 0;
    for (const size_t m : chunks) {
      stream.Add(out, points.data() + off * d, m);
      off += m;
    }
    stream.Finish(out);
    out.Flush();
    end = out.Position();
  }
  return ReadPayload(file, start, end);
}

void ExpectBytesEq(const bstring& want, const bstring& got) {
  ASSERT_EQ(want.size(), got.size());
  for (size_t i = 0; i < want.size(); ++i) {
    ASSERT_EQ(want[i], got[i]) << "byte " << i;
  }
}

// Drives a reader the way QVectorIterator does: reads over the cluster
// payload aligned on the reader's group size, whole groups per ComputeBlock
// call.
class ClusterScorer {
 public:
  ClusterScorer(const std::shared_ptr<const QuantizerCodebook>& codebook,
                MemoryFile& file, uint64_t pay_start, size_t total,
                const float* centroid, uint32_t lane0 = 0)
    : _qr{MakeQuantizerReader(codebook)},
      _setting{_qr->BlockSetting()},
      _in{std::make_unique<MemoryIndexInput>(file)},
      _base{pay_start},
      _lane0{lane0},
      _total{total},
      _end{_lane0 + total},
      _records{_setting.RecordCount(_end)} {
    _qr->StartCluster(centroid);
  }

  const PayloadBlockSetting& Setting() const noexcept { return _setting; }

  size_t ByteSize() const noexcept {
    return _records * size_t{_setting.record_size};
  }

  // Whole cluster in one ComputeBlock call, so multi-group blocks are covered.
  // With a shared stream the cluster's lanes need not start on a group, so this
  // scores every group it touches and then drops the neighbours' lanes.
  std::vector<score_t> All(score_t threshold = kNoPrune) {
    std::vector<score_t> out(_records);
    _qr->ComputeBlock(Read(0, _records), threshold, out.data());
    out.erase(out.begin(), out.begin() + _lane0);
    out.resize(_total);
    return out;
  }

  void Block(size_t offset, size_t count, score_t threshold, score_t* out) {
    const size_t gs = _setting.group_size;
    size_t lane = _lane0 + offset;
    while (count != 0) {
      const size_t first = lane / gs * gs;
      const size_t records = std::min(first + gs, _records) - first;
      _cache.resize(records);
      _qr->ComputeBlock(Read(first, records), threshold, _cache.data());
      const size_t take =
        std::min(count, std::min(records, _end - first) - (lane - first));
      std::copy_n(_cache.begin() + (lane - first), take, out);
      lane += take;
      out += take;
      count -= take;
    }
  }

 private:
  std::span<const byte_type> Read(size_t index, size_t count) {
    const uint64_t offset = _base + index * _setting.record_size;
    const size_t bytes = count * size_t{_setting.record_size};
    if (const byte_type* p = _in->ReadVolatile(offset, bytes)) {
      return {p, bytes};
    }
    _buf.resize(bytes);
    _in->ReadData(offset, _buf.data(), bytes);
    return _buf;
  }

  std::unique_ptr<QuantizerReader> _qr;
  PayloadBlockSetting _setting;
  IndexInput::ptr _in;
  std::vector<byte_type> _buf;
  uint64_t _base;
  size_t _lane0;
  size_t _total;
  size_t _end;
  size_t _records;
  std::vector<score_t> _cache;
};

// Builds a writer, trains it on `n_train` copies of the 3 canonical
// `points` (so k-means has >= ksub=16 samples to work with), then encodes
// exactly `points` (n=3) as a single fast-scan cluster and returns the
// scores from a fresh reader positioned on that cluster.
std::array<score_t, 3> PqRoundtrip(uint32_t d, uint32_t pq_m,
                                   VectorMetric metric,
                                   const std::vector<float>& centroid,
                                   const std::vector<float>& points,
                                   const std::vector<float>& query) {
  auto writer = MakeQuantizerWriter(
    {.kind = VectorQuantization::PQ, .pq_m = pq_m}, d, metric);
  EXPECT_EQ(writer->Kind(), VectorQuantization::PQ);
  writer->SetClusterCentroid(centroid.data());

  std::vector<float> residual_train;
  constexpr size_t kCopiesPerPoint = 6;
  for (size_t c = 0; c < kCopiesPerPoint; ++c) {
    for (size_t p = 0; p < 3; ++p) {
      for (uint32_t j = 0; j < d; ++j) {
        residual_train.push_back(points[p * d + j] - centroid[j]);
      }
    }
  }
  writer->Train(residual_train.data(), residual_train.size() / d);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    EncodeCluster(*writer, out, points.data(), 3);
    out.Flush();
  }

  const bstring blob = SerializeStats(*writer);
  auto stats = MakeQuantizerStats(VectorQuantization::PQ, d, blob, metric);
  EXPECT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  EXPECT_NE(codebook, nullptr);

  ClusterScorer scorer{codebook, file, pay_start, 3, centroid.data()};
  const auto scores = scorer.All();
  return {scores[0], scores[1], scores[2]};
}

}  // namespace

class none_quantizer_test : public ::testing::TestWithParam<VectorMetric> {};

TEST_P(none_quantizer_test, roundtrip_is_bit_exact) {
  const VectorMetric metric = GetParam();
  constexpr uint32_t d = 5;
  constexpr size_t n = 7;
  constexpr size_t kFirstBatch = 4;

  std::vector<float> points(n * d);
  for (size_t i = 0; i < points.size(); ++i) {
    points[i] = static_cast<float>(i % 13) * 0.375f - 2.f;
  }
  std::vector<float> query{0.5f, -1.25f, 3.f, 0.f, 2.75f};
  // Cosine is stored normalized and scored as inner product.
  if (metric == VectorMetric::Cosine) {
    NormalizeRows(points.data(), n, d);
    NormalizeRows(query.data(), 1, d);
  }

  auto writer =
    MakeQuantizerWriter({.kind = VectorQuantization::None}, d, metric);
  ASSERT_NE(writer, nullptr);
  EXPECT_EQ(writer->Kind(), VectorQuantization::None);
  EXPECT_EQ(writer->BlockSetting().record_size, d * sizeof(float));
  EXPECT_EQ(writer->BlockSetting().group_size, 1);
  EXPECT_TRUE(SerializeStats(*writer).empty());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    out.WriteByte(0xAB);
    out.WriteByte(0xCD);
    out.WriteByte(0xEF);
    pay_start = out.Position();
    ASSERT_NE(pay_start % alignof(float), 0);
    {
      PayloadStream stream{*writer};
      stream.Add(out, points.data(), kFirstBatch);
      stream.Add(out, points.data() + kFirstBatch * d, n - kFirstBatch);
      stream.Finish(out);
    }
    out.Flush();
    EXPECT_EQ(out.Position() - pay_start, n * d * sizeof(float));
  }

  const bstring blob = SerializeStats(*writer);
  auto stats = MakeQuantizerStats(VectorQuantization::None, d, blob, metric);
  ASSERT_NE(stats, nullptr);
  EXPECT_EQ(stats->Kind(), VectorQuantization::None);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  ClusterScorer scorer{codebook, file, pay_start, n, /*centroid=*/nullptr};
  EXPECT_EQ(scorer.ByteSize(), n * d * sizeof(float));
  const auto scores = scorer.All();

  bstring stored(n * d * sizeof(float), 0);
  {
    MemoryIndexInput in{file};
    in.ReadData(pay_start, stored.data(), stored.size());
  }
  EXPECT_EQ(std::memcmp(stored.data(), points.data(), stored.size()), 0);

  ResolveEnum<VectorMetric>(metric, [&]<VectorMetric M> {
    for (size_t i = 0; i < n; ++i) {
      const score_t want = ComputeDistance<EffectiveQuantMetric(M)>(
        query.data(), points.data() + i * d, static_cast<uint16_t>(d));
      EXPECT_NEAR(scores[i], want, ScoreTol(want)) << "row " << i;
    }
  });

  std::vector<score_t> tail(n - kFirstBatch);
  scorer.Block(kFirstBatch, tail.size(), kNoPrune, tail.data());
  for (size_t i = 0; i < tail.size(); ++i) {
    EXPECT_EQ(tail[i], scores[kFirstBatch + i]) << "row " << i;
  }
}

INSTANTIATE_TEST_SUITE_P(metrics, none_quantizer_test,
                         ::testing::Values(VectorMetric::L2Sqr,
                                           VectorMetric::InnerProduct,
                                           VectorMetric::Cosine,
                                           VectorMetric::L1));

class panorama_quantizer_test : public ::testing::TestWithParam<VectorMetric> {
};

namespace {

// Rows with variance decaying along a random orthonormal basis, so a PCA
// rotation has something to concentrate, plus cluster offsets so the k-th best
// tightens the way it does inside probed clusters.
std::vector<float> MakePanoramaData(uint32_t d, size_t n, uint32_t seed) {
  auto basis = MakeRotation(d, seed);
  std::mt19937 rng{seed};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> coef(d);
  std::vector<float> out(n * d, 0.f);
  for (size_t i = 0; i < n; ++i) {
    const float shift = static_cast<float>(i % 8) * 0.25f;
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

struct PanoramaIndex {
  SimpleMemoryAccounter memory;
  MemoryFile file;
  std::unique_ptr<QuantizerWriter> writer;
  bstring blob;
  uint64_t pay_start = 0;
  size_t n = 0;

  PanoramaIndex() : file{memory} {}
};

// Trains and encodes `points` as one cluster, in several EncodeCluster batches
// so records that straddle a batch boundary are covered.
void BuildPanorama(PanoramaIndex& index, uint32_t d, VectorMetric metric,
                   const std::vector<float>& raw) {
  // Cosine is stored normalized and scored as inner product, exactly as
  // IvfTermReader feeds the writer.
  auto points = raw;
  index.n = points.size() / d;
  if (metric == VectorMetric::Cosine) {
    NormalizeRows(points.data(), index.n, d);
  }
  index.writer =
    MakeQuantizerWriter({.kind = VectorQuantization::None}, d, metric);
  ASSERT_NE(index.writer, nullptr);
  index.writer->Train(points.data(), index.n);
  index.blob = SerializeStats(*index.writer);
  ASSERT_FALSE(index.blob.empty())
    << "the rotation must have been trained for d=" << d;
  EXPECT_EQ(index.blob.size(),
            2 * sizeof(uint32_t) + size_t{d} * d * sizeof(float));

  MemoryIndexOutput out{index.file};
  index.pay_start = out.Position();
  constexpr size_t kBatch = kPostingBlock;
  {
    PayloadStream stream{*index.writer};
    for (size_t b = 0; b < index.n; b += kBatch) {
      const size_t m = std::min(kBatch, index.n - b);
      stream.Add(out, points.data() + b * d, m);
    }
    stream.Finish(out);
  }
  out.Flush();
}

ClusterScorer OpenPanorama(PanoramaIndex& index, uint32_t d,
                           VectorMetric metric,
                           const std::vector<float>& raw_q) {
  auto query = raw_q;
  if (metric == VectorMetric::Cosine) {
    NormalizeRows(query.data(), 1, d);
  }
  auto stats =
    MakeQuantizerStats(VectorQuantization::None, d, index.blob, metric);
  EXPECT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  EXPECT_NE(codebook, nullptr);
  return ClusterScorer{codebook, index.file, index.pay_start, index.n,
                       /*centroid=*/nullptr};
}

}  // namespace

// Without a bound threshold the progressive scan must reproduce the distance a
// full raw-basis scan computes -- up to the rounding a rotation introduces.
TEST_P(panorama_quantizer_test, unpruned_matches_raw_basis) {
  const VectorMetric metric = GetParam();
  constexpr uint32_t d = 64;
  constexpr size_t n = 600;
  auto points = MakePanoramaData(d, n, 3);
  auto query = MakePanoramaData(d, 1, 77);

  PanoramaIndex index;
  BuildPanorama(index, d, metric, points);
  ASSERT_FALSE(::testing::Test::HasFatalFailure());
  auto scorer = OpenPanorama(index, d, metric, query);

  std::vector<score_t> scores(n);
  for (size_t b = 0; b < n; b += kPostingBlock) {
    const size_t m = std::min<size_t>(kPostingBlock, n - b);
    scorer.Block(b, m, kNoPrune, scores.data() + b);
  }

  ResolveEnum<VectorMetric>(metric, [&]<VectorMetric M> {
    for (size_t i = 0; i < n; ++i) {
      const score_t want = ComputeDistance<M>(
        query.data(), points.data() + i * d, static_cast<uint16_t>(d));
      EXPECT_NEAR(scores[i], want, 2e-3 * (1.f + std::fabs(want)))
        << "row " << i;
    }
  });
}

// The whole contract: a pruned candidate is one that could not have entered the
// top-k anyway, so pruning must not change the result. Runs the deployed loop
// -- blocks of kPostingBlock, threshold refreshed between blocks.
TEST_P(panorama_quantizer_test, pruning_preserves_topk) {
  const VectorMetric metric = GetParam();
  constexpr uint32_t d = 64;
  constexpr size_t n = 4096;
  constexpr size_t k = 10;
  auto points = MakePanoramaData(d, n, 5);

  PanoramaIndex index;
  BuildPanorama(index, d, metric, points);
  ASSERT_FALSE(::testing::Test::HasFatalFailure());

  for (uint32_t qi = 0; qi < 8; ++qi) {
    auto query = MakePanoramaData(d, 1, 1000 + qi);

    const auto topk = [&](bool prune) {
      auto scorer = OpenPanorama(index, d, metric, query);
      score_t threshold = kNoPrune;
      std::vector<score_t> block(kPostingBlock);
      std::vector<std::pair<score_t, size_t>> best;
      for (size_t b = 0; b < n; b += kPostingBlock) {
        const size_t m = std::min<size_t>(kPostingBlock, n - b);
        scorer.Block(b, m, prune ? threshold : kNoPrune, block.data());
        for (size_t i = 0; i < m; ++i) {
          if (block[i] > threshold) {
            best.emplace_back(block[i], b + i);
            std::push_heap(best.begin(), best.end(), std::greater<>{});
            if (best.size() > k) {
              std::pop_heap(best.begin(), best.end(), std::greater<>{});
              best.pop_back();
              threshold = best.front().first;
            }
          }
        }
      }
      std::sort(best.begin(), best.end(), std::greater<>{});
      return best;
    };

    const auto want = topk(false);
    const auto got = topk(true);
    ASSERT_EQ(want.size(), got.size()) << "query " << qi;
    std::vector<size_t> want_ids;
    std::vector<size_t> got_ids;
    for (size_t i = 0; i < want.size(); ++i) {
      want_ids.push_back(want[i].second);
      got_ids.push_back(got[i].second);
    }
    std::sort(want_ids.begin(), want_ids.end());
    std::sort(got_ids.begin(), got_ids.end());
    EXPECT_EQ(want_ids, got_ids) << "query " << qi;
    for (size_t i = 0; i < want.size(); ++i) {
      EXPECT_NEAR(want[i].first, got[i].first, ScoreTol(want[i].first))
        << "query " << qi << " rank " << i;
    }
  }
}

// A candidate is only ever dropped when its true score cannot clear the
// threshold; a surviving one carries its exact score.
TEST_P(panorama_quantizer_test, bound_never_drops_a_live_candidate) {
  const VectorMetric metric = GetParam();
  constexpr uint32_t d = 128;
  constexpr size_t n = 512;
  auto points = MakePanoramaData(d, n, 9);
  auto query = MakePanoramaData(d, 1, 21);

  PanoramaIndex index;
  BuildPanorama(index, d, metric, points);
  ASSERT_FALSE(::testing::Test::HasFatalFailure());

  std::vector<score_t> exact(n);
  {
    auto scorer = OpenPanorama(index, d, metric, query);
    for (size_t b = 0; b < n; b += kPostingBlock) {
      scorer.Block(b, std::min<size_t>(kPostingBlock, n - b), kNoPrune,
                   exact.data() + b);
    }
  }

  auto sorted = exact;
  std::sort(sorted.begin(), sorted.end());
  size_t pruned = 0;
  std::vector<score_t> got(n);
  // Quantiles of the score distribution, so every sweep step prunes a known
  // fraction rather than depending on the metric's absolute scale.
  for (size_t q = 1; q < 10; ++q) {
    const score_t threshold = sorted[n * q / 10];
    auto scorer = OpenPanorama(index, d, metric, query);
    for (size_t b = 0; b < n; b += kPostingBlock) {
      scorer.Block(b, std::min<size_t>(kPostingBlock, n - b), threshold,
                   got.data() + b);
    }
    for (size_t i = 0; i < n; ++i) {
      if (got[i] != kNoPrune) {
        EXPECT_NEAR(got[i], exact[i], ScoreTol(exact[i]))
          << "row " << i << " quantile " << q;
        continue;
      }
      ++pruned;
      EXPECT_LE(exact[i], threshold) << "row " << i << " quantile " << q;
    }
  }
  EXPECT_GT(pruned, 0u) << "the sweep must exercise the pruning branch";
}

// L1 is absent on purpose: a rotation does not preserve it, so the writer
// declines to train and the payload stays row-major.
INSTANTIATE_TEST_SUITE_P(metrics, panorama_quantizer_test,
                         ::testing::Values(VectorMetric::L2Sqr,
                                           VectorMetric::InnerProduct,
                                           VectorMetric::Cosine));

TEST(panorama_writer_test, l1_and_small_dims_decline_the_rotation) {
  constexpr uint32_t d = 64;
  constexpr size_t n = 600;
  auto points = MakePanoramaData(d, n, 13);

  auto l1 = MakeQuantizerWriter({.kind = VectorQuantization::None}, d,
                                VectorMetric::L1);
  ASSERT_NE(l1, nullptr);
  l1->Train(points.data(), n);
  EXPECT_TRUE(SerializeStats(*l1).empty());
  EXPECT_EQ(l1->BlockSetting().record_size, d * sizeof(float));
  EXPECT_EQ(l1->BlockSetting().group_size, 1);
  EXPECT_FALSE(PanoramaApplies(VectorMetric::L1, d));

  constexpr uint32_t small = 32;
  ASSERT_FALSE(PanoramaApplies(VectorMetric::L2Sqr, small));
  auto narrow = MakePanoramaData(small, n, 17);
  auto tiny = MakeQuantizerWriter({.kind = VectorQuantization::None}, small,
                                  VectorMetric::L2Sqr);
  ASSERT_NE(tiny, nullptr);
  tiny->Train(narrow.data(), n);
  EXPECT_TRUE(SerializeStats(*tiny).empty());
}

class rabitq_quantizer_test : public ::testing::TestWithParam<uint32_t> {};

// The FWHT rotation pads d to a power of two; verify the encode+query roundtrip
// ranking is preserved across power-of-two AND non-power-of-two dimensions, and
// that the stats blob carries only the header plus one sign bit per rotated
// dimension -- not a dense rotation matrix.
TEST_P(rabitq_quantizer_test, roundtrip_ranking_across_dims) {
  const uint32_t d = GetParam();
  constexpr uint32_t nb_bits = 8;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 0.f);

  std::vector<float> points(3 * static_cast<size_t>(d), 0.f);
  points[0] = 1.f;                            // p0 (nearest)
  points[static_cast<size_t>(d)] = 4.f;       // p1
  points[2 * static_cast<size_t>(d)] = 20.f;  // p2 (farthest)
  constexpr size_t n = 3;

  auto writer = MakeQuantizerWriter(
    {.kind = VectorQuantization::RaBitQ, .nb_bits = nb_bits}, d, metric);
  ASSERT_NE(writer, nullptr);
  EXPECT_EQ(writer->Kind(), VectorQuantization::RaBitQ);
  const size_t rd = std::max<uint32_t>(4, std::bit_ceil(d));
  const size_t stats_bytes = SerializeStats(*writer).size();
  EXPECT_EQ(stats_bytes, 2 * sizeof(uint32_t) + (rd + 7) / 8);
  EXPECT_LT(stats_bytes,
            size_t{d} * d * sizeof(float));  // never a dense matrix
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    EncodeCluster(*writer, out, points.data(), n);
    out.Flush();
  }

  std::vector<float> query(d, 0.f);
  query[0] = 1.5f;
  const bstring blob = SerializeStats(*writer);
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d, blob, metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  ClusterScorer scorer{codebook, file, pay_start, n, centroid.data()};
  const auto scores = scorer.All();

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

INSTANTIATE_TEST_SUITE_P(dims, rabitq_quantizer_test,
                         ::testing::Values(8u, 32u, 96u, 128u, 1536u));

TEST(rabitq_quantizer_test, roundtrip_ranking_matches_exact_l2) {
  constexpr uint32_t d = 8;
  constexpr uint32_t nb_bits = 8;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 0.f);

  // Well-separated points along one axis: distances to centroid 1, 3, 18.
  const std::vector<float> points{
    /*p0*/ 1.f,  0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p1*/ 4.f,  0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p2*/ 20.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
  };
  constexpr size_t n = 3;

  auto writer = MakeQuantizerWriter(
    {.kind = VectorQuantization::RaBitQ, .nb_bits = nb_bits}, d, metric);
  ASSERT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    EncodeCluster(*writer, out, points.data(), n);
    out.Flush();
  }

  // Query closest to p0 (distance 0.5), then p1 (2.5), then p2 (18.5).
  const std::vector<float> query{1.5f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  const bstring blob = SerializeStats(*writer);
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d, blob, metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  ClusterScorer scorer{codebook, file, pay_start, n, centroid.data()};
  const auto scores = scorer.All();

  // L2 scores are negated distances (larger = nearer), so p0 > p1 > p2.
  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

TEST(rabitq_quantizer_test, roundtrip_ranking_matches_exact_inner_product) {
  constexpr uint32_t d = 8;
  constexpr uint32_t nb_bits = 8;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const std::vector<float> centroid(d, 0.f);

  const std::vector<float> points{
    /*p0 aligned with query*/ 2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    /*p1 orthogonal*/ 0.f,
    2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    /*p2 opposed*/ -2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
  };
  constexpr size_t n = 3;

  auto writer = MakeQuantizerWriter(
    {.kind = VectorQuantization::RaBitQ, .nb_bits = nb_bits}, d, metric);
  ASSERT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    EncodeCluster(*writer, out, points.data(), n);
    out.Flush();
  }

  const std::vector<float> query{3.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  const bstring blob = SerializeStats(*writer);
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d, blob, metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  ClusterScorer scorer{codebook, file, pay_start, n, centroid.data()};
  const auto scores = scorer.All();

  // IP: higher raw value means a larger inner product with the query.
  // Exact order by <query, p_i> is p0 (6) > p1 (0) > p2 (-6).
  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

namespace {

// Writes `points` as one RaBitQ cluster around `centroid` and returns the
// scores for `query`.
std::vector<score_t> RaBitQRoundtrip(uint32_t d, uint32_t nb_bits,
                                     VectorMetric metric,
                                     const std::vector<float>& centroid,
                                     const std::vector<float>& points,
                                     const std::vector<float>& query) {
  const size_t n = points.size() / d;
  auto writer = MakeQuantizerWriter(
    {.kind = VectorQuantization::RaBitQ, .nb_bits = nb_bits}, d, metric);
  EXPECT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    EncodeCluster(*writer, out, points.data(), n);
    out.Flush();
  }

  const bstring blob = SerializeStats(*writer);
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d, blob, metric);
  EXPECT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  EXPECT_NE(codebook, nullptr);

  ClusterScorer scorer{codebook, file, pay_start, n, centroid.data()};
  return scorer.All();
}

}  // namespace

TEST(rabitq_quantizer_test, one_bit_ranking_l2_nonzero_centroid) {
  constexpr uint32_t d = 8;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 5.f);

  std::vector<float> points(3 * static_cast<size_t>(d), 5.f);
  points[0] = 6.f;
  points[d] = 9.f;
  points[2 * d] = 25.f;
  std::vector<float> query(d, 5.f);
  query[0] = 6.5f;

  const auto scores =
    RaBitQRoundtrip(d, /*nb_bits=*/1, metric, centroid, points, query);

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

TEST(rabitq_quantizer_test, one_bit_ranking_inner_product_nonzero_centroid) {
  constexpr uint32_t d = 8;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const std::vector<float> centroid(d, 5.f);

  std::vector<float> points(3 * static_cast<size_t>(d), 5.f);
  points[0] = 7.f;
  points[d] = 4.f;
  points[2 * d] = -2.f;
  std::vector<float> query(d, 0.f);
  query[0] = 3.f;

  const auto scores =
    RaBitQRoundtrip(d, /*nb_bits=*/1, metric, centroid, points, query);

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

TEST(rabitq_quantizer_test, one_bit_scores_comparable_across_clusters) {
  constexpr uint32_t d = 8;
  const VectorMetric metric = VectorMetric::L2Sqr;

  std::vector<float> c1(d, 5.f);
  std::vector<float> c2(d, 5.f);
  c2[0] = 105.f;

  std::vector<float> points1(2 * static_cast<size_t>(d), 5.f);
  points1[0] = 6.f;
  points1[d] = 9.f;
  std::vector<float> points2(2 * static_cast<size_t>(d), 5.f);
  points2[0] = 101.f;
  points2[d] = 104.f;

  auto writer = MakeQuantizerWriter(
    {.kind = VectorQuantization::RaBitQ, .nb_bits = 1}, d, metric);
  ASSERT_NE(writer, nullptr);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  PayloadStream::Cluster cl1;
  PayloadStream::Cluster cl2;
  {
    MemoryIndexOutput out{file};
    PayloadStream stream{*writer};
    writer->SetClusterCentroid(c1.data());
    cl1 = stream.Add(out, points1.data(), 2);
    writer->SetClusterCentroid(c2.data());
    cl2 = stream.Add(out, points2.data(), 2);
    stream.Finish(out);
    out.Flush();
  }

  std::vector<float> query(d, 5.f);
  query[0] = 7.f;
  const bstring blob = SerializeStats(*writer);
  auto stats = MakeQuantizerStats(VectorQuantization::RaBitQ, d, blob, metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  std::array<score_t, 4> scores{};
  {
    ClusterScorer s1{codebook, file, cl1.pay_start, 2, c1.data(), cl1.lane0};
    const auto got = s1.All();
    std::copy(got.begin(), got.end(), scores.begin());
  }
  {
    ClusterScorer s2{codebook, file, cl2.pay_start, 2, c2.data(), cl2.lane0};
    const auto got = s2.All();
    std::copy(got.begin(), got.end(), scores.begin() + 2);
  }

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
  EXPECT_GT(scores[2], scores[3]);
}

TEST(rabitq_quantizer_test, multibit_selective_refine_above_pool) {
  constexpr uint32_t d = 8;
  constexpr size_t n = 80;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 5.f);

  std::vector<float> points(n * static_cast<size_t>(d), 5.f);
  std::vector<bool> is_near(n);
  for (size_t i = 0; i < n; ++i) {
    is_near[i] = (i % 2 == 0);
    points[i * d] = is_near[i] ? 6.f : 105.f;
  }
  std::vector<float> query(d, 5.f);
  query[0] = 6.5f;

  const auto scores =
    RaBitQRoundtrip(d, /*nb_bits=*/8, metric, centroid, points, query);

  score_t min_near = std::numeric_limits<score_t>::infinity();
  score_t max_far = -std::numeric_limits<score_t>::infinity();
  for (size_t i = 0; i < n; ++i) {
    if (is_near[i]) {
      min_near = std::min(min_near, scores[i]);
    } else {
      max_far = std::max(max_far, scores[i]);
    }
  }
  EXPECT_GT(min_near, max_far);
}

TEST(pq_quantizer_test, roundtrip_ranking_matches_exact_l2) {
  constexpr uint32_t d = 8;
  constexpr uint32_t pq_m = 2;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 0.f);

  // Well-separated points along one axis: distances to centroid 1, 4, 20.
  const std::vector<float> points{
    /*p0*/ 1.f,  0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p1*/ 4.f,  0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
    /*p2*/ 20.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f,
  };
  // Query closest to p0 (distance 0.25), then p1 (6.25), then p2 (342.25).
  const std::vector<float> query{1.5f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};

  const auto scores = PqRoundtrip(d, pq_m, metric, centroid, points, query);

  // L2 scores are negated distances (larger = nearer), so p0 > p1 > p2.
  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

TEST(pq_quantizer_test, roundtrip_ranking_matches_exact_inner_product) {
  constexpr uint32_t d = 8;
  constexpr uint32_t pq_m = 2;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const std::vector<float> centroid(d, 0.f);

  const std::vector<float> points{
    /*p0 aligned with query*/ 2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    /*p1 orthogonal*/ 0.f,
    2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    /*p2 opposed*/ -2.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
    0.f,
  };
  const std::vector<float> query{3.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};

  const auto scores = PqRoundtrip(d, pq_m, metric, centroid, points, query);

  // IP: higher raw value means a larger inner product with the query.
  // Exact order by <query, p_i> is p0 (6) > p1 (0) > p2 (-6).
  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
}

TEST(pq_quantizer_test, l2_ranking_with_nonzero_centroid) {
  constexpr uint32_t d = 8;
  constexpr uint32_t pq_m = 2;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 5.f);

  std::vector<float> points(3 * static_cast<size_t>(d), 5.f);
  points[0] = 6.f;
  points[d] = 9.f;
  points[2 * d] = 25.f;
  std::vector<float> query(d, 5.f);
  query[0] = 6.5f;

  const auto scores = PqRoundtrip(d, pq_m, metric, centroid, points, query);

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
  EXPECT_LT(scores[0], 0.f);
}

TEST(pq_quantizer_test, l2_scores_comparable_across_clusters) {
  constexpr uint32_t d = 8;
  constexpr uint32_t pq_m = 2;
  const VectorMetric metric = VectorMetric::L2Sqr;

  const std::vector<float> c1(d, 0.f);
  std::vector<float> c2(d, 0.f);
  c2[0] = 100.f;

  std::vector<float> points1(2 * static_cast<size_t>(d), 0.f);
  points1[0] = 1.f;
  points1[d] = 4.f;
  std::vector<float> points2(2 * static_cast<size_t>(d), 0.f);
  points2[0] = 96.f;
  points2[d] = 99.f;

  auto writer = MakeQuantizerWriter(
    {.kind = VectorQuantization::PQ, .pq_m = pq_m}, d, metric);
  ASSERT_NE(writer, nullptr);

  std::vector<float> train;
  constexpr size_t kCopies = 5;
  for (size_t c = 0; c < kCopies; ++c) {
    for (const float r : {1.f, 4.f, -4.f, -1.f}) {
      std::vector<float> v(d, 0.f);
      v[0] = r;
      train.insert(train.end(), v.begin(), v.end());
    }
  }
  writer->Train(train.data(), train.size() / d);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  PayloadStream::Cluster cl1;
  PayloadStream::Cluster cl2;
  {
    MemoryIndexOutput out{file};
    PayloadStream stream{*writer};
    writer->SetClusterCentroid(c1.data());
    cl1 = stream.Add(out, points1.data(), 2);
    writer->SetClusterCentroid(c2.data());
    cl2 = stream.Add(out, points2.data(), 2);
    stream.Finish(out);
    out.Flush();
  }

  std::vector<float> query(d, 0.f);
  query[0] = 2.f;
  const bstring blob = SerializeStats(*writer);
  auto stats = MakeQuantizerStats(VectorQuantization::PQ, d, blob, metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  std::array<score_t, 4> scores{};
  {
    ClusterScorer s1{codebook, file, cl1.pay_start, 2, c1.data(), cl1.lane0};
    const auto got = s1.All();
    std::copy(got.begin(), got.end(), scores.begin());
  }
  {
    ClusterScorer s2{codebook, file, cl2.pay_start, 2, c2.data(), cl2.lane0};
    const auto got = s2.All();
    std::copy(got.begin(), got.end(), scores.begin() + 2);
  }

  EXPECT_GT(scores[0], scores[1]);
  EXPECT_GT(scores[1], scores[2]);
  EXPECT_GT(scores[2], scores[3]);
}

// A cluster spanning more than one 32-vector fast-scan SIMD block (bbs=32),
// with an odd M (M=3, rounded up to nsq=4 for packing/scanning) to exercise
// the padding subquantizer. Near/far points are interleaved so both blocks
// mix the two groups instead of neatly separating along the block boundary.
TEST(pq_quantizer_test, cluster_spans_multiple_fastscan_blocks_with_odd_m) {
  constexpr uint32_t d = 9;
  constexpr uint32_t pq_m = 3;
  constexpr size_t kNear = 20;
  constexpr size_t kFar = 20;
  constexpr size_t n = kNear + kFar;
  const VectorMetric metric = VectorMetric::L2Sqr;
  const std::vector<float> centroid(d, 0.f);

  std::vector<float> points(n * d, 0.f);
  std::vector<bool> is_near(n);
  for (size_t i = 0; i < n; ++i) {
    is_near[i] = (i % 2 == 0);
    points[i * d] = is_near[i] ? 1.f : 100.f;
  }

  auto writer = MakeQuantizerWriter(
    {.kind = VectorQuantization::PQ, .pq_m = pq_m}, d, metric);
  ASSERT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());
  writer->Train(points.data(), n);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    EncodeCluster(*writer, out, points.data(), n);
    out.Flush();
  }

  const std::vector<float> query{1.5f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f, 0.f};
  const bstring blob = SerializeStats(*writer);
  auto stats = MakeQuantizerStats(VectorQuantization::PQ, d, blob, metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  ClusterScorer scorer{codebook, file, pay_start, n, centroid.data()};
  const auto scores = scorer.All();

  score_t min_near = std::numeric_limits<score_t>::infinity();
  score_t max_far = -std::numeric_limits<score_t>::infinity();
  for (size_t i = 0; i < n; ++i) {
    if (is_near[i]) {
      min_near = std::min(min_near, scores[i]);
    } else {
      max_far = std::max(max_far, scores[i]);
    }
  }
  // L2 scores are negated distances (larger = nearer). Near/far are separated
  // by two orders of magnitude, so quantization noise shouldn't be able to
  // flip the group ordering even though it can perturb individual scores
  // within a group.
  EXPECT_GT(min_near, max_far);
}

namespace {

std::unique_ptr<QuantizerWriter> MakeTrainedPq(VectorMetric metric, uint32_t d,
                                               uint32_t pq_m,
                                               const std::vector<float>& pts,
                                               const std::vector<float>& cen) {
  auto w = MakeQuantizerWriter({.kind = VectorQuantization::PQ, .pq_m = pq_m},
                               d, metric);
  w->Train(pts.data(), pts.size() / d);
  w->SetClusterCentroid(cen.data());
  return w;
}

std::unique_ptr<QuantizerWriter> MakeRaBitQ(uint32_t d, uint32_t nb_bits,
                                            const std::vector<float>& cen) {
  auto w = MakeQuantizerWriter(
    {.kind = VectorQuantization::RaBitQ, .nb_bits = nb_bits}, d,
    VectorMetric::L2Sqr);
  w->SetClusterCentroid(cen.data());
  return w;
}

// A cluster split into group-aligned blocks must produce the same bytes as the
// same cluster written in one call: blocks are self-contained.
void ExpectBlockSplitIsTransparent(
  const std::function<std::unique_ptr<QuantizerWriter>()>& make, uint32_t d,
  const std::vector<float>& points, std::span<const size_t> chunks) {
  auto whole_w = make();
  auto split_w = make();
  const size_t all[] = {points.size() / d};
  ExpectBytesEq(EncodeChunks(*whole_w, points, d, all),
                EncodeChunks(*split_w, points, d, chunks));
}

// Stronger: each block is written by a *fresh* writer, so any state leaking
// across a group boundary -- a stale sign bit, an unzeroed tail slot --
// diverges from the single-writer payload.
void ExpectGroupsAreSelfContained(
  const std::function<std::unique_ptr<QuantizerWriter>()>& make, uint32_t d,
  const std::vector<float>& points, size_t head) {
  const size_t n = points.size() / d;
  const std::vector<float> tail(points.begin() + head * d, points.end());
  auto whole_w = make();
  auto head_w = make();
  auto tail_w = make();
  const size_t all[] = {n};
  const size_t first[] = {head};
  const size_t rest[] = {n - head};
  ExpectBytesEq(EncodeChunks(*whole_w, points, d, all),
                EncodeChunks(*head_w, points, d, first) +
                  EncodeChunks(*tail_w, tail, d, rest));
}

// A cluster's lanes need not start on a group boundary, so one group can hold
// two clusters' documents -- encoded against different centroids. Every
// document must score exactly as it does when its cluster is alone in the
// stream: a lane mis-mapping silently returns a neighbour's distance, which
// still looks like a plausible result.
void ExpectSharedGroupsMatchSoloClusters(
  const std::function<std::unique_ptr<QuantizerWriter>()>& make,
  VectorQuantization quant, uint32_t d, VectorMetric metric,
  std::span<const size_t> sizes, const std::vector<float>& points,
  const std::vector<float>& centroids, const std::vector<float>& query) {
  ASSERT_EQ(centroids.size(), sizes.size() * d);
  ASSERT_EQ(SerializeStats(*make()), SerializeStats(*make()));

  SimpleMemoryAccounter memory;
  MemoryFile shared_file{memory};
  std::vector<PayloadStream::Cluster> spans;
  auto shared_w = make();
  uint64_t shared_bytes = 0;
  {
    MemoryIndexOutput out{shared_file};
    PayloadStream stream{*shared_w};
    size_t off = 0;
    for (size_t c = 0; c < sizes.size(); ++c) {
      shared_w->SetClusterCentroid(centroids.data() + c * d);
      spans.push_back(stream.Add(out, points.data() + off * d, sizes[c]));
      off += sizes[c];
    }
    stream.Finish(out);
    out.Flush();
    shared_bytes = out.Position();
  }
  // The whole point: one padded group for the stream, not one per cluster.
  size_t total = 0;
  size_t shared_groups = 0;
  size_t solo_groups = 0;
  const size_t group = shared_w->BlockSetting().group_size;
  for (const size_t s : sizes) {
    total += s;
    solo_groups += (s + group - 1) / group;
  }
  shared_groups = (total + group - 1) / group;
  ASSERT_LT(shared_groups, solo_groups);
  EXPECT_EQ(shared_bytes, shared_groups * group *
                            size_t{shared_w->BlockSetting().record_size})
    << "the stream must hold exactly " << shared_groups << " groups";
  auto stats = MakeQuantizerStats(quant, d, SerializeStats(*shared_w), metric);
  ASSERT_NE(stats, nullptr);
  auto codebook = stats->MakeCodebook(query);
  ASSERT_NE(codebook, nullptr);

  bool shares = false;
  size_t off = 0;
  for (size_t c = 0; c < sizes.size(); ++c) {
    const float* cen = centroids.data() + c * d;
    shares = shares || spans[c].lane0 != 0;

    MemoryFile solo_file{memory};
    auto solo_w = make();
    uint64_t solo_start = 0;
    {
      MemoryIndexOutput out{solo_file};
      solo_w->SetClusterCentroid(cen);
      solo_start =
        EncodeCluster(*solo_w, out, points.data() + off * d, sizes[c]);
      out.Flush();
    }
    auto solo_stats =
      MakeQuantizerStats(quant, d, SerializeStats(*solo_w), metric);
    ASSERT_NE(solo_stats, nullptr);
    auto solo_cb = solo_stats->MakeCodebook(query);
    ASSERT_NE(solo_cb, nullptr);

    ClusterScorer want{solo_cb, solo_file, solo_start, sizes[c], cen};
    ClusterScorer got{codebook, shared_file, spans[c].pay_start,
                      sizes[c], cen,         spans[c].lane0};
    const auto want_scores = want.All();
    const auto got_scores = got.All();
    ASSERT_EQ(want_scores.size(), sizes[c]);
    ASSERT_EQ(got_scores.size(), sizes[c]);
    for (size_t i = 0; i < sizes[c]; ++i) {
      EXPECT_EQ(want_scores[i], got_scores[i])
        << "cluster " << c << " lane0 " << spans[c].lane0 << " doc " << i;
    }

    // Again through the windowed path, which serves a partially consumed group
    // from its cache instead of scoring the whole cluster at once.
    std::vector<score_t> windowed(sizes[c], kNoPrune);
    constexpr size_t kWindow = 7;
    for (size_t b = 0; b < sizes[c]; b += kWindow) {
      const size_t m = std::min(kWindow, sizes[c] - b);
      got.Block(b, m, kNoPrune, windowed.data() + b);
    }
    for (size_t i = 0; i < sizes[c]; ++i) {
      EXPECT_EQ(want_scores[i], windowed[i])
        << "windowed cluster " << c << " doc " << i;
    }
    off += sizes[c];
  }
  EXPECT_TRUE(shares) << "sizes must not all be group-aligned";
}

}  // namespace

TEST(pq_quantizer_test, block_split_is_transparent_l2) {
  constexpr uint32_t d = 8;
  const auto pts = MakeSpread(d, 100, 11);
  const std::vector<float> cen(d, 0.25f);
  const auto make = [&] {
    return MakeTrainedPq(VectorMetric::L2Sqr, d, 2, pts, cen);
  };
  const size_t chunks[] = {5, 40, 7, 48};
  ExpectBlockSplitIsTransparent(make, d, pts, chunks);
}

TEST(pq_quantizer_test, block_split_is_transparent_inner_product) {
  constexpr uint32_t d = 8;
  const auto pts = MakeSpread(d, 100, 11);
  const std::vector<float> cen(d, 0.25f);
  const auto make = [&] {
    return MakeTrainedPq(VectorMetric::InnerProduct, d, 2, pts, cen);
  };
  const size_t chunks[] = {kFastScanBbs, 2 * kFastScanBbs, 4};
  ExpectBlockSplitIsTransparent(make, d, pts, chunks);
}

TEST(pq_quantizer_test, block_split_is_transparent_odd_m) {
  constexpr uint32_t d = 9;
  const auto pts = MakeSpread(d, 100, 13);
  const std::vector<float> cen(d, 0.1f);
  const auto make = [&] {
    return MakeTrainedPq(VectorMetric::L2Sqr, d, 3, pts, cen);
  };
  const size_t chunks[] = {3 * kFastScanBbs, 4};
  ExpectBlockSplitIsTransparent(make, d, pts, chunks);
}

TEST(pq_quantizer_test, groups_are_self_contained) {
  constexpr uint32_t d = 8;
  const auto pts = MakeSpread(d, 40, 11);
  const std::vector<float> cen(d, 0.25f);
  const auto make = [&] {
    return MakeTrainedPq(VectorMetric::L2Sqr, d, 2, pts, cen);
  };
  ASSERT_EQ(SerializeStats(*make()), SerializeStats(*make()));
  ExpectGroupsAreSelfContained(make, d, pts, kFastScanBbs);
}

TEST(pq_quantizer_test, tail_group_padding_is_zero) {
  constexpr uint32_t d = 8;
  constexpr size_t n = 40;
  const auto pts = MakeSpread(d, n, 11);
  const std::vector<float> cen(d, 0.25f);
  auto writer = MakeTrainedPq(VectorMetric::L2Sqr, d, 2, pts, cen);
  const size_t all[] = {n};
  const bstring payload = EncodeChunks(*writer, pts, d, all);
  const size_t code_bytes = kFastScanBbs * 2 / 2;
  const size_t group_bytes = code_bytes + kFastScanBbs * sizeof(float);
  ASSERT_EQ(payload.size(), 2 * group_bytes);
  std::array<float, kFastScanBbs> norms{};
  std::memcpy(norms.data(), payload.data() + group_bytes + code_bytes,
              sizeof(norms));
  for (size_t i = n - kFastScanBbs; i < kFastScanBbs; ++i) {
    EXPECT_EQ(norms[i], 0.f) << "slot " << i;
  }
}

TEST(rabitq_quantizer_test, block_split_is_transparent_one_bit) {
  constexpr uint32_t d = 8;
  const auto pts = MakeSpread(d, 100, 23);
  const std::vector<float> cen(d, 0.25f);
  const auto make = [&] { return MakeRaBitQ(d, /*nb_bits=*/1, cen); };
  const size_t chunks[] = {5, 40, 7, 48};
  ExpectBlockSplitIsTransparent(make, d, pts, chunks);
}

TEST(rabitq_quantizer_test, block_split_is_transparent_multibit) {
  constexpr uint32_t d = 96;
  const auto pts = MakeSpread(d, 72, 29);
  const std::vector<float> cen(d, 0.25f);
  const auto make = [&] { return MakeRaBitQ(d, /*nb_bits=*/8, cen); };
  const size_t chunks[] = {2 * kFastScanBbs, 8};
  ExpectBlockSplitIsTransparent(make, d, pts, chunks);
}

TEST(rabitq_quantizer_test, groups_are_self_contained) {
  constexpr uint32_t d = 96;
  const auto pts = MakeSpread(d, 40, 29);
  const std::vector<float> cen(d, 0.25f);
  const auto make = [&] { return MakeRaBitQ(d, /*nb_bits=*/8, cen); };
  ExpectGroupsAreSelfContained(make, d, pts, kFastScanBbs);
}

TEST(rabitq_quantizer_test, groups_are_self_contained_one_bit) {
  constexpr uint32_t d = 8;
  const auto pts = MakeSpread(d, 40, 23);
  const std::vector<float> cen(d, 0.25f);
  const auto make = [&] { return MakeRaBitQ(d, /*nb_bits=*/1, cen); };
  ExpectGroupsAreSelfContained(make, d, pts, kFastScanBbs);
}

namespace {

// Clusters of 5, 40 and 7 at a group size of 32: every boundary lands inside a
// group, and the last group is the only padded one.
constexpr size_t kRaggedSizes[] = {5, 40, 7};
constexpr size_t kRaggedTotal = 52;

std::vector<float> RaggedCentroids(uint32_t d) {
  std::vector<float> cen(std::size(kRaggedSizes) * d);
  for (size_t c = 0; c < std::size(kRaggedSizes); ++c) {
    std::fill_n(cen.begin() + c * d, d, 0.25f + 0.5f * static_cast<float>(c));
  }
  return cen;
}

}  // namespace

TEST(pq_quantizer_test, shared_groups_match_solo_clusters_l2) {
  constexpr uint32_t d = 8;
  const auto pts = MakeSpread(d, kRaggedTotal, 31);
  const auto cen = RaggedCentroids(d);
  const std::vector<float> cen0(cen.begin(), cen.begin() + d);
  const auto make = [&] {
    return MakeTrainedPq(VectorMetric::L2Sqr, d, 2, pts, cen0);
  };
  const auto query = MakeSpread(d, 1, 41);
  ExpectSharedGroupsMatchSoloClusters(make, VectorQuantization::PQ, d,
                                      VectorMetric::L2Sqr, kRaggedSizes, pts,
                                      cen, query);
}

TEST(pq_quantizer_test, shared_groups_match_solo_clusters_inner_product) {
  constexpr uint32_t d = 8;
  const auto pts = MakeSpread(d, kRaggedTotal, 31);
  const auto cen = RaggedCentroids(d);
  const std::vector<float> cen0(cen.begin(), cen.begin() + d);
  const auto make = [&] {
    return MakeTrainedPq(VectorMetric::InnerProduct, d, 2, pts, cen0);
  };
  const auto query = MakeSpread(d, 1, 41);
  ExpectSharedGroupsMatchSoloClusters(make, VectorQuantization::PQ, d,
                                      VectorMetric::InnerProduct, kRaggedSizes,
                                      pts, cen, query);
}

TEST(pq_quantizer_test, shared_groups_match_solo_clusters_odd_m) {
  constexpr uint32_t d = 9;
  const auto pts = MakeSpread(d, kRaggedTotal, 33);
  const auto cen = RaggedCentroids(d);
  const std::vector<float> cen0(cen.begin(), cen.begin() + d);
  const auto make = [&] {
    return MakeTrainedPq(VectorMetric::L2Sqr, d, 3, pts, cen0);
  };
  const auto query = MakeSpread(d, 1, 43);
  ExpectSharedGroupsMatchSoloClusters(make, VectorQuantization::PQ, d,
                                      VectorMetric::L2Sqr, kRaggedSizes, pts,
                                      cen, query);
}

TEST(rabitq_quantizer_test, shared_groups_match_solo_clusters_one_bit) {
  constexpr uint32_t d = 8;
  const auto pts = MakeSpread(d, kRaggedTotal, 35);
  const auto cen = RaggedCentroids(d);
  const auto make = [&] {
    return MakeRaBitQ(d, /*nb_bits=*/1, std::vector<float>(d, 0.25f));
  };
  const auto query = MakeSpread(d, 1, 45);
  ExpectSharedGroupsMatchSoloClusters(make, VectorQuantization::RaBitQ, d,
                                      VectorMetric::L2Sqr, kRaggedSizes, pts,
                                      cen, query);
}

TEST(rabitq_quantizer_test, shared_groups_match_solo_clusters_multibit) {
  constexpr uint32_t d = 96;
  const auto pts = MakeSpread(d, kRaggedTotal, 37);
  const auto cen = RaggedCentroids(d);
  const auto make = [&] {
    return MakeRaBitQ(d, /*nb_bits=*/8, std::vector<float>(d, 0.25f));
  };
  const auto query = MakeSpread(d, 1, 47);
  ExpectSharedGroupsMatchSoloClusters(make, VectorQuantization::RaBitQ, d,
                                      VectorMetric::L2Sqr, kRaggedSizes, pts,
                                      cen, query);
}

namespace {

struct CodeFixture {
  bstring blob;
  bstring codes;
  uint32_t record_size = 0;
  std::shared_ptr<const QuantizerStats> stats;
};

CodeFixture BuildCodes(VectorQuantization quant, uint32_t d, uint32_t nb_bits,
                       VectorMetric metric, const std::vector<float>& points,
                       size_t n, const std::vector<float>& centroid,
                       bool half_bit = false, uint32_t query_bits = 0) {
  CodeFixture f;
  auto writer = MakeQuantizerWriter({.kind = quant,
                                     .nb_bits = nb_bits,
                                     .half_bit = half_bit,
                                     .query_bits = query_bits},
                                    d, metric, 0, /*row_major=*/true);
  EXPECT_NE(writer, nullptr);
  if (writer->TrainSamples(n) != 0) {
    writer->Train(points.data(), n);
  }
  if (QuantizerNeedsCentroid(quant)) {
    writer->SetClusterCentroid(centroid.data());
  }
  if (const size_t refine = writer->RefineSamples(n); refine != 0) {
    writer->Refine(points.data(), std::min(n, refine));
    writer->RefineDone();
  }
  f.record_size = writer->BlockSetting().record_size;
  EXPECT_EQ(writer->BlockSetting().group_size, 1U);

  f.codes.resize(n * f.record_size, 0);
  EXPECT_TRUE(writer->EncodeInto(f.codes.data(), points.data(), n));

  f.blob = SerializeStats(*writer);
  f.stats = MakeQuantizerStats(quant, d, f.blob, metric, /*row_major=*/true);
  EXPECT_NE(f.stats, nullptr);
  return f;
}

std::unique_ptr<QuantizerReader> MakeKeyedReader(const CodeFixture& f,
                                                 std::span<const float> query,
                                                 const float* centroid) {
  auto codebook = f.stats->MakeCodebook(query);
  EXPECT_NE(codebook, nullptr);
  auto reader = MakeQuantizerReader(codebook);
  EXPECT_NE(reader, nullptr);
  reader->StartCluster(centroid);
  return reader;
}

float Cosine(std::span<const float> a, std::span<const float> b) {
  double dot = 0.0;
  double na = 0.0;
  double nb = 0.0;
  for (size_t i = 0; i < a.size(); ++i) {
    dot += double{a[i]} * double{b[i]};
    na += double{a[i]} * double{a[i]};
    nb += double{b[i]} * double{b[i]};
  }
  if (na == 0.0 || nb == 0.0) {
    return 0.f;
  }
  return static_cast<float>(dot / std::sqrt(na * nb));
}

struct CodeQuant {
  VectorQuantization quant;
  uint32_t nb_bits;
  float min_cosine;
  double min_tau;
  size_t min_top10;
  bool half_bit = false;
  uint32_t query_bits = 0;
};

std::string CodeQuantName(const ::testing::TestParamInfo<CodeQuant>& info) {
  const std::string bits =
    std::to_string(info.param.nb_bits) + (info.param.half_bit ? "_5" : "");
  switch (info.param.quant) {
    case VectorQuantization::SQ8:
      return "sq8";
    case VectorQuantization::SQ4:
      return "sq4";
    case VectorQuantization::TQ:
      return "tq" + bits;
    case VectorQuantization::BQ:
      return "bq" + bits + "_q" + std::to_string(info.param.query_bits);
    case VectorQuantization::OSQ:
      return "osq" + bits + "_q" + std::to_string(info.param.query_bits);
    case VectorQuantization::FP16:
      return "fp16";
    case VectorQuantization::BF16:
      return "bf16";
    default:
      return "other";
  }
}

class code_rekey_test : public ::testing::TestWithParam<CodeQuant> {};

}  // namespace

// EncodeInto must produce exactly what the streaming Encode path writes,
// otherwise a graph built on the buffer would disagree with the payload on
// disk that queries later read.
TEST_P(code_rekey_test, encode_into_matches_encode) {
  const auto quant = GetParam().quant;
  const auto nb_bits = GetParam().nb_bits;
  constexpr uint32_t d = 128;
  constexpr size_t n = 40;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const auto points = MakeSpread(d, n, 7);
  const std::vector<float> centroid(d, 0.05f);

  const auto f = BuildCodes(quant, d, nb_bits, metric, points, n, centroid,
                            GetParam().half_bit, GetParam().query_bits);

  auto writer = MakeQuantizerWriter({.kind = quant,
                                     .nb_bits = nb_bits,
                                     .half_bit = GetParam().half_bit,
                                     .query_bits = GetParam().query_bits},
                                    d, metric, 0, /*row_major=*/true);
  ASSERT_NE(writer, nullptr);
  if (writer->TrainSamples(n) != 0) {
    writer->Train(points.data(), n);
  }
  if (QuantizerNeedsCentroid(quant)) {
    writer->SetClusterCentroid(centroid.data());
  }
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t start = 0;
  uint64_t end = 0;
  {
    MemoryIndexOutput out{file};
    start = out.Position();
    writer->Encode(out, points.data(), n);
    writer->Finish(out);
    out.Flush();
    end = out.Position();
  }
  ExpectBytesEq(ReadPayload(file, start, end), f.codes);
}

TEST_P(code_rekey_test, decode_recovers_direction) {
  const auto quant = GetParam().quant;
  const auto nb_bits = GetParam().nb_bits;
  const auto min_cosine = GetParam().min_cosine;
  constexpr uint32_t d = 256;
  constexpr size_t n = 64;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const auto points = MakeSpread(d, n, 11);
  const std::vector<float> centroid(d, 0.f);

  const auto f = BuildCodes(quant, d, nb_bits, metric, points, n, centroid,
                            GetParam().half_bit, GetParam().query_bits);
  auto reader = MakeKeyedReader(f, {points.data(), d}, centroid.data());

  std::vector<float> decoded(d, 0.f);
  double total = 0.0;
  for (size_t i = 0; i < n; ++i) {
    ASSERT_TRUE(
      reader->Decode(f.codes.data() + i * f.record_size, decoded.data()));
    const float cos = Cosine({points.data() + i * d, d}, decoded);
    EXPECT_GE(cos, min_cosine) << "row " << i;
    total += cos;
  }
  EXPECT_GE(total / static_cast<double>(n), min_cosine);
}

// The build path scores a stored row against its neighbours by re-keying the
// reader from that row's own code. Those scores must rank the same way as a
// reader keyed on the raw vector, or the graph degrades.
TEST_P(code_rekey_test, rekey_from_code_ranks_like_raw_query) {
  const auto quant = GetParam().quant;
  const auto nb_bits = GetParam().nb_bits;
  constexpr uint32_t d = 256;
  constexpr size_t n = 96;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const auto points = MakeSpread(d, n, 13);
  const std::vector<float> centroid(d, 0.f);

  const auto f = BuildCodes(quant, d, nb_bits, metric, points, n, centroid,
                            GetParam().half_bit, GetParam().query_bits);

  std::vector<uint32_t> ids(n - 1);
  for (size_t i = 0; i < ids.size(); ++i) {
    ids[i] = static_cast<uint32_t>(i + 1);
  }

  auto raw = MakeKeyedReader(f, {points.data(), d}, centroid.data());
  std::vector<score_t> want(ids.size(), 0.f);
  raw->ComputeGathered(f.codes.data(), f.record_size, ids, kNoPrune,
                       want.data());

  std::vector<float> decoded(d, 0.f);
  ASSERT_TRUE(raw->Decode(f.codes.data(), decoded.data()));
  auto keyed = MakeKeyedReader(f, {points.data(), d}, centroid.data());
  ASSERT_TRUE(keyed->SetQuery(decoded));
  std::vector<score_t> got(ids.size(), 0.f);
  keyed->ComputeGathered(f.codes.data(), f.record_size, ids, kNoPrune,
                         got.data());

  size_t concordant = 0;
  size_t pairs = 0;
  for (size_t i = 0; i < ids.size(); ++i) {
    for (size_t j = i + 1; j < ids.size(); ++j) {
      ++pairs;
      const bool a = want[i] > want[j];
      const bool b = got[i] > got[j];
      concordant += a == b ? 1 : 0;
    }
  }
  ASSERT_NE(pairs, 0U);

  constexpr size_t kK = 10;
  const auto topk = [&ids](const std::vector<score_t>& s) {
    std::vector<uint32_t> order(ids.size());
    for (size_t i = 0; i < order.size(); ++i) {
      order[i] = static_cast<uint32_t>(i);
    }
    std::partial_sort(order.begin(), order.begin() + kK, order.end(),
                      [&s](uint32_t a, uint32_t b) { return s[a] > s[b]; });
    order.resize(kK);
    std::sort(order.begin(), order.end());
    return order;
  };
  const auto a = topk(want);
  const auto b = topk(got);
  std::vector<uint32_t> both;
  std::set_intersection(a.begin(), a.end(), b.begin(), b.end(),
                        std::back_inserter(both));

  const double tau =
    static_cast<double>(concordant) / static_cast<double>(pairs);
  EXPECT_GE(tau, GetParam().min_tau);
  EXPECT_GE(both.size(), GetParam().min_top10);
}

// The graph build scores two STORED rows against each other through the
// symmetric code-to-code estimator rather than by decoding one side back into a
// query. That estimator has to agree with the asymmetric one it replaces -- a
// sign or scale slip would silently corrupt the diversity heuristic and the
// back-link pruning, which is the kind of damage that surfaces only as a recall
// regression much later.
TEST_P(code_rekey_test, pair_scores_track_the_keyed_reader) {
  const auto quant = GetParam().quant;
  const auto nb_bits = GetParam().nb_bits;
  constexpr uint32_t d = 128;
  constexpr size_t n = 64;
  const auto points = MakeSpread(d, n, 19);
  const std::vector<float> centroid(d, 0.05f);
  for (const VectorMetric metric :
       {VectorMetric::InnerProduct, VectorMetric::L2Sqr}) {
    const auto f = BuildCodes(quant, d, nb_bits, metric, points, n, centroid,
                              GetParam().half_bit, GetParam().query_bits);

    const std::vector<float> scratch(d, 0.f);
    auto reader = MakeKeyedReader(f, scratch, centroid.data());
    ASSERT_NE(reader, nullptr);
    if (!reader->SupportsPairScores()) {
      GTEST_SKIP() << "quantizer has no symmetric estimator";
    }

    std::vector<float> terms;
    ASSERT_TRUE(
      reader->PreparePairTerms(f.codes.data(), f.record_size, n, terms));
    ASSERT_FALSE(terms.empty());

    std::vector<uint32_t> ids(n);
    for (uint32_t i = 0; i < n; ++i) {
      ids[i] = i;
    }

    std::vector<float> sym;
    std::vector<float> keyed;
    std::vector<score_t> sym_row(n, 0.f);
    std::vector<score_t> keyed_row(n, 0.f);
    std::vector<float> decoded(d, 0.f);

    for (uint32_t a = 0; a < n; ++a) {
      reader->ScorePairBatch(f.codes.data(), f.record_size, terms, a, ids,
                             sym_row.data());

      // Reference: decode row `a` and score through the ordinary keyed path --
      // precisely what the build used to do per accepted neighbour.
      auto keyed_reader = MakeKeyedReader(f, scratch, centroid.data());
      ASSERT_TRUE(keyed_reader->Decode(
        f.codes.data() + size_t{a} * f.record_size, decoded.data()));
      ASSERT_TRUE(keyed_reader->SetQuery(decoded));
      keyed_reader->ComputeGathered(f.codes.data(), f.record_size, ids,
                                    std::numeric_limits<score_t>::lowest(),
                                    keyed_row.data());

      for (uint32_t b = 0; b < n; ++b) {
        if (a == b) {
          continue;  // a self-pair is not a decision the build ever makes
        }
        sym.push_back(sym_row[b]);
        keyed.push_back(keyed_row[b]);
      }
    }

    // Both estimate the same inner product, so they agree in shape rather than
    // bit-for-bit: the symmetric side quantizes both operands where the keyed
    // side quantizes only one.
    // Shape agreement alone is not enough. The build mixes these symmetric pair
    // scores with ASYMMETRIC candidate scores inside the diversity heuristic
    // ("is the accepted node closer to this candidate than the query is?"), so
    // a constant offset between the two estimators is just as damaging as noise
    // -- and a correlation check cannot see one. Pin the offset against the
    // spread of the scores themselves, and pin how often the two would decide a
    // comparison differently.
    double mean_sym = 0;
    double mean_keyed = 0;
    for (size_t i = 0; i < sym.size(); ++i) {
      mean_sym += sym[i];
      mean_keyed += keyed[i];
    }
    mean_sym /= static_cast<double>(sym.size());
    mean_keyed /= static_cast<double>(keyed.size());
    double var_keyed = 0;
    for (const auto v : keyed) {
      var_keyed += (v - mean_keyed) * (v - mean_keyed);
    }
    const double sd_keyed = std::sqrt(var_keyed / keyed.size());
    EXPECT_LE(std::abs(mean_sym - mean_keyed), 0.05 * sd_keyed)
      << "systematic offset " << (mean_sym - mean_keyed) << " against sd "
      << sd_keyed << " for metric " << static_cast<int>(metric);

    size_t disagree = 0;
    for (size_t i = 0; i < sym.size(); ++i) {
      disagree += (sym[i] > mean_keyed) != (keyed[i] > mean_keyed) ? 1 : 0;
    }
    EXPECT_LE(static_cast<double>(disagree) / static_cast<double>(sym.size()),
              0.05)
      << "metric " << static_cast<int>(metric);

    EXPECT_GE(Cosine(sym, keyed), GetParam().min_cosine)
      << "metric " << static_cast<int>(metric);
  }
}

// Two scoring paths now serve one query. Batches below a full block go through
// the per-row int8 kernel (no 32-lane transpose); a full block goes through
// fastscan. They quantize the same dot product differently, so they are NOT
// bit-identical -- what has to hold is that each is self-consistent under
// chunking, and that the two agree closely enough to rank the same way.
TEST_P(code_rekey_test, gathered_scores_are_chunking_invariant) {
  const auto quant = GetParam().quant;
  const auto nb_bits = GetParam().nb_bits;
  constexpr uint32_t d = 256;
  constexpr size_t n = 80;
  const VectorMetric metric = VectorMetric::InnerProduct;
  const auto points = MakeSpread(d, n, 29);
  const std::vector<float> centroid(d, 0.f);

  const auto f = BuildCodes(quant, d, nb_bits, metric, points, n, centroid,
                            GetParam().half_bit, GetParam().query_bits);

  std::vector<uint32_t> ids(n - 1);
  for (size_t i = 0; i < ids.size(); ++i) {
    ids[i] = static_cast<uint32_t>(i + 1);
  }

  auto reader = MakeKeyedReader(f, {points.data(), d}, centroid.data());
  const auto score_in_chunks = [&](size_t chunk) {
    std::vector<score_t> got(ids.size(), 0.f);
    for (size_t off = 0; off < ids.size(); off += chunk) {
      const size_t take = std::min(chunk, ids.size() - off);
      reader->ComputeGathered(f.codes.data(), f.record_size,
                              std::span<const uint32_t>{ids}.subspan(off, take),
                              kNoPrune, got.data() + off);
    }
    return got;
  };

  // 1 and 5 are what HNSW actually passes; 16 is the last size below a block.
  // All three take the per-row path, so they must agree exactly with each
  // other.
  const auto small = score_in_chunks(10);
  for (const size_t chunk : {size_t{1}, size_t{5}, size_t{16}}) {
    const auto got = score_in_chunks(chunk);
    for (size_t i = 0; i < ids.size(); ++i) {
      EXPECT_EQ(small[i], got[i]) << "chunk " << chunk << " index " << i;
    }
  }

  // A full block takes fastscan. Compare the two paths on offset-against-spread
  // and on how often they would order a pair differently -- an absolute
  // tolerance would say nothing without knowing the scale.
  const auto block = score_in_chunks(32);
  double mean_abs_delta = 0.0;
  double mean = 0.0;
  for (size_t i = 0; i < ids.size(); ++i) {
    mean_abs_delta += std::fabs(double{block[i]} - double{small[i]});
    mean += block[i];
  }
  mean_abs_delta /= static_cast<double>(ids.size());
  mean /= static_cast<double>(ids.size());
  double var = 0.0;
  for (const auto v : block) {
    var += (v - mean) * (v - mean);
  }
  const double spread = std::sqrt(var / static_cast<double>(ids.size()));
  ASSERT_GT(spread, 0.0);
  EXPECT_LT(mean_abs_delta, 0.15 * spread)
    << "per-row vs fastscan: mean |delta| " << mean_abs_delta << " against sd "
    << spread;

  size_t pairs = 0;
  size_t disagree = 0;
  for (size_t i = 0; i < ids.size(); ++i) {
    for (size_t j = i + 1; j < ids.size(); ++j) {
      ++pairs;
      if ((block[i] > block[j]) != (small[i] > small[j])) {
        ++disagree;
      }
    }
  }
  ASSERT_NE(pairs, 0U);
  EXPECT_LT(static_cast<double>(disagree) / static_cast<double>(pairs), 0.03)
    << disagree << " of " << pairs << " pairs ordered differently";
}

INSTANTIATE_TEST_SUITE_P(
  quants, code_rekey_test,
  ::testing::Values(
    CodeQuant{VectorQuantization::SQ8, 0, 0.99f, 0.99, 10},
    CodeQuant{VectorQuantization::SQ4, 0, 0.9f, 0.95, 9},
    CodeQuant{VectorQuantization::TQ, 4, 0.9f, 0.95, 9},
    CodeQuant{VectorQuantization::TQ, 2, 0.7f, 0.85, 6},
    CodeQuant{VectorQuantization::TQ, 3, 0.7f, 0.85, 6},
    CodeQuant{VectorQuantization::TQ, 1, 0.6f, 0.8, 5, true},
    CodeQuant{VectorQuantization::BQ, 1, 0.7f, 0.8, 5, false, 1},
    CodeQuant{VectorQuantization::BQ, 1, 0.7f, 0.75, 5, false, 8},
    CodeQuant{VectorQuantization::BQ, 1, 0.7f, 0.8, 5, true, 8},
    CodeQuant{VectorQuantization::BQ, 2, 0.8f, 0.85, 3, false, 8},
    CodeQuant{VectorQuantization::OSQ, 1, 0.7f, 0.75, 5, false, 4},
    CodeQuant{VectorQuantization::OSQ, 2, 0.8f, 0.8, 5, false, 4},
    CodeQuant{VectorQuantization::OSQ, 4, 0.9f, 0.85, 6, false, 4},
    CodeQuant{VectorQuantization::OSQ, 7, 0.95f, 0.9, 7, false, 7},
    CodeQuant{VectorQuantization::OSQ, 8, 0.95f, 0.9, 7, false, 8},
    CodeQuant{VectorQuantization::FP16, 0, 0.999f, 0.99, 10},
    CodeQuant{VectorQuantization::BF16, 0, 0.99f, 0.97, 9}),
  CodeQuantName);

namespace {

struct BoundCase {
  VectorQuantization quant;
  VectorMetric metric;
};

std::string BoundCaseName(const ::testing::TestParamInfo<BoundCase>& info) {
  std::string name =
    info.param.quant == VectorQuantization::USQ8 ? "usq8" : "sq8";
  switch (info.param.metric) {
    case VectorMetric::L2Sqr:
      return name + "_l2";
    case VectorMetric::InnerProduct:
      return name + "_ip";
    default:
      return name + "_cosine";
  }
}

float ExactScore(VectorMetric metric, const float* q, const float* x,
                 uint32_t d) {
  float out = 0.f;
  ResolveEnum<VectorMetric>(metric, [&]<VectorMetric M>() {
    out = ComputeDistance<M>(q, x, static_cast<uint16_t>(d));
  });
  return out;
}

std::vector<float> Normalized(std::span<const float> v) {
  std::vector<float> out(v.size());
  duckdb::L2NormalizeOp::Operation(v.data(), out.data(), v.size());
  return out;
}

class code_bound_test : public ::testing::TestWithParam<BoundCase> {};

}  // namespace

TEST_P(code_bound_test, bounds_hold_every_stored_vector) {
  const auto [quant, metric] = GetParam();
  const bool cosine = metric == VectorMetric::Cosine;
  for (const uint32_t d : {7U, 64U, 129U, 1024U}) {
    constexpr size_t kRows = 600;
    auto raw = MakeSpread(d, kRows, d * 31 + 7);
    std::mt19937 rng{d};
    std::uniform_real_distribution<float> offset{-3.f, 3.f};
    for (size_t i = 0; i < kRows; ++i) {
      const float shift = offset(rng);
      for (uint32_t j = 0; j < d; ++j) {
        raw[i * d + j] = raw[i * d + j] * (1.f + j % 5) + shift;
      }
    }
    for (uint32_t j = 0; j < d; ++j) {
      raw[j] = 40.f;
      raw[d + j] = -40.f;
    }
    auto points = raw;
    if (cosine) {
      for (size_t i = 0; i < kRows; ++i) {
        duckdb::L2NormalizeOp::Operation(points.data() + i * d,
                                         points.data() + i * d, d);
      }
    }
    const auto f =
      BuildCodes(quant, d, 0, EffectiveQuantMetric(metric), points, kRows, {});
    ASSERT_EQ(f.record_size, d);
    std::vector<uint32_t> ids(kRows);
    for (size_t i = 0; i < kRows; ++i) {
      ids[i] = static_cast<uint32_t>(i);
    }
    const auto queries = MakeSpread(d, 16, d * 17 + 3);
    for (size_t qi = 0; qi < 16; ++qi) {
      std::vector<float> query(queries.begin() + qi * d,
                               queries.begin() + (qi + 1) * d);
      for (uint32_t j = 0; j < d; ++j) {
        query[j] = query[j] * (qi % 3 == 0 ? 25.f : 1.f) + (qi % 2 ? 2.f : 0.f);
      }
      const auto keyed = cosine ? Normalized(query) : query;
      const auto bound = f.stats->MakeCodeBound(keyed, metric);
      ASSERT_NE(bound, nullptr);
      std::vector<score_t> approx(kRows);
      std::vector<score_t> lower(kRows);
      std::vector<score_t> upper(kRows);
      bound->Score(f.codes.data(), f.record_size, ids, approx.data());
      bound->Bounds(approx, lower.data(), upper.data());
      for (size_t i = 0; i < kRows; ++i) {
        const auto exact =
          ExactScore(metric, query.data(), raw.data() + i * d, d);
        ASSERT_LE(lower[i], exact) << "d=" << d << " query " << qi << " row "
                                   << i << " approx " << approx[i];
        ASSERT_GE(upper[i], exact) << "d=" << d << " query " << qi << " row "
                                   << i << " approx " << approx[i];
      }
    }
  }
}

TEST_P(code_bound_test, constant_rows_bound_to_their_score) {
  const auto [quant, metric] = GetParam();
  constexpr uint32_t kDim = 33;
  constexpr size_t kRows = 8;
  std::vector<float> points(kRows * kDim, 0.25f);
  auto encoded = points;
  if (metric == VectorMetric::Cosine) {
    for (size_t i = 0; i < kRows; ++i) {
      duckdb::L2NormalizeOp::Operation(encoded.data() + i * kDim,
                                       encoded.data() + i * kDim, kDim);
    }
  }
  const auto f = BuildCodes(quant, kDim, 0, EffectiveQuantMetric(metric),
                            encoded, kRows, {});
  std::vector<float> query(kDim);
  for (uint32_t j = 0; j < kDim; ++j) {
    query[j] = 0.1f * static_cast<float>(j) - 1.f;
  }
  const auto keyed = metric == VectorMetric::Cosine ? Normalized(query) : query;
  const auto bound = f.stats->MakeCodeBound(keyed, metric);
  ASSERT_NE(bound, nullptr);
  std::vector<uint32_t> ids(kRows, 0);
  for (size_t i = 0; i < kRows; ++i) {
    ids[i] = static_cast<uint32_t>(i);
  }
  std::vector<score_t> approx(kRows);
  std::vector<score_t> lower(kRows);
  std::vector<score_t> upper(kRows);
  bound->Score(f.codes.data(), f.record_size, ids, approx.data());
  bound->Bounds(approx, lower.data(), upper.data());
  const auto exact = ExactScore(metric, query.data(), points.data(), kDim);
  for (size_t i = 0; i < kRows; ++i) {
    EXPECT_LE(lower[i], exact);
    EXPECT_GE(upper[i], exact);
    EXPECT_LT(upper[i] - lower[i], 1e-3f * (1.f + std::fabs(exact)));
  }
}

INSTANTIATE_TEST_SUITE_P(
  quants, code_bound_test,
  ::testing::Values(
    BoundCase{VectorQuantization::USQ8, VectorMetric::L2Sqr},
    BoundCase{VectorQuantization::USQ8, VectorMetric::InnerProduct},
    BoundCase{VectorQuantization::USQ8, VectorMetric::Cosine},
    BoundCase{VectorQuantization::SQ8, VectorMetric::L2Sqr},
    BoundCase{VectorQuantization::SQ8, VectorMetric::InnerProduct},
    BoundCase{VectorQuantization::SQ8, VectorMetric::Cosine}),
  BoundCaseName);

TEST(sq8_quantizer_test, inner_product_tracks_decoded_rows) {
  constexpr uint32_t d = 512;
  constexpr size_t n = 400;
  constexpr size_t kTop = 10;
  for (const auto quant : {VectorQuantization::SQ8, VectorQuantization::USQ8}) {
    const auto points = MakeSpread(d, n, 21);
    const std::vector<float> centroid(d, 0.f);
    const auto f =
      BuildCodes(quant, d, 0, VectorMetric::InnerProduct, points, n, centroid);
    std::vector<uint32_t> ids(n);
    std::iota(ids.begin(), ids.end(), 0U);
    for (uint32_t seed = 0; seed < 8; ++seed) {
      const auto query = MakeSpread(d, 1, 100 + seed);
      auto reader = MakeKeyedReader(f, query, centroid.data());
      std::vector<score_t> got(n);
      reader->ComputeGathered(f.codes.data(), f.record_size, ids, kNoPrune,
                              got.data());
      std::vector<float> decoded(d);
      std::vector<double> want(n);
      for (size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(
          reader->Decode(f.codes.data() + i * f.record_size, decoded.data()));
        double dot = 0;
        for (uint32_t j = 0; j < d; ++j) {
          dot += double{query[j]} * double{decoded[j]};
        }
        want[i] = dot;
      }
      const auto [lo, hi] = std::ranges::minmax(want);
      double offset = 0;
      for (size_t i = 0; i < n; ++i) {
        offset += got[i] - want[i];
      }
      offset /= static_cast<double>(n);
      EXPECT_LT(std::abs(offset), 2e-2 * (hi - lo)) << "query " << seed;
      for (size_t i = 0; i < n; ++i) {
        ASSERT_NEAR(got[i] - offset, want[i], 1e-2 * (hi - lo))
          << (quant == VectorQuantization::SQ8 ? "sq8" : "usq8") << " query "
          << seed << " row " << i;
      }
      const auto top = [&](const auto& scores) {
        std::vector<uint32_t> order(n);
        std::iota(order.begin(), order.end(), 0U);
        std::partial_sort(
          order.begin(), order.begin() + kTop, order.end(),
          [&](uint32_t a, uint32_t b) { return scores[a] > scores[b]; });
        order.resize(kTop);
        std::ranges::sort(order);
        return order;
      };
      std::vector<uint32_t> common;
      std::ranges::set_intersection(top(got), top(want),
                                    std::back_inserter(common));
      EXPECT_GE(common.size(), kTop - 1) << "query " << seed;
    }
  }
}

TEST(sq8_quantizer_test, level_bound_prunes_only_rows_below_the_threshold) {
  constexpr uint32_t d = 300;
  constexpr uint32_t kWidth = 128;
  constexpr uint32_t kStep = 64;
  constexpr size_t n = 400;
  std::mt19937 rng{59};
  std::normal_distribution<float> nd{0.f, 1.f};
  const auto draw = [&](size_t rows) {
    std::vector<float> out(rows * d);
    for (size_t i = 0; i < rows; ++i) {
      for (uint32_t j = 0; j < d; ++j) {
        out[i * d + j] = nd(rng) / (1.f + static_cast<float>(j) / 8.f);
      }
    }
    return out;
  };
  const auto points = draw(n);
  const auto f = BuildCodes(VectorQuantization::SQ8, d, 0,
                            VectorMetric::InnerProduct, points, n, {});
  constexpr uint32_t kTails = (d + kWidth - 1) / kWidth - 1;
  std::vector<uint32_t> ids(n);
  std::iota(ids.begin(), ids.end(), 0U);
  for (uint32_t seed = 0; seed < 4; ++seed) {
    const auto query = draw(1);
    auto reader = MakeKeyedReader(f, query, nullptr);
    std::vector<float> decoded(n * d);
    std::vector<float> tails(n * kTails);
    for (size_t i = 0; i < n; ++i) {
      float* row = decoded.data() + i * d;
      ASSERT_TRUE(reader->Decode(f.codes.data() + i * f.record_size, row));
      for (uint32_t k = 1; k <= kTails; ++k) {
        double sum = 0;
        for (uint32_t j = k * kWidth; j < d; ++j) {
          sum += double{row[j]} * row[j];
        }
        tails[i * kTails + k - 1] = static_cast<float>(std::sqrt(sum));
      }
    }
    std::vector<float> q_tails(d / kStep + 2, 0.f);
    for (uint32_t t = 0; t * kStep < d; ++t) {
      double sum = 0;
      for (uint32_t j = t * kStep; j < d; ++j) {
        sum += double{query[j]} * query[j];
      }
      q_tails[t] = static_cast<float>(std::sqrt(sum));
    }
    const QuantizerLevelBound bound{.width = kWidth,
                                    .query_step = kStep,
                                    .query = q_tails,
                                    .rows = tails.data(),
                                    .row_stride = kTails};
    std::vector<score_t> full(n);
    if (!reader->ComputeGatheredBounded(f.codes.data(), f.record_size, ids,
                                        bound, kNoPrune, full.data())) {
      GTEST_SKIP() << "no level-bounded sq8 kernel on this target";
    }
    for (size_t i = 0; i < n; ++i) {
      const auto want = ExactScore(VectorMetric::InnerProduct, query.data(),
                                   decoded.data() + i * d, d);
      ASSERT_NEAR(full[i], want, 1e-4f * (1.f + std::abs(want)))
        << "query " << seed << " row " << i;
    }
    auto sorted = full;
    std::ranges::sort(sorted);
    for (const double share : {0.1, 0.5, 0.9}) {
      const auto threshold =
        sorted[static_cast<size_t>(share * static_cast<double>(n - 1))];
      std::vector<score_t> got(n);
      ASSERT_TRUE(reader->ComputeGatheredBounded(
        f.codes.data(), f.record_size, ids, bound, threshold, got.data()));
      size_t dropped = 0;
      for (size_t i = 0; i < n; ++i) {
        if (got[i] == kNoPrune) {
          ++dropped;
          EXPECT_LE(full[i], threshold) << "share " << share << " row " << i;
        } else {
          EXPECT_EQ(got[i], full[i]) << "share " << share << " row " << i;
        }
      }
      if (share > 0.5) {
        EXPECT_GT(dropped, 0U) << "query " << seed;
      }
    }
  }
}

TEST(code_bound_test, only_byte_scalar_codes_are_bounded) {
  constexpr uint32_t kDim = 16;
  constexpr size_t kRows = 64;
  const auto points = MakeSpread(kDim, kRows, 5);
  const std::vector<float> query(kDim, 0.5f);
  for (const auto quant : {VectorQuantization::SQ4, VectorQuantization::USQ4}) {
    const auto f =
      BuildCodes(quant, kDim, 0, VectorMetric::L2Sqr, points, kRows, {});
    EXPECT_EQ(f.stats->MakeCodeBound(query, VectorMetric::L2Sqr), nullptr);
  }
  const auto f = BuildCodes(VectorQuantization::USQ8, kDim, 0,
                            VectorMetric::L2Sqr, points, kRows, {});
  EXPECT_EQ(f.stats->MakeCodeBound(query, VectorMetric::L1), nullptr);
  EXPECT_NE(f.stats->MakeCodeBound(query, VectorMetric::L2Sqr), nullptr);
}

TEST(tq_quantizer_test, codes_span_the_dimension_rounded_to_sixteen) {
  struct Case {
    uint32_t bits;
    double min_cosine;
    size_t min_top10;
  };
  constexpr size_t n = 200;
  constexpr size_t kK = 10;
  for (const uint32_t d : {200U, 768U, 3072U}) {
    const uint32_t rd = (d + 15) / 16 * 16;
    const auto points = MakeSpread(d, n, 31);
    const auto query = MakeSpread(d, 1, 37);
    const std::vector<float> centroid(d, 0.f);
    std::vector<float> want(n);
    for (size_t i = 0; i < n; ++i) {
      want[i] = ExactScore(VectorMetric::InnerProduct, query.data(),
                           points.data() + i * d, d);
    }
    std::vector<uint32_t> ids(n);
    std::iota(ids.begin(), ids.end(), uint32_t{0});
    const auto top = [&ids](const auto& s) {
      auto order = ids;
      std::partial_sort(order.begin(), order.begin() + kK, order.end(),
                        [&s](uint32_t a, uint32_t b) { return s[a] > s[b]; });
      order.resize(kK);
      std::sort(order.begin(), order.end());
      return order;
    };
    const auto best = top(want);
    for (const Case c : {Case{4, 0.95, 8}, Case{2, 0.85, 6}, Case{1, 0.7, 4}}) {
      const auto f =
        BuildCodes(VectorQuantization::TQ, d, c.bits,
                   VectorMetric::InnerProduct, points, n, centroid);
      EXPECT_EQ(f.record_size, rd * c.bits / 8 + 2 * sizeof(float))
        << "d=" << d << " bits=" << c.bits;
      auto reader = MakeKeyedReader(f, query, centroid.data());
      std::vector<float> decoded(d);
      double cosine = 0.0;
      for (size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(
          reader->Decode(f.codes.data() + i * f.record_size, decoded.data()));
        cosine += Cosine({points.data() + i * d, d}, decoded);
      }
      EXPECT_GE(cosine / static_cast<double>(n), c.min_cosine)
        << "d=" << d << " bits=" << c.bits;
      std::vector<score_t> got(n);
      reader->ComputeGathered(f.codes.data(), f.record_size, ids, kNoPrune,
                              got.data());
      const auto found = top(got);
      std::vector<uint32_t> both;
      std::set_intersection(best.begin(), best.end(), found.begin(),
                            found.end(), std::back_inserter(both));
      EXPECT_GE(both.size(), c.min_top10) << "d=" << d << " bits=" << c.bits;
    }
  }
}

TEST(tq_quantizer_test, segments_of_an_index_share_one_codebook_per_query) {
  constexpr uint32_t d = 200;
  constexpr size_t n = 300;
  const auto points = MakeSpread(d, n, 41);
  const auto query = MakeSpread(d, 1, 43);
  const std::vector<float> centroid(d, 0.f);
  const auto build = [&](VectorQuantization quant, uint32_t bits) {
    return BuildCodes(quant, d, bits, VectorMetric::InnerProduct, points, n,
                      centroid);
  };
  const auto first = build(VectorQuantization::TQ, 4);
  const auto second = build(VectorQuantization::TQ, 4);
  const auto narrower = build(VectorQuantization::TQ, 2);
  const auto sq8 = build(VectorQuantization::SQ8, 8);
  AnnQueryCache cache;
  const auto shared = cache.Codebook(*first.stats, query);
  EXPECT_EQ(shared, cache.Codebook(*second.stats, query));
  EXPECT_NE(shared, cache.Codebook(*narrower.stats, query));
  EXPECT_NE(cache.Codebook(*sq8.stats, query),
            cache.Codebook(*sq8.stats, query));

  auto own = MakeKeyedReader(second, query, centroid.data());
  auto via_cache = MakeQuantizerReader(shared);
  via_cache->StartCluster(centroid.data());
  std::vector<score_t> want(n);
  std::vector<score_t> got(n);
  own->ComputeBlock(second.codes, kNoPrune, want.data());
  via_cache->ComputeBlock(second.codes, kNoPrune, got.data());
  EXPECT_EQ(want, got);
}

namespace {

CodeFixture BuildPqCodes(uint32_t d, uint32_t pq_m, VectorMetric metric,
                         const std::vector<float>& points, size_t n) {
  CodeFixture f;
  auto writer =
    MakeQuantizerWriter({.kind = VectorQuantization::PQ, .pq_m = pq_m}, d,
                        metric, 0, /*row_major=*/true);
  EXPECT_NE(writer, nullptr);
  writer->Train(points.data(), std::min(n, writer->TrainSamples(n)));
  f.record_size = writer->BlockSetting().record_size;
  EXPECT_EQ(writer->BlockSetting().group_size, 1U);
  EXPECT_EQ(f.record_size, pq_m);
  f.codes.resize(n * f.record_size, 0);
  EXPECT_TRUE(writer->EncodeInto(f.codes.data(), points.data(), n));
  f.blob = SerializeStats(*writer);
  f.stats = MakeQuantizerStats(VectorQuantization::PQ, d, f.blob, metric,
                               /*row_major=*/true);
  EXPECT_NE(f.stats, nullptr);
  return f;
}

}  // namespace

TEST(hnsw_pq_quantizer_test, codes_rank_like_exact) {
  constexpr uint32_t d = 32;
  constexpr uint32_t pq_m = 8;
  constexpr size_t n = 1024;
  constexpr size_t nq = 16;
  const auto points = MakeSpread(d, n, 41);
  const auto queries = MakeSpread(d, nq, 43);
  const std::vector<float> centroid(d, 0.f);
  for (const VectorMetric metric :
       {VectorMetric::L2Sqr, VectorMetric::InnerProduct}) {
    const auto f = BuildPqCodes(d, pq_m, metric, points, n);
    size_t found = 0;
    for (size_t qi = 0; qi < nq; ++qi) {
      const std::span<const float> q{queries.data() + qi * d, d};
      auto reader = MakeKeyedReader(f, q, centroid.data());
      std::vector<score_t> got(n);
      reader->ComputeBlock(f.codes, kNoPrune, got.data());
      std::vector<float> want(n);
      for (size_t i = 0; i < n; ++i) {
        want[i] = ExactScore(metric, q.data(), points.data() + i * d, d);
      }
      EXPECT_GE(Cosine(got, want), 0.95f) << static_cast<int>(metric);
      const auto best =
        std::max_element(want.begin(), want.end()) - want.begin();
      size_t above = 0;
      for (size_t i = 0; i < n; ++i) {
        above += got[i] > got[best] ? 1 : 0;
      }
      found += above < 10 ? 1 : 0;
    }
    EXPECT_GE(found, nq - 1) << static_cast<int>(metric);
  }
}

TEST(hnsw_pq_quantizer_test, pair_scores_are_decoded_distances) {
  constexpr uint32_t d = 24;
  constexpr size_t n = 300;
  const auto points = MakeSpread(d, n, 53);
  const std::vector<float> centroid(d, 0.f);
  for (const uint32_t pq_m : {6U, 24U}) {
    for (const VectorMetric metric :
         {VectorMetric::L2Sqr, VectorMetric::InnerProduct}) {
      const auto f = BuildPqCodes(d, pq_m, metric, points, n);
      const std::vector<float> scratch(d, 0.f);
      auto reader = MakeKeyedReader(f, scratch, centroid.data());
      ASSERT_TRUE(reader->SupportsPairScores());
      std::vector<float> terms;
      ASSERT_TRUE(
        reader->PreparePairTerms(f.codes.data(), f.record_size, n, terms));

      std::vector<float> decoded(n * d);
      for (size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(reader->Decode(f.codes.data() + i * f.record_size,
                                   decoded.data() + i * d));
      }
      std::vector<uint32_t> ids(n);
      for (uint32_t i = 0; i < n; ++i) {
        ids[i] = i;
      }
      std::vector<score_t> pair(n);
      std::vector<score_t> keyed(n);
      for (const uint32_t a : {0U, 7U, 0U, 151U, 7U, 299U}) {
        reader->ScorePairBatch(f.codes.data(), f.record_size, terms, a, ids,
                               pair.data());
        auto keyed_reader = MakeKeyedReader(f, scratch, centroid.data());
        ASSERT_TRUE(keyed_reader->SetQuery({decoded.data() + a * d, d}));
        keyed_reader->ComputeGathered(f.codes.data(), f.record_size, ids,
                                      kNoPrune, keyed.data());
        for (size_t b = 0; b < n; ++b) {
          const float want = ExactScore(metric, decoded.data() + a * d,
                                        decoded.data() + b * d, d);
          EXPECT_NEAR(pair[b], want, 1e-4f * (1.f + std::fabs(want)))
            << "a=" << a << " b=" << b;
          const float* va = decoded.data() + a * d;
          const float* vb = decoded.data() + b * d;
          const float norms =
            ExactScore(VectorMetric::InnerProduct, va, va, d) +
            ExactScore(VectorMetric::InnerProduct, vb, vb, d);
          EXPECT_NEAR(keyed[b], want, 1e-3f * (1.f + std::fabs(want) + norms))
            << "a=" << a << " b=" << b;
        }
      }
    }
  }
}

TEST(hnsw_pq_quantizer_test, line_codebook_is_tight) {
  constexpr uint32_t d = 8;
  constexpr size_t n = 4096;
  const auto points = MakeSpread(d, n, 83);
  const std::vector<float> centroid(d, 0.f);
  const auto f = BuildPqCodes(d, d, VectorMetric::L2Sqr, points, n);
  auto reader = MakeKeyedReader(f, centroid, centroid.data());
  std::vector<float> row(d);
  double sum = 0.0;
  float worst = 0.f;
  for (size_t i = 0; i < n; ++i) {
    ASSERT_TRUE(reader->Decode(f.codes.data() + i * f.record_size, row.data()));
    for (uint32_t j = 0; j < d; ++j) {
      const float e = row[j] - points[i * d + j];
      sum += double{e} * e;
      worst = std::max(worst, std::fabs(e));
    }
  }
  EXPECT_LT(sum / static_cast<double>(n * d), 1.2e-4);
  EXPECT_LT(worst, 0.5f);
}

TEST(hnsw_pq_quantizer_test, gathered_matches_block) {
  constexpr uint32_t d = 16;
  constexpr uint32_t pq_m = 4;
  constexpr size_t n = 400;
  const auto points = MakeSpread(d, n, 61);
  const std::vector<float> centroid(d, 0.f);
  const auto f = BuildPqCodes(d, pq_m, VectorMetric::L2Sqr, points, n);
  const auto query = MakeSpread(d, 1, 67);
  auto reader = MakeKeyedReader(f, query, centroid.data());
  std::vector<score_t> block(n);
  reader->ComputeBlock(f.codes, kNoPrune, block.data());
  std::vector<uint32_t> ids(n);
  for (uint32_t i = 0; i < n; ++i) {
    ids[i] = (i * 37) % n;
  }
  std::vector<score_t> gathered(n);
  for (size_t off = 0; off < n; off += 33) {
    const size_t take = std::min<size_t>(33, n - off);
    reader->ComputeGathered(f.codes.data(), f.record_size,
                            std::span<const uint32_t>{ids}.subspan(off, take),
                            kNoPrune, gathered.data() + off);
  }
  for (size_t i = 0; i < n; ++i) {
    EXPECT_NEAR(gathered[i], block[ids[i]],
                1e-4f * (1.f + std::fabs(block[ids[i]])))
      << i;
  }
}

TEST(hnsw_pq_quantizer_test, gathered_matches_block_at_every_batch_size) {
  constexpr uint32_t d = 96;
  constexpr size_t n = 400;
  const auto points = MakeSpread(d, n, 73);
  const std::vector<float> centroid(d, 0.f);
  const auto query = MakeSpread(d, 1, 79);
  for (const VectorMetric metric :
       {VectorMetric::L2Sqr, VectorMetric::InnerProduct}) {
    for (const uint32_t pq_m : {96U, 48U, 24U, 12U, 6U, 32U}) {
      const auto f = BuildPqCodes(d, pq_m, metric, points, n);
      auto reader = MakeKeyedReader(f, query, centroid.data());
      std::vector<score_t> block(n);
      reader->ComputeBlock(f.codes, kNoPrune, block.data());
      std::vector<uint32_t> ids(n);
      std::vector<score_t> gathered(n);
      for (size_t batch = 1; batch <= 70; ++batch) {
        for (size_t i = 0; i < batch; ++i) {
          ids[i] = static_cast<uint32_t>((i * 37 + batch * 11) % n);
        }
        reader->ComputeGathered(f.codes.data(), f.record_size,
                                std::span<const uint32_t>{ids}.first(batch),
                                kNoPrune, gathered.data());
        for (size_t i = 0; i < batch; ++i) {
          EXPECT_NEAR(gathered[i], block[ids[i]],
                      1e-4f * (1.f + std::fabs(block[ids[i]])))
            << "pq_m=" << pq_m << " batch=" << batch << " i=" << i;
        }
      }
    }
  }
}

TEST(hnsw_pq_quantizer_test, wide_codes_score_against_decoded_rows) {
  constexpr uint32_t pq_m = 264;
  constexpr size_t n = 300;
  constexpr size_t chunk = 13;
  for (const uint32_t dsub : {1U, 2U, 4U, 8U, 16U}) {
    const uint32_t d = pq_m * dsub;
    const auto points = MakeSpread(d, n, 89);
    const auto query = MakeSpread(d, 1, 97);
    const std::vector<float> centroid(d, 0.f);
    const float qq =
      ExactScore(VectorMetric::InnerProduct, query.data(), query.data(), d);
    for (const VectorMetric metric :
         {VectorMetric::L2Sqr, VectorMetric::InnerProduct}) {
      const auto f = BuildPqCodes(d, pq_m, metric, points, n);
      auto reader = MakeKeyedReader(f, query, centroid.data());
      std::vector<uint32_t> ids(n);
      for (uint32_t i = 0; i < n; ++i) {
        ids[i] = static_cast<uint32_t>((i * 37) % n);
      }
      std::vector<score_t> got(n);
      for (size_t off = 0; off < n; off += chunk) {
        const size_t take = std::min(chunk, n - off);
        reader->ComputeGathered(
          f.codes.data(), f.record_size,
          std::span<const uint32_t>{ids}.subspan(off, take), kNoPrune,
          got.data() + off);
      }
      std::vector<float> row(d);
      for (size_t i = 0; i < n; ++i) {
        ASSERT_TRUE(reader->Decode(
          f.codes.data() + size_t{ids[i]} * f.record_size, row.data()));
        const float want = ExactScore(metric, query.data(), row.data(), d);
        const float rr =
          ExactScore(VectorMetric::InnerProduct, row.data(), row.data(), d);
        EXPECT_NEAR(got[i], want, 1e-3f * (1.f + std::fabs(want) + qq + rr))
          << "dsub=" << dsub << " metric " << static_cast<int>(metric)
          << " i=" << i;
      }
    }
  }
}

TEST(hnsw_pq_quantizer_test, encode_into_matches_encode) {
  constexpr uint32_t d = 32;
  constexpr uint32_t pq_m = 16;
  constexpr size_t n = 300;
  const auto points = MakeSpread(d, n, 71);
  const auto f = BuildPqCodes(d, pq_m, VectorMetric::InnerProduct, points, n);
  auto writer =
    MakeQuantizerWriter({.kind = VectorQuantization::PQ, .pq_m = pq_m}, d,
                        VectorMetric::InnerProduct, 0, /*row_major=*/true);
  ASSERT_NE(writer, nullptr);
  writer->Train(points.data(), std::min(n, writer->TrainSamples(n)));
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t start = 0;
  uint64_t end = 0;
  {
    MemoryIndexOutput out{file};
    start = out.Position();
    writer->Encode(out, points.data(), n);
    writer->Finish(out);
    out.Flush();
    end = out.Position();
  }
  ExpectBytesEq(ReadPayload(file, start, end), f.codes);
}

namespace {

struct BqMode {
  uint32_t bits;
  bool half;
  uint32_t query_bits;
  double min_recall;
};

std::string BqModeName(const BqMode& mode) {
  return "bq" + std::to_string(mode.bits) + (mode.half ? ".5" : "") + "/q" +
         std::to_string(mode.query_bits);
}

std::vector<float> MakeShifted(uint32_t d, size_t n, uint32_t seed) {
  std::mt19937 rng{seed};
  std::uniform_real_distribution<float> shift{-2.f, 2.f};
  std::uniform_real_distribution<float> spread{0.3f, 2.f};
  std::vector<float> mean(d);
  std::vector<float> scale(d);
  for (uint32_t i = 0; i < d; ++i) {
    mean[i] = shift(rng);
    scale[i] = spread(rng);
  }
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> out(n * d);
  for (size_t r = 0; r < n; ++r) {
    for (uint32_t i = 0; i < d; ++i) {
      out[r * d + i] = mean[i] + scale[i] * nd(rng);
    }
  }
  return out;
}

CodeFixture BuildBqCodes(uint32_t d, const BqMode& mode, VectorMetric metric,
                         const std::vector<float>& points, size_t n) {
  return BuildCodes(VectorQuantization::BQ, d, mode.bits, metric, points, n, {},
                    mode.half, mode.query_bits);
}

double RecallInPool(std::span<const float> want, std::span<const score_t> got,
                    size_t k, size_t pool) {
  std::vector<uint32_t> order(want.size());
  for (size_t i = 0; i < order.size(); ++i) {
    order[i] = static_cast<uint32_t>(i);
  }
  auto by_want = order;
  std::partial_sort(by_want.begin(), by_want.begin() + k, by_want.end(),
                    [&](uint32_t a, uint32_t b) { return want[a] > want[b]; });
  std::partial_sort(order.begin(), order.begin() + pool, order.end(),
                    [&](uint32_t a, uint32_t b) { return got[a] > got[b]; });
  size_t hit = 0;
  for (size_t i = 0; i < k; ++i) {
    hit += std::find(order.begin(), order.begin() + pool, by_want[i]) !=
               order.begin() + pool
             ? 1
             : 0;
  }
  return static_cast<double>(hit) / static_cast<double>(k);
}

constexpr VectorMetric kBqMetrics[] = {VectorMetric::L2Sqr,
                                       VectorMetric::InnerProduct};

}  // namespace

TEST(bq_quantizer_test, codes_rank_like_exact) {
  constexpr uint32_t d = 64;
  constexpr size_t n = 2000;
  constexpr size_t nq = 20;
  const auto all = MakeShifted(d, n + nq, 83);
  const std::vector<float> points(all.begin(), all.begin() + n * d);
  for (const BqMode mode : {BqMode{1, false, 1, 0.3}, BqMode{1, false, 4, 0.55},
                            BqMode{1, false, 8, 0.55}, BqMode{1, true, 1, 0.33},
                            BqMode{1, true, 8, 0.65}, BqMode{2, false, 1, 0.45},
                            BqMode{2, false, 8, 0.85}}) {
    for (const VectorMetric metric : kBqMetrics) {
      const auto f = BuildBqCodes(d, mode, metric, points, n);
      double recall = 0;
      for (size_t qi = 0; qi < nq; ++qi) {
        const std::span<const float> q{all.data() + (n + qi) * d, d};
        auto reader = MakeKeyedReader(f, q, nullptr);
        std::vector<score_t> got(n);
        reader->ComputeBlock(f.codes, kNoPrune, got.data());
        std::vector<float> want(n);
        for (size_t i = 0; i < n; ++i) {
          want[i] = ExactScore(metric, q.data(), points.data() + i * d, d);
        }
        recall += RecallInPool(want, got, 10, 100);
      }
      recall /= static_cast<double>(nq);
      EXPECT_GE(recall, mode.min_recall)
        << BqModeName(mode) << " metric " << static_cast<int>(metric);
    }
  }
}

TEST(bq_quantizer_test,
     four_bit_query_ranks_like_eight_bit_on_wide_dimensions) {
  constexpr uint32_t d = 64;
  constexpr size_t n = 4000;
  constexpr size_t nq = 32;
  auto all = MakeShifted(d, n + nq, 101);
  for (size_t r = 0; r < n + nq; ++r) {
    for (uint32_t i = 0; i < d; i += 16) {
      all[r * d + i] *= 8.f;
    }
  }
  const std::vector<float> points(all.begin(), all.begin() + n * d);
  for (const uint32_t bits : {1U, 2U}) {
    for (const VectorMetric metric : kBqMetrics) {
      const auto f4 =
        BuildBqCodes(d, BqMode{bits, false, 4, 0}, metric, points, n);
      const auto f8 =
        BuildBqCodes(d, BqMode{bits, false, 8, 0}, metric, points, n);
      double recall4 = 0;
      double recall8 = 0;
      for (size_t qi = 0; qi < nq; ++qi) {
        const std::span<const float> q{all.data() + (n + qi) * d, d};
        std::vector<float> want(n);
        for (size_t i = 0; i < n; ++i) {
          want[i] = ExactScore(metric, q.data(), points.data() + i * d, d);
        }
        std::vector<score_t> got(n);
        MakeKeyedReader(f4, q, nullptr)
          ->ComputeBlock(f4.codes, kNoPrune, got.data());
        recall4 += RecallInPool(want, got, 10, 40);
        MakeKeyedReader(f8, q, nullptr)
          ->ComputeBlock(f8.codes, kNoPrune, got.data());
        recall8 += RecallInPool(want, got, 10, 40);
      }
      EXPECT_GE(recall4 / nq, recall8 / nq - 0.02)
        << "bq" << bits << " metric " << static_cast<int>(metric);
    }
  }
}

TEST(bq_quantizer_test, scalar_query_estimates_the_distance) {
  constexpr uint32_t d = 48;
  constexpr size_t n = 1500;
  constexpr size_t nq = 8;
  const auto all = MakeShifted(d, n + nq, 89);
  const std::vector<float> points(all.begin(), all.begin() + n * d);
  for (const BqMode mode : {BqMode{1, false, 8, 0.45}, BqMode{1, true, 8, 0.6},
                            BqMode{2, false, 8, 0.7}}) {
    for (const VectorMetric metric : kBqMetrics) {
      const auto f = BuildBqCodes(d, mode, metric, points, n);
      for (size_t qi = 0; qi < nq; ++qi) {
        const std::span<const float> q{all.data() + (n + qi) * d, d};
        auto reader = MakeKeyedReader(f, q, nullptr);
        std::vector<score_t> got(n);
        reader->ComputeBlock(f.codes, kNoPrune, got.data());
        std::vector<float> want(n);
        double mean_want = 0;
        double mean_delta = 0;
        for (size_t i = 0; i < n; ++i) {
          want[i] = ExactScore(metric, q.data(), points.data() + i * d, d);
          mean_want += want[i];
          mean_delta += double{got[i]} - double{want[i]};
        }
        mean_want /= static_cast<double>(n);
        mean_delta /= static_cast<double>(n);
        double var_want = 0;
        double cov = 0;
        double var_got = 0;
        double mean_got = mean_want + mean_delta;
        for (size_t i = 0; i < n; ++i) {
          var_want += (want[i] - mean_want) * (want[i] - mean_want);
          var_got += (got[i] - mean_got) * (got[i] - mean_got);
          cov += (want[i] - mean_want) * (got[i] - mean_got);
        }
        const double corr = cov / std::sqrt(var_want * var_got);
        EXPECT_GE(corr, mode.min_recall)
          << BqModeName(mode) << " metric " << static_cast<int>(metric);
        if (metric == VectorMetric::InnerProduct) {
          EXPECT_LE(std::fabs(mean_delta),
                    0.05 * std::sqrt(var_want / static_cast<double>(n)))
            << BqModeName(mode);
        }
      }
    }
  }
}

TEST(bq_quantizer_test, decode_is_unbiased_per_dimension) {
  constexpr uint32_t d = 32;
  constexpr size_t n = 4000;
  const auto points = MakeShifted(d, n, 97);
  for (const BqMode mode : {BqMode{1, false, 1, 0}, BqMode{1, true, 8, 0},
                            BqMode{2, false, 8, 0}}) {
    const auto f = BuildBqCodes(d, mode, VectorMetric::L2Sqr, points, n);
    auto reader = MakeKeyedReader(f, {points.data(), d}, nullptr);
    std::vector<double> bias(d, 0.0);
    std::vector<double> var(d, 0.0);
    std::vector<float> decoded(d);
    for (size_t r = 0; r < n; ++r) {
      ASSERT_TRUE(
        reader->Decode(f.codes.data() + r * f.record_size, decoded.data()));
      for (uint32_t i = 0; i < d; ++i) {
        const double x = points[r * d + i];
        bias[i] += decoded[i] - x;
        var[i] += x * x;
      }
    }
    for (uint32_t i = 0; i < d; ++i) {
      EXPECT_LE(std::fabs(bias[i]) / static_cast<double>(n),
                1e-3 * std::sqrt(var[i] / static_cast<double>(n)))
        << BqModeName(mode) << " dim " << i;
    }
  }
}

TEST(bq_quantizer_test, codes_nest_and_balance_on_gaussian_data) {
  constexpr uint32_t d = 40;
  constexpr size_t n = 3000;
  const auto points = MakeShifted(d, n, 101);
  for (const uint32_t bits : {1U, 2U}) {
    const auto f = BuildBqCodes(d, BqMode{bits, false, 1, 0},
                                VectorMetric::L2Sqr, points, n);
    const uint32_t plane = (d + 7) / 8;
    ASSERT_EQ(f.record_size, plane * bits);
    for (uint32_t i = 0; i < d; ++i) {
      size_t low = 0;
      size_t high = 0;
      for (size_t r = 0; r < n; ++r) {
        const auto* code = f.codes.data() + r * f.record_size;
        const bool b0 = ((code[i / 8] >> (i % 8)) & 1) != 0;
        low += b0 ? 0 : 1;
        if (bits == 2) {
          const bool b1 = ((code[plane + i / 8] >> (i % 8)) & 1) != 0;
          EXPECT_TRUE(b0 || !b1) << "dim " << i << " row " << r;
          high += b1 ? 1 : 0;
        }
      }
      const double low_share = static_cast<double>(low) / n;
      if (bits == 1) {
        EXPECT_NEAR(low_share, 0.5, 0.1) << "dim " << i;
      } else {
        EXPECT_NEAR(low_share, 0.27, 0.08) << "dim " << i;
        EXPECT_NEAR(static_cast<double>(high) / n, 0.27, 0.08) << "dim " << i;
      }
    }
  }
}

namespace {

std::vector<float> MakeSkewed(uint32_t d, size_t n, uint32_t seed) {
  std::mt19937 rng{seed};
  std::uniform_real_distribution<float> zeros{0.3f, 0.6f};
  std::uniform_real_distribution<float> scale{1.f, 30.f};
  std::uniform_real_distribution<float> coin{0.f, 1.f};
  std::vector<float> p(d);
  std::vector<std::exponential_distribution<float>> tail;
  for (uint32_t i = 0; i < d; ++i) {
    p[i] = zeros(rng);
    tail.emplace_back(1.f / scale(rng));
  }
  std::vector<float> out(n * d);
  for (size_t r = 0; r < n; ++r) {
    for (uint32_t i = 0; i < d; ++i) {
      out[r * d + i] = coin(rng) < p[i] ? 0.f : tail[i](rng);
    }
  }
  return out;
}

}  // namespace

TEST(bq_quantizer_test, cuts_follow_skewed_dimensions) {
  constexpr uint32_t d = 32;
  constexpr size_t n = 4000;
  constexpr size_t nq = 20;
  const auto all = MakeSkewed(d, n + nq, 139);
  const std::vector<float> points(all.begin(), all.begin() + n * d);
  const auto two =
    BuildBqCodes(d, BqMode{2, false, 8, 0}, VectorMetric::L2Sqr, points, n);
  const uint32_t plane = (d + 7) / 8;
  for (uint32_t i = 0; i < d; ++i) {
    size_t low = 0;
    size_t high = 0;
    for (size_t r = 0; r < n; ++r) {
      const auto* code = two.codes.data() + r * two.record_size;
      low += ((code[i / 8] >> (i % 8)) & 1) == 0 ? 1 : 0;
      high += ((code[plane + i / 8] >> (i % 8)) & 1) != 0 ? 1 : 0;
    }
    EXPECT_GT(low, n / 10) << "dim " << i;
    EXPECT_GT(high, n / 50) << "dim " << i;
  }
  const auto one =
    BuildBqCodes(d, BqMode{1, false, 8, 0}, VectorMetric::L2Sqr, points, n);
  const auto recall_of = [&](const CodeFixture& f) {
    double recall = 0;
    for (size_t qi = 0; qi < nq; ++qi) {
      const std::span<const float> q{all.data() + (n + qi) * d, d};
      auto reader = MakeKeyedReader(f, q, nullptr);
      std::vector<score_t> got(n);
      reader->ComputeBlock(f.codes, kNoPrune, got.data());
      std::vector<float> want(n);
      for (size_t i = 0; i < n; ++i) {
        want[i] =
          ExactScore(VectorMetric::L2Sqr, q.data(), points.data() + i * d, d);
      }
      recall += RecallInPool(want, got, 10, 50);
    }
    return recall / static_cast<double>(nq);
  };
  EXPECT_GT(recall_of(two), recall_of(one) + 0.05);
}

TEST(bq_quantizer_test, binary_pair_scores_match_the_keyed_reader) {
  constexpr uint32_t d = 72;
  constexpr size_t n = 300;
  const auto points = MakeShifted(d, n, 103);
  std::vector<uint32_t> ids(n);
  for (uint32_t i = 0; i < n; ++i) {
    ids[i] = i;
  }
  for (const BqMode mode : {BqMode{1, false, 1, 0}, BqMode{2, false, 1, 0}}) {
    const auto f = BuildBqCodes(d, mode, VectorMetric::L2Sqr, points, n);
    const std::vector<float> scratch(d, 0.f);
    auto reader = MakeKeyedReader(f, scratch, nullptr);
    ASSERT_TRUE(reader->SupportsPairScores());
    std::vector<float> terms;
    ASSERT_TRUE(
      reader->PreparePairTerms(f.codes.data(), f.record_size, n, terms));
    std::vector<float> decoded(d);
    std::vector<score_t> pair(n);
    std::vector<score_t> keyed(n);
    for (const uint32_t a : {0U, 13U, 150U, 299U}) {
      reader->ScorePairBatch(f.codes.data(), f.record_size, terms, a, ids,
                             pair.data());
      ASSERT_TRUE(reader->Decode(f.codes.data() + size_t{a} * f.record_size,
                                 decoded.data()));
      auto keyed_reader = MakeKeyedReader(f, decoded, nullptr);
      keyed_reader->ComputeGathered(f.codes.data(), f.record_size, ids,
                                    kNoPrune, keyed.data());
      for (size_t b = 0; b < n; ++b) {
        EXPECT_EQ(pair[b], keyed[b])
          << BqModeName(mode) << " a=" << a << " b=" << b;
      }
      EXPECT_EQ(pair[a], 0.f);
    }
  }
}

TEST(bq_quantizer_test, scalar_query_builds_through_decoded_rows) {
  constexpr uint32_t d = 16;
  constexpr size_t n = 64;
  const auto points = MakeShifted(d, n, 107);
  const auto f =
    BuildBqCodes(d, BqMode{2, false, 8, 0}, VectorMetric::L2Sqr, points, n);
  auto reader = MakeKeyedReader(f, {points.data(), d}, nullptr);
  EXPECT_FALSE(reader->SupportsPairScores());
  std::vector<float> decoded(d);
  ASSERT_TRUE(reader->Decode(f.codes.data(), decoded.data()));
  EXPECT_TRUE(reader->SetQuery(decoded));
}

TEST(bq_quantizer_test, build_codebook_is_binary_for_every_query_width) {
  constexpr uint32_t d = 48;
  constexpr size_t n = 100;
  const auto points = MakeShifted(d, n, 167);
  std::vector<uint32_t> ids(n);
  for (uint32_t i = 0; i < n; ++i) {
    ids[i] = i;
  }
  for (const BqMode mode : {BqMode{1, false, 8, 0}, BqMode{2, false, 4, 0},
                            BqMode{1, true, 8, 0}, BqMode{1, false, 1, 0}}) {
    for (const VectorMetric metric : kBqMetrics) {
      const auto f = BuildBqCodes(d, mode, metric, points, n);
      auto build =
        MakeQuantizerReader(f.stats->MakeBuildCodebook({points.data(), d}));
      build->StartCluster(nullptr);
      ASSERT_TRUE(build->SupportsPairScores()) << BqModeName(mode);
      std::vector<score_t> keyed(n);
      build->ComputeGathered(f.codes.data(), f.record_size, ids, kNoPrune,
                             keyed.data());
      std::vector<float> terms;
      ASSERT_TRUE(
        build->PreparePairTerms(f.codes.data(), f.record_size, n, terms));
      std::vector<score_t> pair(n);
      build->ScorePairBatch(f.codes.data(), f.record_size, terms, 0, ids,
                            pair.data());
      EXPECT_EQ(keyed[0], 0.f) << BqModeName(mode);
      for (size_t b = 0; b < n; ++b) {
        EXPECT_EQ(pair[b], keyed[b]) << BqModeName(mode) << " b=" << b;
      }
    }
  }
}

TEST(bq_quantizer_test, gathered_matches_block) {
  constexpr uint32_t d = 100;
  constexpr size_t n = 400;
  const auto all = MakeShifted(d, n + 1, 109);
  const std::vector<float> points(all.begin(), all.begin() + n * d);
  const std::span<const float> query{all.data() + n * d, d};
  std::vector<uint32_t> ids(n);
  for (uint32_t i = 0; i < n; ++i) {
    ids[i] = (i * 37) % n;
  }
  for (const BqMode mode : {BqMode{1, false, 1, 0}, BqMode{1, true, 4, 0},
                            BqMode{2, false, 8, 0}}) {
    for (const VectorMetric metric : kBqMetrics) {
      const auto f = BuildBqCodes(d, mode, metric, points, n);
      auto reader = MakeKeyedReader(f, query, nullptr);
      std::vector<score_t> block(n);
      reader->ComputeBlock(f.codes, kNoPrune, block.data());
      std::vector<score_t> gathered(n);
      for (size_t off = 0; off < n; off += 33) {
        const size_t take = std::min<size_t>(33, n - off);
        reader->ComputeGathered(
          f.codes.data(), f.record_size,
          std::span<const uint32_t>{ids}.subspan(off, take), kNoPrune,
          gathered.data() + off);
      }
      for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(gathered[i], block[ids[i]]) << BqModeName(mode) << " " << i;
      }
    }
  }
}

namespace {

std::vector<float> ColumnMean(const std::vector<float>& points, uint32_t d,
                              size_t n) {
  std::vector<float> mean(d, 0.f);
  for (size_t r = 0; r < n; ++r) {
    for (uint32_t i = 0; i < d; ++i) {
      mean[i] += points[r * d + i];
    }
  }
  for (auto& v : mean) {
    v /= static_cast<float>(n);
  }
  return mean;
}

double Pearson(std::span<const float> a, std::span<const score_t> b) {
  double ma = 0;
  double mb = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    ma += a[i];
    mb += b[i];
  }
  ma /= static_cast<double>(a.size());
  mb /= static_cast<double>(b.size());
  double cov = 0;
  double va = 0;
  double vb = 0;
  for (size_t i = 0; i < a.size(); ++i) {
    cov += (a[i] - ma) * (b[i] - mb);
    va += (a[i] - ma) * (a[i] - ma);
    vb += (b[i] - mb) * (b[i] - mb);
  }
  return cov / std::sqrt(va * vb);
}

struct OsqMode {
  uint32_t bits;
  uint32_t query_bits;
  double min_recall;
};

std::string OsqModeName(const OsqMode& mode) {
  return "osq" + std::to_string(mode.bits) + "/q" +
         std::to_string(mode.query_bits);
}

constexpr OsqMode kOsqModes[] = {{1, 4, 0.75}, {1, 8, 0.75}, {2, 4, 0.85},
                                 {4, 4, 0.9},  {4, 8, 0.9},  {7, 7, 0.95},
                                 {8, 8, 0.95}};

uint32_t OsqRecordSize(uint32_t d, uint32_t bits) {
  const uint32_t codes = bits <= 2   ? bits * ((d + 7) / 8)
                         : bits == 4 ? (d + 1) / 2
                                     : d;
  return codes + 4 * static_cast<uint32_t>(sizeof(float));
}

CodeFixture BuildOsqCodes(uint32_t d, const OsqMode& mode, VectorMetric metric,
                          const std::vector<float>& points, size_t n,
                          const std::vector<float>& centroid) {
  return BuildCodes(VectorQuantization::OSQ, d, mode.bits, metric, points, n,
                    centroid, false, mode.query_bits);
}

}  // namespace

TEST(osq_quantizer_test, codes_rank_like_exact) {
  constexpr uint32_t d = 64;
  constexpr size_t n = 2000;
  constexpr size_t nq = 20;
  const auto all = MakeShifted(d, n + nq, 113);
  const std::vector<float> points(all.begin(), all.begin() + n * d);
  const auto centroid = ColumnMean(points, d, n);
  for (const OsqMode mode : kOsqModes) {
    for (const VectorMetric metric : kBqMetrics) {
      const auto f = BuildOsqCodes(d, mode, metric, points, n, centroid);
      double recall = 0;
      double corr = 0;
      for (size_t qi = 0; qi < nq; ++qi) {
        const std::span<const float> q{all.data() + (n + qi) * d, d};
        auto reader = MakeKeyedReader(f, q, centroid.data());
        std::vector<score_t> got(n);
        reader->ComputeBlock(f.codes, kNoPrune, got.data());
        std::vector<float> want(n);
        for (size_t i = 0; i < n; ++i) {
          want[i] = ExactScore(metric, q.data(), points.data() + i * d, d);
        }
        recall += RecallInPool(want, got, 10, 100);
        corr += Pearson(want, got);
      }
      EXPECT_GE(recall / nq, mode.min_recall)
        << OsqModeName(mode) << " metric " << static_cast<int>(metric);
      EXPECT_GE(corr / nq, 0.8)
        << OsqModeName(mode) << " metric " << static_cast<int>(metric);
    }
  }
}

TEST(osq_quantizer_test, pair_scores_match_a_reader_keyed_on_the_row) {
  constexpr uint32_t d = 40;
  constexpr size_t n = 200;
  const auto points = MakeShifted(d, n, 151);
  const std::vector<float> zero(d, 0.f);
  std::vector<uint32_t> ids(n);
  for (uint32_t i = 0; i < n; ++i) {
    ids[i] = i;
  }
  for (const OsqMode mode : kOsqModes) {
    const auto f =
      BuildOsqCodes(d, mode, VectorMetric::InnerProduct, points, n, zero);
    auto reader = MakeKeyedReader(f, zero, zero.data());
    ASSERT_TRUE(reader->SupportsPairScores());
    std::vector<float> terms;
    ASSERT_TRUE(
      reader->PreparePairTerms(f.codes.data(), f.record_size, n, terms));
    std::vector<float> decoded(d);
    std::vector<score_t> pair(n);
    std::vector<score_t> keyed(n);
    double offset = 0;
    double spread = 0;
    for (const uint32_t a : {0U, 17U, 99U, 199U}) {
      reader->ScorePairBatch(f.codes.data(), f.record_size, terms, a, ids,
                             pair.data());
      ASSERT_TRUE(reader->Decode(f.codes.data() + size_t{a} * f.record_size,
                                 decoded.data()));
      auto keyed_reader = MakeKeyedReader(f, decoded, zero.data());
      keyed_reader->ComputeGathered(f.codes.data(), f.record_size, ids,
                                    kNoPrune, keyed.data());
      for (size_t b = 0; b < n; ++b) {
        offset += pair[b] - keyed[b];
        spread += std::fabs(keyed[b]);
      }
      if (mode.bits == 1) {
        for (size_t b = 0; b < n; ++b) {
          EXPECT_NEAR(pair[b], keyed[b], 1e-3f * (1.f + std::fabs(keyed[b])))
            << OsqModeName(mode) << " a=" << a << " b=" << b;
        }
      }
    }
    EXPECT_LE(std::fabs(offset), 0.02 * spread) << OsqModeName(mode);
  }
}

TEST(osq_quantizer_test, scores_comparable_across_clusters) {
  constexpr uint32_t d = 48;
  constexpr size_t n = 800;
  const auto all = MakeShifted(d, 2 * n + 1, 127);
  const std::span<const float> query{all.data() + 2 * n * d, d};
  for (const VectorMetric metric : kBqMetrics) {
    std::array<double, 2> bias{};
    double spread = 0;
    for (size_t c = 0; c < 2; ++c) {
      std::vector<float> points(all.begin() + c * n * d,
                                all.begin() + (c + 1) * n * d);
      for (size_t r = 0; r < n; ++r) {
        for (uint32_t i = 0; i < d; ++i) {
          points[r * d + i] += c == 0 ? 1.f : 4.f;
        }
      }
      const auto centroid = ColumnMean(points, d, n);
      const auto f =
        BuildOsqCodes(d, kOsqModes[0], metric, points, n, centroid);
      auto reader = MakeKeyedReader(f, query, centroid.data());
      std::vector<score_t> scores(n);
      reader->ComputeBlock(f.codes, kNoPrune, scores.data());
      double mean = 0;
      double mean2 = 0;
      for (size_t i = 0; i < n; ++i) {
        const double want =
          ExactScore(metric, query.data(), points.data() + i * d, d);
        bias[c] += scores[i] - want;
        mean += want;
        mean2 += want * want;
      }
      bias[c] /= static_cast<double>(n);
      mean /= static_cast<double>(n);
      spread = std::max(spread, std::sqrt(mean2 / n - mean * mean));
    }
    EXPECT_LE(std::fabs(bias[0] - bias[1]), 0.25 * spread)
      << "metric " << static_cast<int>(metric) << " bias " << bias[0] << " "
      << bias[1] << " spread " << spread;
  }
}

TEST(osq_quantizer_test, gathered_matches_block) {
  constexpr uint32_t d = 100;
  constexpr size_t n = 300;
  const auto all = MakeShifted(d, n + 1, 131);
  const std::vector<float> points(all.begin(), all.begin() + n * d);
  const std::span<const float> query{all.data() + n * d, d};
  const auto centroid = ColumnMean(points, d, n);
  std::vector<uint32_t> ids(n);
  for (uint32_t i = 0; i < n; ++i) {
    ids[i] = (i * 41) % n;
  }
  for (const OsqMode mode : kOsqModes) {
    for (const VectorMetric metric : kBqMetrics) {
      const auto f = BuildOsqCodes(d, mode, metric, points, n, centroid);
      EXPECT_EQ(f.record_size, OsqRecordSize(d, mode.bits))
        << OsqModeName(mode);
      auto reader = MakeKeyedReader(f, query, centroid.data());
      std::vector<score_t> block(n);
      reader->ComputeBlock(f.codes, kNoPrune, block.data());
      std::vector<score_t> gathered(n);
      for (size_t off = 0; off < n; off += 29) {
        const size_t take = std::min<size_t>(29, n - off);
        reader->ComputeGathered(
          f.codes.data(), f.record_size,
          std::span<const uint32_t>{ids}.subspan(off, take), kNoPrune,
          gathered.data() + off);
      }
      for (size_t i = 0; i < n; ++i) {
        EXPECT_EQ(gathered[i], block[ids[i]]) << OsqModeName(mode) << " " << i;
      }
    }
  }
}

TEST(osq_quantizer_test, more_bits_rank_better) {
  constexpr uint32_t d = 64;
  constexpr size_t n = 2000;
  constexpr size_t nq = 20;
  const auto all = MakeShifted(d, n + nq, 157);
  const std::vector<float> points(all.begin(), all.begin() + n * d);
  const auto centroid = ColumnMean(points, d, n);
  double previous = 0;
  for (const OsqMode mode : {OsqMode{1, 4, 0}, OsqMode{2, 4, 0},
                             OsqMode{4, 4, 0}, OsqMode{7, 7, 0}}) {
    const auto f =
      BuildOsqCodes(d, mode, VectorMetric::L2Sqr, points, n, centroid);
    double recall = 0;
    for (size_t qi = 0; qi < nq; ++qi) {
      const std::span<const float> q{all.data() + (n + qi) * d, d};
      auto reader = MakeKeyedReader(f, q, centroid.data());
      std::vector<score_t> got(n);
      reader->ComputeBlock(f.codes, kNoPrune, got.data());
      std::vector<float> want(n);
      for (size_t i = 0; i < n; ++i) {
        want[i] =
          ExactScore(VectorMetric::L2Sqr, q.data(), points.data() + i * d, d);
      }
      recall += RecallInPool(want, got, 10, 20);
    }
    recall /= static_cast<double>(nq);
    EXPECT_GT(recall, previous) << OsqModeName(mode);
    previous = recall;
  }
}

TEST(osq_quantizer_test, decode_needs_a_centroid) {
  constexpr uint32_t d = 16;
  constexpr size_t n = 32;
  const auto points = MakeShifted(d, n, 137);
  const auto centroid = ColumnMean(points, d, n);
  const auto f =
    BuildOsqCodes(d, kOsqModes[0], VectorMetric::L2Sqr, points, n, centroid);
  auto codebook = f.stats->MakeCodebook({points.data(), d});
  auto bare = MakeQuantizerReader(codebook);
  std::vector<float> decoded(d);
  EXPECT_FALSE(bare->Decode(f.codes.data(), decoded.data()));
  EXPECT_FALSE(bare->SupportsPairScores());
  bare->StartCluster(centroid.data());
  EXPECT_TRUE(bare->Decode(f.codes.data(), decoded.data()));
  EXPECT_TRUE(bare->SupportsPairScores());
}

namespace {

struct HalfCase {
  VectorQuantization quant;
  double tolerance;
  double min_recall;
};

}  // namespace

TEST(half_quantizer_test, scores_track_exact) {
  constexpr uint32_t d = 96;
  constexpr size_t n = 1500;
  constexpr size_t nq = 10;
  const auto all = MakeShifted(d, n + nq, 163);
  const std::vector<float> points(all.begin(), all.begin() + n * d);
  for (const auto [quant, tolerance, min_recall] :
       {HalfCase{VectorQuantization::FP16, 2e-3, 0.99},
        HalfCase{VectorQuantization::BF16, 2e-2, 0.9}}) {
    for (const VectorMetric metric : kBqMetrics) {
      const auto f = BuildCodes(quant, d, 0, metric, points, n, {});
      EXPECT_EQ(f.record_size, 2 * d);
      double recall = 0;
      for (size_t qi = 0; qi < nq; ++qi) {
        const std::span<const float> q{all.data() + (n + qi) * d, d};
        auto reader = MakeKeyedReader(f, q, nullptr);
        std::vector<score_t> got(n);
        reader->ComputeBlock(f.codes, kNoPrune, got.data());
        std::vector<float> want(n);
        double scale = 0;
        for (size_t i = 0; i < n; ++i) {
          want[i] = ExactScore(metric, q.data(), points.data() + i * d, d);
          scale = std::max(scale, std::fabs(double{want[i]}));
        }
        for (size_t i = 0; i < n; ++i) {
          EXPECT_NEAR(got[i], want[i], tolerance * scale)
            << static_cast<int>(quant) << " metric " << static_cast<int>(metric)
            << " row " << i;
        }
        recall += RecallInPool(want, got, 10, 10);
      }
      EXPECT_GE(recall / nq, min_recall)
        << static_cast<int>(quant) << " metric " << static_cast<int>(metric);
    }
  }
}

TEST(quantizer_kernel_test, bit_kernels_match_a_bit_by_bit_sum) {
  std::mt19937 rng{101};
  std::uniform_int_distribution<int> byte{0, 255};
  std::uniform_int_distribution<int> weight{-127, 127};
  for (uint32_t bytes = 1; bytes <= 200; ++bytes) {
    std::vector<uint8_t> a(bytes);
    std::vector<uint8_t> b(bytes);
    std::vector<int8_t> q(size_t{bytes} * 8);
    for (auto& v : a) {
      v = static_cast<uint8_t>(byte(rng));
    }
    for (auto& v : b) {
      v = static_cast<uint8_t>(byte(rng));
    }
    for (auto& v : q) {
      v = static_cast<int8_t>(weight(rng));
    }
    uint32_t differ = 0;
    int32_t plane = 0;
    for (uint32_t i = 0; i < bytes * 8; ++i) {
      const bool x = ((a[i / 8] >> (i % 8)) & 1U) != 0;
      const bool y = ((b[i / 8] >> (i % 8)) & 1U) != 0;
      differ += x != y ? 1 : 0;
      plane += x ? q[i] : 0;
    }
    EXPECT_EQ(quantizers::BqXorPopcount(a.data(), b.data(), bytes), differ)
      << bytes;
    EXPECT_EQ(quantizers::BitPlaneSum(a.data(), bytes, q.data()), plane)
      << bytes;
  }
}

TEST(quantizer_kernel_test, fwht_matches_the_scalar_butterflies) {
  std::mt19937 rng{103};
  std::normal_distribution<float> nd{0.f, 1.f};
  for (uint32_t len = 1; len <= 4096; len <<= 1) {
    std::vector<float> want(len);
    for (auto& v : want) {
      v = nd(rng);
    }
    auto got = want;
    for (uint32_t h = 1; h < len; h <<= 1) {
      for (uint32_t i = 0; i < len; i += h << 1) {
        for (uint32_t j = i; j < i + h; ++j) {
          const float x = want[j];
          const float y = want[j + h];
          want[j] = x + y;
          want[j + h] = x - y;
        }
      }
    }
    quantizers::Fwht(got.data(), len);
    EXPECT_EQ(got, want) << len;
  }
}
