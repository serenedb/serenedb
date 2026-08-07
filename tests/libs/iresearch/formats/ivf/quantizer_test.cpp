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
#include <cmath>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <random>
#include <span>
#include <utility>
#include <vector>

#include "basics/misc.hpp"
#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/bytes_output.hpp"
#include "tests_shared.hpp"

using namespace irs;

namespace {

constexpr score_t kNoPrune = std::numeric_limits<score_t>::lowest();

// The writer emits a u64 length prefix ahead of the blob, exactly as
// IvfWriter::FlushTree stores it; strip it to get what MakeQuantizerStats sees.
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

bstring EncodeChunks(QuantizerWriter& writer, const std::vector<float>& points,
                     uint32_t d, std::span<const size_t> chunks) {
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t start = 0;
  uint64_t end = 0;
  {
    MemoryIndexOutput out{file};
    start = out.Position();
    size_t off = 0;
    for (const size_t m : chunks) {
      writer.EncodeBlock(out, points.data() + off * d, m);
      off += m;
    }
    out.Flush();
    end = out.Position();
  }
  bstring payload(end - start, 0);
  MemoryIndexInput in{file};
  in.ReadData(start, payload.data(), payload.size());
  return payload;
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
                const float* centroid)
    : _qr{MakeQuantizerReader(codebook)},
      _setting{_qr->BlockSetting()},
      _in{std::make_unique<MemoryIndexInput>(file)},
      _base{pay_start},
      _total{total},
      _records{_setting.RecordCount(total)} {
    _qr->StartCluster(centroid);
  }

  const PayloadBlockSetting& Setting() const noexcept { return _setting; }

  size_t ByteSize() const noexcept {
    return _records * size_t{_setting.record_size};
  }

  // Whole cluster in one ComputeBlock call, so multi-group blocks are covered.
  std::vector<score_t> All(score_t threshold = kNoPrune) {
    std::vector<score_t> out(_records);
    _qr->ComputeBlock(Read(0, _records), threshold, out.data());
    out.resize(_total);
    return out;
  }

  void Block(size_t offset, size_t count, score_t threshold, score_t* out) {
    const size_t gs = _setting.group_size;
    while (count != 0) {
      const size_t first = offset / gs * gs;
      const size_t records = std::min(first + gs, _records) - first;
      _cache.resize(records);
      _qr->ComputeBlock(Read(first, records), threshold, _cache.data());
      const size_t take =
        std::min(count, std::min(records, _total - first) - (offset - first));
      std::copy_n(_cache.begin() + (offset - first), take, out);
      offset += take;
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
  size_t _total;
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
  auto writer = MakeQuantizerWriter(VectorQuantization::PQ, d, metric, pq_m,
                                    /*pq_niter=*/0, /*nb_bits=*/0);
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
    writer->EncodeBlock(out, points.data(), 3);
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

  auto writer = MakeQuantizerWriter(VectorQuantization::None, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, /*nb_bits=*/0);
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
    writer->EncodeBlock(out, points.data(), kFirstBatch);
    writer->EncodeBlock(out, points.data() + kFirstBatch * d, n - kFirstBatch);
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

  ResolveEnum<VectorMetric>(metric, [&]<VectorMetric M> {
    for (size_t i = 0; i < n; ++i) {
      const score_t want = ComputeDistance<EffectiveQuantMetric(M)>(
        query.data(), points.data() + i * d, static_cast<uint16_t>(d));
      EXPECT_EQ(scores[i], want) << "row " << i;
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
  index.writer = MakeQuantizerWriter(VectorQuantization::None, d, metric,
                                     /*pq_m=*/0, /*pq_niter=*/0, /*nb_bits=*/0);
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
  for (size_t b = 0; b < index.n; b += kBatch) {
    const size_t m = std::min(kBatch, index.n - b);
    index.writer->EncodeBlock(out, points.data() + b * d, m);
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
    for (size_t i = 0; i < want.size(); ++i) {
      EXPECT_EQ(want[i].second, got[i].second)
        << "query " << qi << " rank " << i << ": " << want[i].first << " vs "
        << got[i].first;
      EXPECT_EQ(want[i].first, got[i].first) << "query " << qi << " rank " << i;
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
      if (got[i] == exact[i]) {
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

  auto l1 = MakeQuantizerWriter(VectorQuantization::None, d, VectorMetric::L1,
                                /*pq_m=*/0, /*pq_niter=*/0, /*nb_bits=*/0);
  ASSERT_NE(l1, nullptr);
  l1->Train(points.data(), n);
  EXPECT_TRUE(SerializeStats(*l1).empty());
  EXPECT_EQ(l1->BlockSetting().record_size, d * sizeof(float));
  EXPECT_EQ(l1->BlockSetting().group_size, 1);
  EXPECT_FALSE(PanoramaApplies(VectorMetric::L1, d));

  constexpr uint32_t small = 32;
  ASSERT_FALSE(PanoramaApplies(VectorMetric::L2Sqr, small));
  auto narrow = MakePanoramaData(small, n, 17);
  auto tiny = MakeQuantizerWriter(VectorQuantization::None, small,
                                  VectorMetric::L2Sqr, /*pq_m=*/0,
                                  /*pq_niter=*/0, /*nb_bits=*/0);
  ASSERT_NE(tiny, nullptr);
  tiny->Train(narrow.data(), n);
  EXPECT_TRUE(SerializeStats(*tiny).empty());
}

class rabitq_quantizer_test : public ::testing::TestWithParam<uint32_t> {};

// The FWHT rotation pads d to a power of two; verify the encode+query roundtrip
// ranking is preserved across power-of-two AND non-power-of-two dimensions, and
// that the stats blob no longer carries a dense rotation matrix.
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

  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, nb_bits);
  ASSERT_NE(writer, nullptr);
  EXPECT_EQ(writer->Kind(), VectorQuantization::RaBitQ);
  EXPECT_EQ(SerializeStats(*writer).size(), 2 * sizeof(uint32_t));
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    writer->EncodeBlock(out, points.data(), n);
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

  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, nb_bits);
  ASSERT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    writer->EncodeBlock(out, points.data(), n);
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

  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, nb_bits);
  ASSERT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    writer->EncodeBlock(out, points.data(), n);
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
  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, nb_bits);
  EXPECT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    writer->EncodeBlock(out, points.data(), n);
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

  auto writer = MakeQuantizerWriter(VectorQuantization::RaBitQ, d, metric,
                                    /*pq_m=*/0, /*pq_niter=*/0, /*nb_bits=*/1);
  ASSERT_NE(writer, nullptr);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start1;
  uint64_t pay_start2;
  {
    MemoryIndexOutput out{file};
    pay_start1 = out.Position();
    writer->SetClusterCentroid(c1.data());
    writer->EncodeBlock(out, points1.data(), 2);
    pay_start2 = out.Position();
    writer->SetClusterCentroid(c2.data());
    writer->EncodeBlock(out, points2.data(), 2);
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
    ClusterScorer s1{codebook, file, pay_start1, 2, c1.data()};
    const auto got = s1.All();
    std::copy(got.begin(), got.end(), scores.begin());
  }
  {
    ClusterScorer s2{codebook, file, pay_start2, 2, c2.data()};
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

  auto writer = MakeQuantizerWriter(VectorQuantization::PQ, d, metric, pq_m,
                                    /*pq_niter=*/0, /*nb_bits=*/0);
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
  uint64_t pay_start1;
  uint64_t pay_start2;
  {
    MemoryIndexOutput out{file};
    pay_start1 = out.Position();
    writer->SetClusterCentroid(c1.data());
    writer->EncodeBlock(out, points1.data(), 2);
    pay_start2 = out.Position();
    writer->SetClusterCentroid(c2.data());
    writer->EncodeBlock(out, points2.data(), 2);
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
    ClusterScorer s1{codebook, file, pay_start1, 2, c1.data()};
    const auto got = s1.All();
    std::copy(got.begin(), got.end(), scores.begin());
  }
  {
    ClusterScorer s2{codebook, file, pay_start2, 2, c2.data()};
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

  auto writer = MakeQuantizerWriter(VectorQuantization::PQ, d, metric, pq_m,
                                    /*pq_niter=*/0, /*nb_bits=*/0);
  ASSERT_NE(writer, nullptr);
  writer->SetClusterCentroid(centroid.data());
  writer->Train(points.data(), n);

  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  uint64_t pay_start;
  {
    MemoryIndexOutput out{file};
    pay_start = out.Position();
    writer->EncodeBlock(out, points.data(), n);
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
  auto w = MakeQuantizerWriter(VectorQuantization::PQ, d, metric, pq_m,
                               /*pq_niter=*/0, /*nb_bits=*/0);
  w->Train(pts.data(), pts.size() / d);
  w->SetClusterCentroid(cen.data());
  return w;
}

std::unique_ptr<QuantizerWriter> MakeRaBitQ(uint32_t d, uint32_t nb_bits,
                                            const std::vector<float>& cen) {
  auto w = MakeQuantizerWriter(VectorQuantization::RaBitQ, d,
                               VectorMetric::L2Sqr, /*pq_m=*/0,
                               /*pq_niter=*/0, nb_bits);
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

}  // namespace

TEST(pq_quantizer_test, block_split_is_transparent_l2) {
  constexpr uint32_t d = 8;
  const auto pts = MakeSpread(d, 100, 11);
  const std::vector<float> cen(d, 0.25f);
  const auto make = [&] {
    return MakeTrainedPq(VectorMetric::L2Sqr, d, 2, pts, cen);
  };
  const size_t chunks[] = {kFastScanBbs, 2 * kFastScanBbs, 4};
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
  const size_t chunks[] = {kFastScanBbs, 2 * kFastScanBbs, 4};
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
