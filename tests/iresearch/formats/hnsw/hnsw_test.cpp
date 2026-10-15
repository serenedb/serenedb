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

#include <algorithm>
#include <cmath>
#include <duckdb.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <iresearch/formats/ann_build_env.hpp>
#include <iresearch/formats/hnsw/hnsw_graph.hpp>
#include <iresearch/formats/hnsw/hnsw_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/detail/doc_collector.hpp>
#include <iresearch/search/filters/vector_similarity_filter.hpp>
#include <iresearch/search/scorers/vector_similarity_scorer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <numeric>
#include <random>
#include <tuple>
#include <vector>
#include <yaclib/runtime/fair_thread_pool.hpp>

#include "formats/column/test_cs_helpers.hpp"
#include "search/filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kVec = 1;
inline constexpr uint32_t kDim = 8;

irs::IndexWriterOptions MakeWriterOptions(irs::VectorMetric metric,
                                          irs::VectorQuantization quant,
                                          uint32_t nb_bits = 0,
                                          uint32_t d = kDim) {
  auto opts = irs::tests::DefaultWriterOptions();
  opts.column_options = [metric, quant, nb_bits,
                         d](irs::field_id id) -> irs::ColumnOptions {
    irs::ColumnOptions col;
    if (id == kVec) {
      col.ann_info = irs::AnnInfo{
        .kind = irs::AnnKind::Hnsw,
        .centroids_id = kVec,
        .postings_id = kVec,
        .d = static_cast<int>(d),
        .metric = metric,
        .quant = {.kind = quant, .nb_bits = nb_bits},
        .m = 8,
        .ef_construction = 64,
      };
    }
    return col;
  };
  return opts;
}

std::vector<std::vector<float>> MakeVectors(size_t n, uint32_t seed,
                                            uint32_t d = kDim) {
  std::mt19937 rng{seed};
  std::uniform_real_distribution<float> dist{-1.f, 1.f};
  std::vector<std::vector<float>> out;
  out.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    std::vector<float> v(d);
    for (auto& x : v) {
      x = dist(rng);
    }
    out.push_back(std::move(v));
  }
  return out;
}

void WriteVectorAt(irs::ColWriter& cs, irs::doc_id_t doc,
                   const std::vector<float>& vec) {
  const auto vtype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, vec.size());
  auto& cw = cs.OpenColumn(kVec, vtype);
  duckdb::Vector v{vtype, 1};
  auto& child = duckdb::ArrayVector::GetChildMutable(v);
  auto* data = duckdb::FlatVector::GetDataMutable<float>(child);
  std::ranges::copy(vec, data);
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  duckdb::FlatVector::ValidityMutable(child).SetAllValid(vec.size());
  duckdb::FlatVector::SetSize(v, 1);
  cw.Append(static_cast<uint64_t>(doc) - irs::doc_limits::min(), v,
            /*count=*/1);
}

void WriteNullAt(irs::ColWriter& cs, irs::doc_id_t doc, float payload) {
  const auto vtype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);
  auto& cw = cs.OpenColumn(kVec, vtype);
  duckdb::Vector v{vtype, 1};
  auto& child = duckdb::ArrayVector::GetChildMutable(v);
  auto* data = duckdb::FlatVector::GetDataMutable<float>(child);
  std::fill_n(data, kDim, payload);
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  duckdb::FlatVector::ValidityMutable(v).SetInvalid(0);
  duckdb::FlatVector::ValidityMutable(child).SetAllValid(kDim);
  duckdb::FlatVector::SetSize(v, 1);
  cw.Append(static_cast<uint64_t>(doc) - irs::doc_limits::min(), v,
            /*count=*/1);
}

irs::DirectoryReader BuildIndex(irs::Directory& dir,
                                const std::vector<std::vector<float>>& vecs,
                                irs::VectorMetric metric,
                                irs::VectorQuantization quant,
                                uint32_t nb_bits = 0) {
  auto writer = irs::IndexWriter::Make(
    dir, irs::kOmCreate, MakeWriterOptions(metric, quant, nb_bits));
  EXPECT_NE(nullptr, writer);
  {
    auto trx = writer->GetBatch();
    for (const auto& vec : vecs) {
      auto doc = trx.Insert();
      WriteVectorAt(*doc.GetColWriter(), doc.DocId(), vec);
    }
    trx.Commit();
  }
  writer->RefreshCommit();
  return writer->GetSnapshot();
}

irs::ByVectorSimilarity MakeKnnFilter(const std::vector<float>& query,
                                      irs::VectorMetric metric,
                                      irs::VectorQuantization quant,
                                      uint32_t ef) {
  irs::ByVectorSimilarity filter;
  *filter.mutable_field_id() = kVec;
  auto& opts = *filter.mutable_options();
  opts.query = query;
  opts.centroids_id = kVec;
  opts.postings_id = kVec;
  opts.metric = metric;
  opts.quant = quant;
  opts.nprobe = ef;
  opts.ef_search = ef;
  return filter;
}

std::vector<irs::doc_id_t> BruteForceTopK(
  const std::vector<std::vector<float>>& vecs, const std::vector<float>& query,
  irs::VectorMetric metric, size_t k) {
  std::vector<std::pair<float, irs::doc_id_t>> scored;
  scored.reserve(vecs.size());
  for (size_t i = 0; i < vecs.size(); ++i) {
    float s = 0;
    irs::ResolveEnum<irs::VectorMetric>(metric, [&]<irs::VectorMetric M>() {
      s = irs::ComputeDistance<M>(query.data(), vecs[i].data(),
                                  static_cast<uint16_t>(query.size()));
    });
    scored.emplace_back(s,
                        static_cast<irs::doc_id_t>(i) + irs::doc_limits::min());
  }
  std::ranges::sort(
    scored, [](const auto& l, const auto& r) { return l.first > r.first; });
  std::vector<irs::doc_id_t> out;
  for (size_t i = 0; i < std::min(k, scored.size()); ++i) {
    out.push_back(scored[i].second);
  }
  return out;
}

std::vector<irs::doc_id_t> RunKnn(const irs::DirectoryReader& reader,
                                  irs::ByVectorSimilarity& filter,
                                  size_t k = 128) {
  irs::VectorSimilarityScorer scorer;
  std::vector<irs::ScoreDoc> hits(k);
  const auto count =
    irs::ExecuteTopK(reader, filter, scorer, k, false, std::span{hits});
  std::vector<irs::doc_id_t> docs;
  const auto n = std::min<size_t>(count, k);
  docs.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    docs.push_back(hits[i].doc);
  }
  return docs;
}

// (quant, nb_bits, recall floor). TurboQuant needs its bit width spelled out --
// TQ accepts 1-5 -- and estimates a shorter code than the scalar quantizers
// do, so it carries its own floor.
struct HnswQuant {
  irs::VectorQuantization kind;
  uint32_t nb_bits;
  double recall_floor;
};

using HnswParam = std::tuple<irs::VectorMetric, HnswQuant>;

class HnswIndexTest : public ::testing::TestWithParam<HnswParam> {
 protected:
  irs::VectorMetric Metric() const { return std::get<0>(GetParam()); }
  irs::VectorQuantization Quant() const { return std::get<1>(GetParam()).kind; }
  uint32_t NbBits() const { return std::get<1>(GetParam()).nb_bits; }

  irs::IndexWriterOptions WriterOptions() const {
    return MakeWriterOptions(Metric(), Quant(), NbBits());
  }

  void SetUp() override {
    if (Quant() != irs::VectorQuantization::None &&
        Metric() == irs::VectorMetric::L1) {
      GTEST_SKIP() << "l1 supports only quant = none";
    }
  }

  double RecallFloor() const { return std::get<1>(GetParam()).recall_floor; }
};

TEST_P(HnswIndexTest, RecallAgainstBruteForce) {
  const auto metric = Metric();
  constexpr size_t kRows = 500;
  constexpr size_t kK = 10;

  const auto vecs = MakeVectors(kRows, 7);
  irs::MemoryDirectory dir;
  auto reader = BuildIndex(dir, vecs, metric, Quant(), NbBits());
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(1U, reader->size());
  ASSERT_EQ(kRows, reader->docs_count());

  const auto* ann = (*reader)[0].Ann(kVec);
  ASSERT_NE(nullptr, ann);
  ASSERT_EQ(irs::AnnKind::Hnsw, ann->Kind());
  ASSERT_EQ(kDim, ann->Dim());
  ASSERT_FALSE(ann->Empty());
  ASSERT_TRUE(ann->SupportsFilter());

  size_t total_hits = 0;
  size_t matched = 0;
  const auto queries = MakeVectors(20, 99);
  for (const auto& q : queries) {
    auto filter = MakeKnnFilter(q, metric, Quant(), 128);
    const auto got = RunKnn(reader, filter);
    ASSERT_FALSE(got.empty());
    const auto want = BruteForceTopK(vecs, q, metric, kK);
    for (const auto doc : want) {
      ++total_hits;
      matched += std::ranges::find(got, doc) != got.end() ? 1 : 0;
    }
  }
  const double recall =
    static_cast<double>(matched) / static_cast<double>(total_hits);
  EXPECT_GE(recall, RecallFloor())
    << "recall " << recall << " for metric " << static_cast<int>(metric)
    << " quant " << static_cast<int>(Quant());
}

TEST_P(HnswIndexTest, RecallAcrossManyRowBatches) {
  const auto metric = Metric();
  constexpr size_t kRows = 5 * STANDARD_VECTOR_SIZE + 37;
  constexpr size_t kK = 10;
  constexpr uint32_t kEf = 64;

  const auto vecs = MakeVectors(kRows, 21);
  irs::MemoryDirectory dir;
  auto reader = BuildIndex(dir, vecs, metric, Quant(), NbBits());
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(kRows, reader->docs_count());

  size_t total_hits = 0;
  size_t matched = 0;
  const auto queries = MakeVectors(20, 5);
  for (const auto& q : queries) {
    auto filter = MakeKnnFilter(q, metric, Quant(), kEf);
    const auto got = RunKnn(reader, filter);
    ASSERT_FALSE(got.empty());
    const auto want = BruteForceTopK(vecs, q, metric, kK);
    for (const auto doc : want) {
      ++total_hits;
      matched += std::ranges::find(got, doc) != got.end() ? 1 : 0;
    }
  }
  const double recall =
    static_cast<double>(matched) / static_cast<double>(total_hits);
  EXPECT_GE(recall, RecallFloor())
    << "recall " << recall << " for metric " << static_cast<int>(metric)
    << " quant " << static_cast<int>(Quant());
}

// Two segments merged into one. The writer adopts the larger segment's graph
// and inserts the smaller one as a delta instead of rebuilding, so the merged
// index must still answer as well as one built from scratch over the same rows.
TEST_P(HnswIndexTest, MergeReusesDonorGraph) {
  const auto metric = Metric();
  constexpr size_t kBig = 1600;
  constexpr size_t kSmall = 400;
  constexpr size_t kK = 10;

  auto vecs = MakeVectors(kBig + kSmall, 31);

  irs::MemoryDirectory dir;
  auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate, WriterOptions());
  ASSERT_NE(nullptr, writer);
  for (const size_t bound : {kBig, kBig + kSmall}) {
    auto trx = writer->GetBatch();
    for (size_t i = bound == kBig ? 0 : kBig; i < bound; ++i) {
      auto doc = trx.Insert();
      WriteVectorAt(*doc.GetColWriter(), doc.DocId(), vecs[i]);
    }
    trx.Commit();
    writer->RefreshCommit();
  }
  ASSERT_EQ(2U, writer->GetSnapshot()->size());

  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  writer->RefreshCommit();
  auto merged = writer->GetSnapshot();
  ASSERT_NE(nullptr, merged);
  ASSERT_EQ(1U, merged->size());
  ASSERT_EQ(kBig + kSmall, merged->docs_count());

  irs::MemoryDirectory fresh_dir;
  auto fresh = BuildIndex(fresh_dir, vecs, metric, Quant(), NbBits());
  ASSERT_NE(nullptr, fresh);

  size_t total = 0;
  size_t hit_merged = 0;
  size_t hit_fresh = 0;
  const auto queries = MakeVectors(20, 77);
  for (const auto& q : queries) {
    auto f1 = MakeKnnFilter(q, metric, Quant(), 128);
    auto f2 = MakeKnnFilter(q, metric, Quant(), 128);
    const auto got_merged = RunKnn(merged, f1);
    const auto got_fresh = RunKnn(fresh, f2);
    ASSERT_FALSE(got_merged.empty());
    for (const auto doc : BruteForceTopK(vecs, q, metric, kK)) {
      ++total;
      hit_merged += std::ranges::find(got_merged, doc) != got_merged.end();
      hit_fresh += std::ranges::find(got_fresh, doc) != got_fresh.end();
    }
  }
  const double recall_merged =
    static_cast<double>(hit_merged) / static_cast<double>(total);
  const double recall_fresh =
    static_cast<double>(hit_fresh) / static_cast<double>(total);
  EXPECT_GE(recall_merged, RecallFloor())
    << "merged recall " << recall_merged << " metric "
    << static_cast<int>(metric) << " quant " << static_cast<int>(Quant());
  EXPECT_GE(recall_merged, recall_fresh * 0.9)
    << "merged " << recall_merged << " vs rebuilt " << recall_fresh;
}

TEST_P(HnswIndexTest, NullRowsStayOutOfTrainingAndResults) {
  const auto metric = Metric();
  constexpr size_t kNulls = 900;
  constexpr size_t kRows = 500;
  constexpr size_t kK = 10;
  const auto vecs = MakeVectors(2 * kRows, 41);

  irs::MemoryDirectory dir;
  auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate, WriterOptions());
  ASSERT_NE(nullptr, writer);
  const auto write_part = [&](size_t part) {
    auto trx = writer->GetBatch();
    for (size_t i = 0; i < kNulls; ++i) {
      auto doc = trx.Insert();
      WriteNullAt(*doc.GetColWriter(), doc.DocId(), 50.f);
    }
    for (size_t i = part * kRows; i < (part + 1) * kRows; ++i) {
      auto doc = trx.Insert();
      WriteVectorAt(*doc.GetColWriter(), doc.DocId(), vecs[i]);
    }
    trx.Commit();
    writer->RefreshCommit();
  };
  std::vector<irs::doc_id_t> docs(vecs.size());
  for (size_t i = 0; i < vecs.size(); ++i) {
    docs[i] = static_cast<irs::doc_id_t>(irs::doc_limits::min() +
                                         (i / kRows + 1) * kNulls + i);
  }

  const auto queries = MakeVectors(20, 43);
  const auto check = [&](const irs::DirectoryReader& reader, size_t count,
                         std::string_view stage) {
    const std::span<const std::vector<float>> rows{vecs.data(), count};
    const std::span<const irs::doc_id_t> want_docs{docs.data(), count};
    size_t total = 0;
    size_t matched = 0;
    for (const auto& q : queries) {
      auto filter = MakeKnnFilter(q, metric, Quant(), 128);
      const auto got = RunKnn(reader, filter);
      ASSERT_FALSE(got.empty()) << stage;
      for (const auto doc : got) {
        ASSERT_NE(std::ranges::find(want_docs, doc), want_docs.end())
          << stage << ": doc " << doc << " has no vector";
      }
      const std::vector<std::vector<float>> owned(rows.begin(), rows.end());
      for (const auto ordinal : BruteForceTopK(owned, q, metric, kK)) {
        ++total;
        const auto doc = want_docs[ordinal - irs::doc_limits::min()];
        matched += std::ranges::find(got, doc) != got.end() ? 1 : 0;
      }
    }
    EXPECT_GE(static_cast<double>(matched) / static_cast<double>(total),
              RecallFloor())
      << stage << " metric " << static_cast<int>(metric) << " quant "
      << static_cast<int>(Quant());
  };

  write_part(0);
  auto flushed = writer->GetSnapshot();
  ASSERT_EQ(1U, flushed->size());
  check(flushed, kRows, "flush");

  write_part(1);
  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  writer->RefreshCommit();
  auto merged = writer->GetSnapshot();
  ASSERT_EQ(1U, merged->size());
  check(merged, vecs.size(), "merge");
}

// Eight equal segments merged at once: the shape a CREATE INDEX actually
// produces, since it flushes one tail segment per DuckDB sink thread. Only the
// largest is adopted, so seven eighths of the rows come back through the delta
// insert -- the case where a merge does the most work and has the most room to
// come out worse than a rebuild.
TEST_P(HnswIndexTest, MergeOfManySourcesMatchesRebuild) {
  const auto metric = Metric();
  constexpr size_t kSegments = 8;
  constexpr size_t kPerSegment = 250;
  constexpr size_t kRows = kSegments * kPerSegment;
  constexpr size_t kK = 10;

  auto vecs = MakeVectors(kRows, 41);

  irs::MemoryDirectory dir;
  auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate, WriterOptions());
  ASSERT_NE(nullptr, writer);
  for (size_t seg = 0; seg < kSegments; ++seg) {
    auto trx = writer->GetBatch();
    for (size_t i = seg * kPerSegment; i < (seg + 1) * kPerSegment; ++i) {
      auto doc = trx.Insert();
      WriteVectorAt(*doc.GetColWriter(), doc.DocId(), vecs[i]);
    }
    trx.Commit();
    writer->RefreshCommit();
  }
  ASSERT_EQ(kSegments, writer->GetSnapshot()->size());

  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  writer->RefreshCommit();
  auto merged = writer->GetSnapshot();
  ASSERT_NE(nullptr, merged);
  ASSERT_EQ(1U, merged->size());
  ASSERT_EQ(kRows, merged->docs_count());

  irs::MemoryDirectory fresh_dir;
  auto fresh = BuildIndex(fresh_dir, vecs, metric, Quant(), NbBits());
  ASSERT_NE(nullptr, fresh);

  size_t total = 0;
  size_t hit_merged = 0;
  size_t hit_fresh = 0;
  const auto queries = MakeVectors(25, 4242);
  for (const auto& q : queries) {
    auto f1 = MakeKnnFilter(q, metric, Quant(), 128);
    auto f2 = MakeKnnFilter(q, metric, Quant(), 128);
    const auto got_merged = RunKnn(merged, f1);
    const auto got_fresh = RunKnn(fresh, f2);
    ASSERT_FALSE(got_merged.empty());
    for (const auto doc : BruteForceTopK(vecs, q, metric, kK)) {
      ++total;
      hit_merged += std::ranges::find(got_merged, doc) != got_merged.end();
      hit_fresh += std::ranges::find(got_fresh, doc) != got_fresh.end();
    }
  }
  const double recall_merged =
    static_cast<double>(hit_merged) / static_cast<double>(total);
  const double recall_fresh =
    static_cast<double>(hit_fresh) / static_cast<double>(total);
  EXPECT_GE(recall_merged, RecallFloor())
    << "merged recall " << recall_merged << " metric "
    << static_cast<int>(metric) << " quant " << static_cast<int>(Quant());
  EXPECT_GE(recall_merged, recall_fresh * 0.9)
    << "merged " << recall_merged << " vs rebuilt " << recall_fresh;
}

TEST_P(HnswIndexTest, SerializedGraphSurvivesReopen) {
  const auto metric = Metric();
  const auto vecs = MakeVectors(200, 11);
  irs::MemoryDirectory dir;
  auto reader = BuildIndex(dir, vecs, metric, Quant(), NbBits());
  ASSERT_NE(nullptr, reader);

  auto filter = MakeKnnFilter(vecs[0], metric, Quant(), 64);
  const auto first = RunKnn(reader, filter);
  ASSERT_FALSE(first.empty());
  auto reopened = irs::DirectoryReader{dir, irs::tests::DefaultReaderOptions()};
  ASSERT_NE(nullptr, reopened);
  auto filter2 = MakeKnnFilter(vecs[0], metric, Quant(), 64);
  const auto second = RunKnn(reopened, filter2);
  EXPECT_EQ(first, second);
}

TEST_P(HnswIndexTest, QueryVectorFindsItself) {
  const auto metric = Metric();
  const auto vecs = MakeVectors(300, 13);
  irs::MemoryDirectory dir;
  auto reader = BuildIndex(dir, vecs, metric, Quant(), NbBits());
  ASSERT_NE(nullptr, reader);

  size_t found = 0;
  for (size_t i = 0; i < vecs.size(); i += 25) {
    auto filter = MakeKnnFilter(vecs[i], metric, Quant(), 64);
    const auto got = RunKnn(reader, filter);
    const auto self = static_cast<irs::doc_id_t>(i) + irs::doc_limits::min();
    found += std::ranges::find(got, self) != got.end() ? 1 : 0;
  }
  EXPECT_EQ(12U, found);
}

INSTANTIATE_TEST_SUITE_P(
  metrics, HnswIndexTest,
  ::testing::Combine(
    ::testing::Values(irs::VectorMetric::L2Sqr, irs::VectorMetric::InnerProduct,
                      irs::VectorMetric::Cosine, irs::VectorMetric::L1),
    ::testing::Values(HnswQuant{irs::VectorQuantization::None, 0, 0.9},
                      HnswQuant{irs::VectorQuantization::SQ8, 0, 0.9},
                      HnswQuant{irs::VectorQuantization::SQ4, 0, 0.7},
                      HnswQuant{irs::VectorQuantization::TQ, 4, 0.7},
                      HnswQuant{irs::VectorQuantization::TQ, 2, 0.5},
                      HnswQuant{irs::VectorQuantization::TQ, 3, 0.5},
                      HnswQuant{irs::VectorQuantization::BQ, 1, 0.3},
                      HnswQuant{irs::VectorQuantization::BQ, 2, 0.5},
                      HnswQuant{irs::VectorQuantization::OSQ, 1, 0.5},
                      HnswQuant{irs::VectorQuantization::OSQ, 4, 0.8},
                      HnswQuant{irs::VectorQuantization::FP16, 0, 0.9},
                      HnswQuant{irs::VectorQuantization::BF16, 0, 0.9})));

}  // namespace
namespace {

struct SelectRefDist {
  const std::vector<float>* pts;
  uint32_t d;
  // Selects which HnswSelectNeighbors form runs; both must agree with the
  // reference, since a quantizer without a symmetric estimator still takes the
  // accepted-side path in production.
  bool cheap_pair = true;

  bool CheapPair() const noexcept { return cheap_pair; }

  irs::score_t Pair(uint32_t a, uint32_t b) const noexcept {
    const float* x = pts->data() + static_cast<size_t>(a) * d;
    const float* y = pts->data() + static_cast<size_t>(b) * d;
    float s = 0.f;
    for (uint32_t i = 0; i < d; ++i) {
      const float t = x[i] - y[i];
      s += t * t;
    }
    return -s;
  }

  void PairBatch(uint32_t from, std::span<const uint32_t> to,
                 irs::score_t* out) const noexcept {
    for (size_t i = 0; i < to.size(); ++i) {
      out[i] = Pair(from, to[i]);
    }
  }
};

// The shape HnswSelectNeighbors had before it was scored from the accepted
// side; kept here so the transposed version is pinned against it.
std::vector<uint32_t> SelectReference(const SelectRefDist& dist,
                                      std::span<const irs::HnswCandidate> in,
                                      uint32_t limit) {
  std::vector<uint32_t> out;
  for (const auto& cand : in) {
    if (out.size() >= limit) {
      break;
    }
    bool keep = true;
    for (const uint32_t a : out) {
      if (dist.Pair(cand.node, a) > cand.score) {
        keep = false;
        break;
      }
    }
    if (keep) {
      out.push_back(cand.node);
    }
  }
  return out;
}

}  // namespace

template<irs::VectorMetric M>
void CheckPanoramaDist(const std::vector<float>& base,
                       const std::vector<float>& norms,
                       const std::vector<float>& query, uint32_t d) {
  const size_t rows = norms.size();
  std::vector<float> tails(irs::HnswPanoramaLevels(d) + 1);
  irs::HnswPanoramaTails(query.data(), d, tails.data());
  const irs::HnswPanoramaDist<M> dist{.base = base.data(),
                                      .d = d,
                                      .q = query.data(),
                                      .tails = tails.data(),
                                      .norms = norms.data()};
  std::vector<uint32_t> ids(rows);
  std::iota(ids.begin(), ids.end(), 0U);
  std::vector<irs::score_t> exact(rows);
  irs::HnswComputeDistances<M>(query.data(), base.data(), d, ids, exact.data());
  std::vector<irs::score_t> scored(rows);
  dist.Batch(ids, scored.data());
  for (size_t i = 0; i < rows; ++i) {
    ASSERT_EQ(scored[i], exact[i]) << "d=" << d << " row " << i;
  }
  auto sorted = exact;
  std::ranges::sort(sorted);
  for (const double share : {0.1, 0.5, 0.9}) {
    const auto threshold =
      sorted[static_cast<size_t>(share * static_cast<double>(rows - 1))];
    dist.Batch(ids, scored.data(), threshold);
    size_t dropped = 0;
    for (size_t i = 0; i < rows; ++i) {
      if (scored[i] == irs::kHnswNoThreshold) {
        ++dropped;
        EXPECT_LE(exact[i], threshold)
          << "d=" << d << " share " << share << " row " << i;
      } else {
        EXPECT_NEAR(scored[i], exact[i], 1e-4f * (1.f + std::abs(exact[i])))
          << "d=" << d << " share " << share << " row " << i;
      }
    }
    if (d > irs::kHnswPanoramaLevel && share > 0.5) {
      EXPECT_GT(dropped, 0U) << "d=" << d << " share " << share;
    }
  }
}

TEST(HnswPanoramaDistTest, PrunesOnlyRowsThatCannotBeatTheThreshold) {
  for (const uint32_t d : {64U, 96U, 130U}) {
    constexpr size_t kRows = 400;
    pcg32 rng{d};
    const auto draw = [&](uint32_t j) {
      return absl::Uniform<float>(rng, -1.f, 1.f) /
             (1.f + static_cast<float>(j) / 8.f);
    };
    std::vector<float> base(kRows * d);
    std::vector<float> norms(kRows);
    for (size_t i = 0; i < kRows; ++i) {
      for (uint32_t j = 0; j < d; ++j) {
        base[i * d + j] = draw(j);
      }
      norms[i] = faiss::fvec_norm_L2sqr(base.data() + i * d, d);
    }
    for (size_t qi = 0; qi < 8; ++qi) {
      std::vector<float> query(d);
      for (uint32_t j = 0; j < d; ++j) {
        query[j] = draw(j);
      }
      CheckPanoramaDist<irs::VectorMetric::L2Sqr>(base, norms, query, d);
      CheckPanoramaDist<irs::VectorMetric::InnerProduct>(base, norms, query, d);
    }
  }
}

namespace {

std::vector<std::vector<float>> MakeDecayingVectors(size_t n, uint32_t seed,
                                                    uint32_t d) {
  auto out = MakeVectors(n, seed, d);
  for (auto& v : out) {
    for (uint32_t j = 0; j < d; ++j) {
      v[j] /= 1.f + static_cast<float>(j) / 8.f;
    }
  }
  return out;
}

std::vector<std::shared_ptr<const std::vector<float>>> SegmentBases(
  const irs::DirectoryReader& reader) {
  std::vector<std::shared_ptr<const std::vector<float>>> out;
  for (const auto& segment : reader) {
    const auto* ann = static_cast<const irs::HnswIndex*>(segment.Ann(kVec));
    out.push_back(ann->Load(segment)->rotation);
  }
  return out;
}

void AppendBatch(irs::IndexWriter& writer,
                 std::span<const std::vector<float>> vecs) {
  auto trx = writer.GetBatch();
  for (const auto& vec : vecs) {
    auto doc = trx.Insert();
    WriteVectorAt(*doc.GetColWriter(), doc.DocId(), vec);
  }
  trx.Commit();
  writer.RefreshCommit();
}

}  // namespace

TEST(AnnQueryCacheTest, ThreadsShareOneRotationPerBasis) {
  constexpr uint32_t kD = 4 * irs::kHnswPanoramaLevel + 17;
  pcg32 rng{kD};
  const auto random = [&](size_t n) {
    std::vector<float> out(n);
    for (auto& v : out) {
      v = absl::Uniform<float>(rng, -1.f, 1.f);
    }
    return out;
  };
  const auto first =
    std::make_shared<const std::vector<float>>(random(size_t{kD} * kD));
  const auto second =
    std::make_shared<const std::vector<float>>(random(size_t{kD} * kD));
  const auto query = random(kD);

  irs::AnnQueryCache cache;
  constexpr size_t kThreads = 8;
  std::vector<std::shared_ptr<const irs::PanoramaQuery>> got(2 * kThreads);
  std::vector<std::thread> threads;
  for (size_t t = 0; t < 2 * kThreads; ++t) {
    threads.emplace_back([&, t] {
      got[t] = cache.Get(t % 2 == 0 ? first : second, nullptr, query);
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  for (const auto* basis : {first.get(), second.get()}) {
    const size_t parity = basis == first.get() ? 0 : 1;
    std::vector<float> rotated(kD);
    for (uint32_t row = 0; row < kD; ++row) {
      double sum = 0;
      for (uint32_t col = 0; col < kD; ++col) {
        sum += double{(*basis)[size_t{row} * kD + col]} * query[col];
      }
      rotated[row] = static_cast<float>(sum);
    }
    std::vector<float> tails(irs::HnswPanoramaLevels(kD) + 1);
    irs::HnswPanoramaTails(rotated.data(), kD, tails.data());
    const auto& shared = got[parity];
    for (size_t t = parity; t < got.size(); t += 2) {
      ASSERT_EQ(shared, got[t]);
    }
    for (uint32_t i = 0; i < kD; ++i) {
      ASSERT_NEAR(rotated[i], shared->rotated[i],
                  1e-4f * (1.f + std::abs(rotated[i])))
        << "row " << i;
    }
    for (size_t i = 0; i < tails.size(); ++i) {
      ASSERT_NEAR(tails[i], shared->tails[i], 1e-3f * (1.f + tails[i]))
        << "level " << i;
    }
  }
  EXPECT_NE(got[0], got[1]);

  auto copy = cache;
  EXPECT_NE(got[0], copy.Get(first, nullptr, query));
}

TEST(AnnQueryCacheTest, ByteBasisRotatesWithinItsPrecision) {
  constexpr uint32_t kD = 4 * irs::kHnswPanoramaLevel + 17;
  pcg32 rng{kD + 1};
  const auto random = [&](size_t n) {
    std::vector<float> out(n);
    for (auto& v : out) {
      v = absl::Uniform<float>(rng, -1.f, 1.f);
    }
    return out;
  };
  const auto basis =
    std::make_shared<const std::vector<float>>(random(size_t{kD} * kD));
  const auto query = random(kD);
  irs::HnswBases bases;
  const auto bytes = bases.Bytes(basis);
  ASSERT_EQ(bytes->codes.size(), basis->size());
  ASSERT_EQ(bytes->scales.size(), kD);
  EXPECT_EQ(bytes, bases.Bytes(basis));
  irs::AnnQueryCache cache;
  const auto got = cache.Get(basis, bytes, query);
  double mass = 0;
  for (const auto v : query) {
    mass += std::abs(double{v});
  }
  for (uint32_t row = 0; row < kD; ++row) {
    double sum = 0;
    double peak = 0;
    for (uint32_t col = 0; col < kD; ++col) {
      const double b = (*basis)[size_t{row} * kD + col];
      sum += b * query[col];
      peak = std::max(peak, std::abs(b));
    }
    ASSERT_NEAR(bytes->scales[row], peak / 127, 1e-6 * peak) << "row " << row;
    ASSERT_NEAR(got->rotated[row], sum, 0.55 * bytes->scales[row] * mass)
      << "row " << row;
  }
}

TEST(AnnQueryCacheTest, ByteBasisRotatesANonFiniteQueryAsTheFloatBasis) {
  constexpr uint32_t kD = 4 * irs::kHnswPanoramaLevel;
  pcg32 rng{kD};
  std::vector<float> values(size_t{kD} * kD);
  for (auto& v : values) {
    v = absl::Uniform<float>(rng, -1.f, 1.f);
  }
  const auto basis =
    std::make_shared<const std::vector<float>>(std::move(values));
  irs::HnswBases bases;
  for (const float bad : {std::numeric_limits<float>::quiet_NaN(),
                          std::numeric_limits<float>::infinity()}) {
    std::vector<float> query(kD, 0.5f);
    query[7] = bad;
    irs::AnnQueryCache narrow;
    irs::AnnQueryCache wide;
    const auto got = narrow.Get(basis, bases.Bytes(basis), query);
    const auto want = wide.Get(basis, nullptr, query);
    for (uint32_t row = 0; row < kD; ++row) {
      if (std::isnan(want->rotated[row])) {
        ASSERT_TRUE(std::isnan(got->rotated[row])) << "row " << row;
      } else {
        ASSERT_EQ(got->rotated[row], want->rotated[row]) << "row " << row;
      }
    }
  }
}

TEST(AnnQueryCacheTest, ByteBasisRotatesAnAlignedQueryAtTheWidestBasis) {
  constexpr uint32_t kD = irs::kHnswPanoramaMaxDim;
  const auto basis =
    std::make_shared<const std::vector<float>>(size_t{kD} * kD, -1.f);
  const std::vector<float> query(kD, 1.f);
  irs::HnswBases bases;
  irs::AnnQueryCache cache;
  const auto got = cache.Get(basis, bases.Bytes(basis), query);
  for (uint32_t row = 0; row < kD; ++row) {
    ASSERT_NEAR(got->rotated[row], -float{kD}, 1e-3f * kD) << "row " << row;
  }
}

TEST(HnswPanoramaBasisTest, SegmentsOfAnIndexShareOneBasis) {
  constexpr uint32_t kD = irs::kHnswPanoramaMinDim;
  constexpr size_t kBatch = 8 * kD;
  constexpr size_t kK = 10;
  for (const auto metric :
       {irs::VectorMetric::L2Sqr, irs::VectorMetric::InnerProduct}) {
    const auto vecs = MakeDecayingVectors(3 * kBatch, 41, kD);
    const std::span all{vecs};
    const std::vector indexed(vecs.begin(), vecs.begin() + 2 * kBatch);
    irs::MemoryDirectory dir;
    {
      auto writer = irs::IndexWriter::Make(
        dir, irs::kOmCreate,
        MakeWriterOptions(metric, irs::VectorQuantization::None, 0, kD));
      ASSERT_NE(nullptr, writer);
      AppendBatch(*writer, all.subspan(0, kBatch));
      AppendBatch(*writer, all.subspan(kBatch, kBatch));
      const auto flushed = SegmentBases(writer->GetSnapshot());
      ASSERT_EQ(2U, flushed.size());
      ASSERT_NE(nullptr, flushed[0]);
      ASSERT_EQ(size_t{kD} * kD, flushed[0]->size());
      EXPECT_EQ(flushed[0], flushed[1]);

      ASSERT_TRUE(writer->Compact(
        irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
      writer->RefreshCommit();
      const auto merged = writer->GetSnapshot();
      ASSERT_EQ(1U, merged->size());
      EXPECT_EQ(flushed[0], SegmentBases(merged)[0]);

      size_t total = 0;
      size_t hits = 0;
      for (const auto& q : MakeDecayingVectors(20, 43, kD)) {
        auto filter =
          MakeKnnFilter(q, metric, irs::VectorQuantization::None, 128);
        const auto got = RunKnn(merged, filter);
        for (const auto doc : BruteForceTopK(indexed, q, metric, kK)) {
          ++total;
          hits += std::ranges::find(got, doc) != got.end() ? 1 : 0;
        }
      }
      EXPECT_GE(static_cast<double>(hits) / static_cast<double>(total), 0.95)
        << "metric " << static_cast<int>(metric);
    }
    auto writer = irs::IndexWriter::Make(
      dir, irs::kOmAppend,
      MakeWriterOptions(metric, irs::VectorQuantization::None, 0, kD));
    ASSERT_NE(nullptr, writer);
    const auto loaded = SegmentBases(writer->GetSnapshot());
    ASSERT_EQ(1U, loaded.size());
    ASSERT_NE(nullptr, loaded[0]);
    AppendBatch(*writer, all.subspan(2 * kBatch, kBatch));
    const auto appended = SegmentBases(writer->GetSnapshot());
    ASSERT_EQ(2U, appended.size());
    EXPECT_EQ(loaded[0], appended[0]);
    EXPECT_EQ(loaded[0], appended[1]);
  }
}

TEST(HnswPanoramaBasisTest, Sq8CodesRotateIntoTheSharedBasis) {
  constexpr uint32_t kD = irs::kHnswPanoramaMinDim + 40;
  constexpr size_t kBatch = 8 * kD;
  constexpr size_t kK = 10;
  constexpr auto kMetric = irs::VectorMetric::InnerProduct;
  constexpr auto kQuant = irs::VectorQuantization::SQ8;
  const auto vecs = MakeDecayingVectors(2 * kBatch, 47, kD);
  const std::span all{vecs};
  irs::MemoryDirectory dir;
  auto writer = irs::IndexWriter::Make(
    dir, irs::kOmCreate, MakeWriterOptions(kMetric, kQuant, 0, kD));
  ASSERT_NE(nullptr, writer);
  AppendBatch(*writer, all.subspan(0, kBatch));
  AppendBatch(*writer, all.subspan(kBatch, kBatch));
  const auto flushed = SegmentBases(writer->GetSnapshot());
  ASSERT_EQ(2U, flushed.size());
  ASSERT_NE(nullptr, flushed[0]);
  EXPECT_EQ(flushed[0], flushed[1]);
  for (const auto& segment : writer->GetSnapshot()) {
    const auto* ann = static_cast<const irs::HnswIndex*>(segment.Ann(kVec));
    EXPECT_EQ(ann->Load(segment)->tails.size(),
              size_t{kBatch} * irs::HnswCodeTails(kD));
  }

  ASSERT_TRUE(writer->Compact(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount())));
  writer->RefreshCommit();
  const auto merged = writer->GetSnapshot();
  ASSERT_EQ(1U, merged->size());
  EXPECT_EQ(flushed[0], SegmentBases(merged)[0]);

  size_t total = 0;
  size_t hits = 0;
  for (const auto& q : MakeDecayingVectors(20, 53, kD)) {
    auto filter = MakeKnnFilter(q, kMetric, kQuant, 128);
    const auto got = RunKnn(merged, filter);
    for (const auto doc : BruteForceTopK(vecs, q, kMetric, kK)) {
      ++total;
      hits += std::ranges::find(got, doc) != got.end() ? 1 : 0;
    }
  }
  EXPECT_GE(static_cast<double>(hits) / static_cast<double>(total), 0.95);
}

// The share of nodes carrying a row above level 0 is what the greedy descent
// navigates on, and it follows directly from how the level sample is rounded:
// truncating (hnswlib) admits at sample >= 1 -> 1/m, rounding (qdrant, and us)
// admits at sample >= 0.5 -> 1/sqrt(m). At m=16 that is 25% against 6.25%.
// Pinned because the two differ by a single cast and the search-side cost of
// getting it wrong -- 2x the ef_search for the same recall -- shows up nowhere
// near this code.
TEST(HnswRandomLevelTest, UpperLevelShareMatchesRoundedDraw) {
  for (const uint32_t m : {8U, 16U, 32U}) {
    constexpr size_t kDraws = 200000;
    pcg32 rng{irs::kHnswBuildSeed};
    size_t above = 0;
    double levels = 0;
    for (size_t i = 0; i < kDraws; ++i) {
      const auto count = irs::HnswRandomLevel(rng, m);
      ASSERT_GE(count, 1U);
      above += count > 1 ? 1 : 0;
      levels += static_cast<double>(count);
    }
    const double share = static_cast<double>(above) / kDraws;
    const double want = 1.0 / std::sqrt(static_cast<double>(m));
    EXPECT_NEAR(share, want, 0.01)
      << "m=" << m << " share above level 0 " << share << " want ~" << want
      << " (truncation would give " << 1.0 / m << ")";
    // Expected rows per node feeds graph memory and insert cost.
    const double want_levels =
      1.0 + std::sqrt(static_cast<double>(m)) / (static_cast<double>(m) - 1.0);
    EXPECT_NEAR(levels / kDraws, want_levels, 0.02) << "m=" << m;
  }
}

TEST(HnswSelectNeighborsTest, TransposedMatchesReference) {
  constexpr uint32_t kD = 6;
  constexpr size_t kNodes = 400;
  std::mt19937 rng{20260901};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> pts(kNodes * kD);
  for (float& v : pts) {
    v = nd(rng);
  }
  const SelectRefDist dist{.pts = &pts, .d = kD};

  irs::HnswBuildScratch scratch;
  std::uniform_int_distribution<uint32_t> pick{
    0, static_cast<uint32_t>(kNodes) - 1};

  for (int trial = 0; trial < 200; ++trial) {
    const size_t n = 1 + (static_cast<size_t>(rng()) % 64);
    const uint32_t query = pick(rng);
    std::vector<irs::HnswCandidate> cands;
    cands.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      const uint32_t node = pick(rng);
      if (node == query) {
        continue;
      }
      cands.push_back({dist.Pair(query, node), node});
    }
    std::ranges::sort(
      cands, [](const auto& l, const auto& r) { return l.score > r.score; });
    cands.erase(std::unique(cands.begin(), cands.end(),
                            [](const auto& l, const auto& r) {
                              return l.node == r.node;
                            }),
                cands.end());
    if (cands.empty()) {
      continue;
    }

    for (const uint32_t limit : {1U, 3U, 8U, 16U, 32U}) {
      const auto want = SelectReference(dist, cands, limit);
      for (const bool cheap : {true, false}) {
        SelectRefDist form{.pts = &pts, .d = kD, .cheap_pair = cheap};
        irs::HnswSelectNeighbors(form, cands, limit, scratch);
        ASSERT_EQ(want, scratch.selected)
          << "trial " << trial << " limit " << limit << " n " << cands.size()
          << " cheap_pair " << cheap;
      }
    }
  }
}

namespace {

struct WalkDist {
  const std::vector<float>* pts;
  uint32_t d;
  const float* q = nullptr;

  const float* Row(uint32_t id) const noexcept {
    return pts->data() + static_cast<size_t>(id) * d;
  }

  static irs::score_t Score(const float* x, const float* y,
                            uint32_t d) noexcept {
    float s = 0.f;
    for (uint32_t i = 0; i < d; ++i) {
      const float t = x[i] - y[i];
      s += t * t;
    }
    return -s;
  }

  void SetQuery(uint32_t id) noexcept { q = Row(id); }

  irs::score_t One(uint32_t id) const noexcept { return Score(q, Row(id), d); }

  void Batch(std::span<const uint32_t> ids, irs::score_t* out,
             irs::score_t = 0.f) const noexcept {
    for (size_t i = 0; i < ids.size(); ++i) {
      out[i] = One(ids[i]);
    }
  }

  void Prefetch(uint32_t) const noexcept {}

  bool CheapPair() const noexcept { return true; }

  irs::score_t Pair(uint32_t a, uint32_t b) const noexcept {
    return Score(Row(a), Row(b), d);
  }

  void PairBatch(uint32_t from, std::span<const uint32_t> to,
                 irs::score_t* out) const noexcept {
    for (size_t i = 0; i < to.size(); ++i) {
      out[i] = Pair(from, to[i]);
    }
  }
};

struct HopAccept {
  const std::vector<uint8_t>* admitted;
  std::vector<uint8_t>* handed;
  size_t* unhanded;

  void Prepare(std::span<const uint32_t> hop) const {
    for (const auto node : hop) {
      (*handed)[node] = 1;
    }
  }

  bool operator()(uint32_t node) const {
    if ((*handed)[node] == 0) {
      (*handed)[node] = 1;
      ++*unhanded;
    }
    return (*admitted)[node] != 0;
  }
};

template<irs::HnswWalk Walk>
void CheckWalkAsksPerHop() {
  constexpr uint32_t kD = 8;
  constexpr uint32_t kNodes = 3000;
  constexpr uint32_t kM = 8;
  constexpr uint32_t kEf = 64;
  constexpr size_t kK = 10;
  constexpr size_t kQueries = 50;
  pcg32 rng{20261013};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> pts(size_t{kNodes} * kD);
  for (float& v : pts) {
    v = nd(rng);
  }
  std::vector<uint8_t> admitted(kNodes);
  for (auto& a : admitted) {
    a = rng() % 4 == 0 ? 1 : 0;
  }

  irs::HnswGraphWriter writer;
  writer.Reset(kNodes, kM);
  pcg32 levels{irs::kHnswBuildSeed};
  uint32_t entry = 0;
  for (uint32_t i = 0; i < kNodes; ++i) {
    writer.SetLevel(i, irs::HnswRandomLevel(levels, kM));
    if (writer.LevelOf(i) > writer.LevelOf(entry)) {
      entry = i;
    }
  }
  writer.AllocateLinks();
  writer.SetEntryPoint(entry);
  WalkDist build{.pts = &pts, .d = kD};
  irs::HnswBuildScratch scratch;
  scratch.search.visited.Reset(kNodes);
  for (uint32_t i = 0; i < kNodes; ++i) {
    if (i != entry) {
      build.SetQuery(i);
      irs::HnswInsert(writer, i, build, kEf, scratch);
    }
  }
  const auto& graph = writer.Graph();

  std::vector<float> query(kD);
  WalkDist dist{.pts = &pts, .d = kD, .q = query.data()};
  irs::HnswSearchScratch s;
  size_t found_plain = 0;
  size_t found_hops = 0;
  for (size_t qi = 0; qi < kQueries; ++qi) {
    for (float& v : query) {
      v = nd(rng);
    }
    std::vector<std::pair<irs::score_t, uint32_t>> truth;
    for (uint32_t n = 0; n < kNodes; ++n) {
      if (admitted[n] != 0) {
        truth.emplace_back(dist.One(n), n);
      }
    }
    std::partial_sort(truth.begin(), truth.begin() + kK, truth.end(),
                      std::greater<>{});
    truth.resize(kK);
    const auto hits = [&](std::vector<irs::HnswCandidate> nearest) {
      std::ranges::sort(nearest, [](const auto& l, const auto& r) {
        return l.score > r.score;
      });
      nearest.resize(std::min(nearest.size(), kK));
      size_t hit = 0;
      for (const auto& c : nearest) {
        EXPECT_NE(admitted[c.node], 0) << "node " << c.node;
        hit += std::ranges::any_of(
                 truth, [&](const auto& t) { return t.second == c.node; })
                 ? 1
                 : 0;
      }
      return hit;
    };
    irs::HnswSearchTopK<Walk>(graph, dist, kEf, s,
                              [&](uint32_t n) { return admitted[n] != 0; });
    found_plain += hits(s.nearest);
    std::vector<uint8_t> handed(kNodes, 0);
    size_t unhanded = 0;
    irs::HnswSearchTopK<Walk>(graph, dist, kEf, s,
                              HopAccept{&admitted, &handed, &unhanded});
    EXPECT_LE(unhanded, 1U) << "query " << qi;
    found_hops += hits(s.nearest);
  }
  const double plain =
    static_cast<double>(found_plain) / static_cast<double>(kQueries * kK);
  const double hops =
    static_cast<double>(found_hops) / static_cast<double>(kQueries * kK);
  EXPECT_GE(hops, plain - 0.02) << "plain " << plain << " hops " << hops;
}

}  // namespace

TEST(HnswWalkTest, TwoHopAsksTheAcceptorPerHop) {
  CheckWalkAsksPerHop<irs::HnswWalk::TwoHop>();
}

TEST(HnswWalkTest, PruneAsksTheAcceptorPerHop) {
  CheckWalkAsksPerHop<irs::HnswWalk::Prune>();
}

TEST(HnswWalkTest, BridgeAsksTheAcceptorPerHop) {
  CheckWalkAsksPerHop<irs::HnswWalk::Bridge>();
}

TEST(HnswWalkTest, AdaptiveCrossesOverWhereThePredicateAdmitsNothingNearby) {
  constexpr uint32_t kD = 8;
  constexpr uint32_t kNodes = 3000;
  constexpr uint32_t kM = 16;
  constexpr uint32_t kEf = 64;
  constexpr size_t kK = 10;
  constexpr size_t kQueries = 30;
  pcg32 rng{20261015};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> pts(size_t{kNodes} * kD);
  for (float& v : pts) {
    v = nd(rng);
  }
  irs::HnswGraphWriter writer;
  writer.Reset(kNodes, kM);
  pcg32 levels{irs::kHnswBuildSeed};
  uint32_t entry = 0;
  for (uint32_t i = 0; i < kNodes; ++i) {
    writer.SetLevel(i, irs::HnswRandomLevel(levels, kM));
    if (writer.LevelOf(i) > writer.LevelOf(entry)) {
      entry = i;
    }
  }
  writer.AllocateLinks();
  writer.SetEntryPoint(entry);
  WalkDist build{.pts = &pts, .d = kD};
  irs::HnswBuildScratch scratch;
  scratch.search.visited.Reset(kNodes);
  for (uint32_t i = 0; i < kNodes; ++i) {
    if (i != entry) {
      build.SetQuery(i);
      irs::HnswInsert(writer, i, build, kEf, scratch);
    }
  }
  const auto& graph = writer.Graph();

  std::vector<float> query(kD);
  WalkDist dist{.pts = &pts, .d = kD, .q = query.data()};
  irs::HnswSearchScratch s;
  s.admit_share = 0.5;
  size_t crossed_random = 0;
  size_t crossed_far = 0;
  size_t found_adaptive = 0;
  size_t found_two_hop = 0;
  for (size_t qi = 0; qi < kQueries; ++qi) {
    for (float& v : query) {
      v = nd(rng);
    }
    std::vector<uint8_t> random(kNodes);
    for (auto& a : random) {
      a = rng() % 2 == 0 ? 1 : 0;
    }
    irs::HnswSearchTopK<irs::HnswWalk::Adaptive>(
      graph, dist, kEf, s, [&](uint32_t n) { return random[n] != 0; });
    crossed_random += s.two_hop ? 1 : 0;
    for (const auto& c : s.nearest) {
      EXPECT_NE(random[c.node], 0) << "node " << c.node;
    }

    std::vector<std::pair<irs::score_t, uint32_t>> by_score;
    for (uint32_t n = 0; n < kNodes; ++n) {
      by_score.emplace_back(dist.One(n), n);
    }
    std::ranges::sort(by_score);
    std::vector<uint8_t> far(kNodes, 0);
    for (size_t i = 0; i < kNodes / 2; ++i) {
      far[by_score[i].second] = 1;
    }
    std::vector<uint32_t> truth;
    for (size_t i = kNodes / 2; i-- > kNodes / 2 - kK;) {
      truth.push_back(by_score[i].second);
    }
    const auto hits = [&] {
      size_t hit = 0;
      for (const auto& c : s.nearest) {
        EXPECT_NE(far[c.node], 0) << "node " << c.node;
        hit += std::ranges::find(truth, c.node) != truth.end() ? 1 : 0;
      }
      return hit;
    };
    irs::HnswSearchTopK<irs::HnswWalk::Adaptive>(
      graph, dist, kEf, s, [&](uint32_t n) { return far[n] != 0; });
    crossed_far += s.two_hop ? 1 : 0;
    found_adaptive += hits();
    const auto adaptive_scored = s.scored;
    irs::HnswSearchTopK<irs::HnswWalk::TwoHop>(
      graph, dist, kEf, s, [&](uint32_t n) { return far[n] != 0; });
    found_two_hop += hits();
    EXPECT_EQ(adaptive_scored, s.scored) << "query " << qi;
  }
  EXPECT_EQ(crossed_random, 0U);
  EXPECT_EQ(crossed_far, kQueries);
  EXPECT_GE(found_adaptive + kQueries, found_two_hop)
    << "adaptive " << found_adaptive << " twohop " << found_two_hop;
}

class HnswBuildWorkersTest
  : public ::testing::TestWithParam<irs::AnnInfo::Quantizer> {};

TEST_P(HnswBuildWorkersTest, BuildTakesWorkersAsTheyFreeUp) {
  constexpr size_t kRows = 6000;
  constexpr size_t kK = 10;
  const auto quant = GetParam();
  const auto vecs = MakeVectors(kRows, 11);

  auto pool = yaclib::MakeFairThreadPool(4);
  std::atomic<uint32_t> held{0};
  std::atomic<uint32_t> top_ups{0};
  const auto acquire = [&](uint32_t want, uint32_t have) -> uint32_t {
    const uint32_t grant = have == 0 ? 1U : std::min(want, 3U);
    if (have != 0 && grant != 0) {
      top_ups.fetch_add(1);
    }
    held.fetch_add(grant);
    return grant;
  };
  const auto release = [&](uint32_t n) { held.fetch_sub(n); };
  const irs::AnnBuildEnv env{
    .executor = pool.Get(), .acquire = acquire, .release = release};

  auto opts = irs::tests::DefaultWriterOptions();
  opts.column_options = [quant](irs::field_id id) -> irs::ColumnOptions {
    irs::ColumnOptions col;
    if (id == kVec) {
      col.ann_info = irs::AnnInfo{
        .kind = irs::AnnKind::Hnsw,
        .centroids_id = kVec,
        .postings_id = kVec,
        .d = kDim,
        .metric = irs::VectorMetric::L2Sqr,
        .quant = quant,
        .m = 8,
        .ef_construction = 64,
      };
    }
    return col;
  };
  opts.ann_env = &env;

  irs::MemoryDirectory dir;
  auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate, std::move(opts));
  ASSERT_NE(nullptr, writer);
  {
    auto trx = writer->GetBatch();
    for (const auto& vec : vecs) {
      auto doc = trx.Insert();
      WriteVectorAt(*doc.GetColWriter(), doc.DocId(), vec);
    }
    trx.Commit();
  }
  writer->RefreshCommit();
  const auto reader = writer->GetSnapshot();
  ASSERT_EQ(kRows, reader->docs_count());
  EXPECT_GT(top_ups.load(), 0U);
  EXPECT_EQ(held.load(), 0U);

  size_t total_hits = 0;
  size_t matched = 0;
  for (const auto& q : MakeVectors(20, 99)) {
    auto filter = MakeKnnFilter(q, irs::VectorMetric::L2Sqr, quant.kind, 128);
    const auto got = RunKnn(reader, filter);
    for (const auto doc :
         BruteForceTopK(vecs, q, irs::VectorMetric::L2Sqr, kK)) {
      ++total_hits;
      matched += std::ranges::find(got, doc) != got.end() ? 1 : 0;
    }
  }
  EXPECT_GE(static_cast<double>(matched) / static_cast<double>(total_hits),
            0.95);
  pool->SoftStop();
  pool->Wait();
}

INSTANTIATE_TEST_SUITE_P(
  Quants, HnswBuildWorkersTest,
  ::testing::Values(
    irs::AnnInfo::Quantizer{.kind = irs::VectorQuantization::None},
    irs::AnnInfo::Quantizer{.kind = irs::VectorQuantization::PQ, .pq_m = 4}));

TEST(HnswPqGraphTest, CoarseCodesLinkNodesAsTheVectorsDo) {
  constexpr uint32_t kD = 64;
  constexpr size_t kRows = 3000;
  const auto vecs = MakeVectors(kRows, 17, kD);
  const auto link_quality = [&](irs::AnnInfo::Quantizer quant) {
    auto opts = irs::tests::DefaultWriterOptions();
    opts.column_options = [quant](irs::field_id id) -> irs::ColumnOptions {
      irs::ColumnOptions col;
      if (id == kVec) {
        col.ann_info = irs::AnnInfo{
          .kind = irs::AnnKind::Hnsw,
          .centroids_id = kVec,
          .postings_id = kVec,
          .d = static_cast<int>(kD),
          .metric = irs::VectorMetric::L2Sqr,
          .quant = quant,
          .m = 8,
          .ef_construction = 64,
        };
      }
      return col;
    };
    irs::MemoryDirectory dir;
    auto writer = irs::IndexWriter::Make(dir, irs::kOmCreate, std::move(opts));
    EXPECT_NE(nullptr, writer);
    AppendBatch(*writer, vecs);
    const auto reader = writer->GetSnapshot();
    EXPECT_EQ(1U, reader->size());
    const auto& segment = (*reader)[0];
    const auto* ann = static_cast<const irs::HnswIndex*>(segment.Ann(kVec));
    const auto data = ann->Load(segment);
    const auto& graph = data->graph;
    size_t links = 0;
    size_t near = 0;
    for (uint32_t node = 0; node < kRows; ++node) {
      const auto want = BruteForceTopK(
        vecs, vecs[node], irs::VectorMetric::L2Sqr, size_t{2} * graph.M0() + 1);
      for (const auto peer : graph.Neighbors(node, 0)) {
        if (peer == irs::kHnswInvalidNode) {
          continue;
        }
        ++links;
        const auto doc =
          static_cast<irs::doc_id_t>(peer) + irs::doc_limits::min();
        near += std::ranges::find(want, doc) != want.end() ? 1 : 0;
      }
    }
    return static_cast<double>(near) / static_cast<double>(links);
  };
  const double exact = link_quality({.kind = irs::VectorQuantization::None});
  const double coarse =
    link_quality({.kind = irs::VectorQuantization::PQ, .pq_m = 4});
  EXPECT_GE(coarse, exact - 0.02) << "unquantized " << exact;
}
