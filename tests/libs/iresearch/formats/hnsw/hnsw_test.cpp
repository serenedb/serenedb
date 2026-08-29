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

#include <duckdb.hpp>
#include <duckdb/common/vector/array_vector.hpp>

#include <algorithm>
#include <random>
#include <tuple>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/utils/index_utils.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/vector_similarity_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "search/filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kVec = 1;
inline constexpr uint32_t kDim = 8;

irs::IndexWriterOptions MakeWriterOptions(irs::VectorMetric metric,
                                          irs::VectorQuantization quant) {
  auto opts = irs::tests::DefaultWriterOptions();
  opts.column_options = [metric, quant](irs::field_id id) -> irs::ColumnOptions {
    irs::ColumnOptions col;
    if (id == kVec) {
      col.ann_info = irs::AnnInfo{
        .kind = irs::AnnKind::Hnsw,
        .centroids_id = kVec,
        .postings_id = kVec,
        .d = kDim,
        .metric = metric,
        .quant = {.kind = quant},
        .m = 8,
        .ef_construction = 64,
      };
    }
    return col;
  };
  return opts;
}

std::vector<std::vector<float>> MakeVectors(size_t n, uint32_t seed) {
  std::mt19937 rng{seed};
  std::uniform_real_distribution<float> dist{-1.f, 1.f};
  std::vector<std::vector<float>> out;
  out.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    std::vector<float> v(kDim);
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
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);
  auto& cw = cs.OpenColumn(kVec, vtype);
  duckdb::Vector v{vtype, 1};
  auto& child = duckdb::ArrayVector::GetChildMutable(v);
  auto* data = duckdb::FlatVector::GetDataMutable<float>(child);
  std::ranges::copy(vec, data);
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  duckdb::FlatVector::ValidityMutable(child).SetAllValid(kDim);
  duckdb::FlatVector::SetSize(v, 1);
  cw.Append(static_cast<uint64_t>(doc) - irs::doc_limits::min(), v,
            /*count=*/1);
}

irs::DirectoryReader BuildIndex(irs::Directory& dir,
                                const std::vector<std::vector<float>>& vecs,
                                irs::VectorMetric metric,
                                irs::VectorQuantization quant) {
  constexpr auto kFormatId = "1_5simd";
  auto codec = irs::formats::Get(kFormatId);
  EXPECT_NE(nullptr, codec);
  auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                       MakeWriterOptions(metric, quant));
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
                                  static_cast<uint16_t>(kDim));
    });
    scored.emplace_back(s, static_cast<irs::doc_id_t>(i) +
                             irs::doc_limits::min());
  }
  std::ranges::sort(scored, [](const auto& l, const auto& r) {
    return l.first > r.first;
  });
  std::vector<irs::doc_id_t> out;
  for (size_t i = 0; i < std::min(k, scored.size()); ++i) {
    out.push_back(scored[i].second);
  }
  return out;
}

std::vector<irs::doc_id_t> RunKnn(const irs::DirectoryReader& reader,
                                  irs::ByVectorSimilarity& filter) {
  ::tests::PreparedFilter prepared{
    filter, *reader, nullptr, irs::IResourceManager::gNoop, nullptr,
    ::tests::PreparedFilter::CollectMode::Single};
  EXPECT_EQ(1U, prepared.size());
  auto it = prepared.Execute(0);
  EXPECT_NE(nullptr, it);
  std::vector<irs::doc_id_t> docs;
  while (!irs::doc_limits::eof(it->advance())) {
    docs.push_back(it->value());
  }
  return docs;
}

using HnswParam = std::tuple<irs::VectorMetric, irs::VectorQuantization>;

class HnswIndexTest : public ::testing::TestWithParam<HnswParam> {
 protected:
  irs::VectorMetric Metric() const { return std::get<0>(GetParam()); }
  irs::VectorQuantization Quant() const { return std::get<1>(GetParam()); }

  void SetUp() override {
    if (Quant() != irs::VectorQuantization::None &&
        Metric() == irs::VectorMetric::L1) {
      GTEST_SKIP() << "l1 supports only quant = none";
    }
  }

  double RecallFloor() const {
    return Quant() == irs::VectorQuantization::SQ4 ? 0.7 : 0.9;
  }
};

TEST_P(HnswIndexTest, RecallAgainstBruteForce) {
  const auto metric = Metric();
  constexpr size_t kRows = 500;
  constexpr size_t kK = 10;

  const auto vecs = MakeVectors(kRows, 7);
  irs::MemoryDirectory dir;
  auto reader = BuildIndex(dir, vecs, metric, Quant());
  ASSERT_NE(nullptr, reader);
  ASSERT_EQ(1U, reader->size());
  ASSERT_EQ(kRows, reader->docs_count());

  const auto* ann = (*reader)[0].Ann(kVec);
  ASSERT_NE(nullptr, ann);
  ASSERT_EQ(irs::AnnKind::Hnsw, ann->Kind());
  ASSERT_EQ(kDim, ann->Dim());
  ASSERT_FALSE(ann->Empty());
  ASSERT_FALSE(ann->SupportsFilter());

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
  auto reader = BuildIndex(dir, vecs, metric, Quant());
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
  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);

  irs::MemoryDirectory dir;
  auto writer = irs::IndexWriter::Make(dir, codec, irs::kOmCreate,
                                       MakeWriterOptions(metric, Quant()));
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
  auto fresh = BuildIndex(fresh_dir, vecs, metric, Quant());
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
  EXPECT_GE(recall_merged, recall_fresh - 0.05)
    << "merged " << recall_merged << " vs rebuilt " << recall_fresh;
}

TEST_P(HnswIndexTest, SerializedGraphSurvivesReopen) {
  const auto metric = Metric();
  const auto vecs = MakeVectors(200, 11);
  irs::MemoryDirectory dir;
  auto reader = BuildIndex(dir, vecs, metric, Quant());
  ASSERT_NE(nullptr, reader);

  auto filter = MakeKnnFilter(vecs[0], metric, Quant(), 64);
  const auto first = RunKnn(reader, filter);
  ASSERT_FALSE(first.empty());

  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);
  auto reopened =
    irs::DirectoryReader{dir, codec, irs::tests::DefaultReaderOptions()};
  ASSERT_NE(nullptr, reopened);
  auto filter2 = MakeKnnFilter(vecs[0], metric, Quant(), 64);
  const auto second = RunKnn(reopened, filter2);
  EXPECT_EQ(first, second);
}

TEST_P(HnswIndexTest, QueryVectorFindsItself) {
  const auto metric = Metric();
  const auto vecs = MakeVectors(300, 13);
  irs::MemoryDirectory dir;
  auto reader = BuildIndex(dir, vecs, metric, Quant());
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
  ::testing::Combine(::testing::Values(irs::VectorMetric::L2Sqr,
                                       irs::VectorMetric::InnerProduct,
                                       irs::VectorMetric::Cosine,
                                       irs::VectorMetric::L1),
                     ::testing::Values(irs::VectorQuantization::None,
                                       irs::VectorQuantization::SQ8,
                                       irs::VectorQuantization::SQ4)));

}  // namespace
