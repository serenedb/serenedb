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

#include <algorithm>
#include <core_functions/array_kernels.hpp>
#include <deque>
#include <duckdb.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <iresearch/formats/ivf/ivf_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/ann/batch.hpp>
#include <iresearch/search/ann/family.hpp>
#include <iresearch/search/detail/ann_index.hpp>
#include <iresearch/search/detail/column_collector.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/filters/vector_similarity_filter.hpp>
#include <iresearch/search/scorers/vector_similarity_scorer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <random>
#include <string>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "tests_shared.hpp"

namespace {

constexpr irs::field_id kVec = 1;
constexpr irs::field_id kTag = 2;
constexpr uint32_t kDim = 16;
constexpr uint32_t kRows = 3000;
constexpr uint32_t kQueries = 40;
constexpr uint32_t kTopK = 10;
constexpr uint32_t kNprobe = 6;

struct BatchCase {
  irs::AnnKind kind;
  irs::VectorMetric metric;
  irs::AnnInfo::Quantizer quant;
  float rescore_width;
};

std::string CaseName(const ::testing::TestParamInfo<BatchCase>& info) {
  const auto& c = info.param;
  return absl::StrCat(magic_enum::enum_name(c.kind), "_",
                      magic_enum::enum_name(c.metric), "_",
                      magic_enum::enum_name(c.quant.kind), c.quant.nb_bits,
                      c.rescore_width > 0.f ? "_rescore" : "");
}

std::vector<float> MakeVectors(size_t n, uint32_t seed) {
  std::mt19937 rng{seed};
  std::uniform_real_distribution<float> dist{-1.f, 1.f};
  std::vector<float> out(n * kDim);
  for (auto& x : out) {
    x = dist(rng);
  }
  return out;
}

void WriteVectorAt(irs::ColWriter& cs, irs::doc_id_t doc, const float* vec) {
  const auto vtype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);
  auto& cw = cs.OpenColumn(kVec, vtype);
  duckdb::Vector v{vtype, 1};
  auto& child = duckdb::ArrayVector::GetChildMutable(v);
  std::copy_n(vec, kDim, duckdb::FlatVector::GetDataMutable<float>(child));
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  duckdb::FlatVector::ValidityMutable(child).SetAllValid(kDim);
  duckdb::FlatVector::SetSize(v, 1);
  cw.Append(static_cast<uint64_t>(doc) - irs::doc_limits::min(), v, 1);
}

using Hits = std::vector<irs::ScoreDoc>;

Hits TopK(std::span<irs::ScoreDoc> hits) {
  Hits out{hits.begin(), hits.end()};
  absl::c_sort(out, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return l.score != r.score ? l.score > r.score : l.doc < r.doc;
  });
  out.resize(std::min<size_t>(out.size(), kTopK));
  return out;
}

class AnnBatchTest : public ::testing::TestWithParam<BatchCase> {
 protected:
  void SetUp() final {
    const auto& c = GetParam();
    auto opts = irs::tests::DefaultWriterOptions();
    const irs::AnnInfo ann{
      .kind = c.kind,
      .index_id = kVec,
      .column_id = kVec,
      .d = static_cast<int>(kDim),
      .metric = c.metric,
      .quant = c.quant,
      .sample_factor = 1.f,
      .posting_size = 64,
      .m = 8,
      .ef_construction = 64,
    };
    opts.anns[kVec].push_back(kVec);
    opts.column_options = [ann](irs::field_id id) {
      irs::ColumnOptions col;
      if (id == kVec) {
        col.ann_info = ann;
      }
      return col;
    };
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(opts));
    ASSERT_NE(nullptr, _writer);
    const auto rows = MakeVectors(kRows, 7);
    {
      auto trx = _writer->GetBatch();
      const duckdb::string_t even{"even", 4};
      const duckdb::string_t odd{"odd", 3};
      for (uint32_t i = 0; i < kRows; ++i) {
        auto doc = trx.Insert();
        const auto d = doc.DocId();
        EXPECT_TRUE(doc.WithField(kTag, irs::StringLayout::kStrings,
                                  irs::IndexFeatures::Freq,
                                  [&](irs::FieldInverter& fld) {
                                    return fld.InvertKeywords([&](auto&& emit) {
                                      emit(d % 2 == 0 ? even : odd, d);
                                    });
                                  }));
        WriteVectorAt(*doc.GetColWriter(), d, rows.data() + i * kDim);
      }
      trx.Commit();
    }
    _writer->RefreshCommit();
    _reader = _writer->GetSnapshot();
    ASSERT_EQ(1U, _reader.size());
    if (c.kind == irs::AnnKind::Ivf) {
      const auto* ivf = static_cast<const irs::IvfIndex*>(_reader[0].Ann(kVec));
      ASSERT_NE(nullptr, ivf);
      ASSERT_GT(ivf->Tree().Levels(), 1U);
    }
    _queries = MakeVectors(kQueries, 11);
  }

  irs::ByVectorSimilarityOptions Options(std::span<const float> query) const {
    const auto& c = GetParam();
    irs::ByVectorSimilarityOptions opts;
    opts.query.assign(query.begin(), query.end());
    opts.column_id = kVec;
    opts.ann_id = kVec;
    opts.metric = c.metric;
    opts.quant = c.quant.kind;
    opts.nprobe = kNprobe;
    opts.ef_search = 64;
    opts.min_ef = kTopK;
    opts.top_k = kTopK;
    opts.posting_size = 64;
    opts.rescore_width = c.rescore_width;
    opts.inner = _inner;
    return opts;
  }

  void RemoveEveryThird() {
    irs::DocRemovalBuilder removal;
    for (irs::doc_id_t doc = irs::doc_limits::min();
         doc < irs::doc_limits::min() + kRows; ++doc) {
      if (doc % 3 == 0) {
        removal.Add(0, doc);
      }
    }
    auto trx = _writer->GetBatch();
    trx.Remove(std::move(removal).Finish(_reader));
    trx.Commit();
    _writer->RefreshCommit();
    _reader = _writer->GetSnapshot();
    ASSERT_EQ(1U, _reader.size());
    ASSERT_LT(_reader[0].live_docs_count(), _reader[0].docs_count());
  }

  Hits Single(std::span<const float> query) const {
    irs::ByVectorSimilarity filter;
    *filter.mutable_field_id() = kVec;
    static_cast<irs::ByVectorSimilarityOptions&>(*filter.mutable_options()) =
      Options(query);
    const auto& segment = _reader[0];
    const auto prepared = filter.PrepareSegment(segment, {});
    irs::ann::SharedFloor floor{kTopK};
    irs::ann::Collector collector{floor, kTopK};
    const irs::VectorSimilarityScorer scorer;
    irs::ColumnArgsFetcher fetcher;
    auto root = irs::ann::MakeRoot(
      *prepared, {.scorer = scorer, .fetcher = fetcher, .k = kTopK});
    EXPECT_NE(nullptr, root);
    root->Run(collector);
    return TopK(collector.Flush());
  }

  std::span<const float> Query(uint32_t q) const {
    return std::span<const float>{_queries}.subspan(q * kDim, kDim);
  }

  std::vector<Hits> Batch(std::span<const float> queries) const {
    const auto& segment = _reader[0];
    const auto* ann = segment.Ann(kVec);
    EXPECT_NE(nullptr, ann);
    std::deque<irs::ann::SharedFloor> floors;
    std::vector<irs::ann::Collector> collectors;
    for (uint32_t q = 0; q < queries.size() / kDim; ++q) {
      collectors.emplace_back(floors.emplace_back(kTopK), kTopK);
    }
    const auto opts = Options({});
    std::vector<float> normalized{queries.begin(), queries.end()};
    if (opts.metric == irs::VectorMetric::Cosine) {
      for (size_t q = 0; q < normalized.size(); q += kDim) {
        duckdb::L2NormalizeOp::Operation(normalized.data() + q,
                                         normalized.data() + q, kDim);
      }
    }
    ann->SearchBatch(segment, {}, opts,
                     irs::ann::QueryBatch{.vectors = normalized, .d = kDim},
                     opts.nprobe, collectors);
    std::vector<Hits> out;
    for (auto& collector : collectors) {
      out.push_back(TopK(collector.Flush()));
    }
    return out;
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  irs::DirectoryReader _reader;
  std::vector<float> _queries;
  std::shared_ptr<const irs::Filter> _inner;
};

void ExpectSame(const Hits& expected, const Hits& actual, uint32_t q) {
  ASSERT_EQ(kTopK, expected.size()) << "query " << q;
  ASSERT_EQ(expected.size(), actual.size()) << "query " << q;
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(expected[i].doc, actual[i].doc) << "query " << q << " rank " << i;
    EXPECT_FLOAT_EQ(expected[i].score, actual[i].score)
      << "query " << q << " rank " << i;
  }
}

bool ScoresDependOnFloor(const BatchCase& c) {
  return c.quant.kind == irs::VectorQuantization::RaBitQ && c.quant.nb_bits > 1;
}

TEST_P(AnnBatchTest, BatchOfOneMatchesSingleQuery) {
  for (uint32_t q = 0; q < kQueries; ++q) {
    const auto batch = Batch(Query(q));
    ASSERT_EQ(1U, batch.size());
    ExpectSame(Single(Query(q)), batch[0], q);
  }
}

void ExpectMatches(const BatchCase& c, const Hits& single, const Hits& batch,
                   uint32_t q) {
  if (!ScoresDependOnFloor(c)) {
    ExpectSame(single, batch, q);
    return;
  }
  ASSERT_EQ(single.size(), batch.size()) << "query " << q;
  size_t shared = 0;
  for (const auto& hit : batch) {
    shared += absl::c_any_of(
      single, [&](const irs::ScoreDoc& other) { return other.doc == hit.doc; });
  }
  EXPECT_GE(shared * 10, single.size() * 9) << "query " << q;
}

TEST_P(AnnBatchTest, MatchesSingleQueries) {
  const auto batch = Batch(_queries);
  ASSERT_EQ(kQueries, batch.size());
  for (uint32_t q = 0; q < kQueries; ++q) {
    ExpectMatches(GetParam(), Single(Query(q)), batch[q], q);
  }
}

TEST_P(AnnBatchTest, FilteredMatchesSingleQueries) {
  auto even = std::make_shared<irs::ByTerm>();
  *even->mutable_field_id() = kTag;
  even->mutable_options()->term =
    irs::ViewCast<irs::byte_type>(std::string_view{"even"});
  _inner = std::move(even);
  const auto batch = Batch(_queries);
  ASSERT_EQ(kQueries, batch.size());
  for (uint32_t q = 0; q < kQueries; ++q) {
    for (const auto& hit : batch[q]) {
      EXPECT_EQ(0U, hit.doc % 2) << "query " << q;
    }
    ExpectMatches(GetParam(), Single(Query(q)), batch[q], q);
  }
}

TEST_P(AnnBatchTest, RemovalsMatchSingleQueries) {
  RemoveEveryThird();
  const auto batch = Batch(_queries);
  ASSERT_EQ(kQueries, batch.size());
  for (uint32_t q = 0; q < kQueries; ++q) {
    for (const auto& hit : batch[q]) {
      EXPECT_NE(0U, hit.doc % 3) << "query " << q;
    }
    ExpectMatches(GetParam(), Single(Query(q)), batch[q], q);
  }
}

using enum irs::VectorQuantization;
using enum irs::VectorMetric;

INSTANTIATE_TEST_SUITE_P(
  Ivf, AnnBatchTest,
  ::testing::Values(
    BatchCase{irs::AnnKind::Ivf, L2Sqr, {.kind = None}, 0.f},
    BatchCase{irs::AnnKind::Ivf, InnerProduct, {.kind = None}, 0.f},
    BatchCase{irs::AnnKind::Ivf, Cosine, {.kind = None}, 0.f},
    BatchCase{irs::AnnKind::Ivf, L1, {.kind = None}, 0.f},
    BatchCase{irs::AnnKind::Ivf, L2Sqr, {.kind = SQ8}, 0.f},
    BatchCase{irs::AnnKind::Ivf, L2Sqr, {.kind = SQ8}, 2.f},
    BatchCase{irs::AnnKind::Ivf, Cosine, {.kind = SQ8}, 2.f},
    BatchCase{irs::AnnKind::Ivf, InnerProduct, {.kind = SQ4}, 0.f},
    BatchCase{irs::AnnKind::Ivf, L2Sqr, {.kind = PQ, .pq_m = 4}, 2.f},
    BatchCase{irs::AnnKind::Ivf, InnerProduct, {.kind = PQ, .pq_m = 4}, 0.f},
    BatchCase{irs::AnnKind::Ivf, L2Sqr, {.kind = RaBitQ, .nb_bits = 1}, 2.f},
    BatchCase{
      irs::AnnKind::Ivf, InnerProduct, {.kind = RaBitQ, .nb_bits = 3}, 0.f},
    BatchCase{irs::AnnKind::Ivf, L2Sqr, {.kind = TQ, .nb_bits = 3}, 2.f},
    BatchCase{irs::AnnKind::Ivf, L2Sqr, {.kind = BQ, .nb_bits = 1}, 0.f},
    BatchCase{irs::AnnKind::Ivf, L2Sqr, {.kind = OSQ, .nb_bits = 4}, 2.f},
    BatchCase{irs::AnnKind::Ivf, L2Sqr, {.kind = FP16}, 0.f}),
  CaseName);

INSTANTIATE_TEST_SUITE_P(
  Hnsw, AnnBatchTest,
  ::testing::Values(
    BatchCase{irs::AnnKind::Hnsw, L2Sqr, {.kind = None}, 0.f},
    BatchCase{irs::AnnKind::Hnsw, InnerProduct, {.kind = None}, 0.f},
    BatchCase{irs::AnnKind::Hnsw, Cosine, {.kind = None}, 0.f},
    BatchCase{irs::AnnKind::Hnsw, L1, {.kind = None}, 0.f},
    BatchCase{irs::AnnKind::Hnsw, L2Sqr, {.kind = SQ8}, 2.f},
    BatchCase{irs::AnnKind::Hnsw, Cosine, {.kind = SQ8}, 2.f},
    BatchCase{irs::AnnKind::Hnsw, InnerProduct, {.kind = SQ4}, 0.f},
    BatchCase{irs::AnnKind::Hnsw, L2Sqr, {.kind = PQ, .pq_m = 4}, 2.f},
    BatchCase{irs::AnnKind::Hnsw, L2Sqr, {.kind = TQ, .nb_bits = 3}, 2.f},
    BatchCase{irs::AnnKind::Hnsw, L2Sqr, {.kind = BQ, .nb_bits = 1}, 0.f},
    BatchCase{irs::AnnKind::Hnsw, L2Sqr, {.kind = OSQ, .nb_bits = 4}, 2.f}),
  CaseName);

}  // namespace
