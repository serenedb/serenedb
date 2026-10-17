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
#include <atomic>
#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <iresearch/formats/ann_build_env.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/hnsw/hnsw_graph.hpp>
#include <iresearch/formats/hnsw/hnsw_reader.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/sort_spec.hpp>
#include <iresearch/search/detail/doc_collector.hpp>
#include <iresearch/search/filters/vector_similarity_filter.hpp>
#include <iresearch/search/scorers/vector_similarity_scorer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <memory>
#include <random>
#include <span>
#include <utility>
#include <vector>
#include <yaclib/runtime/fair_thread_pool.hpp>

#include "formats/column/test_cs_helpers.hpp"
#include "index/merge/support/parallel_merge.hpp"
#include "tests_shared.hpp"

namespace {

constexpr irs::field_id kVec = 1;
constexpr irs::field_id kKey = 2;
constexpr uint32_t kDim = 8;
constexpr size_t kK = 10;

using Vectors = std::vector<std::vector<float>>;

irs::IndexWriterOptions HnswWriterOptions(irs::SortSpec sort = {}) {
  auto opts = irs::tests::DefaultWriterOptions();
  opts.column_options = [](irs::field_id id) {
    irs::ColumnOptions col;
    if (id == kVec) {
      col.ann_info = irs::AnnInfo{
        .kind = irs::AnnKind::Hnsw,
        .centroids_id = kVec,
        .postings_id = kVec,
        .d = kDim,
        .metric = irs::VectorMetric::L2Sqr,
        .quant = {.kind = irs::VectorQuantization::None},
        .m = 8,
        .ef_construction = 64,
      };
    }
    return col;
  };
  opts.sort = std::move(sort);
  return opts;
}

Vectors MakeVectors(size_t n, uint32_t seed) {
  std::mt19937 rng{seed};
  std::uniform_real_distribution<float> dist{-1.f, 1.f};
  Vectors out(n, std::vector<float>(kDim));
  for (auto& v : out) {
    for (auto& x : v) {
      x = dist(rng);
    }
  }
  return out;
}

void WriteVector(irs::ColWriter& cs, irs::doc_id_t doc,
                 const std::vector<float>& vec) {
  const auto type =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);
  duckdb::Vector v{type, 1};
  auto& child = duckdb::ArrayVector::GetChildMutable(v);
  absl::c_copy(vec, duckdb::FlatVector::GetDataMutable<float>(child));
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  duckdb::FlatVector::ValidityMutable(child).SetAllValid(kDim);
  duckdb::FlatVector::SetSize(v, 1);
  cs.OpenColumn(kVec, type).Append(doc - irs::doc_limits::min(), v, 1);
}

void WriteKey(irs::ColWriter& cs, irs::doc_id_t doc, int64_t key) {
  duckdb::Vector v{duckdb::LogicalType::BIGINT, 1};
  duckdb::FlatVector::GetDataMutable<int64_t>(v)[0] = key;
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  duckdb::FlatVector::SetSize(v, 1);
  cs.OpenColumn(kKey, duckdb::LogicalType::BIGINT)
    .Append(doc - irs::doc_limits::min(), v, 1);
}

float Distance(const std::vector<float>& l, const std::vector<float>& r) {
  float sum = 0;
  for (uint32_t i = 0; i != kDim; ++i) {
    const auto diff = l[i] - r[i];
    sum += diff * diff;
  }
  return sum;
}

std::shared_ptr<const irs::HnswData> Graph(const irs::SubReader& segment) {
  const auto* ann = segment.Ann(kVec);
  EXPECT_NE(nullptr, ann);
  return irs::utils::downCast<const irs::HnswIndex>(*ann).Load(segment);
}

double NeighborDistanceRatio(const irs::HnswGraph& graph, const Vectors& vecs) {
  double linked = 0;
  size_t links = 0;
  double spread = 0;
  for (uint32_t node = 0; node != graph.Size(); ++node) {
    if (graph.LevelOf(node) == 0) {
      continue;
    }
    for (const auto neighbor : graph.Neighbors(node, 0)) {
      if (neighbor == irs::kHnswInvalidNode) {
        continue;
      }
      linked += Distance(vecs[node], vecs[neighbor]);
      ++links;
    }
    spread += Distance(vecs[node], vecs[(node * 7919 + 13) % vecs.size()]);
  }
  EXPECT_NE(0U, links);
  return linked / static_cast<double>(links) /
         (spread / static_cast<double>(graph.Size()));
}

double Recall(const irs::DirectoryReader& reader, const Vectors& vecs,
              const std::vector<bool>& present, uint32_t ef) {
  EXPECT_EQ(1U, reader.size());
  irs::VectorSimilarityScorer scorer;
  size_t matched = 0;
  size_t total = 0;
  for (const auto& query : MakeVectors(20, 4711)) {
    irs::ByVectorSimilarity filter;
    *filter.mutable_field_id() = kVec;
    auto& opts = *filter.mutable_options();
    opts.query = query;
    opts.centroids_id = kVec;
    opts.postings_id = kVec;
    opts.metric = irs::VectorMetric::L2Sqr;
    opts.quant = irs::VectorQuantization::None;
    opts.nprobe = ef;
    opts.ef_search = ef;
    std::vector<irs::ScoreDoc> hits(kK);
    const auto count =
      irs::ExecuteTopK(reader, filter, scorer, kK, false, std::span{hits});
    hits.resize(std::min<size_t>(count, kK));

    std::vector<std::pair<float, size_t>> scored;
    for (size_t row = 0; row != vecs.size(); ++row) {
      if (present[row]) {
        scored.emplace_back(Distance(query, vecs[row]), row);
      }
    }
    absl::c_partial_sort(scored, scored.begin() + kK);
    for (size_t i = 0; i != kK; ++i) {
      const auto doc =
        static_cast<irs::doc_id_t>(scored[i].second) + irs::doc_limits::min();
      ++total;
      matched += absl::c_any_of(
        hits, [doc](const irs::ScoreDoc& hit) { return hit.doc == doc; });
    }
  }
  return static_cast<double>(matched) / static_cast<double>(total);
}

void CompactAll(irs::IndexWriter& writer, const irs::AnnBuildEnv* env,
                irs::MergeExecutor* executor) {
  const auto result = irs::GetReady(writer.CompactAsync(
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount()), nullptr,
    {}, env, executor));
  ASSERT_TRUE(result);
  writer.RefreshCommit();
}

TEST(MergeAnnTest, SortedMergeBuildsTheGraphOverTheSortedRows) {
  constexpr size_t kRows = 2000;
  constexpr uint32_t kEf = 16;
  const auto vecs = MakeVectors(kRows, 53);
  const std::vector<bool> all(kRows, true);

  irs::MemoryDirectory dir;
  auto writer = irs::IndexWriter::Make(
    dir, irs::kOmCreate, HnswWriterOptions({.keys = {{.column = kKey}}}));
  ASSERT_NE(nullptr, writer);
  for (const size_t parity : {0, 1}) {
    auto trx = writer->GetBatch();
    for (size_t key = parity; key < kRows; key += 2) {
      auto doc = trx.Insert();
      WriteVector(*doc.GetColWriter(), doc.DocId(), vecs[key]);
      WriteKey(*doc.GetColWriter(), doc.DocId(), static_cast<int64_t>(key));
    }
    trx.Commit();
    writer->RefreshCommit();
  }
  ASSERT_EQ(2U, writer->GetSnapshot().size());
  CompactAll(*writer, nullptr, nullptr);
  const auto merged = writer->GetSnapshot();
  ASSERT_EQ(1U, merged.size());

  irs::MemoryDirectory fresh_dir;
  auto fresh_writer =
    irs::IndexWriter::Make(fresh_dir, irs::kOmCreate, HnswWriterOptions());
  ASSERT_NE(nullptr, fresh_writer);
  {
    auto trx = fresh_writer->GetBatch();
    for (const auto& vec : vecs) {
      auto doc = trx.Insert();
      WriteVector(*doc.GetColWriter(), doc.DocId(), vec);
    }
    trx.Commit();
    fresh_writer->RefreshCommit();
  }
  const auto fresh = fresh_writer->GetSnapshot();
  ASSERT_EQ(1U, fresh.size());

  const auto merged_graph = Graph(merged[0]);
  const auto fresh_graph = Graph(fresh[0]);
  ASSERT_NE(nullptr, merged_graph);
  ASSERT_NE(nullptr, fresh_graph);
  ASSERT_EQ(kRows, merged_graph->graph.Size());
  const auto merged_ratio = NeighborDistanceRatio(merged_graph->graph, vecs);
  const auto fresh_ratio = NeighborDistanceRatio(fresh_graph->graph, vecs);
  EXPECT_LE(merged_ratio, fresh_ratio * 1.25)
    << "merged " << merged_ratio << " vs fresh " << fresh_ratio;
  const auto merged_recall = Recall(merged, vecs, all, kEf);
  const auto fresh_recall = Recall(fresh, vecs, all, kEf);
  EXPECT_GE(merged_recall, fresh_recall - 0.05)
    << "merged " << merged_recall << " vs fresh " << fresh_recall;
}

TEST(MergeAnnTest, MergeUnitsBuildTheGraphOnAnnWorkers) {
  constexpr size_t kBig = 6000;
  constexpr size_t kSmall = 3000;
  constexpr size_t kNullEvery = 7;
  const auto vecs = MakeVectors(kBig + kSmall, 61);
  std::vector<bool> present(vecs.size());
  for (size_t row = 0; row != vecs.size(); ++row) {
    present[row] = row % kNullEvery != 3;
  }

  irs::MemoryDirectory dir;
  auto writer =
    irs::IndexWriter::Make(dir, irs::kOmCreate, HnswWriterOptions());
  ASSERT_NE(nullptr, writer);
  for (const auto [begin, end] :
       {std::pair{size_t{0}, kBig}, std::pair{kBig, kBig + kSmall}}) {
    auto trx = writer->GetBatch();
    for (size_t row = begin; row != end; ++row) {
      auto doc = trx.Insert();
      if (present[row]) {
        WriteVector(*doc.GetColWriter(), doc.DocId(), vecs[row]);
      }
    }
    trx.Commit();
    writer->RefreshCommit();
  }
  ASSERT_EQ(2U, writer->GetSnapshot().size());

  auto pool = yaclib::MakeFairThreadPool(3);
  std::atomic<uint32_t> held{0};
  std::atomic<uint32_t> shared_builds{0};
  const auto acquire = [&](uint32_t want) {
    const auto grant = std::min(want, 4U);
    held += grant;
    shared_builds += grant > 1 ? 1 : 0;
    return grant;
  };
  const auto release = [&](uint32_t workers) { held -= workers; };
  const irs::AnnBuildEnv env{
    .executor = pool.Get(), .acquire = acquire, .release = release};
  tests::merge::ThreadedMergeExecutor executor{4};
  CompactAll(*writer, &env, &executor);
  pool->SoftStop();
  pool->Wait();
  EXPECT_EQ(0U, held.load());
  EXPECT_NE(0U, shared_builds.load());

  const auto merged = writer->GetSnapshot();
  ASSERT_EQ(1U, merged.size());
  const auto data = Graph(merged[0]);
  ASSERT_NE(nullptr, data);
  for (size_t row = 0; row != vecs.size(); ++row) {
    EXPECT_EQ(present[row],
              data->graph.LevelOf(static_cast<uint32_t>(row)) != 0)
      << row;
  }
  EXPECT_GE(Recall(merged, vecs, present, 128), 0.9);
}

}  // namespace
