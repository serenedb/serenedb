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
#include <array>
#include <duckdb.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <memory>
#include <optional>
#include <random>
#include <span>
#include <string>
#include <tuple>
#include <vector>

#include "basics/down_cast.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/index/term_dict.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/ivf_writer.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/formats/seek_cookie.hpp"
#include "iresearch/search/vector_filter_util.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "tests_shared.hpp"

namespace {

constexpr std::string_view kCodec = "1_5simd";
constexpr std::string_view kSegment = "ivf_rg_segment";
// The vector column, the centroid tree and the cluster postings all carry the
// same id in production (`InvertedIndex::GetIvfInfo`).
constexpr irs::field_id kVec = 1;
constexpr uint32_t kDim = 8;
constexpr uint32_t kColRowGroup = 4096;
constexpr uint32_t kPostingSize = 100;

// Well-separated blobs with a deterministic jitter, laid out so consecutive
// rows land in different clusters: every cluster then spreads over every row
// group, which is what makes a term's postings split into many runs.
std::vector<float> MakeVectors(size_t rows, size_t blobs) {
  std::mt19937 rng{20260730};
  std::uniform_real_distribution<float> jitter{-0.35f, 0.35f};
  std::vector<float> data(rows * kDim);
  for (size_t g = 0; g < rows; ++g) {
    const size_t blob = g % blobs;
    for (uint32_t j = 0; j < kDim; ++j) {
      const auto center = static_cast<float>((blob >> j) & 1U) * 100.f;
      data[g * kDim + j] = center + jitter(rng);
    }
  }
  return data;
}

irs::IvfInfo MakeInfo(irs::VectorQuantization quant, uint32_t pq_m,
                      uint32_t nb_bits) {
  return irs::IvfInfo{
    .centroids_id = kVec,
    .postings_id = kVec,
    .d = static_cast<int>(kDim),
    .metric = irs::VectorMetric::L2Sqr,
    .quant = {.kind = quant, .pq_m = pq_m, .nb_bits = nb_bits},
    .posting_size = kPostingSize,
  };
}

void WriteVectorColumn(irs::ColWriter& cw, std::span<const float> data) {
  const auto atype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);
  const uint64_t rows = data.size() / kDim;
  auto& col = cw.OpenColumn(kVec, atype, /*skip_validity=*/false, kColRowGroup);
  uint64_t pos = 0;
  while (pos < rows) {
    const auto take = std::min<duckdb::idx_t>(rows - pos, STANDARD_VECTOR_SIZE);
    duckdb::Vector v{atype, STANDARD_VECTOR_SIZE};
    auto& child = duckdb::ArrayVector::GetChildMutable(v);
    auto* cd = duckdb::FlatVector::GetDataMutable<float>(child);
    duckdb::FlatVector::ValidityMutable(v).Reset(STANDARD_VECTOR_SIZE);
    duckdb::FlatVector::ValidityMutable(child).Reset(STANDARD_VECTOR_SIZE *
                                                     kDim);
    for (duckdb::idx_t k = 0; k < take; ++k) {
      const auto* src = data.data() + (pos + k) * kDim;
      std::copy_n(src, kDim, cd + k * kDim);
    }
    duckdb::FlatVector::SetSize(v, take);
    col.Append(v, take);
    pos += take;
  }
}

// One segment: the vector column, the IVF centroid tree and the quantized
// cluster postings, written at the given `row_group_size`. The oracle is the
// same writer at `kRowGroupSizeUnbounded`: one row group spans the segment, so
// the payload writer sees one `Write` per term and gathers at row 0 -- the two
// things partitioning changes, and both bugs this gate exists for.
void WriteSegment(irs::Directory& dir, duckdb::DatabaseInstance& db,
                  std::span<const float> data, const irs::IvfInfo& info,
                  uint32_t row_group_size) {
  auto codec = irs::formats::Get(kCodec);
  ASSERT_NE(nullptr, codec);
  const uint64_t rows = data.size() / kDim;

  irs::IdxWriter idx{dir, kSegment, db};
  std::vector<std::unique_ptr<irs::IvfWriter>> ivf_writers;
  {
    irs::ColWriter cw{dir, kSegment, db};
    WriteVectorColumn(cw, data);
    cw.AttachIVF(kVec, info);
    cw.SetIdxWriter(idx);
    cw.Commit(rows);
    ivf_writers = cw.TakeIvfWriters();
  }
  ASSERT_EQ(1, ivf_writers.size());

  irs::ColReader col_reader{dir, kSegment, db};
  std::optional<irs::ReadContext> ivf_ctx;
  const auto cluster_readers =
    irs::PrepareIvfClusterReaders(ivf_writers, &col_reader, ivf_ctx);
  ASSERT_EQ(1, cluster_readers.size());

  irs::FlushState state{
    .dir = &dir,
    .name = kSegment,
    .doc_count = rows,
    .index_features = cluster_readers.front()->properties().index_features,
  };
  ASSERT_EQ(irs::IndexFeatures::Pay, state.index_features);

  const auto write = [&](auto& writer) {
    writer.SetIdxWriter(idx);
    writer.prepare(state);
    writer.write(*cluster_readers.front());
    writer.end();
  };
  irs::term_dict::FieldWriter writer{
    codec->get_postings_writer(/*compaction=*/false,
                               irs::IResourceManager::gNoop),
    /*compaction=*/false, irs::IResourceManager::gNoop};
  irs::term_dict::WriterOptions options;
  options.row_group_size = row_group_size;
  writer.SetOptions(options);
  write(writer);

  for (const auto& w : ivf_writers) {
    w->FlushTree();
  }
  idx.Commit();
}

// Keeps everything the returned readers point into alive.
template<typename Reader>
struct Opened {
  irs::SegmentMeta meta;
  std::unique_ptr<irs::IdxReader> idx;
  std::unique_ptr<Reader> fields;

  const irs::TermReader* Field() const { return fields->field(kVec); }
  const irs::CentroidsTree* Ivf() const { return idx->Ivf(kVec); }
};

template<typename Reader>
Opened<Reader> Open(const irs::Directory& dir) {
  auto codec = irs::formats::Get(kCodec);
  Opened<Reader> opened;
  opened.meta.name = kSegment;
  opened.idx = std::make_unique<irs::IdxReader>(dir, kSegment);
  opened.fields = std::make_unique<Reader>(codec->get_postings_reader(),
                                           irs::IResourceManager::gNoop);
  opened.fields->prepare(irs::ReaderState{
    .dir = &dir, .meta = &opened.meta, .idx = opened.idx.get()});
  return opened;
}

// One document of a cluster, addressed the way the scan will address it.
struct Hit {
  uint32_t rg;
  irs::doc_id_t local;

  bool operator==(const Hit&) const = default;
};

// One posting list of a cluster: a whole cluster for the unpartitioned oracle,
// a (cluster, row group) run for the partitioned index.
struct Run {
  uint32_t rg;
  uint32_t docs_count;
};

struct ClusterRead {
  std::vector<Run> runs;
  std::vector<Hit> hits;
  // Parallel to `hits`: the distance that document's payload decodes to.
  std::vector<irs::score_t> dists;
  // The stream lane the cluster's first document's codes sit at.
  uint64_t first_lane = 0;
};

std::shared_ptr<const irs::QuantizerCodebook> MakeCodebook(
  const irs::IdxReader& idx, const irs::CentroidsTree& tree,
  irs::VectorQuantization quant, std::span<const float> query) {
  auto in = idx.ReopenIn();
  EXPECT_NE(nullptr, in);
  EXPECT_TRUE(tree.HasQuantStats());
  in->Seek(tree.QuantStatsOffset());
  const auto size = static_cast<size_t>(in->ReadI64());
  irs::bstring stats;
  stats.resize(size);
  in->ReadData(stats.data(), size);
  auto quant_stats =
    irs::MakeQuantizerStats(quant, kDim, stats, irs::VectorMetric::L2Sqr);
  EXPECT_NE(nullptr, quant_stats);
  return quant_stats->MakeCodebook(query);
}

// Every cluster of the segment, probed the way `ByVectorSimilarity` probes:
// through the centroid tree, so the read side gets its centroids from where
// production gets them.
std::vector<uint32_t> AllClusters(const irs::IdxReader& idx,
                                  const irs::CentroidsTree& tree,
                                  std::span<const float> query,
                                  std::vector<float>& centroids) {
  auto in = idx.ReopenIn();
  EXPECT_NE(nullptr, in);
  std::vector<uint32_t> ids;
  tree.Search(query, *in, /*nprobe=*/std::numeric_limits<uint32_t>::max(), ids,
              &centroids);
  return ids;
}

// Reads one cluster: its runs and, per document, the (rg, local) address plus
// the distance its payload decodes to. `layout` is the partitioned index's row
// group directory; the unpartitioned side decomposes its segment-wide doc ids
// through it, which is the only legal direction (rebuilding a segment id from
// (rg, local) is what the id model forbids).
ClusterRead ReadCluster(const irs::TermReader& field,
                        const irs::CentroidsTree& tree,
                        const std::shared_ptr<const irs::QuantizerCodebook>& cb,
                        const irs::RowGroupLayout& layout, uint32_t cluster,
                        const float* centroid, bool partitioned) {
  ClusterRead out;
  auto terms = field.iterator(irs::SeekMode::NORMAL);
  EXPECT_NE(nullptr, terms);
  std::array<irs::byte_type, irs::kCentroidTermWidth> term_buf{};
  if (!irs::SeekClusterTerm(*terms, cluster, term_buf)) {
    return out;
  }
  const auto cookie = terms->cookie();
  EXPECT_FALSE(cookie.rgs.empty());

  const auto groups = cookie.RowGroups();
  out.runs.reserve(groups.size());
  for (const auto& group : groups) {
    out.runs.emplace_back(group.rg, group.docs_count);
  }
  out.first_lane = cookie.pay_start;

  auto pay_in = field.ReopenPayload();
  EXPECT_NE(nullptr, pay_in);
  auto qr =
    irs::MakeQuantizerReader(cb, std::move(pay_in), field.PayloadBase());
  EXPECT_NE(nullptr, qr);

  // The cluster is started once over its whole lane range, exactly as the scan
  // will: the refine pool of a multi-bit quantizer is cluster-scoped, so a
  // per-run StartCluster would pick a different candidate set.
  size_t cluster_docs = 0;
  for (const auto& run : out.runs) {
    cluster_docs += run.docs_count;
  }
  EXPECT_EQ(cookie.stats.docs_count, cluster_docs);
  qr->StartCluster(out.first_lane, cluster_docs, centroid);
  std::vector<irs::score_t> cluster_dists(cluster_docs, 0.f);
  if (cluster_docs != 0) {
    qr->ComputeBlock(0, cluster_docs, cluster_dists.data());
  }

  const irs::TermLeaf leaf{.cookie = &cookie, .field = field.meta()};
  size_t run_base = 0;
  for (const auto& run : out.runs) {
    auto docs = field.RowGroupIterator(irs::IndexFeatures::None, leaf,
                                       partitioned ? run.rg : 0);
    EXPECT_NE(nullptr, docs);
    if (!docs) {
      continue;
    }
    const std::span<const irs::score_t> dists{cluster_dists.data() + run_base,
                                              run.docs_count};
    run_base += run.docs_count;

    size_t i = 0;
    irs::doc_id_t doc;
    while (!irs::doc_limits::eof(doc = docs->advance())) {
      EXPECT_LT(i, dists.size());
      if (partitioned) {
        out.hits.emplace_back(run.rg, doc);
      } else {
        const auto row = doc - irs::doc_limits::min();
        const auto rg = row / layout.rows_per_group;
        out.hits.emplace_back(rg, doc - rg * layout.rows_per_group);
      }
      out.dists.push_back(dists[i]);
      ++i;
    }
    EXPECT_EQ(run.docs_count, i);
  }
  return out;
}

// The runs the oracle's hits imply: one per row group the cluster touches, in
// ascending order, each holding that row group's documents.
std::vector<Run> ExpectedRuns(std::span<const Hit> oracle_hits) {
  std::vector<Run> runs;
  for (const auto& hit : oracle_hits) {
    if (runs.empty() || runs.back().rg != hit.rg) {
      runs.emplace_back(hit.rg, 0);
    }
    ++runs.back().docs_count;
  }
  return runs;
}

uint64_t FileSize(const irs::Directory& dir, std::string_view ext) {
  uint64_t size = 0;
  EXPECT_TRUE(dir.length(size, absl::StrCat(kSegment, ".", ext)));
  return size;
}

irs::bstring ReadWhole(const irs::Directory& dir, std::string_view ext) {
  const std::string name = absl::StrCat(kSegment, ".", ext);
  irs::bstring out;
  out.resize(FileSize(dir, ext));
  auto in = dir.open(name, irs::IOAdvice::NORMAL);
  EXPECT_NE(nullptr, in);
  if (in) {
    in->ReadData(out.data(), out.size());
  }
  return out;
}

struct QuantCase {
  irs::VectorQuantization quant;
  uint32_t pq_m;
  uint32_t nb_bits;
  std::string_view name;
};

}  // namespace

class IvfRowGroupTest : public ::testing::TestWithParam<QuantCase> {};

// The equality gate for the quantized payload leg: the same corpus written by
// the unpartitioned writer and by the row-group partitioned one must hand the
// scan the same document at the same (rg, local) address with the same decoded
// distance -- which is only true if the payload writer stayed term-scoped
// across a term's several per-row-group `Write` calls (so each cluster keeps
// its own centroid) and fetched its source vectors at `rg_base + local`.
TEST_P(IvfRowGroupTest, partitioned_payload_matches_unpartitioned) {
  const auto& param = GetParam();
  constexpr size_t kRows = 2000;
  const auto data = MakeVectors(kRows, 16);
  const auto info = MakeInfo(param.quant, param.pq_m, param.nb_bits);

  duckdb::DuckDB db;
  irs::MemoryDirectory oracle_dir;
  WriteSegment(oracle_dir, *db.instance, data, info,
               irs::term_dict::kRowGroupSizeUnbounded);
  auto oracle = Open<irs::term_dict::FieldReader>(oracle_dir);

  // A row group size that divides the row count, one that does not (short last
  // group), and one that is not a multiple of the fast-scan block either, so
  // partial packs and odd run lengths are all covered.
  for (const uint32_t kRowGroupSize : {128u, 256u, 999u}) {
    irs::MemoryDirectory split_dir;
    WriteSegment(split_dir, *db.instance, data, info, kRowGroupSize);
    auto split = Open<irs::term_dict::FieldReader>(split_dir);
    const auto* oracle_field = oracle.Field();
    const auto* split_field = split.Field();
    ASSERT_NE(nullptr, oracle_field);
    ASSERT_NE(nullptr, split_field);
    ASSERT_EQ(oracle_field->size(), split_field->size());
    ASSERT_EQ(oracle_field->docs_count(), split_field->docs_count());

    const auto layout = split_field->RowGroups();
    ASSERT_EQ(kRowGroupSize, layout.rows_per_group);
    ASSERT_EQ(kRows, layout.segment_docs);
    ASSERT_EQ((kRows + kRowGroupSize - 1) / kRowGroupSize, layout.count);
    ASSERT_EQ(1, oracle_field->RowGroups().count);

    const std::vector<float> query(kDim, 50.f);
    const auto* oracle_tree = oracle.Ivf();
    const auto* split_tree = split.Ivf();
    ASSERT_NE(nullptr, oracle_tree);
    ASSERT_NE(nullptr, split_tree);
    auto oracle_cb =
      MakeCodebook(*oracle.idx, *oracle_tree, param.quant, query);
    auto split_cb = MakeCodebook(*split.idx, *split_tree, param.quant, query);
    ASSERT_NE(nullptr, oracle_cb);
    ASSERT_NE(nullptr, split_cb);

    std::vector<float> oracle_centroids;
    std::vector<float> split_centroids;
    const auto clusters =
      AllClusters(*oracle.idx, *oracle_tree, query, oracle_centroids);
    const auto split_clusters =
      AllClusters(*split.idx, *split_tree, query, split_centroids);
    ASSERT_FALSE(clusters.empty());
    ASSERT_EQ(clusters, split_clusters);
    ASSERT_EQ(oracle_centroids, split_centroids);
    ASSERT_EQ(clusters.size() * kDim, oracle_centroids.size());

    size_t total_docs = 0;
    size_t total_runs = 0;
    size_t split_clusters_seen = 0;
    // (cluster, first lane, documents), one entry per non-empty cluster.
    std::vector<std::tuple<uint32_t, uint64_t, size_t>> lanes;
    for (size_t i = 0; i != clusters.size(); ++i) {
      const uint32_t cluster = clusters[i];
      const float* centroid = oracle_centroids.data() + i * kDim;
      const auto expected =
        ReadCluster(*oracle_field, *oracle_tree, oracle_cb, layout, cluster,
                    centroid, /*partitioned=*/false);
      const auto actual =
        ReadCluster(*split_field, *split_tree, split_cb, layout, cluster,
                    centroid, /*partitioned=*/true);
      ASSERT_EQ(expected.hits, actual.hits) << "cluster " << cluster;
      ASSERT_EQ(expected.dists.size(), actual.dists.size());
      for (size_t h = 0; h != expected.dists.size(); ++h) {
        ASSERT_EQ(expected.dists[h], actual.dists[h])
          << "cluster " << cluster << " hit " << h;
      }
      if (expected.hits.empty()) {
        continue;
      }
      ++split_clusters_seen;
      total_docs += actual.hits.size();

      // The cut: one run per row group the cluster touches, holding exactly
      // that row group's documents.
      const auto want_runs = ExpectedRuns(expected.hits);
      ASSERT_EQ(want_runs.size(), actual.runs.size()) << "cluster " << cluster;
      ASSERT_EQ(1, expected.runs.size());
      for (size_t r = 0; r != want_runs.size(); ++r) {
        EXPECT_EQ(want_runs[r].rg, actual.runs[r].rg);
        EXPECT_EQ(want_runs[r].docs_count, actual.runs[r].docs_count);
      }
      total_runs += actual.runs.size();

      // The cluster owns one lane range however its postings were cut, and it
      // is the same range the unpartitioned writer gave it: codes go into the
      // stream in cluster document order, which the row group cut preserves.
      EXPECT_EQ(expected.first_lane, actual.first_lane)
        << "cluster " << cluster;
      lanes.emplace_back(cluster, actual.first_lane, actual.hits.size());
    }
    ASSERT_EQ(kRows, total_docs);

    // The stream is the clusters' documents in dictionary order, gapless: a
    // cluster's anchor is how many documents the clusters before it hold.
    std::sort(lanes.begin(), lanes.end());
    uint64_t lane = 0;
    for (const auto& [cluster, first_lane, docs] : lanes) {
      EXPECT_EQ(lane, first_lane) << "cluster " << cluster;
      lane += docs;
    }
    EXPECT_EQ(kRows, lane);
    // The partitioning has to be real: more runs than clusters, and every
    // cluster spread over several row groups.
    ASSERT_GT(total_runs, split_clusters_seen * 2);

    // One stream, one partial pack at its end whatever the row group size: the
    // cut moves no byte of ".pay" for any quantizer.
    EXPECT_EQ(ReadWhole(oracle_dir, "pay"), ReadWhole(split_dir, "pay"));
    EXPECT_EQ(oracle.Field()->PayloadBase(), split_field->PayloadBase());

    // 4-byte fixed-width centroid ids with no shared prefix: the cluster
    // dictionary is exactly the fixed-stride leaf's case.
    EXPECT_EQ("FIXED_STRIDE", split_field->StorageInfo(false).layout);
  }
}
INSTANTIATE_TEST_SUITE_P(
  quantizers, IvfRowGroupTest,
  ::testing::Values(QuantCase{irs::VectorQuantization::SQ8, 0, 0, "sq8"},
                    QuantCase{irs::VectorQuantization::SQ4, 0, 0, "sq4"},
                    QuantCase{irs::VectorQuantization::PQ, 4, 0, "pq"},
                    QuantCase{irs::VectorQuantization::RaBitQ, 0, 1, "rabitq1"},
                    QuantCase{irs::VectorQuantization::RaBitQ, 0, 8,
                              "rabitq8"}),
  [](const ::testing::TestParamInfo<QuantCase>& info) {
    return std::string{info.param.name};
  });
