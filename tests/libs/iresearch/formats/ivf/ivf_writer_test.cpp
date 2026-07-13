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

#include <cstring>
#include <duckdb.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/formats/ivf/ivf_writer.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "tests_shared.hpp"

using namespace irs;

namespace {

constexpr uint32_t kDim = 4;
constexpr field_id kVecColumn = 1;
constexpr field_id kPostingsColumn = 2;

// Two well-separated 4-d blobs (rows 0-3 near (8,8), rows 4-7 near (8,-8)),
// same shape as clustering_test.cpp's kBlobs2x4.
const std::vector<float> kBlobs2x4{
  8.f, 8.f,   0.f, 0.f,  //
  7.f, 9.f,   0.f, 0.f,  //
  9.f, 7.f,   0.f, 0.f,  //
  8.f, 8.5f,  0.f, 0.f,  //
  8.f, -8.f,  0.f, 0.f,  //
  7.f, -9.f,  0.f, 0.f,  //
  9.f, -7.f,  0.f, 0.f,  //
  8.f, -8.5f, 0.f, 0.f,  //
};

void WriteVectorColumn(ColWriter& w, field_id id,
                       const std::vector<float>& rows, uint32_t d) {
  const auto atype = duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, d);
  auto& cw = w.OpenColumn(id, atype);
  const auto n = static_cast<duckdb::idx_t>(rows.size() / d);
  duckdb::Vector v{atype, n};
  auto& child = duckdb::ArrayVector::GetChildMutable(v);
  auto* cd = duckdb::FlatVector::GetDataMutable<float>(child);
  duckdb::FlatVector::ValidityMutable(v).Reset(n);
  duckdb::FlatVector::ValidityMutable(child).Reset(n * d);
  std::memcpy(cd, rows.data(), rows.size() * sizeof(float));
  duckdb::FlatVector::SetSize(v, n);
  cw.Append(v, n);
}

class ivf_writer_test : public ::testing::Test {
 protected:
  duckdb::DatabaseInstance& Db() { return *_db.instance; }
  duckdb::DuckDB _db;
};

}  // namespace

TEST_F(ivf_writer_test,
       compute_partitions_docs_by_cluster_and_roundtrips_tree) {
  MemoryDirectory dir{};
  const size_t rows = kBlobs2x4.size() / kDim;
  {
    ColWriter w{dir, "seg", Db()};
    WriteVectorColumn(w, kVecColumn, kBlobs2x4, kDim);
    w.Commit(rows);
  }

  ColReader r{dir, "seg", Db()};
  const auto* col = r.Column(kVecColumn);
  ASSERT_NE(col, nullptr);
  ASSERT_EQ(col->RowCount(), rows);

  IvfInfo info{.centroids_id = kVecColumn,
               .postings_id = kPostingsColumn,
               .d = static_cast<int>(kDim),
               .metric = VectorMetric::L2Sqr};
  IvfBuilder builder{info};

  auto built = builder.Compute(*col, r.Ctx(), /*qw=*/nullptr);

  ASSERT_FALSE(built.empty);
  EXPECT_EQ(built.d, kDim);

  const auto leaf = built.centroids.LeafCentroids();
  const size_t n_leaf = leaf.size() / kDim;
  ASSERT_GT(n_leaf, 0u);
  ASSERT_EQ(built.cluster_offsets.size(), n_leaf + 1);

  // CSR partitions every doc exactly once.
  std::vector<bool> seen(rows, false);
  std::vector<uint32_t> doc_cluster(rows);
  for (size_t c = 0; c < n_leaf; ++c) {
    ASSERT_LE(built.cluster_offsets[c], built.cluster_offsets[c + 1]);
    for (uint64_t j = built.cluster_offsets[c];
         j < built.cluster_offsets[c + 1]; ++j) {
      const doc_id_t doc = built.cluster_docs[j];
      const size_t row = doc - doc_limits::min();
      ASSERT_LT(row, rows);
      ASSERT_FALSE(seen[row]);
      seen[row] = true;
      doc_cluster[row] = static_cast<uint32_t>(c);
    }
  }
  for (const bool s : seen) {
    EXPECT_TRUE(s);
  }

  // Blob A (rows 0-3) and blob B (rows 4-7) are far apart -- no leaf cluster
  // should ever hold docs from both.
  for (size_t a = 0; a < 4; ++a) {
    for (size_t b = 4; b < 8; ++b) {
      EXPECT_NE(doc_cluster[a], doc_cluster[b]);
    }
  }

  // Serialize() round-trips, and Search() near blob A lands on the same
  // cluster row 0 was itself assigned to.
  SimpleMemoryAccounter memory;
  MemoryFile file{memory};
  CentroidsSpan tree_span;
  {
    MemoryIndexOutput out{file};
    tree_span = built.centroids.Serialize(out);
    out.Flush();
  }

  MemoryIndexInput in{file};
  in.Seek(tree_span.offset);
  auto tree = CentroidsTree::Deserialize(in, tree_span.byte_size);
  EXPECT_FALSE(tree.Empty());
  EXPECT_EQ(tree.Dim(), kDim);

  const std::vector<float> q{8.f, 8.f, 0.f, 0.f};
  std::vector<uint32_t> ids;
  tree.Search(q, in, /*nprobe=*/1, ids, nullptr);
  ASSERT_EQ(ids.size(), 1u);
  EXPECT_EQ(ids[0], doc_cluster[0]);
}
