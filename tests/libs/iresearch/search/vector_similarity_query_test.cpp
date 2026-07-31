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

#include "formats/column/test_cs_helpers.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/vector_similarity_filter.hpp"
#include "iresearch/store/memory_directory.hpp"
#include "search/filter_test_case_base.hpp"
#include "search_fields.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kVec = 1;
inline constexpr irs::field_id kName = 2;
inline constexpr uint32_t kDim = 4;

// The executed KnnVectorQuery iterator must honor arbitrary interleavings of
// advance() and seek(): the quantized per-cluster iterator serves docs from a
// buffered posting leaf block whose underlying cursor already sits at the
// block's last doc, so seek() has to resolve targets from the buffer instead
// of delegating to the (already drained) source. A regression here silently
// skips docs when a pushed-down filter makes the vector side the conjunction
// lead (see the sqllogic counterpart in inverted_index_ivf_filter.test); this
// drives the iterator directly, with no SQL planner in between.

irs::IndexWriterOptions MakeWriterOptions() {
  auto opts = irs::tests::DefaultWriterOptions();
  opts.column_options = [](irs::field_id id) -> irs::ColumnOptions {
    irs::ColumnOptions col;
    if (id == kVec) {
      col.ivf_info = irs::IvfInfo{
        .centroids_id = kVec,
        .postings_id = kVec,
        .d = kDim,
        .metric = irs::VectorMetric::L2Sqr,
        .quant = {.kind = irs::VectorQuantization::SQ8},
        .sample_factor = 1.f,
        // Larger than any doc count used here, so every test indexes a single
        // cluster and doc ids map 1:1 onto one posting list.
        .posting_size = 1024,
      };
    }
    return col;
  };
  return opts;
}

void WriteVectorAt(irs::ColWriter& cs, irs::doc_id_t doc, float x) {
  const auto vtype =
    duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, kDim);
  auto& cw = cs.OpenColumn(kVec, vtype);
  duckdb::Vector v{vtype, 1};
  auto& child = duckdb::ArrayVector::GetChildMutable(v);
  auto* data = duckdb::FlatVector::GetDataMutable<float>(child);
  data[0] = x;
  for (uint32_t i = 1; i < kDim; ++i) {
    data[i] = 0.f;
  }
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  duckdb::FlatVector::ValidityMutable(child).SetAllValid(kDim);
  duckdb::FlatVector::SetSize(v, 1);
  cw.Append(static_cast<uint64_t>(doc) - irs::doc_limits::min(), v,
            /*count=*/1);
}

// Docs 1..n with emb = [doc, 0, 0, 0].
irs::DirectoryReader BuildIndex(irs::Directory& dir, irs::doc_id_t n) {
  constexpr auto kFormatId = "1_5simd";
  auto codec = irs::formats::Get(kFormatId);
  EXPECT_NE(nullptr, codec);
  auto writer =
    irs::IndexWriter::Make(dir, codec, irs::kOmCreate, MakeWriterOptions());
  EXPECT_NE(nullptr, writer);

  irs::tests::StringField name_field;
  name_field.field_name = "name";
  name_field.id = kName;
  name_field.value = "doc";
  {
    auto trx = writer->GetBatch();
    for (irs::doc_id_t i = 0; i < n; ++i) {
      auto doc = trx.Insert();
      EXPECT_TRUE(doc.Insert(name_field));
      WriteVectorAt(*doc.GetColWriter(), doc.DocId(), doc.DocId());
    }
    trx.Commit();
  }
  writer->RefreshCommit();
  return writer->GetSnapshot();
}

irs::ByVectorSimilarity MakeKnnFilter() {
  irs::ByVectorSimilarity filter;
  *filter.mutable_field_id() = kVec;
  auto& opts = *filter.mutable_options();
  opts.query.assign(kDim, 0.f);
  opts.centroids_id = kVec;
  opts.postings_id = kVec;
  opts.metric = irs::VectorMetric::L2Sqr;
  opts.quant = irs::VectorQuantization::SQ8;
  opts.nprobe = 1000;
  return filter;
}

// The bug only exists on the quantized path; assert the segment provides
// everything PrepareSegment needs to stay on it, so the test cannot silently
// degrade to the raw-rerank path and pass vacuously.
void AssertQuantizedPath(const irs::SubReader& segment) {
  const auto* ivf = segment.Ivf(kVec);
  ASSERT_NE(nullptr, ivf);
  ASSERT_FALSE(ivf->Empty());
  ASSERT_TRUE(ivf->HasQuantStats());
  const auto* postings = segment.field(kVec);
  ASSERT_NE(nullptr, postings);
  ASSERT_NE(irs::IndexFeatures::None,
            postings->meta().index_features & irs::IndexFeatures::Pay);
  ASSERT_NE(nullptr, segment.Column(kVec));
}

class VectorSimilarityQueryTest : public ::testing::Test {
 protected:
  void Build(irs::doc_id_t n) {
    _docs = n;
    _reader = BuildIndex(_dir, n);
    ASSERT_NE(nullptr, _reader);
    ASSERT_EQ(1U, _reader->size());
    ASSERT_EQ(n, _reader->docs_count());
    AssertQuantizedPath((*_reader)[0]);
    _filter = MakeKnnFilter();
    _prepared.emplace(_filter, *_reader, nullptr, irs::IResourceManager::gNoop,
                      nullptr, ::tests::PreparedFilter::CollectMode::Single);
    ASSERT_EQ(1U, _prepared->size());
  }

  irs::DocIterator::ptr Execute() {
    auto it = _prepared->Execute(0);
    EXPECT_NE(nullptr, it);
    EXPECT_FALSE(irs::doc_limits::valid(it->value()));
    const auto* cost = irs::get<irs::CostAttr>(*it);
    EXPECT_NE(nullptr, cost);
    EXPECT_EQ(_docs, cost->estimate());
    return it;
  }

  irs::doc_id_t _docs = 0;
  irs::MemoryDirectory _dir;
  irs::DirectoryReader _reader;
  irs::ByVectorSimilarity _filter;
  std::optional<::tests::PreparedFilter> _prepared;
};

TEST_F(VectorSimilarityQueryTest, AdvanceOnly) {
  Build(100);
  auto it = Execute();
  for (irs::doc_id_t doc = 1; doc <= _docs; ++doc) {
    ASSERT_EQ(doc, it->advance());
  }
  ASSERT_TRUE(irs::doc_limits::eof(it->advance()));
}

TEST_F(VectorSimilarityQueryTest, SeekOnly) {
  Build(100);
  auto it = Execute();
  ASSERT_EQ(1, it->seek(1));
  ASSERT_EQ(42, it->seek(42));
  ASSERT_EQ(42, it->seek(42));
  ASSERT_EQ(42, it->seek(7));
  ASSERT_EQ(100, it->seek(100));
  ASSERT_TRUE(irs::doc_limits::eof(it->seek(101)));

  auto beyond = Execute();
  ASSERT_TRUE(irs::doc_limits::eof(beyond->seek(101)));
}

// Single posting leaf block (docs < 128): the first advance() buffers every
// doc, then each seek() target lands inside that buffer.
TEST_F(VectorSimilarityQueryTest, MixedAdvanceSeekSingleBlock) {
  Build(100);
  {
    auto it = Execute();
    ASSERT_EQ(1, it->advance());
    ASSERT_EQ(5, it->seek(5));
    ASSERT_EQ(6, it->advance());
    ASSERT_EQ(6, it->seek(6));
    ASSERT_EQ(6, it->seek(3));
    ASSERT_EQ(7, it->advance());
    ASSERT_EQ(50, it->seek(50));
    ASSERT_EQ(51, it->advance());
    ASSERT_EQ(100, it->seek(100));
    ASSERT_TRUE(irs::doc_limits::eof(it->advance()));
  }
  // Every gap size: advance() to buffer the block, seek over 0..N-2 docs.
  for (irs::doc_id_t target = 2; target <= _docs; ++target) {
    auto it = Execute();
    ASSERT_EQ(1, it->advance());
    ASSERT_EQ(target, it->seek(target));
    if (target < _docs) {
      ASSERT_EQ(target + 1, it->advance());
    } else {
      ASSERT_TRUE(irs::doc_limits::eof(it->advance()));
    }
  }
}

// Multiple leaf blocks (docs > 128): in-buffer seeks, cross-block seeks, and
// advance() resuming after both.
TEST_F(VectorSimilarityQueryTest, MixedAdvanceSeekMultiBlock) {
  Build(300);
  {
    auto it = Execute();
    ASSERT_EQ(1, it->advance());
    ASSERT_EQ(100, it->seek(100));
    ASSERT_EQ(101, it->advance());
    ASSERT_EQ(130, it->seek(130));
    ASSERT_EQ(131, it->advance());
    ASSERT_EQ(256, it->seek(256));
    ASSERT_EQ(257, it->advance());
    ASSERT_EQ(300, it->seek(300));
    ASSERT_TRUE(irs::doc_limits::eof(it->advance()));
  }
  {
    auto it = Execute();
    for (irs::doc_id_t doc = 1; doc <= _docs; ++doc) {
      ASSERT_EQ(doc, it->advance());
    }
    ASSERT_TRUE(irs::doc_limits::eof(it->advance()));
  }
  for (irs::doc_id_t target :
       {2u, 127u, 128u, 129u, 200u, 255u, 256u, 257u, 299u, 300u}) {
    auto it = Execute();
    ASSERT_EQ(1, it->advance());
    ASSERT_EQ(target, it->seek(target));
    if (target < _docs) {
      ASSERT_EQ(target + 1, it->advance());
    } else {
      ASSERT_TRUE(irs::doc_limits::eof(it->advance()));
    }
  }
}

}  // namespace
