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

#include <absl/strings/str_cat.h>

#include <duckdb/common/exception.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <random>
#include <span>
#include <string>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

duckdb::BufferManager& Manager() {
  return duckdb::BufferManager::GetBufferManager(
    irs::DuckDBEngine::Instance().instance());
}

class NoMemoryHeadroom {
 public:
  NoMemoryHeadroom() : _old{Manager().GetMaxMemory()} {
    Manager().SetMemoryLimit(Manager().GetUsedMemory());
  }
  ~NoMemoryHeadroom() { Manager().SetMemoryLimit(_old); }

 private:
  size_t _old;
};

constexpr irs::field_id kTermFieldId = tests::FieldIdFor("budget_terms");
constexpr irs::field_id kVec = tests::FieldIdFor("budget_vector");
constexpr size_t kTermDocs = 50000;

void InsertDistinctTerms(irs::IndexWriter::Transaction& trx) {
  for (size_t i = 0; i < kTermDocs; ++i) {
    const auto value = absl::StrCat("term", i);
    tests::StringField field{"budget_terms", value};
    field.id = kTermFieldId;
    ASSERT_TRUE(tests::InsertFields(trx.Insert(), &field, &field + 1));
  }
}

void InsertVectors(irs::IndexWriter::Transaction& trx, size_t rows,
                   uint32_t d) {
  const auto type = duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, d);
  std::mt19937 rng{7};
  std::uniform_real_distribution<float> dist{-1.f, 1.f};
  for (size_t i = 0; i < rows; ++i) {
    auto doc = trx.Insert();
    auto& cw = doc.GetColWriter()->OpenColumn(kVec, type);
    duckdb::Vector v{type, 1};
    auto& child = duckdb::ArrayVector::GetChildMutable(v);
    auto* data = duckdb::FlatVector::GetDataMutable<float>(child);
    for (uint32_t j = 0; j < d; ++j) {
      data[j] = dist(rng);
    }
    duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
    duckdb::FlatVector::ValidityMutable(child).SetAllValid(d);
    duckdb::FlatVector::SetSize(v, 1);
    cw.Append(static_cast<uint64_t>(doc.DocId()) - irs::doc_limits::min(), v,
              /*count=*/1);
  }
}

irs::IndexWriterOptions WithAnn(irs::AnnInfo ann) {
  auto opts = irs::tests::DefaultWriterOptions();
  opts.anns[ann.column_id].push_back(ann.index_id);
  opts.column_options = [ann = std::move(ann)](irs::field_id id) {
    irs::ColumnOptions col;
    if (id == ann.index_id) {
      col.ann_info = ann;
    }
    return col;
  };
  return opts;
}

std::span<const irs::IndexWriter::FlushedSegment> FlushAtTheLimit(
  irs::IndexWriter::Transaction& trx) {
  trx.ReserveFlush();
  NoMemoryHeadroom limit;
  return trx.FlushAndFsync();
}

TEST(FlushBudgetTest, TextFlushRunsInsideItsReservation) {
  irs::MemoryDirectory dir;
  auto writer = irs::IndexWriter::Make(
    dir, irs::kOmCreate,
    tests::EnsureWriterDb(tests::CsDefaultWriterOptions()));
  auto trx = writer->GetBatch(/*exclusive_segment=*/true);
  InsertDistinctTerms(trx);
  const auto flushed = FlushAtTheLimit(trx);
  ASSERT_EQ(1, flushed.size());
  EXPECT_EQ(kTermDocs, flushed.front().meta.docs_count);
}

struct WriteSteps {
  bool commit;
  bool refresh;
};

size_t HeldAfter(WriteSteps steps, bool reserve) {
  irs::MemoryDirectory dir;
  auto writer = irs::IndexWriter::Make(
    dir, irs::kOmCreate,
    tests::EnsureWriterDb(tests::CsDefaultWriterOptions()));
  writer->RefreshCommit();
  const auto before = Manager().GetUsedMemory();
  {
    auto trx = writer->GetBatch();
    InsertDistinctTerms(trx);
    if (reserve) {
      trx.ReserveFlush();
    }
    if (steps.commit) {
      EXPECT_TRUE(trx.Commit());
    } else {
      trx.Abort();
    }
  }
  if (steps.refresh) {
    writer->RefreshCommit();
  }
  return Manager().GetUsedMemory() - before;
}

class ReservationReleaseTest : public ::testing::TestWithParam<WriteSteps> {};

TEST_P(ReservationReleaseTest, NothingStaysReservedOnceTheWorkIsDone) {
  EXPECT_EQ(HeldAfter(GetParam(), false), HeldAfter(GetParam(), true));
}

std::string WriteStepsName(const ::testing::TestParamInfo<WriteSteps>& info) {
  return absl::StrCat(info.param.commit ? "Commit" : "Abort",
                      info.param.refresh ? "Refresh" : "");
}

INSTANTIATE_TEST_SUITE_P(FlushBudget, ReservationReleaseTest,
                         ::testing::Values(WriteSteps{true, true},
                                           WriteSteps{false, false}),
                         WriteStepsName);

TEST(FlushBudgetTest, FlushWithoutItsReservationFailsBeforeItStarts) {
  irs::MemoryDirectory dir;
  auto writer = irs::IndexWriter::Make(
    dir, irs::kOmCreate,
    tests::EnsureWriterDb(tests::CsDefaultWriterOptions()));
  auto trx = writer->GetBatch(/*exclusive_segment=*/true);
  InsertDistinctTerms(trx);
  {
    NoMemoryHeadroom limit;
    EXPECT_THROW(trx.FlushAndFsync(), duckdb::OutOfMemoryException);
  }
  const auto flushed = trx.FlushAndFsync();
  ASSERT_EQ(1, flushed.size());
  EXPECT_EQ(kTermDocs, flushed.front().meta.docs_count);
  EXPECT_FALSE(writer->LostDocs());
}

struct VectorCase {
  irs::AnnKind kind;
  irs::VectorQuantization quant;
  uint32_t d;
  size_t rows;
};

class VectorFlushBudgetTest : public TestBase,
                              public ::testing::WithParamInterface<VectorCase> {
};

TEST_P(VectorFlushBudgetTest, FlushRunsInsideItsReservation) {
  const auto [kind, quant, d, rows] = GetParam();
  irs::MMapDirectory dir{test_dir(), irs::DirectoryAttributes{},
                         GetResourceManager().options};
  auto writer = irs::IndexWriter::Make(
    dir, irs::kOmCreate,
    WithAnn(irs::AnnInfo{
      .kind = kind,
      .index_id = kVec,
      .column_id = kVec,
      .d = static_cast<int>(d),
      .metric = irs::VectorMetric::L2Sqr,
      .quant = {.kind = quant,
                .pq_m = quant == irs::VectorQuantization::PQ ? d / 4 : 0,
                .nb_bits = quant == irs::VectorQuantization::TQ ||
                               quant == irs::VectorQuantization::RaBitQ
                             ? 4U
                             : 0U},
      .sample_factor = 0.2f,
      .posting_size = 256,
      .m = 8,
      .ef_construction = 32,
    }));
  auto trx = writer->GetBatch(/*exclusive_segment=*/true);
  InsertVectors(trx, rows, d);
  const auto flushed = FlushAtTheLimit(trx);
  ASSERT_EQ(1, flushed.size());
  EXPECT_EQ(rows, flushed.front().meta.docs_count);
}

std::string VectorCaseName(const ::testing::TestParamInfo<VectorCase>& info) {
  return absl::StrCat(info.param.kind == irs::AnnKind::Ivf ? "Ivf" : "Hnsw",
                      static_cast<int>(info.param.quant), "D", info.param.d,
                      "Rows", info.param.rows);
}

INSTANTIATE_TEST_SUITE_P(
  FlushBudget, VectorFlushBudgetTest,
  ::testing::Values(
    VectorCase{irs::AnnKind::Ivf, irs::VectorQuantization::None, 64, 20000},
    VectorCase{irs::AnnKind::Ivf, irs::VectorQuantization::SQ8, 64, 20000},
    VectorCase{irs::AnnKind::Ivf, irs::VectorQuantization::PQ, 64, 20000},
    VectorCase{irs::AnnKind::Ivf, irs::VectorQuantization::RaBitQ, 64, 20000},
    VectorCase{irs::AnnKind::Ivf, irs::VectorQuantization::BQ, 64, 20000},
    VectorCase{irs::AnnKind::Ivf, irs::VectorQuantization::None, 256, 4000},
    VectorCase{irs::AnnKind::Ivf, irs::VectorQuantization::None, 8, 2000},
    VectorCase{irs::AnnKind::Hnsw, irs::VectorQuantization::None, 64, 3000},
    VectorCase{irs::AnnKind::Hnsw, irs::VectorQuantization::SQ8, 64, 3000},
    VectorCase{irs::AnnKind::Hnsw, irs::VectorQuantization::PQ, 64, 3000},
    VectorCase{irs::AnnKind::Hnsw, irs::VectorQuantization::BQ, 64, 3000},
    VectorCase{irs::AnnKind::Hnsw, irs::VectorQuantization::TQ, 64, 3000},
    VectorCase{irs::AnnKind::Hnsw, irs::VectorQuantization::None, 256, 3000},
    VectorCase{irs::AnnKind::Hnsw, irs::VectorQuantization::SQ8, 256, 3000},
    VectorCase{irs::AnnKind::Hnsw, irs::VectorQuantization::SQ8, 8, 40000}),
  VectorCaseName);

}  // namespace
