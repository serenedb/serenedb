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
#include <iresearch/index/column_info.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <iresearch/utils/tracked.hpp>
#include <random>
#include <span>
#include <string>

#include "formats/column/test_cs_helpers.hpp"
#include "index/doc_generator.hpp"
#include "index/index_tests.hpp"
#include "index_dir.hpp"
#include "tests_param.hpp"
#include "tests_shared.hpp"

namespace {

duckdb::DatabaseInstance& Db() {
  return irs::DuckDBEngine::Instance().instance();
}

duckdb::BufferManager& Manager() {
  return duckdb::BufferManager::GetBufferManager(Db());
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
constexpr irs::field_id kMarkId = tests::FieldIdFor("budget_mark");
constexpr irs::field_id kVec = tests::FieldIdFor("budget_vector");
constexpr size_t kTermDocs = 50000;

void InsertDistinctTerms(tests::IndexWriter::Transaction& trx) {
  for (size_t i = 0; i < kTermDocs; ++i) {
    const auto value = absl::StrCat("term", i);
    tests::StringField field{"budget_terms", value};
    field.id = kTermFieldId;
    ASSERT_TRUE(tests::InsertFields(trx.Insert(), &field, &field + 1));
  }
}

void Mark(const irs::IndexWriter::Document& doc, size_t i) {
  tests::StringField mark{"budget_mark", i % 3 == 0 ? "drop" : "keep"};
  mark.id = kMarkId;
  ASSERT_TRUE(tests::InsertFields(doc, &mark, &mark + 1));
}

void InsertMarkedTerms(tests::IndexWriter::Transaction& trx, size_t first,
                       size_t count) {
  for (size_t i = first; i < first + count; ++i) {
    const auto value = absl::StrCat("term", i);
    tests::StringField field{"budget_terms", value, irs::IndexFeatures::Norm};
    field.id = kTermFieldId;
    auto doc = trx.Insert();
    ASSERT_TRUE(tests::InsertFields(doc, &field, &field + 1));
    Mark(doc, i);
  }
}

void InsertVectors(tests::IndexWriter::Transaction& trx, size_t rows,
                   uint32_t d, bool marked = false) {
  const auto type = duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, d);
  std::mt19937 rng{7};
  std::uniform_real_distribution<float> dist{-1.f, 1.f};
  for (size_t i = 0; i < rows; ++i) {
    auto doc = trx.Insert();
    if (marked) {
      Mark(doc, i);
    }
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

tests::IndexWriter::ptr MakeWriter(
  const tests::IndexDir& dir,
  irs::IndexWriterOptions options = tests::CsDefaultWriterOptions()) {
  return tests::IndexWriter::Make(
    dir, tests::kOmCreate, tests::EnsureNormColumnId(std::move(options)));
}

TEST(FlushBudgetTest, FlushWithoutHeadroomFailsBeforeItStarts) {
  const tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  auto writer = MakeWriter(dir);
  auto trx = writer->GetBatch(/*exclusive=*/true);
  InsertDistinctTerms(trx);
  {
    NoMemoryHeadroom limit;
    EXPECT_THROW(trx.FlushAndFsync(), duckdb::OutOfMemoryException);
  }
  const auto flushed = trx.FlushAndFsync();
  ASSERT_EQ(1, flushed.size());
  EXPECT_EQ(kTermDocs, flushed.front().meta.docs_count);
}

TEST(FlushBudgetTest, TransactionGivesItsMemoryBackToTheSession) {
  const tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  auto writer = MakeWriter(dir);
  irs::MemoryBudget session{irs::MemoryBudget::kUnlimited - 1};
  for (const bool commit : {true, false}) {
    {
      auto trx = writer->GetBatch(/*exclusive=*/true);
      trx.Native().SetMemory(session);
      InsertDistinctTerms(trx);
      ASSERT_EQ(1, trx.FlushAndFsync().size());
      if (commit) {
        EXPECT_TRUE(trx.Commit());
      } else {
        trx.Abort();
      }
    }
    EXPECT_EQ(0, session.Used());
  }
}

TEST(FlushBudgetTest, FlushPastTheSessionLimitFailsAndKeepsItsBatch) {
  const tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  auto writer = MakeWriter(dir);
  irs::MemoryBudget session{irs::MemoryBudget::kUnlimited - 1};
  auto trx = writer->GetBatch(/*exclusive=*/true);
  trx.Native().SetMemory(session);
  InsertDistinctTerms(trx);
  session.SetLimit(session.Used() + 1);
  EXPECT_THROW(trx.FlushAndFsync(), duckdb::OutOfMemoryException);
  session.SetLimit(irs::MemoryBudget::kUnlimited);
  const auto flushed = trx.FlushAndFsync();
  ASSERT_EQ(1, flushed.size());
  EXPECT_EQ(kTermDocs, flushed.front().meta.docs_count);
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

irs::AnnInfo AnnInfoOf(const VectorCase& c) {
  return {
    .kind = c.kind,
    .index_id = kVec,
    .column_id = kVec,
    .d = static_cast<int>(c.d),
    .metric = irs::VectorMetric::L2Sqr,
    .quant = {.kind = c.quant,
              .pq_m = c.quant == irs::VectorQuantization::PQ ? c.d / 4 : 0,
              .nb_bits = c.quant == irs::VectorQuantization::TQ ||
                             c.quant == irs::VectorQuantization::RaBitQ
                           ? 4U
                           : 0U},
    .sample_factor = 0.2f,
    .posting_size = 256,
    .m = 8,
    .ef_construction = 32,
  };
}

TEST_P(VectorFlushBudgetTest, BuildWorkersGiveTheirMemoryBackToTheSession) {
  const auto [kind, quant, d, rows] = GetParam();
  const auto dir = tests::LocalDirectory(this);
  auto writer = MakeWriter(*dir, WithAnn(AnnInfoOf(GetParam())));
  irs::MemoryBudget session{irs::MemoryBudget::kUnlimited - 1};
  {
    auto trx = writer->GetBatch(/*exclusive=*/true);
    trx.Native().SetMemory(session);
    InsertVectors(trx, rows, d);
    const auto flushed = trx.FlushAndFsync();
    ASSERT_EQ(1, flushed.size());
    EXPECT_EQ(rows, flushed.front().meta.docs_count);
    EXPECT_TRUE(trx.Commit());
  }
  EXPECT_EQ(0, session.Used());
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

constexpr size_t kMergeSegments = 3;

TEST(MergeBudgetTest, CompactionWithoutHeadroomFailsAndKeepsItsSegments) {
  const tests::IndexDir dir{Db(), TestEnv::MemoryRoot()};
  auto writer = MakeWriter(dir);
  for (size_t s = 0; s < kMergeSegments; ++s) {
    auto trx = writer->GetBatch();
    InsertMarkedTerms(trx, s * kTermDocs, kTermDocs);
    ASSERT_TRUE(trx.Commit());
    ASSERT_TRUE(writer->RefreshCommit());
  }
  const auto files = [&] {
    auto names = dir.Files();
    absl::c_sort(names);
    return names;
  };
  const auto policy =
    irs::index_utils::MakePolicy(irs::index_utils::CompactionCount{});
  const auto before = files();
  {
    NoMemoryHeadroom limit;
    EXPECT_THROW(writer->Compact(policy), duckdb::OutOfMemoryException);
  }
  EXPECT_EQ(before, files());
  EXPECT_EQ(kMergeSegments, writer->GetSnapshot().size());
  EXPECT_EQ(kMergeSegments * kTermDocs,
            writer->GetSnapshot().live_docs_count());

  EXPECT_TRUE(writer->Compact(policy));
  ASSERT_TRUE(writer->RefreshCommit());
  const auto merged = writer->GetSnapshot();
  EXPECT_EQ(1, merged.size());
  EXPECT_EQ(kMergeSegments * kTermDocs, merged.live_docs_count());
}

}  // namespace
