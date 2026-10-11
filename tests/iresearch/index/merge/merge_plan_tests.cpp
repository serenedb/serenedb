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
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <numeric>
#include <random>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/merge/support/segment_dump.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;
using irs::MergeUnitKind;

irs::ColumnOptions PlainColumns(irs::field_id) { return {}; }

class MergePlanTest : public ::testing::Test {
 protected:
  void SetUp() final {
    auto options = irs::tests::DefaultWriterOptions();
    options.column_options = PlainColumns;
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
    Generator gen{91};
    size_t next_id = 0;
    for (const double delete_ratio : {0.0, 0.25, 0.0}) {
      SegmentShape shape;
      shape.docs = 250;
      shape.delete_ratio = delete_ratio;
      shape.null_ratio = 0.1;
      shape.columns = {0, 1, 2, 3, 4, 5, 6};
      WriteSegment(*_writer, gen, shape, next_id);
    }
    _reader = _writer->GetSnapshot();
    _inputs = Segments(_reader);
    ASSERT_EQ(3, _inputs.size());
    for (const auto* input : _inputs) {
      _doc_map.emplace_back(input, irs::IResourceManager::gNoop);
    }
  }

  irs::MergePlan MakePlan(irs::Directory& out) {
    return irs::MergePlan{out,
                          _doc_map,
                          _meta,
                          nullptr,
                          irs::DuckDBEngine::Instance().instance(),
                          &_field_options,
                          {},
                          nullptr};
  }

  std::string MergedDifference(irs::Directory& out) {
    const auto merged = irs::SegmentReaderImpl::Open(
      out, _meta, irs::tests::DefaultReaderOptions());
    if (merged == nullptr) {
      return "merged segment did not open";
    }
    return FirstDifference(ExpectedMerge(_inputs), Dump(*merged));
  }

  irs::MemoryDirectory _dir;
  irs::IndexWriter::ptr _writer;
  irs::DirectoryReader _reader;
  std::vector<const irs::SubReader*> _inputs;
  irs::FunctionFieldOptions _field_options{
    PlainColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::MergeDocMap _doc_map{{irs::IResourceManager::gNoop}};
  irs::SegmentMeta _meta;
};

TEST_F(MergePlanTest, UnitsComeInDependencyOrder) {
  irs::MemoryDirectory out;
  auto plan = MakePlan(out);
  ASSERT_TRUE(plan.Prepare());
  std::vector<MergeUnitKind> kinds;
  std::vector<irs::field_id> norms;
  for (const auto& unit : plan.Units()) {
    kinds.push_back(unit.kind);
    if (unit.kind == MergeUnitKind::Norms) {
      norms.push_back(unit.id);
    }
  }
  EXPECT_TRUE(std::ranges::is_sorted(kinds));
  EXPECT_FALSE(norms.empty());
  EXPECT_TRUE(std::ranges::is_sorted(norms));
  EXPECT_EQ(7, std::ranges::count(kinds, MergeUnitKind::Column));
  EXPECT_EQ(1, std::ranges::count(kinds, MergeUnitKind::ColumnsFooter));
  EXPECT_EQ(0, std::ranges::count(kinds, MergeUnitKind::Ann));
  EXPECT_EQ(1, std::ranges::count(kinds, MergeUnitKind::Postings));
  EXPECT_EQ(1, std::ranges::count(kinds, MergeUnitKind::Index));
  ASSERT_TRUE(irs::RunSerially(plan));
  EXPECT_EQ("", MergedDifference(out));
}

TEST_F(MergePlanTest, OnlyDeletedDocsGiveNoSegment) {
  std::vector<std::shared_ptr<const irs::SegmentReaderImpl>> deleted;
  _doc_map.clear();
  for (size_t i = 0; i < 2; ++i) {
    auto meta = _reader.Meta().index_meta.segments[i].meta;
    irs::DocumentMask all;
    for (irs::doc_id_t doc = irs::doc_limits::min();
         doc < meta.docs_count + irs::doc_limits::min(); ++doc) {
      all.Add(doc);
    }
    meta.docs_mask = std::make_shared<irs::DocumentMask>(std::move(all));
    meta.live_docs_count = 0;
    deleted.push_back(irs::SegmentReaderImpl::Open(
      _dir, meta, irs::tests::DefaultReaderOptions()));
    ASSERT_NE(nullptr, deleted.back());
    _doc_map.emplace_back(deleted.back().get(), irs::IResourceManager::gNoop);
  }
  irs::MemoryDirectory out;
  auto plan = MakePlan(out);
  ASSERT_TRUE(plan.Prepare());
  EXPECT_EQ(0, _meta.docs_count);
  EXPECT_FALSE(irs::RunSerially(plan));
  EXPECT_GT(plan.Profile().Nanos(irs::MergeStep::Postings), 0);
  EXPECT_GT(plan.Profile().Nanos(irs::MergeStep::Commit), 0);
}

class OversizedSegment final : public irs::SubReader {
 public:
  explicit OversizedSegment(uint64_t docs) noexcept {
    _meta.docs_count = docs;
    _meta.live_docs_count = docs;
  }

  uint64_t CountMappedMemory() const final { return 0; }

  const irs::SegmentInfo& Meta() const final { return _meta; }

  const irs::DocumentMask* docs_mask() const final { return nullptr; }

  irs::lead::Node::ptr docs_iterator() const final { return {}; }

  std::span<const irs::field_id> field_ids() const final { return {}; }

  const irs::TermReader* field(irs::field_id) const final { return nullptr; }

  irs::NormReader::ptr norms(irs::field_id) const final { return {}; }

 private:
  irs::SegmentInfo _meta;
};

TEST_F(MergePlanTest, MergeUpToTheDocIdLimitIsPlanned) {
  const OversizedSegment most{irs::kMergeDocsMax - 1};
  const OversizedSegment one{1};
  _doc_map.clear();
  _doc_map.emplace_back(&most, irs::IResourceManager::gNoop);
  _doc_map.emplace_back(&one, irs::IResourceManager::gNoop);
  irs::MemoryDirectory out;
  auto plan = MakePlan(out);
  ASSERT_TRUE(plan.Prepare());
  EXPECT_EQ(irs::kMergeDocsMax, _meta.docs_count);
  EXPECT_FALSE(plan.Units().empty());
}

TEST_F(MergePlanTest, MergePastTheDocIdLimitIsRefused) {
  const OversizedSegment most{irs::kMergeDocsMax};
  const OversizedSegment one{1};
  _doc_map.clear();
  _doc_map.emplace_back(&most, irs::IResourceManager::gNoop);
  _doc_map.emplace_back(&one, irs::IResourceManager::gNoop);
  irs::MemoryDirectory out;
  auto plan = MakePlan(out);
  EXPECT_FALSE(plan.Prepare());
  EXPECT_TRUE(plan.Units().empty());
  EXPECT_EQ(0, _meta.docs_count);
  EXPECT_LT(irs::kMergeDocsMax + irs::doc_limits::min(),
            irs::doc_limits::eof());
}

TEST_F(MergePlanTest, NormAndColumnUnitsMergeInAnyOrder) {
  for (const uint64_t seed : {1, 2, 3, 4}) {
    SCOPED_TRACE(seed);
    irs::MemoryDirectory out;
    _meta = {};
    _doc_map.clear();
    for (const auto* input : _inputs) {
      _doc_map.emplace_back(input, irs::IResourceManager::gNoop);
    }
    auto plan = MakePlan(out);
    ASSERT_TRUE(plan.Prepare());
    const auto units = plan.Units();
    const auto footer = std::ranges::find(units, MergeUnitKind::ColumnsFooter,
                                          &irs::MergeUnit::kind);
    ASSERT_NE(units.end(), footer);
    std::vector<size_t> order(units.size());
    std::iota(order.begin(), order.end(), 0);
    std::mt19937_64 rng{seed};
    std::shuffle(order.begin(), order.begin() + (footer - units.begin()), rng);
    for (const auto unit : order) {
      ASSERT_TRUE(plan.RunUnit(unit)) << "unit " << unit;
    }
    EXPECT_EQ("", MergedDifference(out));
  }
}

}  // namespace
