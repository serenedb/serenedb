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

#include <gtest/gtest.h>

#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <span>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;

struct Flushed {
  std::string name;
  uint64_t docs;
};

void MergeEverything(irs::Compaction& candidates, const irs::IndexReader& index,
                     const irs::CompactingSegments&) {
  for (const auto& segment : index) {
    candidates.emplace_back(&segment);
  }
}

class FlushedSegmentsTest : public ::testing::Test {
 protected:
  void Open(irs::FlushedSegmentsCallback on_flushed) {
    auto options = irs::tests::DefaultWriterOptions();
    options.column_options = SketchedColumns;
    options.on_flushed = std::move(on_flushed);
    _writer = irs::IndexWriter::Make(_dir, irs::kOmCreate, std::move(options));
  }

  void AddSegment(double delete_ratio = 0) {
    SegmentShape shape;
    shape.docs = 50;
    shape.delete_ratio = delete_ratio;
    shape.columns = {0, 1};
    WriteSegment(*_writer, _gen, shape, _next_id);
  }

  irs::MemoryDirectory _dir;
  Generator _gen{7};
  size_t _next_id = 0;
  irs::IndexWriter::ptr _writer;
};

TEST_F(FlushedSegmentsTest, ACommitReportsTheSegmentsItFlushed) {
  std::vector<std::vector<Flushed>> reports;
  Open([&](std::span<const irs::SubReader* const> segments) {
    auto& report = reports.emplace_back();
    for (const auto* segment : segments) {
      report.emplace_back(segment->Meta().name, segment->docs_count());
    }
  });
  AddSegment();
  ASSERT_EQ(1, reports.size());
  ASSERT_EQ(1, reports[0].size());
  EXPECT_EQ(_writer->GetSnapshot()[0].Meta().name, reports[0][0].name);
  EXPECT_EQ(50, reports[0][0].docs);

  AddSegment(0.5);
  ASSERT_EQ(2, reports.size());
  EXPECT_EQ(_writer->GetSnapshot()[1].Meta().name, reports[1][0].name);

  ASSERT_EQ(
    irs::CompactionError::Ok,
    irs::GetReady(_writer->CompactAsync(MergeEverything, nullptr, {}, nullptr))
      .error);
  ASSERT_TRUE(_writer->RefreshCommit());
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(2, reports.size());
}

TEST_F(FlushedSegmentsTest, AThrowingCallbackLeavesTheCommitDone) {
  size_t calls = 0;
  Open([&](std::span<const irs::SubReader* const>) {
    ++calls;
    throw std::runtime_error{"boom"};
  });
  AddSegment();
  EXPECT_EQ(1, calls);
  EXPECT_EQ(1, _writer->GetSnapshot().size());
  EXPECT_EQ(50, _writer->GetSnapshot().live_docs_count());
  AddSegment();
  EXPECT_EQ(2, calls);
  EXPECT_EQ(2, _writer->GetSnapshot().size());
}

}  // namespace
