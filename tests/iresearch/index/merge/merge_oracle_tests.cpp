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

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge_writer.hpp>
#include <iresearch/index/segment_reader_impl.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <memory>
#include <random>
#include <span>
#include <string_view>
#include <vector>

#include "formats/column/test_cs_helpers.hpp"
#include "index/merge/support/segment_dump.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;

irs::IndexWriter::ptr MakeWriter(irs::Directory& dir) {
  auto options = irs::tests::DefaultWriterOptions();
  options.column_options = SketchedColumns;
  return irs::IndexWriter::Make(dir, irs::kOmCreate, std::move(options));
}

std::shared_ptr<const irs::SegmentReaderImpl> MergeAll(
  irs::Directory& out, std::span<const irs::SubReader* const> inputs,
  bool& flushed) {
  irs::FunctionFieldOptions field_options{
    SketchedColumns, irs::tests::MakeNormColumnIdProvider(),
    DEFAULT_ROW_GROUP_SIZE};
  irs::SegmentMeta meta;
  const irs::SegmentWriterOptions options{
    .db = &irs::DuckDBEngine::Instance().instance(),
    .field_options = &field_options,
  };
  irs::MergeWriter merger{out, options};
  merger.Reset(inputs.begin(), inputs.end());
  flushed = irs::GetReady(merger.Flush(meta));
  if (!flushed) {
    return nullptr;
  }
  return irs::SegmentReaderImpl::Open(out, meta,
                                      irs::tests::DefaultReaderOptions());
}

class MergeOracleTest : public ::testing::TestWithParam<uint64_t> {};

TEST_P(MergeOracleTest, RandomSegmentSets) {
  Generator gen{GetParam()};
  irs::MemoryDirectory dir;
  auto writer = MakeWriter(dir);
  size_t next_id = 0;
  const auto segments = gen.Uniform(1, 6);
  for (size_t s = 0; s != segments; ++s) {
    WriteSegment(*writer, gen, RandomShape(gen), next_id);
  }
  const auto reader = writer->GetSnapshot();
  auto inputs = Segments(reader);
  if (inputs.empty()) {
    GTEST_SKIP() << "every document was deleted";
  }
  std::shuffle(inputs.begin(), inputs.end(), std::mt19937_64{GetParam()});
  SCOPED_TRACE(
    absl::StrCat("seed ", GetParam(), ", ", inputs.size(), " input segments"));

  const auto expected = ExpectedMerge(inputs);
  irs::MemoryDirectory out;
  bool flushed = false;
  const auto merged = MergeAll(out, inputs, flushed);
  ASSERT_EQ(expected.docs != 0, flushed);
  if (!flushed) {
    return;
  }
  ASSERT_NE(nullptr, merged);
  EXPECT_EQ(merged->docs_count(), merged->live_docs_count());
  EXPECT_EQ("", FirstDifference(expected, Dump(*merged)));
}

INSTANTIATE_TEST_SUITE_P(seeds, MergeOracleTest,
                         ::testing::Range<uint64_t>(0, 24));

uint64_t Fnv1a(std::string_view text) {
  uint64_t hash = 14695981039346656037ULL;
  for (const auto c : text) {
    hash ^= static_cast<uint8_t>(c);
    hash *= 1099511628211ULL;
  }
  return hash;
}

struct GoldenCase {
  uint64_t seed;
  uint64_t content;
};

constexpr GoldenCase kGoldenCases[] = {
  {101, 0xa323555c28b50a15ULL},
  {202, 0x95362bbe8721b0f6ULL},
  {303, 0x73029a66f17f9c33ULL},
  {404, 0x5b9e2bddbfe5ce3cULL},
};

TEST(MergeGoldenTest, MergedContentMatchesTheRecording) {
  const bool print = std::getenv("SDB_PRINT_MERGE_GOLDEN") != nullptr;
  for (const auto& golden : kGoldenCases) {
    SCOPED_TRACE(golden.seed);
    Generator gen{golden.seed};
    irs::MemoryDirectory dir;
    auto writer = MakeWriter(dir);
    size_t next_id = 0;
    for (size_t s = 0; s != 4; ++s) {
      WriteSegment(*writer, gen, RandomShape(gen), next_id);
    }
    const auto reader = writer->GetSnapshot();
    const auto inputs = Segments(reader);
    ASSERT_FALSE(inputs.empty());

    irs::MemoryDirectory out;
    bool flushed = false;
    const auto merged = MergeAll(out, inputs, flushed);
    ASSERT_TRUE(flushed);
    ASSERT_NE(nullptr, merged);
    const auto content = Fnv1a(Describe(Dump(*merged)));
    if (print) {
      std::fprintf(stderr, "  {%llu, 0x%016llxULL},\n",
                   static_cast<unsigned long long>(golden.seed),
                   static_cast<unsigned long long>(content));
    }
    EXPECT_EQ(golden.content, content);
  }
}

}  // namespace
