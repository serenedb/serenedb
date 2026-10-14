////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <absl/strings/str_cat.h>

#include <iresearch/index/merge/sort_spec.hpp>
#include <iresearch/utils/index_utils.hpp>
#include <limits>
#include <span>
#include <string>
#include <vector>

#include "index/merge/support/segment_dump.hpp"
#include "index/merge/support/segment_gen.hpp"
#include "index_tests.hpp"
#include "tests_shared.hpp"

namespace {

using namespace tests::merge;

constexpr const char kSortedReason[] = "segments are not sorted at flush";
constexpr irs::field_id kKey = kFirstColumn;
constexpr irs::field_id kText = kFirstColumn + 2;

const auto kFullMerge = irs::index_utils::MakePolicy(
  irs::index_utils::CompactionCount{std::numeric_limits<size_t>::max()});

class SortedIndexTestCase : public tests::IndexTestBase {
 protected:
  irs::IndexWriter::ptr OpenSorted(const irs::SortSpec& spec) {
    auto options = tests::CsDefaultWriterOptions();
    options.column_options = SketchedColumns;
    options.sort = spec;
    return open_writer(irs::kOmCreate, std::move(options));
  }

  static SegmentShape Shape(size_t docs, double null_ratio,
                            double delete_ratio = 0,
                            std::vector<size_t> columns = {0, 1, 2, 3, 4}) {
    SegmentShape shape;
    shape.docs = docs;
    shape.null_ratio = null_ratio;
    shape.delete_ratio = delete_ratio;
    shape.columns = std::move(columns);
    shape.mixed = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos;
    return shape;
  }

  void CompactAndCheck(irs::IndexWriter& writer, const irs::SortSpec& spec) {
    const auto before = writer.GetSnapshot();
    const auto inputs = Segments(before);
    ASSERT_LT(1, inputs.size());
    const auto expected = ExpectedSortedMerge(inputs, spec);
    ASSERT_TRUE(writer.Compact(kFullMerge));
    ASSERT_TRUE(writer.RefreshCommit());
    const auto after = writer.GetSnapshot();
    ASSERT_EQ(1, after.size());
    EXPECT_EQ("", FirstDifference(expected, Dump(after[0])));
  }

  static void ExpectSorted(const irs::SubReader& segment,
                           const irs::SortSpec& spec) {
    std::vector<std::vector<duckdb::Value>> keys;
    for (const auto& key : spec.keys) {
      keys.push_back(LiveValues(segment, key.column));
    }
    for (size_t row = 1; row < segment.live_docs_count(); ++row) {
      for (size_t k = 0; k != keys.size(); ++k) {
        const auto cmp =
          CompareKeyValues(keys[k][row - 1], keys[k][row], spec.keys[k]);
        ASSERT_LE(cmp, 0) << "row " << row;
        if (cmp != 0) {
          break;
        }
      }
    }
  }

  void RemoveDuringCompaction(const irs::SortSpec& spec, double null_ratio) {
    auto writer = OpenSorted(spec);
    Generator gen{7};
    size_t next_id = 0;
    for (size_t s = 0; s != 4; ++s) {
      WriteSegment(*writer, gen, Shape(300, null_ratio, 0.2), next_id, &spec);
    }
    const auto live = writer->GetSnapshot().live_docs_count();
    ASSERT_TRUE(writer->Compact(kFullMerge));
    std::vector<std::string> removed;
    for (size_t id = 0; id < next_id; id += 5) {
      removed.push_back(absl::StrCat("doc-", id));
    }
    RemoveTerms(*writer, kIdField, removed);
    const auto after = writer->GetSnapshot();
    ASSERT_EQ(1, after.size());
    EXPECT_GT(live, after.live_docs_count());
    ExpectSorted(after[0], spec);
    const auto* ids = after[0].field(kIdField);
    ASSERT_NE(nullptr, ids);
    for (const auto& key : removed) {
      auto it = ids->iterator();
      if (!it->seek(irs::ViewCast<irs::byte_type>(std::string_view{key}))) {
        continue;
      }
      auto postings = it->postings(irs::IndexFeatures::None);
      irs::doc_id_t docs[irs::doc_limits::kBlockSize];
      uint32_t freqs[irs::doc_limits::kBlockSize];
      auto mask = after[0].MaskedDocs();
      for (auto n = postings->NextDocs(docs, freqs); n != 0;
           n = postings->NextDocs(docs, freqs)) {
        for (uint32_t i = 0; i != n; ++i) {
          EXPECT_TRUE(mask.Contains(docs[i])) << key;
        }
      }
    }
  }
};

class SortedIndexStressTestCase : public tests::IndexTestBase {};

const auto kTestValuesSorted = ::testing::Combine(
  ::testing::ValuesIn(tests::GetDirectories<tests::kTypesDefault>()));

}  // namespace

TEST_P(SortedIndexTestCase, simple_sequential) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexTestCase, reader_components) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexTestCase, simple_sequential_compact) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexTestCase, simple_sequential_already_sorted) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexTestCase, europarl) { GTEST_SKIP() << kSortedReason; }

TEST_P(SortedIndexTestCase, europarl_docs_batched) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexTestCase, multi_valued_sorting_field) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexTestCase, check_document_order_after_compaction_dense) {
  const irs::SortSpec spec{{{.column = kKey}}};
  auto writer = OpenSorted(spec);
  Generator gen{1};
  size_t next_id = 0;
  for (size_t s = 0; s != 3; ++s) {
    WriteSegment(*writer, gen, Shape(400, 0), next_id, &spec);
  }
  CompactAndCheck(*writer, spec);
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_compaction_dense_with_removals) {
  const irs::SortSpec spec{{{.column = kKey, .descending = true}}};
  {
    auto writer = OpenSorted(spec);
    Generator gen{2};
    size_t next_id = 0;
    for (size_t s = 0; s != 3; ++s) {
      WriteSegment(*writer, gen, Shape(400, 0, 0.3), next_id, &spec);
    }
    CompactAndCheck(*writer, spec);
  }
  RemoveDuringCompaction(spec, 0);
}

TEST_P(SortedIndexTestCase, doc_removal_same_key_within_trx) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexTestCase, doc_removal_same_key_within_trx_flush) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_compaction_sparse_with_gaps) {
  const irs::SortSpec spec{
    {{.column = kKey, .nulls_first = true}, {.column = kText}}};
  auto writer = OpenSorted(spec);
  Generator gen{3};
  size_t next_id = 0;
  WriteSegment(*writer, gen, Shape(300, 0.3), next_id, &spec);
  WriteSegment(*writer, gen, Shape(300, 0.3, 0, {1, 2, 3}), next_id, &spec);
  WriteSegment(*writer, gen, Shape(300, 0.3, 0, {1, 3, 4}), next_id, &spec);
  WriteSegment(*writer, gen, Shape(300, 0.3), next_id, &spec);
  CompactAndCheck(*writer, spec);
}

TEST_P(SortedIndexTestCase, check_document_order_after_compaction_sparse) {
  const irs::SortSpec spec{{{.column = kText, .nulls_first = true}}};
  auto writer = OpenSorted(spec);
  Generator gen{4};
  size_t next_id = 0;
  for (size_t s = 0; s != 4; ++s) {
    WriteSegment(*writer, gen, Shape(300, 0.4), next_id, &spec);
  }
  CompactAndCheck(*writer, spec);
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_compaction_sparse_already_sorted) {
  const irs::SortSpec spec{{{.column = kKey}}};
  auto writer = OpenSorted(spec);
  Generator gen{5};
  size_t next_id = 0;
  for (size_t s = 0; s != 3; ++s) {
    const auto shape = Shape(300, 0.3);
    auto docs = GenerateDocs(gen, shape, next_id);
    for (size_t i = 0; i != docs.size(); ++i) {
      docs[i].values[0] =
        duckdb::Value::BIGINT(static_cast<int64_t>(s * 1000 + i));
    }
    InsertDocs(*writer, shape, docs);
  }
  const auto before = writer->GetSnapshot();
  const auto expected = ExpectedMerge(Segments(before));
  CompactAndCheck(*writer, spec);
  EXPECT_EQ("", FirstDifference(expected, Dump(writer->GetSnapshot()[0])));
}

TEST_P(SortedIndexTestCase,
       check_document_order_after_compaction_sparse_with_removals) {
  const irs::SortSpec spec{
    {{.column = kText, .descending = true, .nulls_first = true}}};
  {
    auto writer = OpenSorted(spec);
    Generator gen{6};
    size_t next_id = 0;
    for (size_t s = 0; s != 4; ++s) {
      WriteSegment(*writer, gen, Shape(300, 0.3, 0.4), next_id, &spec);
    }
    CompactAndCheck(*writer, spec);
  }
  RemoveDuringCompaction(spec, 0.3);
}

TEST_P(SortedIndexStressTestCase, doc_removal_same_key_within_trx) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexStressTestCase, commit_on_tick) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexStressTestCase, split_empty_commit) {
  GTEST_SKIP() << kSortedReason;
}

TEST_P(SortedIndexStressTestCase, remove_tick) {
  GTEST_SKIP() << kSortedReason;
}

INSTANTIATE_TEST_SUITE_P(SortedIndexTest, SortedIndexTestCase,
                         kTestValuesSorted, SortedIndexTestCase::to_string);

INSTANTIATE_TEST_SUITE_P(SortedIndexStressTest, SortedIndexStressTestCase,
                         kTestValuesSorted,
                         SortedIndexStressTestCase::to_string);
