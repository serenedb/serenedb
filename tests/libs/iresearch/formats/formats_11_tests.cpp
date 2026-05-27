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

#include <unordered_set>

#include "formats/column/test_cs_helpers.hpp"
#include "formats_test_case_base.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/store/directory_attributes.hpp"
#include "tests_shared.hpp"

namespace {

inline constexpr irs::field_id kNameId = 1;

using tests::FormatTestCase;
using tests::FormatTestCaseWithEncryption;

bool InsertWithName(irs::IndexWriter& writer, const tests::Document& doc) {
  auto ctx = writer.GetBatch();
  auto d = ctx.Insert();
  if (!d.Insert(doc.indexed.begin(), doc.indexed.end())) {
    return false;
  }
  const auto* name =
    dynamic_cast<const tests::StringField*>(doc.indexed.get("name"));
  if (name != nullptr) {
    if (auto* cs = d.Columnstore(); cs != nullptr) {
      irs::tests::StoreFieldAt(*cs, kNameId, d.DocId(), *name);
    }
  }
  return true;
}

class Format11TestCase : public FormatTestCaseWithEncryption {};

TEST_P(Format11TestCase, open_10_with_11) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  // write segment with format10
  {
    auto codec = irs::formats::Get("1_5simd");
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir(), codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);
  }

  // check index
  auto codec = irs::formats::Get("1_5simd");
  ASSERT_NE(nullptr, codec);
  auto index =
    irs::DirectoryReader(dir(), codec, irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(index);
  ASSERT_EQ(1, index->size());
  ASSERT_EQ(1, index->docs_count());
  ASSERT_EQ(1, index->live_docs_count());

  // check segment 0
  {
    auto& segment = index[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());

    std::unordered_set<std::string_view> expected_name = {"A"};
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(1,
                expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
                  values, docs_itr->value())));
    }

    ASSERT_TRUE(expected_name.empty());
  }
}

TEST_P(Format11TestCase, formats_11) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();
  const tests::Document* doc2 = gen.next();

  // write segment with format10
  {
    auto codec = irs::formats::Get("1_5simd");
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir(), codec, irs::kOmCreate,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc1));

    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);
  }

  // write segment with format11
  {
    auto codec = irs::formats::Get("1_5simd");
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir(), codec, irs::kOmAppend,
                                         irs::tests::DefaultWriterOptions());
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(InsertWithName(*writer, *doc2));

    ASSERT_TRUE(writer->RefreshCommit());
    AssertSnapshotEquality(*writer);
  }

  // check index
  auto index =
    irs::DirectoryReader(dir(), nullptr, irs::tests::DefaultReaderOptions());
  ASSERT_TRUE(index);
  ASSERT_EQ(2, index->size());
  ASSERT_EQ(2, index->docs_count());
  ASSERT_EQ(2, index->live_docs_count());

  // check segment 0
  {
    auto& segment = index[0];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());

    std::unordered_set<std::string_view> expected_name = {"A"};
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(1,
                expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
                  values, docs_itr->value())));
    }

    ASSERT_TRUE(expected_name.empty());
  }

  // check segment 1
  {
    auto& segment = index[1];
    ASSERT_EQ(1, segment.size());
    ASSERT_EQ(1, segment.docs_count());
    ASSERT_EQ(1, segment.live_docs_count());

    std::unordered_set<std::string_view> expected_name = {"B"};
    const auto* column = segment.Column(kNameId);
    ASSERT_NE(nullptr, column);
    irs::tests::BlobPointReader values{segment, *column};
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(1,
                expected_name.erase(irs::tests::ReadStoredStr<std::string_view>(
                  values, docs_itr->value())));
    }

    ASSERT_TRUE(expected_name.empty());
  }
}

static constexpr auto kTestDirs =
  tests::GetDirectories<tests::kTypesAllRot13>();
static const auto kTestValues =
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(tests::FormatInfo{"1_5simd"}));

// 1.1 specific tests
INSTANTIATE_TEST_SUITE_P(Format11Test, Format11TestCase, kTestValues,
                         Format11TestCase::to_string);

}  // namespace
