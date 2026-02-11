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

#include <iresearch/index/norm.hpp>
#include <iresearch/store/directory_attributes.hpp>
#include <unordered_set>

#include "formats_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

using tests::FormatTestCase;
using tests::FormatTestCaseWithEncryption;

class Format11TestCase : public FormatTestCaseWithEncryption {};

TEST_P(Format11TestCase, open_10_with_11) {
  tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                              &tests::GenericJsonFieldFactory);

  const tests::Document* doc1 = gen.next();

  // write segment with format10
  {
    auto codec = irs::formats::Get("1_5avx");
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir(), codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // check index
  auto codec = irs::formats::Get("1_5avx");
  ASSERT_NE(nullptr, codec);
  auto index = irs::DirectoryReader(dir(), codec);
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
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                     actual_value->value.data())));
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
    auto codec = irs::formats::Get("1_5avx");
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir(), codec, irs::kOmCreate);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc1->indexed.begin(), doc1->indexed.end(),
                       doc1->stored.begin(), doc1->stored.end()));

    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // write segment with format11
  {
    auto codec = irs::formats::Get("1_5avx");
    ASSERT_NE(nullptr, codec);
    auto writer = irs::IndexWriter::Make(dir(), codec, irs::kOmAppend);
    ASSERT_NE(nullptr, writer);

    ASSERT_TRUE(Insert(*writer, doc2->indexed.begin(), doc2->indexed.end(),
                       doc2->stored.begin(), doc2->stored.end()));

    ASSERT_TRUE(writer->Commit());
    AssertSnapshotEquality(*writer);
  }

  // check index
  auto index = irs::DirectoryReader(dir());
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
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                     actual_value->value.data())));
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
    const auto* column = segment.column("name");
    ASSERT_NE(nullptr, column);
    auto values = column->iterator(irs::ColumnHint::Normal);
    ASSERT_NE(nullptr, values);
    auto* actual_value = irs::get<irs::PayAttr>(*values);
    ASSERT_NE(nullptr, actual_value);
    ASSERT_EQ(expected_name.size(),
              segment.docs_count());  // total count of documents
    auto terms = segment.field("same");
    ASSERT_NE(nullptr, terms);
    auto term_itr = terms->iterator(irs::SeekMode::NORMAL);
    ASSERT_TRUE(term_itr->next());

    for (auto docs_itr = term_itr->postings(irs::IndexFeatures::None);
         docs_itr->next();) {
      ASSERT_EQ(docs_itr->value(), values->seek(docs_itr->value()));
      ASSERT_EQ(1, expected_name.erase(irs::ToString<std::string_view>(
                     actual_value->value.data())));
    }

    ASSERT_TRUE(expected_name.empty());
  }
}

static constexpr auto kTestDirs =
  tests::GetDirectories<tests::kTypesAllRot13>();
static const auto kTestValues = ::testing::Combine(
  ::testing::ValuesIn(kTestDirs),
  ::testing::Values(tests::FormatInfo{"1_5avx"}, tests::FormatInfo{"1_5simd"}));

// 1.1 specific tests
INSTANTIATE_TEST_SUITE_P(format_11_test, Format11TestCase, kTestValues,
                         Format11TestCase::to_string);

}  // namespace
