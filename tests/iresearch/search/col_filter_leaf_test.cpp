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

#include <iresearch/index/table_filter_iterator.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>

#include "filter_test_case_base.hpp"
#include "tests_shared.hpp"

namespace {

std::unique_ptr<irs::ByTerm> MakeFilter(std::string_view field,
                                        std::string_view term) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = tests::FieldIdFor(field);
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return filter;
}

class ColFilterLeafTestCase : public tests::FilterTestCaseBase {
 protected:
  static Docs Members(const irs::Filter& filter,
                      const irs::SubReader& segment) {
    irs::ColFilterLeaf leaf{filter, segment};
    const auto end =
      irs::doc_limits::min() + static_cast<irs::doc_id_t>(segment.docs_count());
    Docs docs;
    for (auto doc = irs::doc_limits::min(); doc < end; ++doc) {
      if (leaf.Contains(doc)) {
        docs.push_back(doc);
      }
    }
    EXPECT_FALSE(leaf.Contains(end));
    return docs;
  }

  void AddSequential(irs::OpenMode mode = irs::kOmCreate) {
    tests::JsonDocGenerator gen(resource("simple_sequential.json"),
                                &tests::GenericJsonFieldFactory);
    add_segment(gen, mode);
  }
};

TEST_P(ColFilterLeafTestCase, term) {
  AddSequential();
  auto rdr = open_reader();
  ASSERT_EQ(1, rdr.size());
  const auto filter = MakeFilter("duplicated", "abcd");
  const auto docs = Members(*filter, rdr[0]);
  EXPECT_GT(docs.size(), 1);
  CheckQuery(*filter, docs, rdr);
}

TEST_P(ColFilterLeafTestCase, missing_term) {
  AddSequential();
  auto rdr = open_reader();
  ASSERT_EQ(1, rdr.size());
  EXPECT_TRUE(Members(*MakeFilter("duplicated", "missing"), rdr[0]).empty());
  EXPECT_TRUE(Members(*MakeFilter("missing", "abcd"), rdr[0]).empty());
}

TEST_P(ColFilterLeafTestCase, disjunction) {
  AddSequential();
  auto rdr = open_reader();
  ASSERT_EQ(1, rdr.size());
  irs::BooleanFilter any;
  any.Add(MakeFilter("name", "A"), irs::Occur::Should);
  any.Add(MakeFilter("duplicated", "vczc"), irs::Occur::Should);
  any.SetMinShouldMatch(1);
  const auto docs = Members(any, rdr[0]);
  EXPECT_GT(docs.size(), 1);
  CheckQuery(any, docs, rdr);
}

TEST_P(ColFilterLeafTestCase, per_segment) {
  AddSequential();
  AddSequential(irs::kOmAppend);
  auto rdr = open_reader();
  ASSERT_EQ(2, rdr.size());
  const auto filter = MakeFilter("name", "C");
  EXPECT_EQ(Members(*filter, rdr[0]), Members(*filter, rdr[1]));
  EXPECT_EQ(1, Members(*filter, rdr[0]).size());
}

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(col_filter_leaf_test, ColFilterLeafTestCase,
                         ::testing::Combine(::testing::ValuesIn(kTestDirs)),
                         ColFilterLeafTestCase::to_string);

}  // namespace
