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

#include <iresearch/search/count/root.hpp>
#include <iresearch/search/detail/bitset_build.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/queries/docs_mask_query.hpp>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>

#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

using irs::doc_id_t;

inline constexpr doc_id_t kWikiDocs = 5032104;

TEST(probe_fold_gate, rare_term_against_sparse_partner_probes) {
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(153, 14032, kWikiDocs));
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(4181, 43333, kWikiDocs));
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(4308, 474769, kWikiDocs));
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(4944, 482117, kWikiDocs));
}

TEST(probe_fold_gate, balanced_pair_folds) {
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(5449, 5522, kWikiDocs));
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(5954, 6133, kWikiDocs));
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(10596, 11157, kWikiDocs));
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(23938, 263792, kWikiDocs));
}

TEST(probe_fold_gate, near_the_boundary) {
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(7502, 66880, kWikiDocs));
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(25112, 387642, kWikiDocs));
}

TEST(probe_fold_gate, dense_partner_costs_its_words) {
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(1000, 4168066, kWikiDocs));
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(31486, 4168066, kWikiDocs));
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(44108, 890767, kWikiDocs));
}

TEST(probe_fold_gate, skewed_pair_probes) {
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(1, 1000, 100000));
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(100, 300000, kWikiDocs));
}

TEST(probe_fold_gate, two_common_terms_fold) {
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(82685, 94693, kWikiDocs));
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(104806, 215507, kWikiDocs));
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(52686, 55391, kWikiDocs));
}

TEST(probe_fold_gate, rare_term_probes) {
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(43, 82685, kWikiDocs));
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(304, 280493, kWikiDocs));
  ASSERT_TRUE(irs::detail::ProbeBeatsFold(162, 4168066, kWikiDocs));
}

inline constexpr irs::field_id kBodyId = 1;
inline constexpr doc_id_t kDocs = 100000;

struct Stride {
  std::string_view term;
  doc_id_t every;
};

inline constexpr Stride kStrides[]{
  {"half", 2},       {"eleven", 11}, {"thirteen", 13},
  {"seventeen", 17}, {"rare", 997},  {"rarer", 3001},
};

doc_id_t Every(std::string_view term) {
  for (const auto& stride : kStrides) {
    if (stride.term == term) {
      return stride.every;
    }
  }
  return 0;
}

doc_id_t DocsWith(std::string_view term) {
  return (kDocs + Every(term) - 1) / Every(term);
}

uint64_t UnionOf(std::string_view a, std::string_view b) {
  uint64_t count = 0;
  for (doc_id_t i = 0; i != kDocs; ++i) {
    count += i % Every(a) == 0 || i % Every(b) == 0;
  }
  return count;
}

uint64_t IntersectionOf(std::string_view a, std::string_view b) {
  uint64_t count = 0;
  for (doc_id_t i = 0; i != kDocs; ++i) {
    count += i % Every(a) == 0 && i % Every(b) == 0;
  }
  return count;
}

irs::TermClause Clause(std::string_view value) {
  return {.field = kBodyId,
          .term = irs::bstring{irs::ViewCast<irs::byte_type>(value)}};
}

class CountSubtractTest : public tests::IndexTestBase {
 protected:
  void Build() {
    auto writer = open_writer(irs::kOmCreate);
    auto body = std::make_shared<tests::TextField<std::string>>("body");
    body->id = kBodyId;
    tests::Document doc;
    doc.insert(body, true, false);
    for (doc_id_t i = 0; i != kDocs; ++i) {
      std::string text = "doc";
      for (const auto& stride : kStrides) {
        if (i % stride.every == 0) {
          absl::StrAppend(&text, " ", stride.term);
        }
      }
      body->value(text);
      ASSERT_TRUE(Insert(*writer, doc));
    }
    writer->RefreshCommit();
  }

  void CheckCount(const irs::Filter& filter, uint64_t expected) {
    auto reader = open_reader();
    ASSERT_EQ(1, reader.size());
    const auto& segment = reader[0];
    auto query = irs::PrepareMasked(filter, segment, {});
    ASSERT_NE(nullptr, query);

    auto whole = query->PlanCount({});
    ASSERT_NE(nullptr, whole);
    ASSERT_EQ(expected,
              whole->Run(irs::doc_limits::min(), irs::doc_limits::eof()) +
                whole->Finish());

    auto split = query->PlanCount({.partial = true});
    ASSERT_NE(nullptr, split);
    constexpr doc_id_t kBounds[]{
      irs::doc_limits::min(), 777, 30001, 65536, 70001, irs::doc_limits::eof()};
    uint64_t parts = 0;
    for (size_t i = 1; i != std::size(kBounds); ++i) {
      parts += split->Run(kBounds[i - 1], kBounds[i]);
    }
    ASSERT_EQ(expected, parts + split->Finish());
  }

  void CheckUnion(std::string_view a, std::string_view b) {
    SCOPED_TRACE(absl::StrCat(a, " ", b));
    const auto expected = UnionOf(a, b);
    {
      irs::BooleanFilter filter;
      filter.Add(Clause(a), irs::Occur::Should);
      filter.Add(Clause(b), irs::Occur::Should);
      filter.SetMinShouldMatch(1);
      CheckCount(filter, expected);
    }
    {
      irs::BooleanFilter filter;
      filter.Add(std::make_unique<irs::All>(), irs::Occur::Must);
      filter.Add(Clause(a), irs::Occur::MustNot);
      filter.Add(Clause(b), irs::Occur::MustNot);
      CheckCount(filter, kDocs - expected);
    }
  }

  void CheckIntersection(std::string_view a, std::string_view b) {
    SCOPED_TRACE(absl::StrCat("+", a, " +", b));
    irs::BooleanFilter filter;
    filter.Add(Clause(a), irs::Occur::Must);
    filter.Add(Clause(b), irs::Occur::Must);
    CheckCount(filter, IntersectionOf(a, b));
  }
};

TEST_P(CountSubtractTest, disjunctions_count_on_both_sides_of_the_gate) {
  Build();
  ASSERT_TRUE(
    irs::detail::ProbeBeatsFold(DocsWith("rarer"), DocsWith("half"), kDocs));
  ASSERT_TRUE(
    irs::detail::ProbeBeatsFold(DocsWith("rarer"), DocsWith("eleven"), kDocs));
  ASSERT_TRUE(
    irs::detail::ProbeBeatsFold(DocsWith("rare"), DocsWith("half"), kDocs));
  ASSERT_FALSE(
    irs::detail::ProbeBeatsFold(DocsWith("eleven"), DocsWith("half"), kDocs));
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(DocsWith("seventeen"),
                                           DocsWith("thirteen"), kDocs));
  CheckUnion("rarer", "half");
  CheckUnion("rarer", "eleven");
  CheckUnion("rare", "half");
  CheckUnion("thirteen", "seventeen");
  CheckUnion("eleven", "half");
  CheckUnion("rare", "rarer");
}

TEST_P(CountSubtractTest, conjunctions_count_on_both_sides_of_the_gate) {
  Build();
  ASSERT_FALSE(
    irs::detail::ProbeBeatsFold(DocsWith("eleven"), DocsWith("half"), kDocs));
  ASSERT_FALSE(irs::detail::ProbeBeatsFold(DocsWith("seventeen"),
                                           DocsWith("thirteen"), kDocs));
  ASSERT_TRUE(
    irs::detail::ProbeBeatsFold(DocsWith("rare"), DocsWith("half"), kDocs));
  ASSERT_TRUE(
    irs::detail::ProbeBeatsFold(DocsWith("rarer"), DocsWith("eleven"), kDocs));
  CheckIntersection("eleven", "half");
  CheckIntersection("thirteen", "seventeen");
  CheckIntersection("rare", "half");
  CheckIntersection("rarer", "eleven");
  CheckIntersection("rare", "rarer");
}

INSTANTIATE_TEST_SUITE_P(count_subtract_test, CountSubtractTest,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>)),
                         CountSubtractTest::to_string);

}  // namespace
