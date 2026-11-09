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

#include <iresearch/index/doc_removal.hpp>
#include <iresearch/search/count/make.hpp>
#include <iresearch/search/count/root.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/granular_range_filter.hpp>
#include <iresearch/search/queries/docs_mask_query.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <vector>

#include "filter_test_case_base.hpp"
#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

using irs::doc_id_t;

inline constexpr irs::field_id kSingle = 1;
inline constexpr irs::field_id kUnique = 2;
inline constexpr irs::field_id kMulti = 3;
inline constexpr doc_id_t kRows = 30000;
inline constexpr int64_t kValues = 9001;
inline constexpr int64_t kMultiValues = 3000;

int64_t SingleOf(doc_id_t row) { return int64_t{row} * 7919 % kValues; }

std::vector<int64_t> MultiOf(doc_id_t row) {
  if (row % 2 == 0) {
    return {row % kMultiValues, row / 7 % kMultiValues};
  }
  return {row % kMultiValues};
}

bool HasBoth(doc_id_t row, int64_t a, int64_t b) {
  const auto values = MultiOf(row);
  return absl::c_linear_search(values, a) && absl::c_linear_search(values, b);
}

irs::bstring Encode(int64_t value) {
  irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
  return irs::bstring{irs::numeric_utils::EncodeNumericTerm(buf, value)};
}

irs::Filter::ptr Range(irs::field_id field, int64_t lo, int64_t hi) {
  auto filter = std::make_unique<irs::ByGranularRange>();
  *filter->mutable_field_id() = field;
  auto& range = filter->mutable_options()->range;
  irs::SetGranularNumericTerm(range.min, lo);
  irs::SetGranularNumericTerm(range.max, hi);
  range.min_type = irs::BoundType::Inclusive;
  range.max_type = irs::BoundType::Inclusive;
  return filter;
}

irs::TermClause Term(irs::field_id field, int64_t value) {
  return {.field = field,
          .dict = irs::NumericLayout::kNumbers,
          .term = Encode(value)};
}

class CountSingleValuedTest : public tests::IndexTestBase {
 protected:
  void Build(irs::IndexWriter& writer) {
    for (doc_id_t row = 0; row != kRows; ++row) {
      tests::Document doc;
      const auto add = [&](irs::field_id id, std::string_view name,
                           int64_t value) {
        auto field = std::make_shared<tests::LongField>();
        field->Name(std::string{name});
        field->id = id;
        field->value(value);
        doc.insert(field, true, false);
      };
      add(kSingle, "single", SingleOf(row));
      add(kUnique, "unique", row);
      for (const auto value : MultiOf(row)) {
        add(kMulti, "multi", value);
      }
      ASSERT_TRUE(Insert(writer, doc.indexed.begin(), doc.indexed.end()));
    }
    writer.RefreshCommit();
    _live.assign(kRows, true);
  }

  void Remove(irs::IndexWriter& writer, int64_t lo, int64_t hi) {
    const auto snapshot = writer.GetSnapshot();
    const auto removal = Range(kUnique, lo, hi);
    tests::PreparedFilter prepared{*removal, snapshot};
    irs::DocRemovalBuilder builder;
    for (uint32_t i = 0; i != snapshot.size(); ++i) {
      auto docs = prepared.Execute(i);
      while (!irs::doc_limits::eof(docs->Next())) {
        builder.Add(i, docs->Value());
      }
    }
    tests::Remove(writer, std::move(builder).Finish(snapshot));
    writer.RefreshCommit();
    for (auto row = lo; row <= hi; ++row) {
      _live[row] = false;
    }
  }

  template<typename Predicate>
  uint64_t Rows(Predicate&& matches) const {
    uint64_t count = 0;
    for (doc_id_t row = 0; row != kRows; ++row) {
      count += _live[row] && matches(row);
    }
    return count;
  }

  uint64_t SingleIn(int64_t lo, int64_t hi) const {
    return Rows([&](doc_id_t row) {
      const auto value = SingleOf(row);
      return lo <= value && value <= hi;
    });
  }

  uint64_t MultiIn(int64_t lo, int64_t hi) const {
    return Rows([&](doc_id_t row) {
      return absl::c_any_of(MultiOf(row), [&](int64_t value) {
        return lo <= value && value <= hi;
      });
    });
  }

  void CheckCount(const irs::Filter& filter, uint64_t expected,
                  std::optional<bool> constant) {
    auto reader = open_reader();
    ASSERT_EQ(1, reader.size());
    const auto& segment = reader[0];
    auto query = irs::PrepareMasked(filter, segment, {});
    ASSERT_NE(nullptr, query);
    if (constant) {
      ASSERT_EQ(*constant, irs::count::IsConstant(*query));
    }

    auto whole = query->PlanCount({});
    ASSERT_NE(nullptr, whole);
    ASSERT_EQ(expected,
              whole->Run(irs::doc_limits::min(), irs::doc_limits::eof()) +
                whole->Finish());

    auto split = query->PlanCount({.partial = true});
    ASSERT_NE(nullptr, split);
    constexpr doc_id_t kBounds[]{
      irs::doc_limits::min(), 777, 9001, 16384, 20001, irs::doc_limits::eof()};
    uint64_t parts = 0;
    for (size_t i = 1; i != std::size(kBounds); ++i) {
      parts += split->Run(kBounds[i - 1], kBounds[i]);
    }
    ASSERT_EQ(expected, parts + split->Finish());
  }

  void CheckDocs(const irs::Filter& filter, uint64_t expected) {
    auto reader = open_reader();
    tests::PreparedFilter prepared{filter, reader};
    uint64_t actual = 0;
    for (size_t i = 0; i != reader.size(); ++i) {
      const auto mask = reader[i].MaskedDocs();
      auto docs = prepared.Execute(i);
      while (!irs::doc_limits::eof(docs->Next())) {
        actual += !mask.Contains(docs->Value());
      }
    }
    ASSERT_EQ(expected, actual);
  }

  std::vector<bool> _live;
};

TEST_P(CountSingleValuedTest, ranges_count_from_term_statistics) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  constexpr std::pair<int64_t, int64_t> kRanges[]{
    {0, 0},    {5, 5},       {0, 63},           {100, 4100},
    {0, 9000}, {4500, 9000}, {-5, kValues * 2}, {9500, 9600},
  };
  for (const auto [lo, hi] : kRanges) {
    SCOPED_TRACE(absl::StrCat(lo, "..", hi));
    CheckCount(*Range(kSingle, lo, hi), SingleIn(lo, hi), true);
  }
  for (const auto [lo, hi] : {std::pair{100, 2000}, std::pair{500, 2999}}) {
    SCOPED_TRACE(absl::StrCat("multi ", lo, "..", hi));
    CheckCount(*Range(kMulti, lo, hi), MultiIn(lo, hi), false);
  }
}

TEST_P(CountSingleValuedTest, disjunctions_sum_only_disjoint_clauses) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  {
    SCOPED_TRACE("distinct terms");
    constexpr int64_t kIn[]{1, 2, 3, 5, 17, 77, 123, 4000, 8888, 9000};
    irs::BooleanFilter filter;
    for (const auto value : kIn) {
      filter.Add(Term(kSingle, value), irs::Occur::Should);
    }
    filter.SetMinShouldMatch(1);
    CheckCount(filter, Rows([&](doc_id_t row) {
                 return absl::c_linear_search(kIn, SingleOf(row));
               }),
               true);
  }
  {
    SCOPED_TRACE("repeated one-document term");
    irs::BooleanFilter filter;
    filter.Add(Term(kUnique, 5), irs::Occur::Should);
    filter.Add(Term(kUnique, 5), irs::Occur::Should);
    filter.Add(Term(kUnique, 9), irs::Occur::Should);
    filter.SetMinShouldMatch(1);
    CheckCount(filter, 2, std::nullopt);
  }
  {
    SCOPED_TRACE("range and a term inside it");
    irs::BooleanFilter filter;
    filter.Add(Range(kSingle, 0, 3000), irs::Occur::Should);
    filter.Add(Term(kSingle, 5), irs::Occur::Should);
    filter.SetMinShouldMatch(1);
    CheckCount(filter, SingleIn(0, 3000), false);
  }
  {
    SCOPED_TRACE("overlapping ranges");
    irs::BooleanFilter filter;
    filter.Add(Range(kSingle, 0, 3000), irs::Occur::Should);
    filter.Add(Range(kSingle, 2000, 5000), irs::Occur::Should);
    filter.SetMinShouldMatch(1);
    CheckCount(filter, SingleIn(0, 5000), false);
  }
  {
    SCOPED_TRACE("terms of a multi-valued field");
    irs::BooleanFilter filter;
    filter.Add(Term(kMulti, 2100), irs::Occur::Should);
    filter.Add(Term(kMulti, 300), irs::Occur::Should);
    filter.SetMinShouldMatch(1);
    ASSERT_NE(0, Rows([](doc_id_t row) { return HasBoth(row, 2100, 300); }));
    CheckCount(filter,
               MultiIn(2100, 2100) + MultiIn(300, 300) -
                 Rows([](doc_id_t row) { return HasBoth(row, 2100, 300); }),
               false);
  }
}

TEST_P(CountSingleValuedTest, conjunctions_of_different_terms_are_empty) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  {
    irs::BooleanFilter filter;
    filter.Add(Term(kSingle, 5), irs::Occur::Must);
    filter.Add(Term(kSingle, 6), irs::Occur::Must);
    CheckCount(filter, 0, true);
  }
  {
    irs::BooleanFilter filter;
    filter.Add(Range(kSingle, 0, 3000), irs::Occur::Must);
    filter.Add(Range(kSingle, 2000, 5000), irs::Occur::Must);
    CheckCount(filter, SingleIn(2000, 3000), std::nullopt);
  }
  {
    irs::BooleanFilter filter;
    filter.Add(Term(kMulti, 2100), irs::Occur::Must);
    filter.Add(Term(kMulti, 300), irs::Occur::Must);
    CheckCount(filter,
               Rows([](doc_id_t row) { return HasBoth(row, 2100, 300); }),
               false);
  }
}

TEST_P(CountSingleValuedTest, negations_subtract_term_statistics) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  {
    SCOPED_TRACE("sparse range");
    irs::BooleanFilter filter;
    filter.Add(std::make_unique<irs::All>(), irs::Occur::Must);
    filter.Add(Range(kSingle, 1000, 2000), irs::Occur::MustNot);
    CheckCount(filter, kRows - SingleIn(1000, 2000), true);
  }
  {
    SCOPED_TRACE("dense range");
    irs::BooleanFilter filter;
    filter.Add(std::make_unique<irs::All>(), irs::Occur::Must);
    filter.Add(Range(kSingle, 1000, 8000), irs::Occur::MustNot);
    CheckCount(filter, kRows - SingleIn(1000, 8000), std::nullopt);
  }
  {
    SCOPED_TRACE("multi-valued range");
    irs::BooleanFilter filter;
    filter.Add(std::make_unique<irs::All>(), irs::Occur::Must);
    filter.Add(Range(kMulti, 100, 2000), irs::Occur::MustNot);
    CheckCount(filter, kRows - MultiIn(100, 2000), false);
  }
  {
    SCOPED_TRACE("overlapping exclusions");
    irs::BooleanFilter filter;
    filter.Add(std::make_unique<irs::All>(), irs::Occur::Must);
    filter.Add(Range(kSingle, 0, 3000), irs::Occur::MustNot);
    filter.Add(Term(kSingle, 5), irs::Occur::MustNot);
    CheckCount(filter, kRows - SingleIn(0, 3000), false);
  }
}

TEST_P(CountSingleValuedTest, deleted_documents_are_counted_out) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  Remove(*writer, 1000, 2999);
  CheckCount(*Range(kSingle, 100, 4100), SingleIn(100, 4100), false);
  irs::BooleanFilter filter;
  for (const auto value : {1, 2, 3, 5, 17}) {
    filter.Add(Term(kSingle, value), irs::Occur::Should);
  }
  filter.SetMinShouldMatch(1);
  CheckCount(filter, Rows([](doc_id_t row) {
               const auto value = SingleOf(row);
               return value == 1 || value == 2 || value == 3 || value == 5 ||
                      value == 17;
             }),
             false);
}

TEST_P(CountSingleValuedTest, dense_ranges_visit_the_documents_they_match) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  constexpr std::pair<int64_t, int64_t> kRanges[]{
    {10, 8990}, {0, 9000}, {-5, kValues * 2}, {4500, 9000}, {100, 120}};
  const auto check = [&] {
    for (const auto [lo, hi] : kRanges) {
      SCOPED_TRACE(absl::StrCat(lo, "..", hi));
      CheckDocs(*Range(kSingle, lo, hi), SingleIn(lo, hi));
      CheckCount(*Range(kSingle, lo, hi), SingleIn(lo, hi), std::nullopt);
    }
  };
  check();
  Remove(*writer, 1000, 2999);
  check();
}

TEST_P(CountSingleValuedTest, dense_ranges_estimate_their_exact_count) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  constexpr std::pair<int64_t, int64_t> kRanges[]{{10, 8990}, {3000, 9000}};
  for (const auto [lo, hi] : kRanges) {
    SCOPED_TRACE(absl::StrCat(lo, "..", hi));
    auto query = irs::PrepareMasked(*Range(kSingle, lo, hi), reader[0], {});
    ASSERT_NE(nullptr, query);
    ASSERT_EQ(irs::QueryKind::Boolean, query->Kind());
    const auto expected = SingleIn(lo, hi);
    EXPECT_EQ(expected, query->EstimateMin());
    EXPECT_EQ(expected, query->EstimateMax());
    EXPECT_EQ(expected, query->EstimateMatches());
  }
}

TEST_P(CountSingleValuedTest,
       ranges_run_as_their_complement_past_three_fifths) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  constexpr struct {
    int64_t lo;
    irs::QueryKind kind;
  } kRanges[]{
    {4500, irs::QueryKind::Terms},
    {3800, irs::QueryKind::Terms},
    {3000, irs::QueryKind::Boolean},
  };
  for (const auto& [lo, kind] : kRanges) {
    SCOPED_TRACE(lo);
    const auto range = Range(kSingle, lo, kValues - 1);
    auto query = irs::PrepareMasked(*range, reader[0], {});
    ASSERT_NE(nullptr, query);
    EXPECT_EQ(kind, query->Kind());
    CheckDocs(*range, SingleIn(lo, kValues - 1));
    CheckCount(*range, SingleIn(lo, kValues - 1), true);
  }
}

TEST_P(CountSingleValuedTest,
       split_counts_fold_only_disjunctions_of_many_terms) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  constexpr doc_id_t kRowGroupDocs = 122880;
  constexpr struct {
    int64_t lo;
    int64_t hi;
  } kRanges[]{
    {100, 109},
    {4500, kValues - 1},
  };
  for (const auto& [lo, hi] : kRanges) {
    SCOPED_TRACE(lo);
    const auto range = Range(kSingle, lo, hi);
    auto query = irs::PrepareMasked(*range, reader[0], {});
    ASSERT_NE(nullptr, query);
    ASSERT_EQ(irs::QueryKind::Terms, query->Kind());
    EXPECT_EQ(
      0, irs::count::FoldTerms(*query, std::numeric_limits<doc_id_t>::max())
           .WordCount());
    if (hi - lo < 16) {
      EXPECT_EQ(0, irs::count::FoldTerms(*query, kRowGroupDocs).WordCount());
    }
    const auto fold =
      irs::count::FoldTerms(*query, irs::detail::BitsetStorage::kBits);
    ASSERT_NE(0, fold.WordCount());
    auto count = irs::count::MakeRoot(*query, {.partial = true, .fold = &fold});
    ASSERT_NE(nullptr, count);
    constexpr doc_id_t kBounds[]{
      irs::doc_limits::min(), 777, 9001, 16384, 20001, irs::doc_limits::eof()};
    uint64_t parts = 0;
    for (size_t i = 1; i != std::size(kBounds); ++i) {
      parts += count->Run(kBounds[i - 1], kBounds[i]);
    }
    EXPECT_EQ(SingleIn(lo, hi), parts + count->Finish());
  }
}

INSTANTIATE_TEST_SUITE_P(count_single_valued_test, CountSingleValuedTest,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>)),
                         CountSingleValuedTest::to_string);

}  // namespace
