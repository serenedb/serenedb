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
#include <iresearch/search/count/boolean_bitset.hpp>
#include <iresearch/search/count/make.hpp>
#include <iresearch/search/count/root.hpp>
#include <iresearch/search/detail/bitset_build.hpp>
#include <iresearch/search/detail/resolve.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/granular_range_filter.hpp>
#include <iresearch/search/queries/docs_mask_query.hpp>
#include <iresearch/search/queries/multiterm_query.hpp>
#include <iresearch/search/queries/term_query.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <iresearch/utils/empty.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iterator>
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
inline constexpr irs::field_id kParity = 4;
inline constexpr irs::field_id kHead = 5;
inline constexpr doc_id_t kRows = 30000;
inline constexpr doc_id_t kHeadRows = 1000;
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
      add(kParity, "parity", row % 2);
      add(kHead, "head", row < kHeadRows ? 0 : 1);
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

TEST_P(CountSingleValuedTest, split_counts_fill_their_range_of_many_terms) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  constexpr struct {
    int64_t lo;
    int64_t hi;
    bool bitset;
  } kRanges[]{
    {100, 109, false},
    {1000, 1999, false},
    {100, 1500, true},
  };
  for (const auto& [lo, hi, bitset] : kRanges) {
    SCOPED_TRACE(absl::StrCat(lo, "..", hi));
    auto query = irs::PrepareMasked(*Range(kMulti, lo, hi), reader[0], {});
    ASSERT_NE(nullptr, query);
    ASSERT_EQ(irs::QueryKind::Terms, query->Kind());
    ASSERT_EQ(
      bitset,
      irs::utils::downCast<irs::MultiTermQuery>(*query).State().TermsSize() >=
        irs::detail::kWindowWords);
    auto count = irs::count::MakeRoot(*query, {.span = 4096, .partial = true});
    ASSERT_NE(nullptr, count);
    EXPECT_EQ(
      bitset,
      nullptr != dynamic_cast<irs::count::BooleanBitset<irs::utils::Empty>*>(
                   count.get()));
    constexpr doc_id_t kBounds[]{
      irs::doc_limits::min(), 777, 9001, 16384, 20001, irs::doc_limits::eof()};
    uint64_t parts = 0;
    for (size_t i = 1; i != std::size(kBounds); ++i) {
      parts += count->Run(kBounds[i - 1], kBounds[i]);
    }
    EXPECT_EQ(MultiIn(lo, hi), parts + count->Finish());
  }
}

TEST_P(CountSingleValuedTest, bitset_fills_skip_what_earlier_clauses_emptied) {
  auto writer = open_writer(irs::kOmCreate);
  Build(*writer);
  Remove(*writer, 2000, 2099);
  auto reader = open_reader();
  ASSERT_EQ(1, reader.size());
  const auto& segment = reader[0];
  const auto* const doc = irs::detail::SegmentDoc(segment);
  ASSERT_NE(nullptr, doc);
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  std::vector<irs::Filter::ptr> filters;
  std::vector<irs::QueryBuilder::ptr> queries;
  const auto prepare = [&](irs::field_id field, int64_t lo,
                           int64_t hi) -> const irs::QueryBuilder& {
    const auto& filter = *filters.emplace_back(Range(field, lo, hi));
    return *queries.emplace_back(filter.PrepareSegment(segment, {}));
  };
  const auto clauses = [&](irs::field_id field, int64_t lo, int64_t hi) {
    const auto& query = prepare(field, lo, hi);
    std::vector<irs::detail::PostingClause> out;
    if (query.Kind() == irs::QueryKind::Term) {
      out.emplace_back(irs::utils::downCast<irs::TermQuery>(query).State());
      return out;
    }
    EXPECT_EQ(irs::QueryKind::Terms, query.Kind());
    if (query.Kind() == irs::QueryKind::Terms) {
      const auto& state =
        irs::utils::downCast<irs::MultiTermQuery>(query).State();
      for (const auto& entry : state.Terms()) {
        out.emplace_back(irs::detail::ClauseOf(entry, state.Reader()));
      }
    }
    return out;
  };
  const auto make = [&] {
    irs::detail::BitsetBuckets buckets;
    auto seed = clauses(kUnique, 0, 2999);
    auto late = clauses(kUnique, 20000, 20999);
    seed.insert(seed.end(), late.begin(), late.end());
    buckets.must.push_back(std::move(seed));
    buckets.must.push_back(clauses(kMulti, 0, 1499));
    buckets.must.push_back(clauses(kParity, 0, 0));
    buckets.must_not = clauses(kHead, 0, 0);
    buckets.exclude_fills.push_back(
      prepare(kSingle, 5000, 5999).PlanFill({}, irs::ScoreMergeType::Noop));
    buckets.masked = &segment;
    return buckets;
  };
  const auto expected = [&](doc_id_t d) {
    const auto row = d - irs::doc_limits::min();
    const auto single = SingleOf(row);
    return _live[row] && (row < 3000 || (20000 <= row && row < 21000)) &&
           absl::c_any_of(MultiOf(row), [](int64_t v) { return v < 1500; }) &&
           row % 2 == 0 && row >= kHeadRows && (single < 5000 || single > 5999);
  };
  const auto check = [&](irs::detail::BitsetBuckets& buckets, doc_id_t min,
                         doc_id_t max) {
    SCOPED_TRACE(absl::StrCat(min, "..", max));
    irs::detail::BitsetStorage bits{max - min};
    irs::detail::FillBitset(buckets, *doc, docs_count, min, max, bits.Words());
    uint64_t found = 0;
    for (auto d = min; d != max; ++d) {
      const auto offset = d - min;
      const bool bit = ((bits.Words()[offset / 64] >> (offset % 64)) & 1) != 0;
      EXPECT_EQ(expected(d), bit) << d;
      found += bit;
    }
    return found;
  };

  const auto end = irs::doc_limits::min() + docs_count;
  auto whole = make();
  ASSERT_EQ(1, whole.must.back().size());
  ASSERT_EQ(1, whole.must_not.size());
  EXPECT_NE(0, check(whole, irs::doc_limits::min(), end));

  auto split = make();
  constexpr doc_id_t kParts[][2]{
    {irs::doc_limits::min(), 500}, {9001, 16384}, {16384, 20001}};
  for (const auto& [min, max] : kParts) {
    check(split, min, max);
  }
  check(split, 20001, end);
}

INSTANTIATE_TEST_SUITE_P(count_single_valued_test, CountSingleValuedTest,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>)),
                         CountSingleValuedTest::to_string);

}  // namespace
