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
#include <absl/strings/str_join.h>

#include <algorithm>
#include <bit>
#include <iresearch/search/count/root.hpp>
#include <iresearch/search/detail/table_filter.hpp>
#include <iresearch/search/docs/root.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/queries/docs_mask_query.hpp>
#include <iterator>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "index/index_tests.hpp"
#include "tests_shared.hpp"

namespace {

using irs::doc_id_t;

inline constexpr irs::field_id kBodyId = 1;
inline constexpr doc_id_t kDocs = 150000;

uint64_t Mix(uint64_t x) noexcept {
  x ^= x >> 33;
  x *= 0xff51afd7ed558ccdULL;
  x ^= x >> 33;
  x *= 0xc4ceb3f99c2b4ad7ULL;
  x ^= x >> 33;
  return x;
}

bool Chance(uint64_t salt, doc_id_t i, uint64_t permille) noexcept {
  return Mix(salt * kDocs + i) % 1000 < permille;
}

struct Spec {
  std::string_view term;
  bool (*has)(doc_id_t);
};

inline constexpr Spec kSpecs[]{
  {"run", [](doc_id_t i) { return i >= 10000 && i < 30000; }},
  {"half", [](doc_id_t i) { return Chance(1, i, 500); }},
  {"dense", [](doc_id_t i) { return Chance(2, i, 300); }},
  {"mid", [](doc_id_t i) { return Chance(3, i, 50); }},
  {"sparse", [](doc_id_t i) { return Chance(4, i, 10); }},
  {"rare", [](doc_id_t i) { return Chance(5, i, 1); }},
  {"single", [](doc_id_t i) { return i == 77777; }},
  {"holes", [](doc_id_t i) { return i >= 50000 && i < 60000 && i % 37 != 0; }},
  {"cluster",
   [](doc_id_t i) {
     return i >= 100000 && i < 110000 ? Chance(6, i, 500) : Chance(6, i, 5);
   }},
};

bool Has(std::string_view term, doc_id_t i) {
  for (const auto& spec : kSpecs) {
    if (spec.term == term) {
      return spec.has(i);
    }
  }
  return false;
}

bool Visible(doc_id_t doc) noexcept { return doc % 3 != 0; }

class EveryThirdOut final : public irs::detail::TableFilter {
 public:
  doc_id_t Live(doc_id_t doc) final { return Visible(doc) ? doc : doc + 1; }

  bool Foldable() const noexcept final { return true; }

  uint64_t CountAndClear(doc_id_t base, uint64_t* mask, uint32_t words) final {
    uint64_t count = 0;
    for (uint32_t w = 0; w != words; ++w) {
      for (auto word = mask[w]; word != 0; word &= word - 1) {
        count += Visible(base + w * 64 +
                         static_cast<doc_id_t>(std::countr_zero(word)));
      }
      mask[w] = 0;
    }
    return count;
  }

  uint32_t Narrow(doc_id_t* docs, irs::score_t* scores, uint32_t n) final {
    uint32_t kept = 0;
    for (uint32_t i = 0; i != n; ++i) {
      if (Visible(docs[i])) {
        docs[kept] = docs[i];
        if (scores != nullptr) {
          scores[kept] = scores[i];
        }
        ++kept;
      }
    }
    return kept;
  }

  uint32_t Narrow(doc_id_t base, const uint64_t* in, uint64_t* out,
                  irs::score_t*, uint32_t words) final {
    for (uint32_t w = 0; w != words; ++w) {
      uint64_t kept = 0;
      for (auto word = in[w]; word != 0; word &= word - 1) {
        const auto bit = std::countr_zero(word);
        if (Visible(base + w * 64 + static_cast<doc_id_t>(bit))) {
          kept |= uint64_t{1} << bit;
        }
      }
      out[w] = kept;
    }
    return 0;
  }
};

irs::TermClause Clause(std::string_view value) {
  return {.field = kBodyId,
          .term = irs::bstring{irs::ViewCast<irs::byte_type>(value)}};
}

struct Shape {
  std::vector<std::string_view> must;
  std::vector<std::string_view> must_not;
};

class BooleanWindowTest : public tests::IndexTestBase {
 protected:
  void Build() {
    auto writer = open_writer(irs::kOmCreate);
    auto body = std::make_shared<tests::TextField<std::string>>("body");
    body->id = kBodyId;
    tests::Document doc;
    doc.insert(body, true, false);
    for (doc_id_t i = 0; i != kDocs; ++i) {
      std::string text = "doc";
      for (const auto& spec : kSpecs) {
        if (spec.has(i)) {
          absl::StrAppend(&text, " ", spec.term);
        }
      }
      body->value(text);
      ASSERT_TRUE(Insert(*writer, doc));
    }
    writer->RefreshCommit();
  }

  void Check(const Shape& shape) {
    SCOPED_TRACE(absl::StrCat("+", absl::StrJoin(shape.must, " +"), " -",
                              absl::StrJoin(shape.must_not, " -")));
    irs::BooleanFilter filter;
    for (const auto term : shape.must) {
      filter.Add(Clause(term), irs::Occur::Must);
    }
    for (const auto term : shape.must_not) {
      filter.Add(Clause(term), irs::Occur::MustNot);
    }
    std::vector<doc_id_t> expected;
    for (doc_id_t i = 0; i != kDocs; ++i) {
      const auto has = [&](std::string_view term) { return Has(term, i); };
      if (absl::c_all_of(shape.must, has) &&
          absl::c_none_of(shape.must_not, has)) {
        expected.push_back(irs::doc_limits::min() + i);
      }
    }

    auto reader = open_reader();
    ASSERT_EQ(1, reader.size());
    const auto& segment = reader[0];
    auto query = irs::PrepareMasked(filter, segment, {});
    ASSERT_NE(nullptr, query);
    const auto end = irs::doc_limits::min() + kDocs;

    {
      auto count = query->PlanCount({});
      ASSERT_NE(nullptr, count);
      ASSERT_EQ(expected.size(),
                count->Run(irs::doc_limits::min(), irs::doc_limits::eof()) +
                  count->Finish());
    }
    {
      auto split = query->PlanCount({.partial = true});
      ASSERT_NE(nullptr, split);
      constexpr doc_id_t kBounds[]{
        irs::doc_limits::min(), 777, 10001, 30001, 54321, 65536, 100003, 109999,
        irs::doc_limits::eof()};
      uint64_t parts = 0;
      for (size_t i = 1; i != std::size(kBounds); ++i) {
        parts += split->Run(kBounds[i - 1], kBounds[i]);
      }
      ASSERT_EQ(expected.size(), parts + split->Finish());
    }
    {
      auto fine = query->PlanCount({.partial = true});
      ASSERT_NE(nullptr, fine);
      uint64_t pieces = 0;
      doc_id_t from = irs::doc_limits::min();
      for (doc_id_t step = 1; from < end; step = step * 7 % 5003 + 1) {
        const auto to = std::min<doc_id_t>(from + step, end);
        pieces += fine->Run(from, to);
        from = to;
      }
      pieces += fine->Run(end, irs::doc_limits::eof()) + fine->Finish();
      ASSERT_EQ(expected.size(), pieces);
    }
    {
      constexpr doc_id_t kSpan = 9973;
      uint64_t ranged = 0;
      std::vector<doc_id_t> actual;
      std::vector<doc_id_t> buf(kSpan + irs::doc_limits::kDocsSlack);
      for (doc_id_t from = irs::doc_limits::min(); from < end; from += kSpan) {
        const auto to = std::min<doc_id_t>(from + kSpan, end);
        auto count = query->PlanCount({.span = kSpan, .partial = true});
        ASSERT_NE(nullptr, count);
        ranged += count->Run(from, to) + count->Finish();
        auto docs = query->PlanDocs({.span = kSpan});
        ASSERT_NE(nullptr, docs);
        const auto n = docs->Run(from, to, buf.data());
        actual.insert(actual.end(), buf.begin(), buf.begin() + n);
      }
      ASSERT_EQ(expected.size(), ranged);
      ASSERT_EQ(expected, actual);
    }
    {
      EveryThirdOut table;
      auto narrowed = query->PlanCount({.table = &table});
      ASSERT_NE(nullptr, narrowed);
      ASSERT_EQ(absl::c_count_if(expected, Visible),
                narrowed->Run(irs::doc_limits::min(), irs::doc_limits::eof()) +
                  narrowed->Finish());
    }
    for (const doc_id_t window : {doc_id_t{3000}, doc_id_t{211}}) {
      auto docs = query->PlanDocs({});
      ASSERT_NE(nullptr, docs);
      std::vector<doc_id_t> buf(window + irs::doc_limits::kDocsSlack);
      std::vector<doc_id_t> actual;
      for (doc_id_t min = irs::doc_limits::min(); min < end; min += window) {
        const auto n = docs->Run(min, std::min(min + window, end), buf.data());
        actual.insert(actual.end(), buf.begin(), buf.begin() + n);
      }
      ASSERT_EQ(expected, actual) << window;
    }
  }
};

TEST_P(BooleanWindowTest, conjunctions_across_block_kinds) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {{"mid", "dense"}, {}},
         {{"sparse", "half"}, {}},
         {{"rare", "dense"}, {}},
         {{"dense", "half"}, {}},
         {{"run", "mid"}, {}},
         {{"run", "dense"}, {}},
         {{"holes", "sparse"}, {}},
         {{"holes", "dense"}, {}},
         {{"holes", "run"}, {}},
         {{"single", "half"}, {}},
         {{"single", "rare"}, {}},
         {{"rare", "sparse"}, {}},
         {{"mid", "dense", "half"}, {}},
         {{"sparse", "mid", "dense", "half"}, {}},
         {{"rare", "mid", "run"}, {}},
       }) {
    Check(shape);
  }
}

TEST_P(BooleanWindowTest, lead_switches_between_windows_and_batches) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {{"cluster", "half"}, {}},
         {{"cluster", "sparse"}, {}},
         {{"cluster", "mid", "dense"}, {}},
         {{"cluster", "dense"}, {"half"}},
         {{"mid", "cluster"}, {"sparse"}},
       }) {
    Check(shape);
  }
}

TEST_P(BooleanWindowTest, exclusions) {
  Build();
  for (const auto& shape : std::vector<Shape>{
         {{"dense"}, {"mid"}},
         {{"mid"}, {"half"}},
         {{"sparse"}, {"dense", "run"}},
         {{"half"}, {"cluster", "single"}},
         {{"run"}, {"holes", "rare"}},
         {{"sparse", "dense"}, {"run", "holes"}},
         {{"mid", "half"}, {"sparse"}},
       }) {
    Check(shape);
  }
}

INSTANTIATE_TEST_SUITE_P(boolean_window_test, BooleanWindowTest,
                         ::testing::Combine(::testing::Values(
                           &tests::Directory<&tests::MemoryDirectory>)),
                         BooleanWindowTest::to_string);

}  // namespace
