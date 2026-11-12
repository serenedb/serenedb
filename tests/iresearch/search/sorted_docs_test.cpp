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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <iresearch/search/detail/sorted_build.hpp>
#include <iresearch/search/docs/sorted_docs.hpp>
#include <iresearch/search/lead/sorted_docs.hpp>
#include <numeric>
#include <random>
#include <vector>

#include "tests_shared.hpp"

namespace {

using irs::doc_id_t;
namespace doc_limits = irs::doc_limits;

std::vector<doc_id_t> Reference(std::vector<doc_id_t> docs) {
  absl::c_sort(docs);
  docs.erase(std::ranges::unique(docs).begin(), docs.end());
  return docs;
}

TEST(sorted_docs_test, sort_orders_and_keeps_every_doc) {
  std::mt19937 rng{5};
  for (const size_t size : {0, 1, 2, 100, 2047, 2048, 2049, 50000}) {
    for (const doc_id_t span : {doc_id_t{64}, doc_id_t{4096},
                                doc_id_t{50'000'000}, doc_limits::eof() - 1}) {
      SCOPED_TRACE(testing::Message() << size << ' ' << span);
      std::uniform_int_distribution<doc_id_t> pick{doc_limits::min(), span};
      std::vector<doc_id_t> docs(size);
      for (auto& doc : docs) {
        doc = pick(rng);
      }
      auto expected = docs;
      absl::c_sort(expected);
      irs::detail::SortDocs(docs, {});
      ASSERT_EQ(expected, docs);
      irs::detail::SortDocs(docs, {});
      ASSERT_EQ(expected, docs);
    }
  }
}

TEST(sorted_docs_test, sort_concatenates_disjoint_runs_in_order) {
  std::mt19937 rng{7};
  for (const doc_id_t run : {doc_id_t{1}, doc_id_t{3}, doc_id_t{700}}) {
    for (const size_t runs : {2, 5, 300}) {
      SCOPED_TRACE(testing::Message() << run << ' ' << runs);
      std::vector<doc_id_t> order(runs);
      std::iota(order.begin(), order.end(), doc_id_t{0});
      std::shuffle(order.begin(), order.end(), rng);
      std::vector<doc_id_t> docs;
      std::vector<uint32_t> starts;
      for (const auto r : order) {
        starts.push_back(static_cast<uint32_t>(docs.size()));
        for (doc_id_t i = 0; i != run; ++i) {
          docs.push_back(doc_limits::min() + r * run * 2 + i);
        }
      }
      auto expected = docs;
      absl::c_sort(expected);
      irs::detail::SortDocs(docs, starts);
      ASSERT_EQ(expected, docs);
    }
  }
  const std::vector<uint32_t> two{0, 3};
  std::vector<doc_id_t> touching{50, 51, 52, 10, 11, 52, 53};
  irs::detail::SortDocs(touching, two);
  ASSERT_EQ((std::vector<doc_id_t>{10, 11, 50, 51, 52, 52, 53}), touching);
  std::vector<doc_id_t> overlapping{5, 30, 70, 10, 40, 60};
  irs::detail::SortDocs(overlapping, two);
  ASSERT_EQ((std::vector<doc_id_t>{5, 10, 30, 40, 60, 70}), overlapping);
}

TEST(sorted_docs_test, lead_walks_and_seeks_like_the_list) {
  std::mt19937 rng{9};
  std::uniform_int_distribution<doc_id_t> pick{doc_limits::min(), 100000};
  for (const size_t size : {0, 1, 7, 3000}) {
    SCOPED_TRACE(size);
    std::vector<doc_id_t> raw(size);
    for (auto& doc : raw) {
      doc = pick(rng);
    }
    const auto expected = Reference(raw);
    auto docs = expected;
    docs.push_back(doc_limits::eof());

    irs::lead::SortedDocs walk{std::vector<doc_id_t>{docs}};
    for (const auto doc : expected) {
      ASSERT_EQ(doc, walk.Next());
    }
    ASSERT_TRUE(doc_limits::eof(walk.Next()));
    ASSERT_TRUE(doc_limits::eof(walk.Next()));
    ASSERT_TRUE(doc_limits::eof(walk.Seek(1)));

    irs::lead::SortedDocs seek{std::vector<doc_id_t>{docs}};
    doc_id_t current = doc_limits::invalid();
    for (doc_id_t target = 1; target < 100010; target += 1 + target % 997) {
      const auto it = absl::c_lower_bound(expected, target);
      const auto want = target <= current      ? current
                        : it == expected.end() ? doc_limits::eof()
                                               : *it;
      current = seek.Seek(target);
      ASSERT_EQ(want, current) << target;
      if (doc_limits::eof(current)) {
        break;
      }
    }
  }
}

TEST(sorted_docs_test, docs_root_emits_each_window_and_reports_the_next) {
  constexpr doc_id_t kEnd = 20001;
  std::mt19937 rng{3};
  std::uniform_int_distribution<doc_id_t> pick{doc_limits::min(), kEnd - 1};
  for (const size_t size : {0, 1, 5, 4000}) {
    std::vector<doc_id_t> raw(size);
    for (auto& doc : raw) {
      doc = pick(rng);
    }
    const auto expected = Reference(raw);
    auto docs = expected;
    docs.push_back(doc_limits::eof());
    for (const doc_id_t window : {1, 7, 2048}) {
      for (const bool skip : {false, true}) {
        SCOPED_TRACE(testing::Message()
                     << size << ' ' << window << ' ' << skip);
        irs::docs::SortedDocs root{std::vector<doc_id_t>{docs}};
        std::vector<doc_id_t> buf(window + doc_limits::kDocsSlack);
        std::vector<doc_id_t> actual;
        for (doc_id_t min = doc_limits::min(); min < kEnd;) {
          const auto max = min + window;
          const auto n = root.Run(min, max, buf.data());
          actual.insert(actual.end(), buf.begin(), buf.begin() + n);
          const auto next = root.NextDoc();
          ASSERT_GE(next, max);
          ASSERT_EQ(absl::c_lower_bound(expected, max),
                    absl::c_lower_bound(expected, next));
          min = skip ? std::min(next, kEnd) : max;
        }
        ASSERT_EQ(expected, actual);
      }
    }
  }
}

}  // namespace
