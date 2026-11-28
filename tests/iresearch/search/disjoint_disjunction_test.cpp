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
#include <iresearch/search/lead/concept.hpp>
#include <iresearch/search/lead/disjoint_disjunction.hpp>
#include <random>
#include <span>
#include <vector>

#include "tests_shared.hpp"

namespace {

using irs::doc_id_t;
namespace doc_limits = irs::doc_limits;

class ListLead {
 public:
  ListLead() = default;

  void Reset(std::span<const doc_id_t> docs) {
    _docs.assign(docs.begin(), docs.end());
    _docs.push_back(doc_limits::eof());
  }

  doc_id_t Next() {
    if (doc_limits::eof(_doc)) {
      return _doc;
    }
    return _doc = _docs[_at++];
  }

  doc_id_t Seek(doc_id_t target) {
    if (target <= _doc) {
      return _doc;
    }
    _at = static_cast<size_t>(
      std::lower_bound(_docs.begin() + _at, _docs.end(), target) -
      _docs.begin());
    return _doc = _docs[_at++];
  }

 private:
  std::vector<doc_id_t> _docs;
  size_t _at = 0;
  doc_id_t _doc = doc_limits::invalid();
};

using Lists = std::vector<std::vector<doc_id_t>>;

std::vector<doc_id_t> Merged(const Lists& lists) {
  std::vector<doc_id_t> docs;
  for (const auto& list : lists) {
    docs.insert(docs.end(), list.begin(), list.end());
  }
  absl::c_sort(docs);
  docs.erase(std::ranges::unique(docs).begin(), docs.end());
  return docs;
}

class Union : public irs::lead::DisjointDisjunction<ListLead> {
 public:
  Union(irs::PlanMemory& memory, const Lists& lists)
    : DisjointDisjunction{memory, lists.size(), [&](ListLead& lead, size_t i) {
                            lead.Reset(lists[i]);
                          }} {}
};

std::vector<doc_id_t> Random(std::mt19937& rng, size_t size, doc_id_t from,
                             doc_id_t to) {
  std::uniform_int_distribution<doc_id_t> pick{from, to};
  std::vector<doc_id_t> docs(size);
  for (auto& doc : docs) {
    doc = pick(rng);
  }
  absl::c_sort(docs);
  docs.erase(std::ranges::unique(docs).begin(), docs.end());
  return docs;
}

std::vector<Lists> Cases() {
  std::mt19937 rng{17};
  std::vector<Lists> cases{
    {},
    {{}},
    {{}, {}},
    {{1}},
    {{5}, {5}},
    {{1, 2, 3}, {1, 2, 3}},
    {{1, 3, 5}, {2, 4, 6}},
    {{1, 2, 3}, {10, 11}, {20}},
    {{100}, {1, 2, 3}, {}},
    {{doc_limits::eof() - 1}, {1}},
  };
  for (const size_t lists : {2, 3, 16, 300}) {
    Lists overlapping;
    Lists disjoint;
    for (size_t i = 0; i != lists; ++i) {
      overlapping.push_back(Random(rng, 1 + i * 7 % 500, 1, 20000));
      const auto from = static_cast<doc_id_t>(1 + i * 1000);
      disjoint.push_back(Random(rng, 1 + i % 200, from, from + 999));
    }
    cases.push_back(std::move(overlapping));
    cases.push_back(std::move(disjoint));
  }
  return cases;
}

TEST(disjoint_disjunction_test, next_walks_the_merged_lists) {
  for (const auto& lists : Cases()) {
    SCOPED_TRACE(lists.size());
    const auto expected = Merged(lists);
    irs::PlanMemory memory;
    Union docs{memory, lists};
    for (const auto doc : expected) {
      ASSERT_EQ(doc, docs.Next());
    }
    ASSERT_TRUE(doc_limits::eof(docs.Next()));
    ASSERT_TRUE(doc_limits::eof(docs.Next()));
    ASSERT_TRUE(doc_limits::eof(docs.Seek(1)));
  }
}

TEST(disjoint_disjunction_test,
     seek_lands_on_the_first_doc_at_or_past_the_target) {
  std::mt19937 rng{23};
  for (const auto& lists : Cases()) {
    const auto expected = Merged(lists);
    for (const doc_id_t step : {1, 3, 97, 4096}) {
      SCOPED_TRACE(testing::Message() << lists.size() << ' ' << step);
      irs::PlanMemory memory;
      Union docs{memory, lists};
      doc_id_t current = doc_limits::invalid();
      for (doc_id_t target = 1; target < 21000; target += 1 + rng() % step) {
        const auto it = absl::c_lower_bound(expected, target);
        const auto want = target <= current      ? current
                          : it == expected.end() ? doc_limits::eof()
                                                 : *it;
        current = docs.Seek(target);
        ASSERT_EQ(want, current) << target;
        if (doc_limits::eof(current)) {
          break;
        }
        if (rng() % 2 == 0) {
          const auto next =
            std::upper_bound(expected.begin(), expected.end(), current);
          current = docs.Next();
          ASSERT_EQ(next == expected.end() ? doc_limits::eof() : *next,
                    current);
          if (doc_limits::eof(current)) {
            break;
          }
        }
      }
    }
  }
}

TEST(disjoint_disjunction_test, gather_fills_each_window) {
  for (const auto& lists : Cases()) {
    const auto expected = Merged(lists);
    for (const doc_id_t window : {1, 7, 64, 4096}) {
      SCOPED_TRACE(testing::Message() << lists.size() << ' ' << window);
      irs::PlanMemory memory;
      Union docs{memory, lists};
      std::vector<doc_id_t> buf(window);
      std::vector<doc_id_t> actual;
      auto min = doc_limits::min();
      for (; min < 21000; min += window) {
        const auto n =
          irs::lead::GatherAll(docs, min, min + window, buf.data());
        actual.insert(actual.end(), buf.begin(), buf.begin() + n);
      }
      ASSERT_EQ(std::vector<doc_id_t>(expected.begin(),
                                      absl::c_lower_bound(expected, min)),
                actual);
    }
  }
}

}  // namespace
