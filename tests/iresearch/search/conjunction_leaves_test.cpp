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
#include <iresearch/search/detail/conjunction_leaves.hpp>
#include <iterator>
#include <numeric>
#include <random>
#include <vector>

#include "tests_shared.hpp"

namespace {

using irs::doc_id_t;
using irs::detail::FirstCommon;

std::vector<doc_id_t> Docs(std::mt19937& rng, size_t len, doc_id_t range) {
  std::vector<doc_id_t> all(range);
  std::iota(all.begin(), all.end(), irs::doc_limits::min());
  std::vector<doc_id_t> docs;
  std::sample(all.begin(), all.end(), std::back_inserter(docs),
              std::min<size_t>(len, range), rng);
  return docs;
}

}  // namespace

TEST(FirstCommonTest, never_skips_a_common_doc) {
  std::mt19937 rng{20261105};
  for (size_t round = 0; round != 20000; ++round) {
    const auto range = std::uniform_int_distribution<doc_id_t>{8, 600}(rng);
    const auto a =
      Docs(rng, std::uniform_int_distribution<size_t>{0, 48}(rng), range);
    const auto b =
      Docs(rng, std::uniform_int_distribution<size_t>{1, 48}(rng), range);
    std::vector<doc_id_t> common;
    absl::c_set_intersection(a, b, std::back_inserter(common));
    const auto target = FirstCommon(a, b);
    SCOPED_TRACE(round);
    ASSERT_LE(b.front(), target);
    if (!common.empty()) {
      ASSERT_LE(target, common.front());
    }
    ASSERT_TRUE(absl::c_binary_search(b, target) || target == b.back() + 1);
    for (const auto doc : b) {
      if (doc < target) {
        ASSERT_TRUE(!a.empty() && doc < a.back());
      }
    }
    if (a.size() % irs::detail::kMergeBlock == 0 &&
        b.size() % irs::detail::kMergeBlock == 0 && !common.empty()) {
      ASSERT_EQ(common.front(), target);
    }
  }
}
