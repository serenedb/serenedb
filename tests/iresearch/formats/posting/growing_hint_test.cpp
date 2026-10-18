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
#include <cstdint>
#include <iresearch/formats/posting/common.hpp>
#include <span>
#include <vector>

#include "gtest/gtest.h"

namespace {

namespace fu = irs::file_utils;

struct Range {
  uint64_t begin = 0;
  uint64_t end = 0;
};

class RecordingInput {
 public:
  explicit RecordingInput(std::vector<Range>& hints,
                          const std::vector<bool>& resident,
                          uint64_t& probes) noexcept
    : _hints{&hints}, _resident{&resident}, _probes{&probes} {}

  bool Warm(uint64_t, uint64_t) const noexcept { return false; }
  bool Resident(uint64_t offset, uint64_t size) const noexcept {
    ++*_probes;
    for (auto page = offset / fu::kPage; page * fu::kPage < offset + size;
         ++page) {
      if (!(*_resident)[page]) {
        return false;
      }
    }
    return true;
  }
  void MarkWarm(uint64_t, uint64_t) const noexcept {}
  void Prefetch(uint64_t offset, uint64_t size) const {
    _hints->emplace_back(offset, offset + size);
  }

 private:
  std::vector<Range>* _hints;
  const std::vector<bool>* _resident;
  uint64_t* _probes;
};

struct Walked {
  std::vector<uint64_t> cold;
  uint64_t hinted = 0;
  uint64_t hinted_end = 0;
  uint64_t probes = 0;
};

std::vector<bool> Pages(uint64_t stop) {
  return std::vector<bool>(stop / fu::kPage + 1);
}

std::vector<bool> PagesBelow(uint64_t stop, uint64_t end) {
  auto pages = Pages(stop);
  for (uint64_t page = 0; (page + 1) * fu::kPage <= end; ++page) {
    pages[page] = true;
  }
  return pages;
}

template<typename Hint>
Walked Walk(std::span<const uint64_t> loads, uint64_t stop,
            const std::vector<bool>& resident) {
  std::vector<Range> hints;
  Walked walked;
  const RecordingInput in{hints, resident, walked.probes};
  auto warm = resident;
  Hint hint;
  hint.Arm(loads.front(), stop);
  for (const auto at : loads) {
    if (!warm[at / fu::kPage]) {
      walked.cold.push_back(at);
      warm[at / fu::kPage] = true;
    }
    const auto issued = hints.size();
    hint.Advance(in, at);
    for (auto i = issued; i != hints.size(); ++i) {
      for (auto page = hints[i].begin / fu::kPage;
           page * fu::kPage < hints[i].end; ++page) {
        warm[page] = true;
      }
      walked.hinted += hints[i].end - hints[i].begin;
      walked.hinted_end = std::max(walked.hinted_end, hints[i].end);
    }
  }
  return walked;
}

template<typename Hint>
Walked Walk(std::span<const uint64_t> loads, uint64_t stop) {
  return Walk<Hint>(loads, stop, Pages(stop));
}

std::vector<uint64_t> TreeWalk(uint32_t fanout, uint32_t depth, uint64_t block,
                               uint32_t leaf_every, uint64_t& next) {
  std::vector<uint64_t> children;
  for (uint32_t i = 0; depth != 0 && i != fanout; ++i) {
    const auto child = TreeWalk(fanout, depth - 1, block, leaf_every, next);
    if (depth > 1 || i % leaf_every == 0) {
      children.insert(children.end(), child.begin(), child.end());
    }
  }
  std::vector<uint64_t> loads{next};
  next += block;
  loads.insert(loads.end(), children.begin(), children.end());
  return loads;
}

TEST(TreeWalkHintTest, FullWalkPrefetchesTheWalkedRange) {
  uint64_t size = 0;
  const auto loads = TreeWalk(16, 3, 4096, 1, size);
  const auto walked = Walk<irs::TreeWalkHint>(loads, 4 * size);
  EXPECT_LE(walked.cold.size(), 16U);
  EXPECT_LE(walked.hinted, size + 2 * fu::kMaxReadahead);
  EXPECT_LE(walked.hinted_end, size + 2 * fu::kMaxReadahead);
}

TEST(TreeWalkHintTest, SelectiveWalkPrefetchesTheWalkedRange) {
  uint64_t size = 0;
  const auto loads = TreeWalk(32, 3, 2048, 8, size);
  const auto walked = Walk<irs::TreeWalkHint>(loads, 4 * size);
  EXPECT_LE(walked.cold.size(), 32U);
  EXPECT_LE(walked.hinted_end, size + 2 * fu::kMaxReadahead);
}

TEST(TreeWalkHintTest, ResidentRangeIsNotPrefetched) {
  uint64_t size = 0;
  const auto loads = TreeWalk(16, 3, 4096, 1, size);
  const auto walked =
    Walk<irs::TreeWalkHint>(loads, 4 * size, PagesBelow(4 * size, 4 * size));
  EXPECT_EQ(walked.hinted, 0U);
  EXPECT_TRUE(walked.cold.empty());
}

TEST(TreeWalkHintTest, ResidentLoadsAreNotPrefetched) {
  uint64_t size = 0;
  const auto loads = TreeWalk(16, 3, 4096, 1, size);
  auto resident = Pages(4 * size);
  for (const auto at : loads) {
    resident[at / fu::kPage] = true;
  }
  const auto walked = Walk<irs::TreeWalkHint>(loads, 4 * size, resident);
  EXPECT_EQ(walked.hinted, 0U);
  EXPECT_TRUE(walked.cold.empty());
}

TEST(TreeWalkHintTest, WarmWalkProbesScaleWithBytes) {
  uint64_t size = 0;
  const auto loads = TreeWalk(32, 4, 512, 1, size);
  const auto stop = size + 8 * fu::kMaxReadahead;
  const auto walked =
    Walk<irs::TreeWalkHint>(loads, stop, PagesBelow(stop, stop));
  EXPECT_EQ(walked.hinted, 0U);
  EXPECT_LE(walked.probes, 16 + 4 * size / fu::kMaxReadahead);
}

TEST(TreeWalkHintTest, ColdTailIsPrefetched) {
  uint64_t size = 0;
  const auto loads = TreeWalk(16, 3, 4096, 1, size);
  const auto resident = size / 2;
  const auto walked =
    Walk<irs::TreeWalkHint>(loads, 4 * size, PagesBelow(4 * size, resident));
  EXPECT_LE(walked.cold.size(), 16U);
  EXPECT_LE(walked.hinted, size - resident + 3 * fu::kMaxReadahead);
}

TEST(TreeWalkHintTest, SparseLoadsAreNotPrefetched) {
  std::vector<uint64_t> loads;
  for (uint64_t i = 0; i != 64; ++i) {
    loads.push_back(i * 1024 * 1024);
  }
  const auto walked = Walk<irs::TreeWalkHint>(loads, 128 * 1024 * 1024);
  EXPECT_EQ(walked.hinted, 0U);
  EXPECT_EQ(walked.cold.size(), loads.size());
}

TEST(TreeWalkHintTest, AncestorLoadsDoNotMoveTheWindow) {
  uint64_t size = 0;
  const auto tree = TreeWalk(16, 3, 4096, 1, size);
  const auto ancestor = size + 64 * 1024 * 1024;
  std::vector<uint64_t> loads;
  for (size_t i = 0; i != tree.size(); ++i) {
    loads.push_back(tree[i]);
    if (i % 300 == 299) {
      loads.push_back(ancestor);
    }
  }
  const auto walked = Walk<irs::TreeWalkHint>(loads, 4 * ancestor);
  EXPECT_LE(
    absl::c_count_if(walked.cold, [&](uint64_t at) { return at < size; }), 16);
  EXPECT_LE(walked.hinted_end, size + 2 * fu::kMaxReadahead);
}

TEST(GrowingHintTest, SequentialLoadsArePrefetchedAhead) {
  std::vector<uint64_t> loads;
  for (uint64_t at = 0; at < 8 * 1024 * 1024; at += 1024) {
    loads.push_back(at);
  }
  const auto walked = Walk<irs::GrowingHint>(loads, 64 * 1024 * 1024);
  EXPECT_LE(walked.cold.size(), 8U);
  EXPECT_LE(walked.hinted_end, 8 * 1024 * 1024 + 2 * fu::kMaxReadahead);
}

}  // namespace
