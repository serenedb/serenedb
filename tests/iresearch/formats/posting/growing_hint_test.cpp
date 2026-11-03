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

  uint64_t Warm(uint64_t, uint64_t) const noexcept { return 0; }
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
