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

#include "iresearch/formats/posting/skip_column.hpp"

#include <algorithm>
#include <random>
#include <vector>

#include "gtest/gtest.h"
#include "iresearch/store/memory_directory.hpp"

namespace {

using namespace irs;

using Columns = std::vector<std::vector<uint32_t>>;

// Round-trips one column set, so a test only has to say what it wrote and
// then ask for it back. The region deliberately starts at an odd byte to
// prove the writer's own alignment padding is what `packed::At` relies on.
class RoundTrip {
 public:
  explicit RoundTrip(const Columns& cols) {
    _columns = static_cast<uint32_t>(cols.size());
    _count = cols.empty() ? 0 : cols.front().size();

    uint64_t dir_start = 0;
    {
      SkipColumnsWriter writer{IResourceManager::gNoop};
      auto out = _dir.create("c");
      out->WriteByte(0xAB);
      writer.Reset(_columns, *out);
      _origin = out->Position();

      std::vector<uint32_t> entry(_columns);
      for (uint64_t i = 0; i != _count; ++i) {
        for (uint32_t c = 0; c != _columns; ++c) {
          entry[c] = cols[c][i];
        }
        writer.Push(entry.data(), *out);
      }
      dir_start = writer.Finish(*out);
      EXPECT_EQ(_count, writer.Size());
      out->Flush();
    }

    _data = _dir.open("c", IOAdvice::RANDOM);
    auto dir_in = _dir.open("c", IOAdvice::RANDOM);
    const auto groups = math::DivCeil64(_count, kSkipBlockSize);
    _dir_bytes.resize(groups * SkipDirStride(_columns));
    if (!_dir_bytes.empty()) {
      dir_in->Seek(dir_start);
      dir_in->ReadData(_dir_bytes.data(), _dir_bytes.size());
    }
    _reader.Prepare(_dir_bytes.data(), _count, _columns, _origin, *_data);
  }

  SkipColumnsReader<>& Reader() noexcept { return _reader; }
  uint64_t Count() const noexcept { return _count; }
  uint32_t ColumnCount() const noexcept { return _columns; }

 private:
  MemoryDirectory _dir;
  IndexInput::ptr _data;
  std::vector<byte_type> _dir_bytes;
  SkipColumnsReader<> _reader;
  uint64_t _origin = 0;
  uint64_t _count = 0;
  uint32_t _columns = 0;
};

void ExpectRoundTrip(const Columns& cols) {
  RoundTrip rt{cols};
  for (uint32_t c = 0; c != rt.ColumnCount(); ++c) {
    for (uint64_t i = 0; i != rt.Count(); ++i) {
      ASSERT_EQ(cols[c][i], rt.Reader().Get(c, i)) << "col " << c << " at " << i;
    }
  }
}

TEST(SkipColumns, SingleValue) { ExpectRoundTrip({{42}}); }

// The common tier-3 case: a handful of entries sharing a group with the
// entries of other terms.
TEST(SkipColumns, ShortTail) { ExpectRoundTrip({{7, 19, 20, 5000}}); }

// All values equal -> width 0, no payload bytes at all.
TEST(SkipColumns, AllEqualCostsNoPayload) {
  ExpectRoundTrip({std::vector<uint32_t>(300, 1234)});
}

TEST(SkipColumns, ExactlyOneGroup) {
  std::vector<uint32_t> values(kSkipBlockSize);
  for (uint32_t i = 0; i != kSkipBlockSize; ++i) {
    values[i] = i * 7;
  }
  ExpectRoundTrip({values});
}

TEST(SkipColumns, FullWidthValues) {
  ExpectRoundTrip({{0, std::numeric_limits<uint32_t>::max(), 1,
                    std::numeric_limits<uint32_t>::max() - 1}});
}

// Each group picks its own base and width, so a run of tight values next to a
// run of sparse ones must not force one width on both.
TEST(SkipColumns, PerGroupWidthAndBase) {
  std::vector<uint32_t> values;
  for (uint32_t i = 0; i != kSkipBlockSize; ++i) {
    values.push_back(1'000'000 + i);  // tight, high base
  }
  for (uint32_t i = 0; i != kSkipBlockSize; ++i) {
    values.push_back(i * 1'000'000);  // sparse, low base
  }
  ExpectRoundTrip({values});
}

// What a real field looks like: columns of very different widths sharing one
// group, which is what the per-column prefix sum inside a group has to get
// right.
TEST(SkipColumns, MixedWidthsShareAGroup) {
  std::mt19937 rng{20260813};
  const uint64_t n = 7 * kSkipBlockSize + 61;
  Columns cols(6, std::vector<uint32_t>(n));
  for (uint64_t i = 0; i != n; ++i) {
    cols[0][i] = static_cast<uint32_t>(i * 4099);  // docs, wide
    cols[1][i] = static_cast<uint32_t>(i * 131);   // docoff
    cols[2][i] = static_cast<uint32_t>(i * 907);   // posoff
    cols[3][i] = rng() % kSkipBlockSize;           // posslot, 7 bits
    cols[4][i] = static_cast<uint32_t>(i * 3);     // payoff
    cols[5][i] = 17;                               // bdelta, width 0
  }
  ExpectRoundTrip(cols);
}

TEST(SkipColumns, ManyGroupsRandom) {
  std::mt19937 rng{20260813};
  std::uniform_int_distribution<uint32_t> dist{0, 5'000'000};
  std::vector<uint32_t> values(10 * kSkipBlockSize + 37);
  std::generate(values.begin(), values.end(), [&] { return dist(rng); });
  ExpectRoundTrip({values});
}

// `Get` caches the group it last touched; jumping back and forth across group
// boundaries must still read the right values.
TEST(SkipColumns, AlternatingGroupAccess) {
  std::vector<uint32_t> values(3 * kSkipBlockSize);
  for (uint32_t i = 0; i != values.size(); ++i) {
    values[i] = i * 3 + 1;
  }
  RoundTrip rt{{values}};
  auto& reader = rt.Reader();
  for (uint32_t i = 0; i != kSkipBlockSize; ++i) {
    EXPECT_EQ(values[i], reader.Get(0, i));
    EXPECT_EQ(values[i + kSkipBlockSize], reader.Get(0, i + kSkipBlockSize));
    EXPECT_EQ(values[i + 2 * kSkipBlockSize],
              reader.Get(0, i + 2 * kSkipBlockSize));
  }
}

// The gallop must agree with std::lower_bound for every start and target,
// including targets past the end.
TEST(SkipColumns, SeekMatchesLowerBound) {
  std::vector<uint32_t> values;
  for (uint32_t i = 0; i != 5 * kSkipBlockSize + 11; ++i) {
    values.push_back(i * 13 + 5);
  }
  RoundTrip rt{{values}};
  auto& reader = rt.Reader();
  const uint64_t last = values.size();

  for (uint64_t first : {uint64_t{0}, uint64_t{1}, uint64_t{127}, uint64_t{128},
                         uint64_t{300}, last - 1, last}) {
    for (uint32_t target : {0u, 5u, 6u, 18u, 1000u, 1001u, 7000u, 1u << 30}) {
      const auto expected = static_cast<uint64_t>(
        std::lower_bound(values.begin() + first, values.end(), target) -
        values.begin());
      EXPECT_EQ(expected, SkipColumnSeek(reader, 0, first, last, target))
        << "first=" << first << " target=" << target;
    }
  }
}

// Duplicates matter: `docs` holds the max doc of a block and equal maxima are
// possible across the groups a term does not own.
TEST(SkipColumns, SeekWithDuplicates) {
  std::vector<uint32_t> values(4 * kSkipBlockSize);
  for (size_t i = 0; i != values.size(); ++i) {
    values[i] = static_cast<uint32_t>(i / 10) * 100;
  }
  RoundTrip rt{{values}};
  const uint64_t last = values.size();
  for (uint32_t target = 0; target <= 5000; target += 50) {
    const auto expected = static_cast<uint64_t>(
      std::lower_bound(values.begin(), values.end(), target) - values.begin());
    EXPECT_EQ(expected, SkipColumnSeek(rt.Reader(), 0, 0, last, target))
      << "target=" << target;
  }
}

// Walking forward with the cursor, the way a conjunction does, must land in
// the same place as a search from scratch.
TEST(SkipColumns, SeekIsMonotoneFromCursor) {
  std::vector<uint32_t> values;
  for (uint32_t i = 0; i != 3 * kSkipBlockSize; ++i) {
    values.push_back(i * 41);
  }
  RoundTrip rt{{values}};
  const uint64_t last = values.size();

  uint64_t cur = 0;
  for (uint32_t target = 0; target < 3 * kSkipBlockSize * 41; target += 137) {
    cur = SkipColumnSeek(rt.Reader(), 0, cur, last, target);
    const auto expected = static_cast<uint64_t>(
      std::lower_bound(values.begin(), values.end(), target) - values.begin());
    ASSERT_EQ(expected, cur) << "target=" << target;
    if (cur == last) {
      break;
    }
  }
}

}  // namespace
