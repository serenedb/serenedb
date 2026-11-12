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
#include <absl/random/random.h>
#include <gtest/gtest.h>

#include <iresearch/utils/space_filling_curve.hpp>
#include <pcg_random.hpp>
#include <span>
#include <string>
#include <vector>

namespace irs::curve {
namespace {

Code ReferenceEncode(Point point, uint32_t dimensions, bool hilbert) {
  if (dimensions < 2 || dimensions > kMaxDimensions) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("curve: dimensions must be 2..8"));
  }
  if (hilbert) {
    for (uint64_t q = uint64_t{1} << 63; q > 1; q >>= 1) {
      const auto mask = q - 1;
      for (uint32_t axis = 0; axis < dimensions; ++axis) {
        if (point[axis] & q) {
          point[0] ^= mask;
        } else {
          const auto exchange = (point[0] ^ point[axis]) & mask;
          point[0] ^= exchange;
          point[axis] ^= exchange;
        }
      }
    }
    for (uint32_t axis = 1; axis < dimensions; ++axis) {
      point[axis] ^= point[axis - 1];
    }
    uint64_t correction = 0;
    for (uint64_t q = uint64_t{1} << 63; q > 1; q >>= 1) {
      if (point[dimensions - 1] & q) {
        correction ^= q - 1;
      }
    }
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      point[axis] ^= correction;
    }
  }
  Code code;
  code.fill(0);
  for (uint32_t bit = 0; bit < 64; ++bit) {
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      const auto offset = bit * dimensions + axis;
      const auto value = (point[axis] >> (63 - bit)) & 1;
      code[offset / 8] |= static_cast<uint8_t>(value << (7 - offset % 8));
    }
  }
  return code;
}

std::string CodePrefix(const Code& code, uint32_t dimensions, uint32_t level) {
  const auto bits = dimensions * level;
  std::string prefix((bits + 7) / 8, '\0');
  for (uint32_t bit = 0; bit < bits; ++bit) {
    const auto value = (code[bit / 8] >> (7 - bit % 8)) & 1;
    prefix[bit / 8] |= static_cast<char>(value << (7 - bit % 8));
  }
  return prefix;
}

std::vector<Point> EdgePoints(uint32_t dimensions) {
  std::vector<Point> points;
  for (uint64_t value : {
         uint64_t{0},
         UINT64_MAX,
         uint64_t{1},
         UINT64_MAX - 1,
         uint64_t{1} << 63,
         (uint64_t{1} << 63) - 1,
         uint64_t{0xaaaaaaaaaaaaaaaa},
         uint64_t{0x5555555555555555},
       }) {
    Point point;
    point.fill(value);
    points.emplace_back(point);
  }
  Point alternating;
  for (uint32_t axis = 0; axis < kMaxDimensions; ++axis) {
    alternating[axis] = axis % 2 ? 0xaaaaaaaaaaaaaaaa : 0x5555555555555555;
  }
  points.emplace_back(alternating);
  for (uint32_t bit = 0; bit < 64; ++bit) {
    Point point;
    point.fill(uint64_t{1} << bit);
    points.emplace_back(point);
    for (uint32_t axis = 0; axis < dimensions; ++axis) {
      point.fill(0);
      point[axis] = uint64_t{1} << bit;
      points.emplace_back(point);
      point.fill(UINT64_MAX);
      point[axis] ^= uint64_t{1} << bit;
      points.emplace_back(point);
    }
  }
  return points;
}

Box RandomBox(pcg64& random, uint32_t dimensions) {
  Box box;
  box.max.fill(UINT64_MAX);
  for (uint32_t axis = 0; axis < dimensions; ++axis) {
    if (absl::Uniform(random, 0U, 4U) == 0) {
      continue;
    }
    const auto half = random() >> (1 + absl::Uniform(random, 0U, 12U));
    const auto center = random();
    box.min[axis] = center > half ? center - half : 0;
    box.max[axis] = center < UINT64_MAX - half ? center + half : UINT64_MAX;
  }
  return box;
}

bool InBox(const Point& point, const Box& box, uint32_t dimensions) {
  for (uint32_t axis = 0; axis < dimensions; ++axis) {
    if (point[axis] < box.min[axis] || point[axis] > box.max[axis]) {
      return false;
    }
  }
  return true;
}

bool Contains(const BoxTest& test, const Code& code, const Options& options) {
  const auto size = CodeSize(options.dimensions, options.max_level);
  return test.Select(std::span{code.data(), size}, nullptr, 0, 0, 1) != 0;
}

TEST(SpaceFillingCurve, EncodeMatchesBitwiseReference) {
  pcg64 random{8137109};
  for (uint32_t dimensions = 2; dimensions <= kMaxDimensions; ++dimensions) {
    SCOPED_TRACE(dimensions);
    const auto edges = EdgePoints(dimensions);
    for (bool hilbert : {false, true}) {
      SCOPED_TRACE(hilbert);
      for (const auto& point : edges) {
        ASSERT_EQ(Encode(point, dimensions, hilbert),
                  ReferenceEncode(point, dimensions, hilbert));
      }
      for (uint32_t sample = 0; sample < 100000; ++sample) {
        Point point;
        for (auto& coordinate : point) {
          coordinate = random();
        }
        ASSERT_EQ(Encode(point, dimensions, hilbert),
                  ReferenceEncode(point, dimensions, hilbert));
      }
    }
  }
}

TEST(SpaceFillingCurve, DecodeInvertsEncode) {
  pcg64 random{2718281};
  for (uint32_t dimensions = 2; dimensions <= kMaxDimensions; ++dimensions) {
    SCOPED_TRACE(dimensions);
    const auto edges = EdgePoints(dimensions);
    for (bool hilbert : {false, true}) {
      SCOPED_TRACE(hilbert);
      const auto check = [&](Point point) {
        auto tail = std::span{point}.subspan(dimensions);
        absl::c_fill(tail, 0);
        const auto code = Encode(point, dimensions, hilbert);
        ASSERT_EQ(Decode(code.data(), static_cast<uint32_t>(code.size()),
                         dimensions, hilbert),
                  point);
      };
      for (const auto& point : edges) {
        check(point);
      }
      for (uint32_t sample = 0; sample < 100000; ++sample) {
        Point point;
        for (auto& coordinate : point) {
          coordinate = random();
        }
        check(point);
      }
    }
  }
}

TEST(SpaceFillingCurve, BoxTestAgreesWithThePoints) {
  pcg64 random{314159};
  struct Entry {
    Code code{};
    Point point{};
  };
  for (uint32_t dimensions = 2; dimensions <= kMaxDimensions; ++dimensions) {
    SCOPED_TRACE(dimensions);
    for (bool hilbert : {false, true}) {
      SCOPED_TRACE(hilbert);
      for (uint32_t max_level :
           {uint32_t{1}, uint32_t{12}, MaxLevel(dimensions)}) {
        SCOPED_TRACE(max_level);
        const Options options{
          .dimensions = dimensions,
          .max_level = max_level,
          .hilbert = hilbert,
        };
        std::vector<Entry> entries(4096);
        for (auto& entry : entries) {
          for (uint32_t axis = 0; axis < dimensions; ++axis) {
            entry.point[axis] = random();
          }
          LeafCode(entry.point, options, entry.code.data());
        }
        absl::c_sort(entries, [](const Entry& lhs, const Entry& rhs) {
          return lhs.code < rhs.code;
        });
        for (uint32_t sample = 0; sample < 5000; ++sample) {
          const auto box = RandomBox(random, dimensions);
          const BoxTest test{box, options};
          const auto first = absl::Uniform(random, size_t{0}, entries.size());
          const auto last =
            std::min(entries.size() - 1,
                     first + absl::Uniform(random, size_t{0}, size_t{64}));
          const auto relation = test.ClassifyRange(entries[first].code.data(),
                                                   entries[last].code.data());
          for (auto i = first; i <= last; ++i) {
            const auto hit = InBox(entries[i].point, box, dimensions);
            ASSERT_FALSE(relation == Relation::Outside && hit);
            ASSERT_FALSE(relation == Relation::Inside && !hit);
            ASSERT_TRUE(!hit || Contains(test, entries[i].code, options));
          }
        }
      }
    }
  }
}

TEST(SpaceFillingCurve, FullDepthContainsIsExact) {
  pcg64 random{577215};
  for (uint32_t dimensions = 2; dimensions <= 4; ++dimensions) {
    SCOPED_TRACE(dimensions);
    for (bool hilbert : {false, true}) {
      SCOPED_TRACE(hilbert);
      const Options options{.dimensions = dimensions, .hilbert = hilbert};
      for (uint32_t sample = 0; sample < 100000; ++sample) {
        const auto box = RandomBox(random, dimensions);
        Point point{};
        for (uint32_t axis = 0; axis < dimensions; ++axis) {
          point[axis] = box.min[axis] + (box.max[axis] - box.min[axis]) /
                                          absl::Uniform(random, 1U, 5U);
        }
        if (sample % 2) {
          point[0] = random();
        }
        Code code{};
        LeafCode(point, options, code.data());
        ASSERT_EQ(Contains(BoxTest(box, options), code, options),
                  InBox(point, box, dimensions));
      }
    }
  }
}

TEST(SpaceFillingCurve, SelectReadsKeysAfterThePrefix) {
  pcg64 random{161803};
  const Options options{.dimensions = 3};
  const auto size = CodeSize(options.dimensions, options.max_level);
  constexpr uint32_t kCount = 64;
  for (uint32_t prefix : {uint32_t{0}, uint32_t{5}, uint32_t{8}, size - 1}) {
    SCOPED_TRACE(prefix);
    const auto box = RandomBox(random, options.dimensions);
    const BoxTest test{box, options};
    const auto width = size - prefix;
    const auto pitch = width + 3;
    std::vector<uint8_t> keys(uint64_t{pitch} * kCount);
    Code first{};
    uint64_t expected = 0;
    for (uint32_t i = 0; i < kCount; ++i) {
      Point point{};
      for (uint32_t axis = 0; axis < options.dimensions; ++axis) {
        point[axis] = random();
      }
      Code code{};
      LeafCode(point, options, code.data());
      if (i == 0) {
        first = code;
      }
      std::memcpy(code.data(), first.data(), prefix);
      std::memcpy(keys.data() + uint64_t{i} * pitch, code.data() + prefix,
                  width);
      if (Contains(test, code, options)) {
        expected |= uint64_t{1} << i;
      }
    }
    ASSERT_EQ(test.Select(std::span{first.data(), prefix}, keys.data(), pitch,
                          width, kCount),
              expected);
  }
}

TEST(SpaceFillingCurve, EmptyAndOpenBoxes) {
  const Options options;
  Code low{};
  Code high;
  high.fill(0xff);
  const Box empty{{5, 0}, {4, UINT64_MAX}};
  EXPECT_EQ(BoxTest(empty, options).ClassifyRange(low.data(), high.data()),
            Relation::Outside);
  EXPECT_FALSE(Contains(BoxTest(empty, options), low, options));
  const Box open{{0, 0}, {UINT64_MAX, UINT64_MAX}};
  EXPECT_EQ(BoxTest(open, options).ClassifyRange(low.data(), high.data()),
            Relation::Inside);
  EXPECT_TRUE(Contains(BoxTest(open, options), high, options));
  const Box corner{{UINT64_MAX, UINT64_MAX}, {UINT64_MAX, UINT64_MAX}};
  EXPECT_TRUE(Contains(BoxTest(corner, options), high, options));
  EXPECT_FALSE(Contains(BoxTest(corner, options), low, options));
  EXPECT_EQ(BoxTest(corner, options).ClassifyRange(high.data(), high.data()),
            Relation::Inside);
}

TEST(SpaceFillingCurve, OrderedNumbers) {
  EXPECT_LT(EncodeSigned(INT64_MIN), EncodeSigned(-1));
  EXPECT_LT(EncodeSigned(-1), EncodeSigned(0));
  EXPECT_LT(EncodeSigned(0), EncodeSigned(INT64_MAX));
  const std::array<double, 9> values{
    -INFINITY, -1e300, -1.0, -1e-300, 0.0, 1e-300, 1.0, 1e300, INFINITY,
  };
  for (size_t i = 1; i < values.size(); ++i) {
    EXPECT_LT(EncodeDouble(values[i - 1]), EncodeDouble(values[i]));
  }
  EXPECT_EQ(EncodeDouble(-0.0), EncodeDouble(0.0));
  EXPECT_LT(EncodeDouble(INFINITY), EncodeDouble(NAN));
  EXPECT_EQ(EncodeDouble(NAN), EncodeDouble(-NAN));
}

TEST(SpaceFillingCurve, NormalizedKeys) {
  EXPECT_EQ(NormalizeKey(5, 10, 20), 0);
  EXPECT_EQ(NormalizeKey(10, 10, 20), 0);
  EXPECT_EQ(NormalizeKey(11, 10, 20), uint64_t{1} << 60);
  EXPECT_EQ(NormalizeKey(20, 10, 20), uint64_t{10} << 60);
  EXPECT_EQ(NormalizeKey(99, 10, 20), uint64_t{10} << 60);
  EXPECT_EQ(NormalizeKey(UINT64_MAX, 0, UINT64_MAX), UINT64_MAX);
  for (uint64_t key = 0; key < 40; ++key) {
    EXPECT_LE(NormalizeKey(key, 10, 20), NormalizeKey(key + 1, 10, 20));
    if (key >= 10 && key < 20) {
      EXPECT_LT(NormalizeKey(key, 10, 20), NormalizeKey(key + 1, 10, 20));
    }
  }

  EXPECT_EQ(NormalizeDouble(-5, 0, 1), 0);
  EXPECT_EQ(NormalizeDouble(0.5, 0, 1), uint64_t{1} << 63);
  EXPECT_EQ(NormalizeDouble(1, 0, 1), UINT64_MAX);
  EXPECT_EQ(NormalizeDouble(NAN, 0, 1), UINT64_MAX);
  EXPECT_EQ(NormalizeDouble(1000.5, 1000, 1001), uint64_t{1} << 63);
  EXPECT_LT(NormalizeDouble(1000.25, 1000, 1001),
            NormalizeDouble(1000.75, 1000, 1001));
  const std::array<double, 12> values{
    -INFINITY, -1.0, -0.0,      0.0, 1e-300, 0.25,
    0.5,       0.75, 1 - 1e-16, 1.0, 2.0,    INFINITY,
  };
  for (size_t i = 1; i < values.size(); ++i) {
    EXPECT_LE(NormalizeDouble(values[i - 1], 0, 1),
              NormalizeDouble(values[i], 0, 1));
  }
  EXPECT_LE(NormalizeDouble(INFINITY, 0, 1), NormalizeDouble(NAN, 0, 1));
}

TEST(SpaceFillingCurve, LeafCodeIsTruncatedEncoding) {
  pcg64 random{1137};
  for (uint32_t dimensions = 2; dimensions <= kMaxDimensions; ++dimensions) {
    SCOPED_TRACE(dimensions);
    for (bool hilbert : {false, true}) {
      for (uint32_t max_level : {1u, 7u, 33u, 64u}) {
        SCOPED_TRACE(max_level);
        const Options options{
          .dimensions = dimensions,
          .max_level = max_level,
          .hilbert = hilbert,
        };
        Point point;
        for (auto& value : point) {
          value = random();
        }
        Code code;
        const auto size = LeafCode(point, options, code.data());
        ASSERT_EQ(size, CodeSize(dimensions, max_level));
        const auto full = Encode(point, dimensions, hilbert);
        const auto bits = dimensions * max_level;
        for (uint32_t bit = 0; bit < size * 8; ++bit) {
          const bool expected =
            bit < bits && (full[bit / 8] >> (7 - bit % 8)) & 1;
          ASSERT_EQ(bool((code[bit / 8] >> (7 - bit % 8)) & 1), expected);
        }
      }
    }
  }
}

TEST(SpaceFillingCurve, MortonReference) {
  const auto encoded =
    Encode({0x8000000000000000, 0x4000000000000000}, 2, false);
  EXPECT_EQ(encoded[0], 0x90);
  EXPECT_TRUE(absl::c_all_of(std::span{encoded}.subspan(1),
                             [](uint8_t b) { return b == 0; }));
}

TEST(SpaceFillingCurve, LindelHilbertReference) {
  constexpr std::array<std::string_view, 16> expected{
    "00000000000000000000000000000000", "3aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "40000000000000000000000000000000", "50000000000000000000000000000000",
    "10000000000000000000000000000000", "20000000000000000000000000000000",
    "7aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "60000000000000000000000000000000",
    "eaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "daaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "80000000000000000000000000000000", "90000000000000000000000000000000",
    "f0000000000000000000000000000000", "caaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "a0000000000000000000000000000000",
  };
  constexpr std::string_view hex = "0123456789abcdef";
  for (uint64_t x = 0; x < 4; ++x) {
    for (uint64_t y = 0; y < 4; ++y) {
      const auto bytes = Encode({x << 62, y << 62}, 2, true);
      std::string encoded;
      for (size_t i = 0; i < 2 * sizeof(uint64_t); ++i) {
        encoded += hex[bytes[i] >> 4];
        encoded += hex[bytes[i] & 15];
      }
      EXPECT_EQ(encoded, expected[x * 4 + y]);
    }
  }
}

struct EncodedPoint {
  Code code;
  Point point;

  auto operator<=>(const EncodedPoint&) const = default;
};

TEST(SpaceFillingCurve, HilbertLocalityAndPrefixes) {
  for (uint32_t dimensions : {2, 3, 4}) {
    std::vector<EncodedPoint> points;
    const uint32_t count = 1u << (dimensions * 2);
    for (uint32_t id = 0; id < count; ++id) {
      Point point;
      point.fill(0);
      for (uint32_t axis = 0; axis < dimensions; ++axis) {
        point[axis] = uint64_t{(id >> (axis * 2)) & 3} << 62;
      }
      points.emplace_back(Encode(point, dimensions, true), point);
    }
    absl::c_sort(points);
    for (size_t i = 1; i < points.size(); ++i) {
      EXPECT_NE(points[i - 1].code, points[i].code);
      uint64_t distance = 0;
      for (uint32_t axis = 0; axis < dimensions; ++axis) {
        const auto a = points[i - 1].point[axis] >> 62;
        const auto b = points[i].point[axis] >> 62;
        distance += a > b ? a - b : b - a;
      }
      EXPECT_EQ(distance, 1);
    }
    for (const auto& [encoded, point] : points) {
      auto interior = point;
      for (uint32_t axis = 0; axis < dimensions; ++axis) {
        interior[axis] |= (uint64_t{1} << 62) - 1;
      }
      EXPECT_EQ(CodePrefix(encoded, dimensions, 2),
                CodePrefix(Encode(interior, dimensions, true), dimensions, 2));
    }
  }
}

TEST(SpaceFillingCurve, InvalidAndExtremeInputs) {
  for (uint32_t dimensions :
       {uint32_t{0}, uint32_t{1}, uint32_t{9}, UINT32_MAX}) {
    for (bool hilbert : {false, true}) {
      EXPECT_THROW(Encode({}, dimensions, hilbert), irs::SqlException);
    }
  }
  EXPECT_THROW(Validate(Options{.dimensions = 1}), irs::SqlException);
  EXPECT_THROW(Validate(Options{.dimensions = 9}), irs::SqlException);
  EXPECT_THROW(Validate(Options{.max_level = 0}), irs::SqlException);
  EXPECT_THROW(Validate(Options{.max_level = 65}), irs::SqlException);
  EXPECT_THROW(Validate(Options{.dimensions = 5, .max_level = 52}),
               irs::SqlException);
  EXPECT_NO_THROW(Validate(Options{.dimensions = 4, .max_level = 64}));
  EXPECT_NO_THROW(Validate(Options{.dimensions = 8, .max_level = 32}));
}

TEST(SpaceFillingCurve, EncodedCellPrefixContainsAllCorners) {
  pcg64 random{1317813};
  uint64_t corners = 0;
  for (uint32_t dimensions = 2; dimensions <= kMaxDimensions; ++dimensions) {
    for (bool hilbert : {false, true}) {
      for (uint32_t level : {0u, 1u, 7u, 24u, 32u, 63u, 64u}) {
        for (uint32_t trial = 0; trial < 32; ++trial) {
          Point min{};
          const uint64_t low = level == 0    ? UINT64_MAX
                               : level == 64 ? 0
                                             : UINT64_MAX >> level;
          for (uint32_t axis = 0; axis < dimensions; ++axis) {
            min[axis] = random() & ~low;
          }
          const auto expected =
            CodePrefix(Encode(min, dimensions, hilbert), dimensions, level);
          for (uint32_t corner = 0; corner < (1u << dimensions); ++corner) {
            auto point = min;
            for (uint32_t axis = 0; axis < dimensions; ++axis) {
              if (corner & (1u << axis)) {
                point[axis] |= low;
              }
            }
            ASSERT_EQ(
              CodePrefix(Encode(point, dimensions, hilbert), dimensions, level),
              expected);
            ++corners;
          }
        }
      }
    }
  }
  RecordProperty("corners", corners);
}

struct DoubleBounds {
  double lo;
  double hi;
};

TEST(SpaceFillingCurve, NormalizedFloatingKeysRemainMonotoneAtExtremes) {
  pcg64 random{131721};
  uint64_t comparisons = 0;
  for (const auto [lo, hi] : {
         DoubleBounds{.lo = -1e308, .hi = 0.0},
         DoubleBounds{.lo = 0.0, .hi = 1e308},
         DoubleBounds{.lo = -1e-300, .hi = 1e-300},
         DoubleBounds{.lo = 0.0, .hi = 0x1p-1074},
         DoubleBounds{.lo = 1000.0, .hi = 1000.0000000001},
         DoubleBounds{.lo = -0.0, .hi = 1.0},
       }) {
    std::vector<double> values{
      -INFINITY, lo,       std::nextafter(lo, hi),
      0.0,       -0.0,     std::nextafter(hi, lo),
      hi,        INFINITY,
    };
    for (uint32_t i = 0; i < 4096; ++i) {
      const auto fraction = static_cast<double>(random() >> 11) * 0x1p-53;
      values.emplace_back(lo + fraction * (hi - lo));
    }
    absl::c_sort(values);
    for (size_t i = 1; i < values.size(); ++i) {
      ASSERT_LE(NormalizeDouble(values[i - 1], lo, hi),
                NormalizeDouble(values[i], lo, hi));
      ++comparisons;
    }
    ASSERT_EQ(NormalizeDouble(NAN, lo, hi), UINT64_MAX);
  }
  RecordProperty("comparisons", comparisons);
}

}  // namespace
}  // namespace irs::curve
