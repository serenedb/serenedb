////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/formats/sparse_bitmap.hpp>

#include "tests_param.hpp"
#include "tests_shared.hpp"

class SparseBitmapTestCase
  : public tests::DirectoryTestCaseBase<irs::SparseBitmapVersion> {
 public:
  static std::string to_string(const testing::TestParamInfo<ParamType>& info) {
    auto& [factory, version] = info.param;
    return (*factory)(nullptr).second + "___" +
           std::to_string(static_cast<uint32_t>(version));
  }

 protected:
  // min, max
  using RangeType = std::pair<irs::doc_id_t, irs::doc_id_t>;
  // position, expected, index
  using SeekType = std::tuple<irs::doc_id_t, irs::doc_id_t, irs::doc_id_t>;

  // clang-format off
  static constexpr RangeType kMixed[]{
      {1, 32},
      {160, 1184},
      {1201, 1734},
      {60000, 64500},
      {196608, 262144},
      {328007, 328284},
      {328412, 329489},
      {329490, 333586},
      {458757, 458758},
      {458777, 460563}};

  static constexpr RangeType kDense[]{
      {1, 32},
      {160, 1184},
      {1201, 1734},
      {60000, 64500},
      {328007, 328284},
      {328412, 329489},
      {329490, 333586}};

  static constexpr RangeType kSparse[]{
      {1, 32},
      {160, 1184},
      {1201, 1734},
      {328007, 328284},
      {328412, 329489}};
  // clang-format on

  static constexpr std::pair<irs::doc_id_t, irs::doc_id_t> kAll[]{
    {65536, 131072}, {196608, 262144}};

  template<size_t N>
  void TestRwNext(const RangeType (&ranges)[N]);

  template<size_t N>
  void TestRwSeek(const RangeType (&ranges)[N]);

  template<size_t N>
  void TestRwSeekNext(const RangeType (&ranges)[N]);

  template<size_t N, size_t K>
  void TestRwSeekRandom(const RangeType (&ranges)[N],
                        const SeekType (&seeks)[K]);

  template<size_t N, size_t K>
  void TestRwSeekRandomStateless(const RangeType (&ranges)[N],
                                 const SeekType (&seeks)[K]);

  bool TrackPrevious() const noexcept {
    return Version() >= irs::SparseBitmapVersion::PrevDoc;
  }

  irs::SparseBitmapVersion Version() const noexcept {
    return std::get<irs::SparseBitmapVersion>(GetParam());
  }

  irs::SparseBitmapIterator::Options IteratorOptions(
    std::span<const irs::SparseBitmapWriter::Block> index,
    bool use_index) const noexcept {
    return {.version = Version(),
            .track_prev_doc = Version() >= irs::SparseBitmapVersion::PrevDoc,
            .use_block_index = use_index,
            .blocks = index};
  }
};

template<size_t N, size_t K>
void SparseBitmapTestCase::TestRwSeekRandomStateless(
  const RangeType (&ranges)[N], const SeekType (&seeks)[K]) {
  std::vector<irs::SparseBitmapWriter::Block> bitmap_index;
  irs::CostAttr::Type count = 0;

  {
    auto stream = dir().create("tmp");
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapWriter writer(*stream, Version());
    ASSERT_EQ(Version(), writer.version());

    for (const auto& range : ranges) {
      irs::doc_id_t doc = range.first;
      std::generate_n(std::back_inserter(writer), range.second - range.first,
                      [&doc, &count] {
                        ++count;
                        return doc++;
                      });
    }

    writer.finish();

    const auto index = writer.index();
    bitmap_index.assign(index.begin(), index.end());
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    uint32_t value_index = 0;
    for (auto& range : ranges) {
      for (auto [min, max] = range; min < max; ++min) {
        stream->Seek(0);

        irs::SparseBitmapIterator it{
          stream.get(), IteratorOptions(bitmap_index, true), count};
        auto* index = irs::get<irs::ValueIndex>(it);
        ASSERT_NE(nullptr,
                  index);  // index value is unspecified for invalid docs
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_NE(nullptr, doc);

        ASSERT_FALSE(irs::doc_limits::valid(doc->value));
        auto* cost = irs::get<irs::CostAttr>(it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(count, cost->estimate());

        ASSERT_EQ(min, it.seek(min));
        ASSERT_EQ(value_index, it.index());
        ASSERT_EQ(min, it.seek(min));
        ASSERT_EQ(value_index, it.index());

        ++value_index;
      }
    }

    for (auto& seek : seeks) {
      stream->Seek(0);
      irs::SparseBitmapIterator it{stream.get(),
                                   IteratorOptions(bitmap_index, true), count};
      auto* index = irs::get<irs::ValueIndex>(it);
      ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_NE(nullptr, doc);
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      auto* cost = irs::get<irs::CostAttr>(it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(count, cost->estimate());

      ASSERT_EQ(std::get<1>(seek), it.seek(std::get<0>(seek)));
      ASSERT_EQ(std::get<1>(seek), doc->value);
      if (!irs::doc_limits::eof(std::get<1>(seek))) {
        ASSERT_EQ(std::get<2>(seek), it.index());
        ASSERT_EQ(std::get<2>(seek), index->value);
      }
    }
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    uint32_t value_index = 0;
    for (auto& range : ranges) {
      for (auto [min, max] = range; min < max; ++min) {
        stream->Seek(0);

        irs::SparseBitmapIterator it{stream.get(), IteratorOptions({}, true),
                                     count};
        auto* index = irs::get<irs::ValueIndex>(it);
        ASSERT_NE(nullptr,
                  index);  // index value is unspecified for invalid docs
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_NE(nullptr, doc);
        ASSERT_FALSE(irs::doc_limits::valid(doc->value));
        auto* cost = irs::get<irs::CostAttr>(it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(count, cost->estimate());

        ASSERT_EQ(min, it.seek(min));
        ASSERT_EQ(value_index, it.index());
        ASSERT_EQ(min, it.seek(min));
        ASSERT_EQ(value_index, it.index());

        ++value_index;
      }
    }

    for (auto& seek : seeks) {
      stream->Seek(0);
      irs::SparseBitmapIterator it{stream.get(), IteratorOptions({}, false),
                                   count};
      auto* index = irs::get<irs::ValueIndex>(it);
      ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_NE(nullptr, doc);
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      auto* cost = irs::get<irs::CostAttr>(it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(count, cost->estimate());

      ASSERT_EQ(std::get<1>(seek), it.seek(std::get<0>(seek)));
      ASSERT_EQ(std::get<1>(seek), doc->value);
      if (!irs::doc_limits::eof(std::get<1>(seek))) {
        ASSERT_EQ(std::get<2>(seek), it.index());
        ASSERT_EQ(std::get<2>(seek), index->value);
      }
    }
  }
}

template<size_t N, size_t K>
void SparseBitmapTestCase::TestRwSeekRandom(const RangeType (&ranges)[N],
                                            const SeekType (&seeks)[K]) {
  std::vector<irs::SparseBitmapWriter::Block> bitmap_index;

  {
    auto stream = dir().create("tmp");
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapWriter writer(*stream, Version());
    ASSERT_EQ(Version(), writer.version());

    for (const auto& range : ranges) {
      irs::doc_id_t doc = range.first;
      std::generate_n(std::back_inserter(writer), range.second - range.first,
                      [&doc] { return doc++; });
    }

    writer.finish();

    const auto index = writer.index();
    bitmap_index.assign(index.begin(), index.end());
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapIterator it{std::move(stream),
                                 IteratorOptions(bitmap_index, true)};
    auto* index = irs::get<irs::ValueIndex>(it);
    ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());

    for (auto& seek : seeks) {
      ASSERT_EQ(std::get<1>(seek), it.seek(std::get<0>(seek)));
      ASSERT_EQ(std::get<1>(seek), doc->value);
      if (!irs::doc_limits::eof(std::get<1>(seek))) {
        ASSERT_EQ(std::get<2>(seek), it.index());
        ASSERT_EQ(std::get<2>(seek), index->value);
      }
    }
  }

  // no index
  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapIterator it{std::move(stream), IteratorOptions({}, true)};
    auto* index = irs::get<irs::ValueIndex>(it);
    ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());

    for (auto& seek : seeks) {
      ASSERT_EQ(std::get<1>(seek), it.seek(std::get<0>(seek)));
      ASSERT_EQ(std::get<1>(seek), doc->value);
      if (!irs::doc_limits::eof(std::get<1>(seek))) {
        ASSERT_EQ(std::get<2>(seek), it.index());
        ASSERT_EQ(std::get<2>(seek), index->value);
      }
    }
  }
}

template<size_t N>
void SparseBitmapTestCase::TestRwNext(const RangeType (&ranges)[N]) {
  std::vector<irs::SparseBitmapWriter::Block> bitmap_index;
  irs::CostAttr::Type count = 0;

  {
    auto stream = dir().create("tmp");
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapWriter writer(*stream, Version());
    ASSERT_EQ(Version(), writer.version());

    for (const auto& range : ranges) {
      irs::doc_id_t doc = range.first;
      std::generate_n(std::back_inserter(writer), range.second - range.first,
                      [&doc, &count] {
                        ++count;
                        return doc++;
                      });
    }

    writer.finish();

    const auto index = writer.index();
    bitmap_index.assign(index.begin(), index.end());
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapIterator it{std::move(stream),
                                 IteratorOptions(bitmap_index, true), count};
    auto* prev = irs::get<irs::PrevDocAttr>(it);
    ASSERT_EQ(TrackPrevious(), prev && *prev);
    auto* index = irs::get<irs::ValueIndex>(it);
    ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(count, cost->estimate());

    irs::doc_id_t expected_index = 0;
    irs::doc_id_t expected_prev = 0;

    for (const auto range : ranges) {
      irs::doc_id_t expected_doc = range.first;

      while (expected_doc < range.second) {
        SCOPED_TRACE(expected_doc);
        ASSERT_TRUE(it.next());
        ASSERT_EQ(expected_doc, it.value());
        ASSERT_EQ(expected_doc, doc->value);
        ASSERT_EQ(expected_index, it.index());
        ASSERT_EQ(expected_index, index->value);
        if (TrackPrevious()) {
          ASSERT_EQ(expected_prev, (*prev)());
          expected_prev = expected_doc;
        }
        ++expected_doc;
        ++expected_index;
      }
    }

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // no index
  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapIterator it{std::move(stream), IteratorOptions({}, false),
                                 count};
    auto* prev = irs::get<irs::PrevDocAttr>(it);
    ASSERT_EQ(TrackPrevious(), prev && *prev);
    auto* index = irs::get<irs::ValueIndex>(it);
    ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(count, cost->estimate());

    irs::doc_id_t expected_index = 0;
    irs::doc_id_t expected_prev = 0;

    for (const auto range : ranges) {
      irs::doc_id_t expected_doc = range.first;

      while (expected_doc < range.second) {
        SCOPED_TRACE(expected_doc);
        ASSERT_TRUE(it.next());
        ASSERT_EQ(expected_doc, it.value());
        ASSERT_EQ(expected_doc, doc->value);
        ASSERT_EQ(expected_index, it.index());
        ASSERT_EQ(expected_index, index->value);
        if (TrackPrevious()) {
          ASSERT_EQ(expected_prev, (*prev)());
          expected_prev = expected_doc;
        }
        ++expected_doc;
        ++expected_index;
      }
    }

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

template<size_t N>
void SparseBitmapTestCase::TestRwSeek(const RangeType (&ranges)[N]) {
  std::vector<irs::SparseBitmapWriter::Block> bitmap_index;
  irs::CostAttr::Type count = 0;

  {
    auto stream = dir().create("tmp");
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapWriter writer(*stream, Version());
    ASSERT_EQ(Version(), writer.version());

    for (const auto& range : ranges) {
      irs::doc_id_t doc = range.first;
      std::generate_n(std::back_inserter(writer), range.second - range.first,
                      [&doc, &count] {
                        ++count;
                        return doc++;
                      });
    }

    writer.finish();

    const auto index = writer.index();
    bitmap_index.assign(index.begin(), index.end());
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapIterator it{std::move(stream),
                                 IteratorOptions(bitmap_index, true), count};
    auto* index = irs::get<irs::ValueIndex>(it);
    ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(count, cost->estimate());

    irs::doc_id_t expected_index = 0;
    irs::doc_id_t expected_doc = 0;

    for (const auto range : ranges) {
      expected_doc = range.first;

      while (expected_doc < range.second) {
        ASSERT_EQ(expected_doc, it.seek(expected_doc));
        ASSERT_EQ(expected_doc, it.value());
        ASSERT_EQ(expected_doc, doc->value);
        ASSERT_EQ(expected_index, it.index());
        ASSERT_EQ(expected_index, index->value);
        ++expected_doc;
        ++expected_index;
      }
    }

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.seek(expected_doc)));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }

  // no index
  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapIterator it{std::move(stream), IteratorOptions({}, false)};
    auto* index = irs::get<irs::ValueIndex>(it);
    ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());

    irs::doc_id_t expected_index = 0;
    irs::doc_id_t expected_doc = 0;

    for (const auto range : ranges) {
      expected_doc = range.first;

      while (expected_doc < range.second) {
        ASSERT_EQ(expected_doc, it.seek(expected_doc));
        ASSERT_EQ(expected_doc, it.value());
        ASSERT_EQ(expected_doc, doc->value);
        ASSERT_EQ(expected_index, it.index());
        ASSERT_EQ(expected_index, index->value);
        ++expected_doc;
        ++expected_index;
      }
    }

    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_TRUE(irs::doc_limits::eof(it.seek(expected_doc)));
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
  }
}

template<size_t N>
void SparseBitmapTestCase::TestRwSeekNext(const RangeType (&ranges)[N]) {
  std::vector<irs::SparseBitmapWriter::Block> bitmap_index;
  irs::CostAttr::Type count = 0;

  {
    auto stream = dir().create("tmp");
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapWriter writer(*stream, Version());
    ASSERT_EQ(Version(), writer.version());

    for (const auto& range : ranges) {
      irs::doc_id_t doc = range.first;
      std::generate_n(std::back_inserter(writer), range.second - range.first,
                      [&doc, &count] {
                        ++count;
                        return doc++;
                      });
    }

    writer.finish();
    const auto index = writer.index();
    bitmap_index.assign(index.begin(), index.end());
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::doc_id_t expected_doc;
    irs::doc_id_t max_doc;

    auto begin = std::begin(ranges);
    for (; begin != std::end(ranges); ++begin) {
      std::tie(expected_doc, max_doc) = *begin;

      while (expected_doc < max_doc) {
        irs::doc_id_t expected_index = 0;

        for (auto range = std::begin(ranges); range != begin; ++range) {
          ASSERT_LE(range->first, range->second);
          expected_index += range->second - range->first;
        }

        stream->Seek(0);
        ASSERT_EQ(0, stream->Position());

        irs::SparseBitmapIterator it{stream->Dup(),
                                     IteratorOptions(bitmap_index, true),
                                     [count]() noexcept { return count; }};
        auto* index = irs::get<irs::ValueIndex>(it);
        ASSERT_NE(nullptr,
                  index);  // index value is unspecified for invalid docs
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_NE(nullptr, doc);
        ASSERT_FALSE(irs::doc_limits::valid(doc->value));
        auto* cost = irs::get<irs::CostAttr>(it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(count, cost->estimate());

        ASSERT_EQ(expected_doc, it.seek(expected_doc));
        ASSERT_EQ(expected_doc, it.value());
        ASSERT_EQ(expected_doc, doc->value);
        ASSERT_EQ(expected_index, it.index());
        ASSERT_EQ(expected_index, index->value);

        ++expected_doc;
        ++expected_index;

        for (auto range = begin;;) {
          for (; expected_doc < max_doc;) {
            ASSERT_TRUE(it.next());
            ASSERT_EQ(expected_doc, it.value());
            ASSERT_EQ(expected_doc, doc->value);
            ASSERT_EQ(expected_index, it.index());
            ASSERT_EQ(expected_index, index->value);
            ++expected_doc;
            ++expected_index;
          }

          if (++range < std::end(ranges)) {
            // attempt to seek backwards
            ASSERT_EQ(it.value(), it.seek(it.value() - 1));

            std::tie(expected_doc, max_doc) = *range;
          } else {
            break;
          }
        }

        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
        ASSERT_TRUE(irs::doc_limits::eof(it.seek(expected_doc)));
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
    }
  }

  // no index
  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::doc_id_t expected_doc;
    irs::doc_id_t max_doc;

    auto begin = std::begin(ranges);
    for (; begin != std::end(ranges); ++begin) {
      std::tie(expected_doc, max_doc) = *begin;

      while (expected_doc < max_doc) {
        irs::doc_id_t expected_index = 0;

        for (auto range = std::begin(ranges); range != begin; ++range) {
          ASSERT_LE(range->first, range->second);
          expected_index += range->second - range->first;
        }

        stream->Seek(0);
        ASSERT_EQ(0, stream->Position());

        irs::SparseBitmapIterator it{stream->Dup(), IteratorOptions({}, false)};
        auto* index = irs::get<irs::ValueIndex>(it);
        ASSERT_NE(nullptr,
                  index);  // index value is unspecified for invalid docs
        auto* doc = irs::get<irs::DocAttr>(it);
        ASSERT_NE(nullptr, doc);
        ASSERT_FALSE(irs::doc_limits::valid(doc->value));
        auto* cost = irs::get<irs::CostAttr>(it);
        ASSERT_NE(nullptr, cost);
        ASSERT_EQ(0, cost->estimate());

        ASSERT_EQ(expected_doc, it.seek(expected_doc));
        ASSERT_EQ(expected_doc, it.value());
        ASSERT_EQ(expected_doc, doc->value);
        ASSERT_EQ(expected_index, it.index());
        ASSERT_EQ(expected_index, index->value);

        ++expected_doc;
        ++expected_index;

        for (auto range = begin;;) {
          for (; expected_doc < max_doc;) {
            ASSERT_TRUE(it.next());
            ASSERT_EQ(expected_doc, it.value());
            ASSERT_EQ(expected_doc, doc->value);
            ASSERT_EQ(expected_index, it.index());
            ASSERT_EQ(expected_index, index->value);
            ++expected_doc;
            ++expected_index;
          }

          if (++range < std::end(ranges)) {
            // attempt to seek backwards
            ASSERT_EQ(it.value(), it.seek(it.value() - 1));

            std::tie(expected_doc, max_doc) = *range;
          } else {
            break;
          }
        }

        ASSERT_FALSE(it.next());
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
        ASSERT_TRUE(irs::doc_limits::eof(it.seek(expected_doc)));
        ASSERT_TRUE(irs::doc_limits::eof(it.value()));
      }
    }
  }
}

TEST_P(SparseBitmapTestCase, read_write_empty) {
  {
    auto stream = dir().create("tmp");
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapWriter writer(*stream, Version());
    ASSERT_EQ(Version(), writer.version());
    ASSERT_TRUE(writer.index().empty());
    writer.finish();

    const auto index = writer.index();
    ASSERT_EQ(1, index.size());
    ASSERT_EQ(0, index.front().index);
    ASSERT_EQ(0, index.front().offset);
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapIterator it(std::move(stream), IteratorOptions({}, true));
    ASSERT_FALSE(irs::doc_limits::valid(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_FALSE(it.next());
    ASSERT_TRUE(irs::doc_limits::eof(it.value()));
    ASSERT_EQ(0, irs::CostAttr::extract(it));
  }
}

TEST_P(SparseBitmapTestCase, rw_mixed_next) { TestRwNext(kMixed); }

TEST_P(SparseBitmapTestCase, rw_mixed_seek) { TestRwSeek(kMixed); }

TEST_P(SparseBitmapTestCase, rw_mixed_seek_next) { TestRwSeekNext(kMixed); }

TEST_P(SparseBitmapTestCase, rw_sparse_blocks) {
  constexpr irs::doc_id_t kStep = 65536;
  static_assert(irs::doc_limits::min() + 1 < kStep);
  std::vector<irs::SparseBitmapWriter::Block> bitmap_index;

  {
    auto stream = dir().create("tmp");
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapWriter writer(*stream, Version());
    ASSERT_EQ(Version(), writer.version());

    for (irs::doc_id_t doc = irs::doc_limits::min() + 1;;) {
      writer.push_back(doc);

      doc += kStep;

      if (doc == (irs::doc_limits::min() + 1)) {
        break;
      }
    }

    writer.finish();
    const auto index = writer.index();
    bitmap_index.assign(index.begin(), index.end());
  }

  ASSERT_EQ(65536, bitmap_index.size());

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    for (irs::doc_id_t expected_doc = irs::doc_limits::min() + 1;;) {
      irs::SparseBitmapIterator it{stream->Dup(),
                                   IteratorOptions(bitmap_index, true), 65536};
      auto* index = irs::get<irs::ValueIndex>(it);
      ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_NE(nullptr, doc);
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      auto* cost = irs::get<irs::CostAttr>(it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(65536, cost->estimate());

      ASSERT_EQ(expected_doc, it.seek(expected_doc));
      ASSERT_EQ(expected_doc, it.seek(expected_doc));

      expected_doc += kStep;

      if (expected_doc == (irs::doc_limits::min() + 1)) {
        break;
      }
    }
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    for (irs::doc_id_t expected_doc = irs::doc_limits::min() + 1;;) {
      irs::SparseBitmapIterator it{
        stream->Dup(), IteratorOptions(bitmap_index, true),
        []() noexcept -> irs::CostAttr::Type { return 65536; }};
      auto* index = irs::get<irs::ValueIndex>(it);
      ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_NE(nullptr, doc);
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      auto* cost = irs::get<irs::CostAttr>(it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(65536, cost->estimate());

      ASSERT_EQ(expected_doc, it.seek(expected_doc - 1));
      ASSERT_EQ(expected_doc, it.seek(expected_doc));
      ASSERT_EQ(expected_doc, it.seek(expected_doc - 1));

      expected_doc += kStep;

      if (expected_doc == (irs::doc_limits::min() + 1)) {
        break;
      }
    }
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    for (irs::doc_id_t expected_doc = irs::doc_limits::min() + 1 + kStep;;) {
      irs::SparseBitmapIterator it{stream->Dup(),
                                   IteratorOptions(bitmap_index, true)};
      auto* index = irs::get<irs::ValueIndex>(it);
      ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
      auto* doc = irs::get<irs::DocAttr>(it);
      ASSERT_NE(nullptr, doc);
      ASSERT_FALSE(irs::doc_limits::valid(doc->value));
      auto* cost = irs::get<irs::CostAttr>(it);
      ASSERT_NE(nullptr, cost);
      ASSERT_EQ(0, cost->estimate());

      ASSERT_EQ(expected_doc, it.seek(expected_doc - kStep + 1));

      expected_doc += kStep;

      if (expected_doc == (irs::doc_limits::min() + 1)) {
        break;
      }
    }
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapIterator it{stream->Dup(),
                                 IteratorOptions(bitmap_index, true)};
    irs::SparseBitmapIterator it_no_index{stream->Dup(),
                                          IteratorOptions(bitmap_index, false)};
    auto* index = irs::get<irs::ValueIndex>(it);
    ASSERT_NE(nullptr, index);  // index value is unspecified for invalid docs
    auto* doc = irs::get<irs::DocAttr>(it);
    ASSERT_NE(nullptr, doc);
    ASSERT_FALSE(irs::doc_limits::valid(doc->value));
    auto* cost = irs::get<irs::CostAttr>(it);
    ASSERT_NE(nullptr, cost);
    ASSERT_EQ(0, cost->estimate());

    ASSERT_TRUE(irs::doc_limits::eof(it.seek(irs::doc_limits::eof())));
    ASSERT_TRUE(irs::doc_limits::eof(it_no_index.seek(irs::doc_limits::eof())));
  }
}

TEST_P(SparseBitmapTestCase, rw_mixed_seek_random) {
  {
    constexpr SeekType kSeeks[]{
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0}};

    TestRwSeekRandom(kMixed, kSeeks);
  }

  {
    constexpr SeekType kSeeks[]{
      {33, 160, 31},
      {158, 160, 31},
      {999, 999, 870},
      {998, 999, 870},
      {60000, 60000, 1588},
      {63000, 63000, 4588},
      {62999, 63000, 4588},
      {64499, 64499, 6087},
      {64500, 196608, 6088},
      {64500, 196608, 6088},
      {328200, 328200, 71817},
      {328199, 328200, 71817},
      {328284, 328412, 71901},
      {458778, 458778, 77076},
      {460563, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    TestRwSeekRandom(kMixed, kSeeks);
  }

  {
    constexpr SeekType kSeeks[]{
      {33, 160, 31},
      {158, 160, 31},
      {999, 999, 870},
      {999, 999, 870},
      {60000, 60000, 1588},
      {63000, 63000, 4588},
      {63000, 63000, 4588},
      {64499, 64499, 6087},
      {64500, 196608, 6088},
      {64500, 196608, 6088},
      {328200, 328200, 71817},
      {328200, 328200, 71817},
      {328284, 328412, 71901},
      {458778, 458778, 77076},
      {460563, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    TestRwSeekRandomStateless(kMixed, kSeeks);
  }
}

TEST_P(SparseBitmapTestCase, rw_dense) { TestRwNext(kDense); }

TEST_P(SparseBitmapTestCase, rw_dense_seek) { TestRwSeek(kDense); }

TEST_P(SparseBitmapTestCase, rw_dense_seek_next) { TestRwSeekNext(kDense); }

TEST_P(SparseBitmapTestCase, rw_dense_seek_random) {
  {
    constexpr SeekType kSeeks[]{
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0}};

    TestRwSeekRandom(kDense, kSeeks);
  }

  {
    constexpr SeekType kSeeks[]{
      {33, 160, 31},
      {158, 160, 31},
      {999, 999, 870},
      {328410, 328412, 6365},
      {329490, 329490, 7442},
      {333585, 333585, 11537},
      {333586, irs::doc_limits::eof(), 0},
      {333587, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    TestRwSeekRandom(kDense, kSeeks);
  }

  {
    constexpr SeekType kSeeks[]{
      {33, 160, 31},
      {158, 160, 31},
      {999, 999, 870},
      {328410, 328412, 6365},
      {329490, 329490, 7442},
      {333585, 333585, 11537},
      {333586, irs::doc_limits::eof(), 0},
      {333587, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    TestRwSeekRandomStateless(kDense, kSeeks);
  }
}

TEST_P(SparseBitmapTestCase, rw_sparse_next) { TestRwNext(kSparse); }

TEST_P(SparseBitmapTestCase, rw_sparse_seek) { TestRwSeek(kSparse); }

TEST_P(SparseBitmapTestCase, rw_sparse_seek_next) { TestRwSeekNext(kSparse); }

TEST_P(SparseBitmapTestCase, rw_sparse_seek_random) {
  {
    constexpr SeekType kSeeks[]{
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0}};

    TestRwSeekRandom(kSparse, kSeeks);
  }

  {
    constexpr SeekType kSeeks[]{
      {33, 160, 31},
      {1600, 1600, 1454},
      {1599, 1600, 1454},
      {328007, 328007, 1588},
      {328107, 328107, 1688},
      {328283, 328283, 1864},
      {329489, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    TestRwSeekRandom(kSparse, kSeeks);
  }

  {
    constexpr SeekType kSeeks[]{
      {33, 160, 31},
      {1600, 1600, 1454},
      {1600, 1600, 1454},
      {328007, 328007, 1588},
      {328107, 328107, 1688},
      {328283, 328283, 1864},
      {329489, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    TestRwSeekRandomStateless(kSparse, kSeeks);
  }
}

TEST_P(SparseBitmapTestCase, rw_all_next) { TestRwNext(kAll); }

TEST_P(SparseBitmapTestCase, rw_all_seek) { TestRwSeek(kAll); }

TEST_P(SparseBitmapTestCase, rw_all_seek_next) { TestRwSeekNext(kAll); }

TEST_P(SparseBitmapTestCase, rw_all_seek_random) {
  {
    constexpr SeekType kSeeks[]{
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0}};

    TestRwSeekRandom(kAll, kSeeks);
  }

  {
    constexpr SeekType kSeeks[]{
      {33, 65536, 0},
      {131071, 131071, 65535},
      {131072, 196608, 65536},
      {196612, 196612, 65540},
      {196612, 196612, 65540},
      {196611, 196612, 65540},
      {262143, 262143, 131071},
      {262144, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    TestRwSeekRandom(kAll, kSeeks);
  }

  {
    constexpr SeekType kSeeks[]{
      {33, 65536, 0},
      {131071, 131071, 65535},
      {131072, 196608, 65536},
      {196612, 196612, 65540},
      {196612, 196612, 65540},
      {196612, 196612, 65540},
      {262143, 262143, 131071},
      {262144, irs::doc_limits::eof(), 0},
      {irs::doc_limits::eof(), irs::doc_limits::eof(), 0},
    };

    TestRwSeekRandomStateless(kAll, kSeeks);
  }
}

TEST_P(SparseBitmapTestCase, insert_erase) {
  {
    auto stream = dir().create("tmp");
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapWriter writer(*stream, Version());
    ASSERT_EQ(Version(), writer.version());
    ASSERT_TRUE(writer.erase(42));
    writer.push_back(42);
    ASSERT_TRUE(writer.erase(42));
    writer.push_back(70000);  // trigger block flush
    // can't erase an element in already flushed block
    ASSERT_FALSE(writer.erase(42));
    writer.finish();
  }

  {
    auto stream = dir().open("tmp", irs::IOAdvice::NORMAL);
    ASSERT_NE(nullptr, stream);

    irs::SparseBitmapIterator it{
      stream.get(), irs::SparseBitmapIterator::Options{.version = Version(),
                                                       .track_prev_doc = false,
                                                       .use_block_index = false,
                                                       .blocks = {}}};
    ASSERT_TRUE(it.next());
    ASSERT_EQ(70000, it.value());
    ASSERT_FALSE(it.next());
  }
}

static_assert(irs::SparseBitmapVersion::Max ==
              irs::SparseBitmapVersion::PrevDoc);

static constexpr auto kTestDirs = tests::GetDirectories<tests::kTypesDefault>();

INSTANTIATE_TEST_SUITE_P(
  sparse_bitmap_test, SparseBitmapTestCase,
  ::testing::Combine(::testing::ValuesIn(kTestDirs),
                     ::testing::Values(irs::SparseBitmapVersion::Min,
                                       irs::SparseBitmapVersion::PrevDoc)),
  SparseBitmapTestCase::to_string);
