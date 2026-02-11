////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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

#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/delta_compression.hpp>
#include <iresearch/utils/lz4compression.hpp>
#include <numeric>
#include <random>

#include "tests_shared.hpp"

namespace {

struct DummyCompressor : irs::compression::Compressor {
  irs::bytes_view compress(irs::byte_type*, size_t,
                           irs::bstring& /*buf*/) final {
    return {};
  }
};

struct DummyDecompressor : irs::compression::Decompressor {
  irs::bytes_view decompress(const irs::byte_type*, size_t, irs::byte_type*,
                             size_t) final {
    return irs::bytes_view{};
  }

  bool prepare(irs::DataInput&) final { return true; }
};

}  // namespace

TEST(compression_test, registration) {
  struct DummyCompression {};

  constexpr auto kType = irs::Type<DummyCompression>::get();

  // check absent
  {
    ASSERT_FALSE(irs::compression::Exists(kType.name()));
    ASSERT_EQ(nullptr, irs::compression::GetCompressor(kType.name(), {}));
    ASSERT_EQ(nullptr, irs::compression::GetDecompressor(kType.name(), {}));
    auto visitor = [&kType](const std::string_view& name) {
      return name != kType.name();
    };
    ASSERT_TRUE(irs::compression::Visit(visitor));
  }

  static size_t gCallsCount;
  irs::compression::CompressionRegistrar initial(
    kType,
    [](const irs::compression::Options&) -> irs::compression::Compressor::ptr {
      ++gCallsCount;
      return irs::memory::make_managed<DummyCompressor>();
    },
    []() -> irs::compression::Decompressor::ptr {
      ++gCallsCount;
      return irs::memory::make_managed<DummyDecompressor>();
    });
  ASSERT_TRUE(initial);  // registered

  // check registered
  {
    ASSERT_TRUE(irs::compression::Exists(kType.name()));
    ASSERT_EQ(0, gCallsCount);
    ASSERT_NE(nullptr, irs::compression::GetCompressor(kType.name(), {}));
    ASSERT_EQ(1, gCallsCount);
    ASSERT_NE(nullptr, irs::compression::GetDecompressor(kType.name(), {}));
    ASSERT_EQ(2, gCallsCount);
    auto visitor = [&kType](const std::string_view& name) {
      return name != kType.name();
    };
    ASSERT_FALSE(irs::compression::Visit(visitor));
  }

  irs::compression::CompressionRegistrar duplicate(
    kType,
    [](const irs::compression::Options&) -> irs::compression::Compressor::ptr {
      return nullptr;
    },
    []() -> irs::compression::Decompressor::ptr { return nullptr; });
  ASSERT_FALSE(duplicate);  // not registered

  // check registered
  {
    ASSERT_TRUE(irs::compression::Exists(kType.name()));
    ASSERT_EQ(2, gCallsCount);
    ASSERT_NE(nullptr, irs::compression::GetCompressor(kType.name(), {}));
    ASSERT_EQ(3, gCallsCount);
    ASSERT_NE(nullptr, irs::compression::GetDecompressor(kType.name(), {}));
    ASSERT_EQ(4, gCallsCount);
    auto visitor = [&kType](const std::string_view& name) {
      return name != kType.name();
    };
    ASSERT_FALSE(irs::compression::Visit(visitor));
  }
}

TEST(compression_test, none) {
  static_assert("irs::compression::none" ==
                irs::Type<irs::compression::None>::name());
  ASSERT_EQ(nullptr, irs::compression::None().compressor({}));
  ASSERT_EQ(nullptr, irs::compression::None().decompressor());
}

TEST(compression_test, lz4) {
  using namespace irs;
  static_assert("irs::compression::lz4" ==
                irs::Type<irs::compression::Lz4>::name());

  std::vector<size_t> data(2047, 0);
  std::random_device rnd_device;
  std::mt19937 mersenne_engine{rnd_device()};
  std::uniform_int_distribution<size_t> dist{1, 2142152};
  auto generator = [&dist, &mersenne_engine]() {
    return dist(mersenne_engine);
  };

  compression::Lz4::Lz4Decompressor decompressor;
  compression::Lz4::Lz4Compressor compressor;
  ASSERT_EQ(0, compressor.acceleration());

  for (size_t i = 0; i < 10; ++i) {
    std::generate(data.begin(), data.end(), generator);

    bstring compression_buf;
    bstring data_buf(data.size() * sizeof(size_t), 0);
    std::memcpy(&data_buf[0], data.data(), data_buf.size());

    ASSERT_EQ(bytes_view(reinterpret_cast<const byte_type*>(data.data()),
                         data.size() * sizeof(size_t)),
              bytes_view(data_buf));

    const auto compressed =
      compressor.compress(&data_buf[0], data_buf.size(), compression_buf);
    ASSERT_EQ(compressed,
              bytes_view(compression_buf.c_str(), compressed.size()));

    // lz4 doesn't modify data_buf
    ASSERT_EQ(bytes_view(reinterpret_cast<const byte_type*>(data.data()),
                         data.size() * sizeof(size_t)),
              bytes_view(data_buf));

    bstring decompression_buf(data_buf.size(),
                              0);  // ensure we have enough space in buffer
    const auto decompressed =
      decompressor.decompress(&compression_buf[0], compressed.size(),
                              &decompression_buf[0], decompression_buf.size());

    ASSERT_EQ(data_buf, decompression_buf);
    ASSERT_EQ(data_buf, decompressed);
  }
}

TEST(compression_test, delta) {
  using namespace irs;
  static_assert("irs::compression::delta" ==
                irs::Type<irs::compression::Delta>::name());

  std::vector<uint64_t> data(2047, 0);
  std::random_device rnd_device;
  std::mt19937 mersenne_engine{rnd_device()};
  std::uniform_int_distribution<uint64_t> dist{1, 52};
  auto generator = [&dist, &mersenne_engine]() {
    return dist(mersenne_engine);
  };

  compression::DeltaDecompressor decompressor;
  compression::DeltaCompressor compressor;

  for (size_t i = 0; i < 10; ++i) {
    std::generate(data.begin(), data.end(), generator);

    bstring compression_buf;
    bstring data_buf(data.size() * sizeof(size_t), 0);
    std::memcpy(&data_buf[0], data.data(), data_buf.size());

    ASSERT_EQ(bytes_view(reinterpret_cast<const byte_type*>(data.data()),
                         data.size() * sizeof(size_t)),
              bytes_view(data_buf));

    const auto compressed =
      compressor.compress(&data_buf[0], data_buf.size(), compression_buf);
    ASSERT_EQ(compressed,
              bytes_view(compression_buf.c_str(), compressed.size()));

    bstring decompression_buf(data_buf.size(),
                              0);  // ensure we have enough space in buffer
    const auto decompressed =
      decompressor.decompress(&compression_buf[0], compressed.size(),
                              &decompression_buf[0], decompression_buf.size());

    ASSERT_EQ(bytes_view(reinterpret_cast<const byte_type*>(data.data()),
                         data.size() * sizeof(size_t)),
              bytes_view(decompression_buf));
    ASSERT_EQ(bytes_view(reinterpret_cast<const byte_type*>(data.data()),
                         data.size() * sizeof(size_t)),
              bytes_view(decompressed));
  }
}
