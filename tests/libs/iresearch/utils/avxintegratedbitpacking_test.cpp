#include <limits>
#include <random>

#include "avxdeltabitpacking.h"
#include "gtest/gtest.h"

TEST(AvxIntegratedBitPacking, PrefixSum) {
  // Test avx_prefix_sum: computes running sum with carry from prev
  alignas(32) uint32_t input[8] = {1, 2, 3, 4, 5, 6, 7, 8};
  alignas(32) uint32_t output[8];
  alignas(32)
    uint32_t prev_arr[8] = {0, 0, 0, 0, 0, 0, 0, 100};  // prev[7] = 100

  __m256i curr = _mm256_load_si256(reinterpret_cast<const __m256i*>(input));
  __m256i prev = _mm256_load_si256(reinterpret_cast<const __m256i*>(prev_arr));
  __m256i result = avx_prefix_sum(curr, prev);
  _mm256_store_si256(reinterpret_cast<__m256i*>(output), result);

  // Expected: prefix_sum[i] = prev[7] + sum(input[0..i])
  // = 100 + 1 = 101, 100 + 1 + 2 = 103, 100 + 1 + 2 + 3 = 106, ...
  uint32_t expected[8];
  uint32_t sum = 100;  // carry from prev[7]
  for (int i = 0; i < 8; ++i) {
    sum += input[i];
    expected[i] = sum;
  }

  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(expected[i], output[i]) << "Mismatch at index " << i;
  }
}

TEST(AvxIntegratedBitPacking, Delta) {
  // Test avx_delta: computes differences with prev[7] as initial
  alignas(32) uint32_t input[8] = {105, 110, 120, 125, 130, 140, 145, 150};
  alignas(32) uint32_t output[8];
  alignas(32)
    uint32_t prev_arr[8] = {0, 0, 0, 0, 0, 0, 0, 100};  // prev[7] = 100

  __m256i curr = _mm256_load_si256(reinterpret_cast<const __m256i*>(input));
  __m256i prev = _mm256_load_si256(reinterpret_cast<const __m256i*>(prev_arr));
  __m256i result = avx_delta(curr, prev);
  _mm256_store_si256(reinterpret_cast<__m256i*>(output), result);

  // Expected: delta[0] = input[0] - prev[7], delta[i] = input[i] - input[i-1]
  uint32_t expected[8];
  expected[0] = input[0] - 100;  // 105 - 100 = 5
  for (int i = 1; i < 8; ++i) {
    expected[i] = input[i] - input[i - 1];
  }

  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(expected[i], output[i]) << "Mismatch at index " << i;
  }
}

TEST(AvxIntegratedBitPacking, DeltaPrefixSumRoundtrip) {
  // Test that delta followed by prefix_sum gives back original
  alignas(32) uint32_t original[8] = {100, 150, 200, 300, 350, 400, 500, 600};
  alignas(32) uint32_t output[8];
  alignas(32) uint32_t prev_arr[8] = {0, 0, 0, 0, 0, 0, 0, 50};  // prev[7] = 50

  __m256i curr = _mm256_load_si256(reinterpret_cast<const __m256i*>(original));
  __m256i prev = _mm256_load_si256(reinterpret_cast<const __m256i*>(prev_arr));

  // delta then prefix_sum should give back original
  __m256i delta = avx_delta(curr, prev);
  __m256i restored = avx_prefix_sum(delta, prev);
  _mm256_store_si256(reinterpret_cast<__m256i*>(output), restored);

  for (int i = 0; i < 8; ++i) {
    EXPECT_EQ(original[i], output[i]) << "Mismatch at index " << i;
  }
}

TEST(AvxIntegratedBitPacking, Maxbitsd1AllSame) {
  // All same values → all deltas are 0 → 0 bits needed
  alignas(32) uint32_t data[256];
  for (int i = 0; i < 256; ++i) {
    data[i] = 100;
  }
  EXPECT_EQ(0u, avxmaxbitsd1(100, data));
}

TEST(AvxIntegratedBitPacking, Maxbitsd1IncrementBy1) {
  // Sequential +1 increments → max delta is 1 → 1 bit needed
  alignas(32) uint32_t data[256];
  for (int i = 0; i < 256; ++i) {
    data[i] = 100 + i;
  }
  EXPECT_EQ(1u, avxmaxbitsd1(99, data));  // first delta is 100-99=1
}

TEST(AvxIntegratedBitPacking, Maxbitsd1IncrementBy2) {
  // Sequential +2 increments → max delta is 2 → 2 bits needed
  alignas(32) uint32_t data[256];
  for (int i = 0; i < 256; ++i) {
    data[i] = 100 + i * 2;
  }
  EXPECT_EQ(2u, avxmaxbitsd1(98, data));  // first delta is 100-98=2
}

TEST(AvxIntegratedBitPacking, Maxbitsd1MaxDelta255) {
  // Max delta of 255 → 8 bits needed
  alignas(32) uint32_t data[256];
  for (int i = 0; i < 256; ++i) {
    data[i] = i * 255;
  }
  EXPECT_EQ(8u, avxmaxbitsd1(0, data));
}

TEST(AvxIntegratedBitPacking, Maxbitsd1MaxDelta256) {
  // Max delta of 256 → 9 bits needed
  alignas(32) uint32_t data[256];
  for (int i = 0; i < 256; ++i) {
    data[i] = i * 256;
  }
  EXPECT_EQ(9u, avxmaxbitsd1(0, data));
}

TEST(AvxIntegratedBitPacking, Maxbitsd1SingleLargeDelta) {
  // One large delta among small ones determines the bit width
  alignas(32) uint32_t data[256];
  for (int i = 0; i < 128; ++i) {
    data[i] = i;  // delta = 1
  }
  data[128] = 127 + 1000;  // large delta of 1000 at position 128
  for (int i = 129; i < 256; ++i) {
    data[i] = data[128] + (i - 128);  // delta = 1 again
  }
  // bits(1000) = 10 (since 2^10 = 1024 > 1000)
  EXPECT_EQ(10u, avxmaxbitsd1(0, data));
}

TEST(AvxIntegratedBitPacking, PackUnpackRoundtrip) {
  auto test = [](uint32_t init_value, uint32_t min, uint32_t max) {
    alignas(32) uint32_t original[256];
    alignas(32) uint32_t decompressed[256];
    alignas(32) __m256i compressed[256];

    uint32_t val = init_value;
    std::mt19937 gen(12345);
    std::uniform_int_distribution<uint32_t> dist(1, 1000000);
    for (int i = 0; i < 256; ++i) {
      val += dist(gen);
      original[i] = val;
    }

    for (uint32_t j = 0; j < 33; ++j) {
      const uint32_t bits = avxmaxbitsd1(init_value, original);
      avxpackwithoutmaskd1(init_value, original, compressed, bits);
      avxunpackd1(init_value, compressed, decompressed, bits);

      for (int i = 0; i < 256; ++i) {
        EXPECT_EQ(original[i], decompressed[i])
          << original[i] << " != " << decompressed[i] << " at index " << i
          << " for bits=" << bits;
      }
    }
  };

  test(0, 1, 1000000);
  test(42, 1, 1000000);
}

TEST(AvxIntegratedBitPacking, PackUnpackUnaligned) {
  // Test with unaligned buffers to ensure storeu is used
  std::vector<uint8_t> original_buf(256 * sizeof(uint32_t) + 4);
  std::vector<uint8_t> decompressed_buf(256 * sizeof(uint32_t) + 4);
  std::vector<uint8_t> compressed_buf(256 * sizeof(__m256i) + 4);

  // Offset by 4 bytes to break 32-byte alignment
  uint32_t* original = reinterpret_cast<uint32_t*>(original_buf.data() + 4);
  uint32_t* decompressed = reinterpret_cast<uint32_t*>(decompressed_buf.data() + 4);
  __m256i* compressed = reinterpret_cast<__m256i*>(compressed_buf.data() + 4);

  // Verify not 32-byte aligned
  ASSERT_NE(0u, reinterpret_cast<uintptr_t>(original) % 32);
  ASSERT_NE(0u, reinterpret_cast<uintptr_t>(compressed) % 32);

  // Test with init_value = 1 and sequential values [1, 256] (1-bit deltas)
  uint32_t init_value = 1;
  for (int i = 0; i < 256; ++i) {
    original[i] = init_value + i;  // 1, 2, 3, ..., 256
  }

  const uint32_t bits = avxmaxbitsd1(init_value - 1, original);
  EXPECT_EQ(1u, bits);  // All deltas are 1

  avxpackwithoutmaskd1(init_value - 1, original, compressed, bits);
  avxunpackd1(init_value - 1, compressed, decompressed, bits);

  for (int i = 0; i < 256; ++i) {
    EXPECT_EQ(original[i], decompressed[i])
      << "Mismatch at index " << i << " for bits=" << bits;
  }
}
