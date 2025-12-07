#include <velox/type/Type.h>
#include <velox/vector/tests/utils/VectorTestBase.h>

#include "connector/primary_key.hpp"
#include "gtest/gtest.h"

namespace {

using TinInt = typename velox::TypeTraits<velox::TypeKind::TINYINT>::NativeType;
using SmlInt =
  typename velox::TypeTraits<velox::TypeKind::SMALLINT>::NativeType;
using NrmInt = typename velox::TypeTraits<velox::TypeKind::INTEGER>::NativeType;
using BigInt = typename velox::TypeTraits<velox::TypeKind::BIGINT>::NativeType;
using HugInt = typename velox::TypeTraits<velox::TypeKind::HUGEINT>::NativeType;
using FltType = typename velox::TypeTraits<velox::TypeKind::REAL>::NativeType;
using DblType = typename velox::TypeTraits<velox::TypeKind::DOUBLE>::NativeType;

class PrimaryKeyTest : public ::testing::Test,
                       public velox::test::VectorTestBase {
 public:
  static void SetUpTestCase() {
    velox::memory::MemoryManager::testingSetInstance(
      velox::memory::MemoryManager::Options{});
  }

  // sanity check for sortability
  template<typename T>
  void MakeFloatColumnSortabilityTest() {
    static_assert(std::is_floating_point_v<T>, "Float number expected here");
    std::vector<velox::column_index_t> key_columns{0};
    auto data = makeRowVector(
      {"foo"},
      {makeFlatVector<T>(
        {-std::numeric_limits<T>::infinity(), std::numeric_limits<T>::lowest(),
         std::numeric_limits<T>::lowest() / T{2},
         -std::numeric_limits<T>::min(), -std::numeric_limits<T>::denorm_min(),
         0, std::numeric_limits<T>::denorm_min(), std::numeric_limits<T>::min(),
         std::numeric_limits<T>::max() / 2, std::numeric_limits<T>::max(),
         std::numeric_limits<T>::infinity(),
         std::numeric_limits<T>::quiet_NaN()})});

    sdb::connector::primary_key::Keys actual{*pool_.get()};
    sdb::connector::primary_key::Create(*data, key_columns, actual);
    ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
  }

  template<typename T>
  void MakeIntegralSortabilityColumnTest() {
    static_assert(std::is_integral_v<T>, "Integral number expected here");
    {
      std::vector<velox::column_index_t> key_columns{0};
      auto data = makeRowVector(
        {"foo"},
        {makeFlatVector<T>(
          {std::numeric_limits<T>::min(), std::numeric_limits<T>::min() + 1,
           std::numeric_limits<T>::min() / 2, -1, 0, 1,
           std::numeric_limits<T>::max() / 2, std::numeric_limits<T>::max() - 1,
           std::numeric_limits<T>::max()})});
      sdb::connector::primary_key::Keys actual{*pool_.get()};
      sdb::connector::primary_key::Create(*data, key_columns, actual);
      ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
    }
  }

  constexpr static auto kKeyCheck = [](const auto& lhs, const auto& rhs) {
    return lhs <= rhs;
  };
};

TEST_F(PrimaryKeyTest, test_singleFloat) {
  {
    const auto payloaded_nan_123 = std::nanf("123");
    const auto payloaded_nan_456 = std::nanf("456");
    // values check
    std::vector<velox::column_index_t> key_columns{0};
    auto data = makeRowVector(
      {"foo"},
      {makeFlatVector<FltType>(
        {-std::numeric_limits<FltType>::quiet_NaN(),
         -std::numeric_limits<FltType>::signaling_NaN(), -payloaded_nan_123,
         -payloaded_nan_456, std::numeric_limits<FltType>::quiet_NaN(),
         std::numeric_limits<FltType>::signaling_NaN(), payloaded_nan_123,
         payloaded_nan_456, -std::numeric_limits<FltType>::infinity(),
         std::numeric_limits<FltType>::infinity(), -FltType{0}, +FltType{0},
         1234.5f, -1234.5f})});

    sdb::connector::primary_key::Keys actual{*pool_.get()};
    sdb::connector::primary_key::Create(*data, key_columns, actual);

    // Negative NaNs
    constexpr std::string_view kPositiveNan{"\xFF\xC0\x00\x00", 4};
    ASSERT_EQ(actual[0], kPositiveNan);
    ASSERT_EQ(actual[1], kPositiveNan);
    ASSERT_EQ(actual[2], kPositiveNan);
    ASSERT_EQ(actual[3], kPositiveNan);

    // Positive NaNs
    ASSERT_EQ(actual[4], kPositiveNan);
    ASSERT_EQ(actual[5], kPositiveNan);
    ASSERT_EQ(actual[6], kPositiveNan);
    ASSERT_EQ(actual[7], kPositiveNan);

    // Infinity
    constexpr std::string_view kNegativeInf{"\x00\x7F\xFF\xFF", 4};
    constexpr std::string_view kPositiveInf{"\xFF\x80\x00\x00", 4};
    ASSERT_EQ(actual[8], kNegativeInf);
    ASSERT_EQ(actual[9], kPositiveInf);

    // Zeroes
    constexpr std::string_view kZero{"\x80\x00\x00\x00", 4};
    ASSERT_EQ(actual[10], kZero);
    ASSERT_EQ(actual[11], kZero);

    // Arbitrary values
    constexpr std::string_view kPositive{"\xC4\x9A\x50\x00", 4};
    constexpr std::string_view kNegative{"\x3B\x65\xAF\xFF", 4};
    ASSERT_EQ(actual[12], kPositive);
    ASSERT_EQ(actual[13], kNegative);
  }
  MakeFloatColumnSortabilityTest<FltType>();
}

TEST_F(PrimaryKeyTest, test_singleDouble) {
  {
    const auto payloaded_nan_123 = std::nan("123");
    const auto payloaded_nan_456 = std::nan("456");
    // values check
    std::vector<velox::column_index_t> key_columns{0};
    auto data = makeRowVector(
      {"foo"},
      {makeFlatVector<DblType>(
        {-std::numeric_limits<DblType>::quiet_NaN(),
         -std::numeric_limits<DblType>::signaling_NaN(), -payloaded_nan_123,
         -payloaded_nan_456, std::numeric_limits<DblType>::quiet_NaN(),
         std::numeric_limits<DblType>::signaling_NaN(), payloaded_nan_123,
         payloaded_nan_456, -std::numeric_limits<DblType>::infinity(),
         std::numeric_limits<DblType>::infinity(), -DblType{0}, +DblType{0},
         1234.5, -1234.5})});

    sdb::connector::primary_key::Keys actual{*pool_.get()};
    sdb::connector::primary_key::Create(*data, key_columns, actual);

    // Negative NaNs
    constexpr std::string_view kPositiveNan{"\xFF\xF8\x00\x00\x00\x00\x00\x00",
                                            8};
    ASSERT_EQ(actual[0], kPositiveNan);
    ASSERT_EQ(actual[1], kPositiveNan);
    ASSERT_EQ(actual[2], kPositiveNan);
    ASSERT_EQ(actual[3], kPositiveNan);

    // Positive NaNs
    ASSERT_EQ(actual[4], kPositiveNan);
    ASSERT_EQ(actual[5], kPositiveNan);
    ASSERT_EQ(actual[6], kPositiveNan);
    ASSERT_EQ(actual[7], kPositiveNan);

    // Infinity
    constexpr std::string_view kNegativeInf{"\x00\x0F\xFF\xFF\xFF\xFF\xFF\xFF",
                                            8};
    constexpr std::string_view kPositiveInf{"\xFF\xF0\x00\x00\x00\x00\x00\x00",
                                            8};
    ASSERT_EQ(actual[8], kNegativeInf);
    ASSERT_EQ(actual[9], kPositiveInf);

    // Zeroes
    constexpr std::string_view kZero{"\x80\x00\x00\x00\x00\x00\x00\x00", 8};
    ASSERT_EQ(actual[10], kZero);
    ASSERT_EQ(actual[11], kZero);

    // Arbitrary values
    constexpr std::string_view kPositive{"\xC0\x93\x4A\x00\x00\x00\x00\x00", 8};
    constexpr std::string_view kNegative{"\x3F\x6C\xB5\xFF\xFF\xFF\xFF\xFF", 8};
    ASSERT_EQ(actual[12], kPositive);
    ASSERT_EQ(actual[13], kNegative);
  }

  MakeFloatColumnSortabilityTest<DblType>();
}

TEST_F(PrimaryKeyTest, test_singleTinyInteger) {
  std::vector<velox::column_index_t> key_columns{0};
  auto data = makeRowVector(
    {"foo"},
    {makeFlatVector<TinInt>({std::numeric_limits<TinInt>::min(), -5, TinInt{0},
                             5, std::numeric_limits<TinInt>::max()})});

  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual[0], std::string_view("\x00", 1));
  ASSERT_EQ(actual[1], std::string_view("\x7B", 1));
  ASSERT_EQ(actual[2], std::string_view("\x80", 1));
  ASSERT_EQ(actual[3], std::string_view("\x85", 1));
  ASSERT_EQ(actual[4], std::string_view("\xFF", 1));
  MakeIntegralSortabilityColumnTest<TinInt>();
}

TEST_F(PrimaryKeyTest, test_singleSmlInteger) {
  std::vector<velox::column_index_t> key_columns{0};
  auto data = makeRowVector(
    {"foo"},
    {makeFlatVector<SmlInt>({std::numeric_limits<SmlInt>::min(), -5, SmlInt{0},
                             5, std::numeric_limits<SmlInt>::max()})});

  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual[0], std::string_view("\x00\x00", 2));
  ASSERT_EQ(actual[1], std::string_view("\x7F\xFB", 2));
  ASSERT_EQ(actual[2], std::string_view("\x80\x00", 2));
  ASSERT_EQ(actual[3], std::string_view("\x80\x05", 2));
  ASSERT_EQ(actual[4], std::string_view("\xFF\xFF", 2));
  MakeIntegralSortabilityColumnTest<SmlInt>();
}

TEST_F(PrimaryKeyTest, test_singleNrmInteger) {
  std::vector<velox::column_index_t> key_columns{0};
  auto data = makeRowVector(
    {"foo"},
    {makeFlatVector<NrmInt>({std::numeric_limits<NrmInt>::min(), -5, NrmInt{0},
                             5, std::numeric_limits<NrmInt>::max()})});

  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual[0], std::string_view("\x00\x00\x00\x00", 4));
  ASSERT_EQ(actual[1], std::string_view("\x7F\xFF\xFF\xFB", 4));
  ASSERT_EQ(actual[2], std::string_view("\x80\x00\x00\x00", 4));
  ASSERT_EQ(actual[3], std::string_view("\x80\x00\x00\x05", 4));
  ASSERT_EQ(actual[4], std::string_view("\xFF\xFF\xFF\xFF", 4));
  MakeIntegralSortabilityColumnTest<NrmInt>();
}

TEST_F(PrimaryKeyTest, test_singleBigInteger) {
  std::vector<velox::column_index_t> key_columns{0};
  auto data = makeRowVector(
    {"foo"},
    {makeFlatVector<BigInt>({std::numeric_limits<BigInt>::min(), -5, BigInt{0},
                             5, std::numeric_limits<BigInt>::max()})});

  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual[0], std::string_view("\x00\x00\x00\x00\x00\x00\x00\x00", 8));
  ASSERT_EQ(actual[1], std::string_view("\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFB", 8));
  ASSERT_EQ(actual[2], std::string_view("\x80\x00\x00\x00\x00\x00\x00\x00", 8));
  ASSERT_EQ(actual[3], std::string_view("\x80\x00\x00\x00\x00\x00\x00\x05", 8));
  ASSERT_EQ(actual[4], std::string_view("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 8));
  MakeIntegralSortabilityColumnTest<BigInt>();
}

TEST_F(PrimaryKeyTest, test_singleHugInteger) {
  std::vector<velox::column_index_t> key_columns{0};
  auto data = makeRowVector(
    {"foo"},
    {makeFlatVector<HugInt>({std::numeric_limits<HugInt>::min(), -5, HugInt{0},
                             5, std::numeric_limits<HugInt>::max()})});

  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(
    actual[0],
    std::string_view(
      "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 16));
  ASSERT_EQ(
    actual[1],
    std::string_view(
      "\x7F\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFB", 16));
  ASSERT_EQ(
    actual[2],
    std::string_view(
      "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 16));
  ASSERT_EQ(
    actual[3],
    std::string_view(
      "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x05", 16));
  ASSERT_EQ(
    actual[4],
    std::string_view(
      "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF", 16));
  MakeIntegralSortabilityColumnTest<HugInt>();
}

TEST_F(PrimaryKeyTest, test_multiSizeIntegerPositive) {
  std::vector<velox::column_index_t> key_columns{0, 1, 2, 3, 4};
  auto data = makeRowVector(
    {"foo", "bar", "bas", "baz", "waz"},
    {makeFlatVector<TinInt>({0, 0, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7,
                             std::numeric_limits<TinInt>::max()}),
     makeFlatVector<NrmInt>({0, 0, 0, 0, 1, 0,
                             std::numeric_limits<NrmInt>::max(), 0, 0, 1, 2,
                             std::numeric_limits<NrmInt>::max(),
                             std::numeric_limits<NrmInt>::max()}),
     makeFlatVector<BigInt>({0, 0, 0, 1, 0, 0,
                             std::numeric_limits<BigInt>::max(), 0, 0, 3, 3,
                             std::numeric_limits<BigInt>::max(),
                             std::numeric_limits<BigInt>::max()}),
     makeFlatVector<SmlInt>({0, 0, 1, 0, 0, 0,
                             std::numeric_limits<SmlInt>::max(), 0, 0, 3, 4,
                             std::numeric_limits<SmlInt>::max(),
                             std::numeric_limits<SmlInt>::max()}),
     makeFlatVector<HugInt>({0, 1, 0, 0, 0, 0,
                             std::numeric_limits<HugInt>::max(), 0, 0, 5, 6,
                             std::numeric_limits<HugInt>::max(),
                             std::numeric_limits<HugInt>::max()})});
  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual.size(), data->size());
  ASSERT_FALSE(absl::c_any_of(
    actual, [&](const auto& str) { return str.size() != actual[0].size(); }));
  ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
}

TEST_F(PrimaryKeyTest, test_multiSizeInteger) {
  std::vector<velox::column_index_t> key_columns{0, 1, 2, 3, 4};
  auto data = makeRowVector(
    {"foo", "bar", "bas", "baz", "waz"},
    {makeFlatVector<TinInt>(
       {std::numeric_limits<TinInt>::min(), std::numeric_limits<TinInt>::min(),
        std::numeric_limits<TinInt>::min(), std::numeric_limits<TinInt>::min(),
        std::numeric_limits<TinInt>::min(),
        std::numeric_limits<TinInt>::min() + 1,
        std::numeric_limits<TinInt>::min() + 2,
        std::numeric_limits<TinInt>::min() + 3, 0, 1, 2, 4,
        std::numeric_limits<TinInt>::max()}),
     makeFlatVector<NrmInt>(
       {std::numeric_limits<NrmInt>::min(), std::numeric_limits<NrmInt>::min(),
        std::numeric_limits<NrmInt>::min(), std::numeric_limits<NrmInt>::min(),
        std::numeric_limits<NrmInt>::min() + 1,
        std::numeric_limits<NrmInt>::min(), std::numeric_limits<NrmInt>::max(),
        std::numeric_limits<NrmInt>::min(), 0, 1, 2,
        std::numeric_limits<NrmInt>::max(),
        std::numeric_limits<NrmInt>::max()}),
     makeFlatVector<BigInt>(
       {std::numeric_limits<BigInt>::min(), std::numeric_limits<BigInt>::min(),
        std::numeric_limits<BigInt>::min(),
        std::numeric_limits<BigInt>::min() + 1,
        std::numeric_limits<BigInt>::min(), std::numeric_limits<BigInt>::min(),
        std::numeric_limits<BigInt>::max(), std::numeric_limits<BigInt>::min(),
        0, 3, 3, std::numeric_limits<BigInt>::max(),
        std::numeric_limits<BigInt>::max()}),
     makeFlatVector<SmlInt>(
       {std::numeric_limits<SmlInt>::min(), std::numeric_limits<SmlInt>::min(),
        std::numeric_limits<SmlInt>::min() + 1,
        std::numeric_limits<SmlInt>::min(), std::numeric_limits<SmlInt>::min(),
        std::numeric_limits<SmlInt>::min(), std::numeric_limits<SmlInt>::max(),
        std::numeric_limits<SmlInt>::min(), 0, 3, 4,
        std::numeric_limits<SmlInt>::max(),
        std::numeric_limits<SmlInt>::max()}),
     makeFlatVector<HugInt>(
       {std::numeric_limits<HugInt>::min(),
        std::numeric_limits<HugInt>::min() + 1,
        std::numeric_limits<HugInt>::min(), std::numeric_limits<HugInt>::min(),
        std::numeric_limits<HugInt>::min(), std::numeric_limits<HugInt>::min(),
        std::numeric_limits<HugInt>::max(), std::numeric_limits<HugInt>::min(),
        0, 5, 6, std::numeric_limits<HugInt>::max(),
        std::numeric_limits<HugInt>::max()})});
  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual.size(), data->size());
  ASSERT_FALSE(absl::c_any_of(
    actual, [&](const auto& str) { return str.size() != actual[0].size(); }));
  ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
}

TEST_F(PrimaryKeyTest, test_FloatingNumbers) {
  std::vector<velox::column_index_t> key_columns{0, 1};
  auto data = makeRowVector(
    {"foo", "bar"},
    {makeFlatVector<FltType>({std::numeric_limits<FltType>::lowest(), -1.f,
                              -1.f, 0., std::numeric_limits<FltType>::min(),
                              9.f, 1000.f, std::numeric_limits<FltType>::max(),
                              std::numeric_limits<FltType>::max(),
                              std::numeric_limits<FltType>::max()}),
     makeFlatVector<DblType>({4., std::numeric_limits<DblType>::max() * -1.,
                              -1., 0., 6., 5., 4., 0., 1000.,
                              std::numeric_limits<DblType>::max()})});
  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual.size(), data->size());
  ASSERT_FALSE(absl::c_any_of(
    actual, [&](const auto& str) { return str.size() != actual[0].size(); }));
  ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
}

TEST_F(PrimaryKeyTest, test_Bools) {
  std::vector<velox::column_index_t> key_columns{0, 1};
  auto data = makeRowVector({"foo", "bar"},
                            {makeFlatVector<bool>({false, false, true, true}),
                             makeFlatVector<bool>({false, true, false, true})});
  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual.size(), data->size());
  ASSERT_FALSE(absl::c_any_of(
    actual, [&](const auto& str) { return str.size() != actual[0].size(); }));
  ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
  ASSERT_EQ(actual[0], std::string_view("\x00\x00", 2));
  ASSERT_EQ(actual[1], std::string_view("\x00\x01", 2));
  ASSERT_EQ(actual[2], std::string_view("\x01\x00", 2));
  ASSERT_EQ(actual[3], std::string_view("\x01\x01", 2));
}

TEST_F(PrimaryKeyTest, test_Strings) {
  std::vector<velox::column_index_t> key_columns{0, 1};
  auto data = makeRowVector(
    {"foo", "bar"},
    {makeFlatVector<velox::StringView>({"foo", "foob", "foobar", "foobar"}),
     makeFlatVector<velox::StringView>({"barfoo", "arfoo", "foo", "goo"})});
  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual.size(), data->size());
  ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
}

TEST_F(PrimaryKeyTest, test_BoolsStrings) {
  std::vector<velox::column_index_t> key_columns{0, 1, 2};
  auto data = makeRowVector(
    {"foo", "bar", "bas"},
    {makeFlatVector<velox::StringView>({"foo", "foob", "foob", "foob", "foob"}),
     makeFlatVector<bool>({true, false, true, true, true}),
     makeFlatVector<velox::StringView>({"a", "a", "a", "b", "bar"})});
  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual.size(), data->size());
  ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
}

TEST_F(PrimaryKeyTest, test_StringsNumbers) {
  std::vector<velox::column_index_t> key_columns{0, 1, 2};
  auto data = makeRowVector(
    {"foo", "bar", "bas"},
    {makeFlatVector<velox::StringView>({"foo", "foo0"}),
     makeFlatVector<int32_t>(
       {-1339019216, -1339019216}),  // would be stored as 0x30303030
     makeFlatVector<velox::StringView>({"00", "0"})});
  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual.size(), data->size());
  ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
}

TEST_F(PrimaryKeyTest, test_StringsWithSeparators) {
  std::vector<velox::column_index_t> key_columns{0};
  auto data = makeRowVector(
    {"foo"}, {makeFlatVector<velox::StringView>(
               {"", velox::StringView{"\0", 1}, velox::StringView{"\0\0", 2},
                velox::StringView{"\0\1", 2}, velox::StringView{"\0c", 2}, "c",
                velox::StringView{"c\0", 2}, velox::StringView{"c\1", 2}, "ccc",
                velox::StringView{"ccc\0", 4}, velox::StringView{"ccc\0\0", 5},
                velox::StringView{"ccc\0\1", 5}, velox::StringView{"ccc\0c", 5},
                velox::StringView{"ccc\1", 4}, "cccc"})});
  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual.size(), data->size());
  ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
}

TEST_F(PrimaryKeyTest, test_Timestamp) {
  std::vector<velox::column_index_t> key_columns{0};
  auto data = makeRowVector(
    {"foo"}, {makeFlatVector<velox::Timestamp>(
               {velox::Timestamp::min(),
                velox::Timestamp(velox::Timestamp::kMinSeconds,
                                 velox::Timestamp::kMaxNanos),
                velox::Timestamp(0, 0),
                velox::Timestamp(velox::Timestamp::kMaxSeconds, 0),
                velox::Timestamp::max()})});
  sdb::connector::primary_key::Keys actual{*pool_.get()};
  sdb::connector::primary_key::Create(*data, key_columns, actual);
  ASSERT_EQ(actual.size(), data->size());
  ASSERT_TRUE(absl::c_is_sorted(actual, kKeyCheck));
  ASSERT_EQ(
    actual[0],
    std::string_view(
      "\x7F\xDF\x3B\x64\x5A\x1C\xAC\x08\x00\x00\x00\x00\x00\x00\x00\x00", 16));
  ASSERT_EQ(
    actual[1],
    std::string_view(
      "\x7F\xDF\x3B\x64\x5A\x1C\xAC\x08\x00\x00\x00\x00\x3B\x9A\xC9\xFF", 16));
  ASSERT_EQ(
    actual[2],
    std::string_view(
      "\x80\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00", 16));
  ASSERT_EQ(
    actual[3],
    std::string_view(
      "\x80\x20\xC4\x9B\xA5\xE3\x53\xF7\x00\x00\x00\x00\x00\x00\x00\x00", 16));
  ASSERT_EQ(
    actual[4],
    std::string_view(
      "\x80\x20\xC4\x9B\xA5\xE3\x53\xF7\x00\x00\x00\x00\x3B\x9A\xC9\xFF", 16));
}

}  // namespace
