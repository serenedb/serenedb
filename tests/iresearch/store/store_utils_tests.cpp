////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <iresearch/store/store_utils.hpp>
#include <iresearch/utils/bytes_output.hpp>
#include <iresearch/utils/bytes_utils.hpp>
#include <unordered_set>

#include "tests_shared.hpp"
#include "utils/write_helpers.hpp"

using namespace irs;

namespace tests::detail {

void DeltaEncodeDecodeCore(size_t step, size_t count) {
  std::vector<size_t> values;
  values.reserve(count);
  for (size_t i = 1; i <= count; ++i) {
    values.push_back(i * step);
  }

  auto encoded = values;
  irs::encode::delta::Encode(encoded.begin(), encoded.end());
  ASSERT_TRUE(std::all_of(encoded.begin(), encoded.end(),
                          [step](auto v) { return step == v; }));

  auto decoded = encoded;
  irs::encode::delta::Decode(decoded.begin(), decoded.end());
  ASSERT_EQ(values, decoded);
}

using irs::DataOutput;
using irs::IndexInput;

template<typename T>
void ReadWriteCore(const std::vector<T>& src,
                   const std::function<T(IndexInput&)>& reader,
                   const std::function<void(DataOutput&, T)>& writer) {
  irs::BytesOutput out;
  std::for_each(src.begin(), src.end(),
                [&out, &writer](const T& v) { writer(out, v); });

  IndexInput in{out.Bytes()};
  std::for_each(src.begin(), src.end(),
                [&in, &reader](const T& v) { ASSERT_EQ(v, reader(in)); });
}

// Since NaN != NaN according to the ieee754
template<typename T>
void ReadWriteCoreNan(const std::function<T(IndexInput&)>& reader,
                      const std::function<void(DataOutput&, T)>& writer) {
  irs::BytesOutput out;
  writer(out, std::numeric_limits<T>::quiet_NaN());
  writer(out, std::numeric_limits<T>::signaling_NaN());

  IndexInput in{out.Bytes()};
  ASSERT_TRUE(std::isnan(reader(in)));
  ASSERT_TRUE(std::isnan(reader(in)));
}

template<typename Cont>
void ReadWriteCoreContainer(
  const Cont& src, const std::function<Cont(IndexInput&)>& reader,
  const std::function<DataOutput&(DataOutput&, const Cont&)>& writer) {
  irs::BytesOutput out;
  writer(out, src);

  IndexInput in{out.Bytes()};
  const Cont read = reader(in);
  ASSERT_EQ(src, read);
}

void ShiftPackUnpackCore32(uint32_t value, bool flag) {
  const uint32_t packed = ShiftPack32(value, flag);
  uint32_t unpacked;
  ASSERT_EQ(flag, ShiftUnpack32(packed, unpacked));
  ASSERT_EQ(value, unpacked);
}

void ShiftPackUnpackCore64(uint64_t value, bool flag) {
  const uint64_t packed = ShiftPack64(value, flag);
  uint64_t unpacked;
  ASSERT_EQ(flag, ShiftUnpack64(packed, unpacked));
  ASSERT_EQ(value, unpacked);
}

template<typename T>
void VencodeFromArray(T expected_value, size_t expected_length) {
  ASSERT_EQ(expected_length, irs::bytes_io<T>::vsize(expected_value));
  irs::byte_type buf[irs::bytes_io<T>::kMaxVSize]{};

  // write data
  {
    auto* ptr = buf;
    irs::WriteVarint<T>(expected_value, ptr);
    ASSERT_EQ(expected_length, std::distance(buf, ptr));
  }

  // read data
  {
    const auto* ptr = buf;
    ASSERT_EQ(expected_value, irs::vread<T>(ptr));
    ASSERT_EQ(expected_length,
              std::distance((const irs::byte_type*)(buf), ptr));
  }

  // skip data
  {
    const auto* ptr = buf;
    irs::vskip<T>(ptr);
    ASSERT_GE(ptr, buf);
    ASSERT_EQ(expected_length,
              std::distance((const irs::byte_type*)(buf), ptr));
  }
}

}  // namespace tests::detail

TEST(store_utils_tests, vint_from_array) {
  tests::detail::VencodeFromArray(UINT32_C(0), 1);
  tests::detail::VencodeFromArray(UINT32_C(1), 1);
  tests::detail::VencodeFromArray(UINT32_C(100), 1);
  tests::detail::VencodeFromArray(UINT32_C(127), 1);
  tests::detail::VencodeFromArray(UINT32_C(128), 2);
  tests::detail::VencodeFromArray(UINT32_C(16383), 2);
  tests::detail::VencodeFromArray(UINT32_C(16384), 3);
  tests::detail::VencodeFromArray(UINT32_C(32000), 3);
  tests::detail::VencodeFromArray(UINT32_C(2097151), 3);
  tests::detail::VencodeFromArray(UINT32_C(2097152), 4);
  tests::detail::VencodeFromArray(UINT32_C(268435455), 4);
  tests::detail::VencodeFromArray(UINT32_C(268435456), 5);
  tests::detail::VencodeFromArray(std::numeric_limits<uint32_t>::max(), 5);
}

TEST(store_utils_tests, vlong_from_array) {
  tests::detail::VencodeFromArray(UINT64_C(0), 1);
  tests::detail::VencodeFromArray(UINT64_C(1), 1);
  tests::detail::VencodeFromArray(UINT64_C(100), 1);
  tests::detail::VencodeFromArray(UINT64_C(127), 1);
  tests::detail::VencodeFromArray(UINT64_C(128), 2);
  tests::detail::VencodeFromArray(UINT64_C(16383), 2);
  tests::detail::VencodeFromArray(UINT64_C(16384), 3);
  tests::detail::VencodeFromArray(UINT64_C(32000), 3);
  tests::detail::VencodeFromArray(UINT64_C(2097151), 3);
  tests::detail::VencodeFromArray(UINT64_C(2097152), 4);
  tests::detail::VencodeFromArray(UINT64_C(268435455), 4);
  tests::detail::VencodeFromArray(UINT64_C(268435456), 5);
  tests::detail::VencodeFromArray(UINT64_C(34359738367), 5);
  tests::detail::VencodeFromArray(UINT64_C(34359738368), 6);
  tests::detail::VencodeFromArray(UINT64_C(4398046511103), 6);
  tests::detail::VencodeFromArray(UINT64_C(4398046511104), 7);
  tests::detail::VencodeFromArray(UINT64_C(72057594037927935), 8);
  tests::detail::VencodeFromArray(UINT64_C(72057594037927936), 9);
  tests::detail::VencodeFromArray(std::numeric_limits<uint64_t>::max(), 10);
}

TEST(store_utils_tests, zvfloat_read_write) {
  tests::detail::ReadWriteCore<float_t>(
    {
      std::numeric_limits<float_t>::max(),
      std::numeric_limits<float_t>::min(),
      0.f,
      std::numeric_limits<float_t>::infinity(),
      -std::numeric_limits<float_t>::infinity(),
      125.f,
      53.f,
      -1.f,
      823132.4332f,
      132621123.234f - 21532764.631984f,
      -9847.23427f,
    },
    tests::ReadZvfloat, tests::WriteZvfloat);

  /* NaN case */
  tests::detail::ReadWriteCoreNan<float_t>(tests::ReadZvfloat,
                                           tests::WriteZvfloat);
}

TEST(store_utils_tests, zvdouble_read_write) {
  tests::detail::ReadWriteCore<double_t>(
    {
      std::numeric_limits<double_t>::max(),
      std::numeric_limits<double_t>::min(),
      std::numeric_limits<float_t>::max(),
      std::numeric_limits<float_t>::min(),
      0.,
      std::numeric_limits<double_t>::infinity(),
      -std::numeric_limits<double_t>::infinity(),
      123.,
      43.,
      -1.,
      247280479234.1239171,
      3694208.1320823132,
      -19274316.123,
      -98743098097.34352532,
    },
    tests::ReadZvdouble, tests::WriteZvdouble);

  /* NaN case */
  tests::detail::ReadWriteCoreNan<double_t>(tests::ReadZvdouble,
                                            tests::WriteZvdouble);
}

static void WriteSize(DataOutput& out, size_t size) { out.WriteU64(size); }
static size_t ReadSize(IndexInput& in) { return in.ReadI64(); }

TEST(store_utils_tests, size_read_write) {
  static_assert(sizeof(uint64_t) == sizeof(size_t));
  tests::detail::ReadWriteCore<size_t>(
    {
      std::numeric_limits<size_t>::max(),
      std::numeric_limits<size_t>::min(),
      0,
      size_t(2037323242),
      size_t(123132142),
      size_t(12371792192121),
      size_t(9719496156),
    },
    ReadSize, WriteSize);
}

TEST(store_utils_tests, zvint_read_write) {
  tests::detail::ReadWriteCore<int32_t>(
    {
      std::numeric_limits<int32_t>::max(),
      std::numeric_limits<int32_t>::min(),
      0,
      103282101,
      51916519,
      -911728376,
      -10725017,
    },
    irs::ReadZV32<IndexInput>, irs::WriteZV32);
}

TEST(store_utils_tests, zvlong_read_write) {
  tests::detail::ReadWriteCore<int64_t>(
    {
      std::numeric_limits<int64_t>::max(),
      std::numeric_limits<int64_t>::min(),
      0LL,
      -7179394719192822LL,
      -9197947324326483264LL,
      -9184236868362391274LL,
      -91724962191921979LL,
    },
    irs::ReadZV64<IndexInput>, irs::WriteZV64);
}

TEST(store_utils_tests, std_string_read_write) {
  tests::detail::ReadWriteCore<std::string>(
    {
      std::string(),
      std::string(""),
      std::string("quick brown fox 1232141"),
      std::string("jumps over the 0218021"),
      std::string("lazy p1230142hlds"),
      std::string("dob  sdofjasoufdsa"),
    },
    irs::ReadString<std::string, IndexInput>, irs::WriteStr<std::string>);
}

TEST(store_utils_tests, bytes_read_write) {
  tests::detail::ReadWriteCore<bstring>(
    {
      bstring(),
      bstring(irs::ViewCast<byte_type>(std::string_view("qalsdflsajfd"))),
      bstring(irs::ViewCast<byte_type>(std::string_view("jfdldsflaflj"))),
      bstring(irs::ViewCast<byte_type>(std::string_view("102174174010"))),
      bstring(irs::ViewCast<byte_type>(std::string_view("0182ljdskfaof"))),
    },
    irs::ReadString<bstring, IndexInput>, irs::WriteStr<bstring>);
}

TEST(store_utils_tests, ShiftPackunpack_32) {
  tests::detail::ShiftPackUnpackCore32(2343242, true);
  tests::detail::ShiftPackUnpackCore32(2343242, false);
  tests::detail::ShiftPackUnpackCore32(63456345, true);
  tests::detail::ShiftPackUnpackCore32(63456345, false);
  tests::detail::ShiftPackUnpackCore32(0, true);
  tests::detail::ShiftPackUnpackCore32(0, false);
  tests::detail::ShiftPackUnpackCore32(0x7FFFFFFF, true);
  tests::detail::ShiftPackUnpackCore32(0x7FFFFFFF, false);
}

TEST(store_utils_tests, ShiftPackunpack_64) {
  tests::detail::ShiftPackUnpackCore64(333618218293788671LL, true);
  tests::detail::ShiftPackUnpackCore64(333618218293788671LL, false);
  tests::detail::ShiftPackUnpackCore64(8653986421, true);
  tests::detail::ShiftPackUnpackCore64(8653986421, false);
  tests::detail::ShiftPackUnpackCore64(0, true);
  tests::detail::ShiftPackUnpackCore64(0, false);
  tests::detail::ShiftPackUnpackCore64(0x7FFFFFFFFFFFFFFFLL, true);
  tests::detail::ShiftPackUnpackCore64(0x7FFFFFFFFFFFFFFFLL, false);
}

TEST(store_utils_tests, delta_encode_decode) {
  tests::detail::DeltaEncodeDecodeCore(1, 1000);  // step = 1, count = 1000
  tests::detail::DeltaEncodeDecodeCore(128,
                                       1000);     // step = 128, count = 1000
  tests::detail::DeltaEncodeDecodeCore(1000, 1);  // step = 1000, count = 1
}
