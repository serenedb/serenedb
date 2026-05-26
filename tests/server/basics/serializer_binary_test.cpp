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

// Exhaustive coverage of the DuckDB-binary sink adapter for
// basics/serializer.h:
//   WriteTuple -> duckdb::BinarySerializer
//   ReadTuple  <- duckdb::BinaryDeserializer
//
// This is a compact, positional wire format (no field tags / names), so the
// reachable "data does not match schema" failures are: element-count
// mismatches between the writer and reader shapes, out-of-range enum values,
// and out-of-range variant indices. Every one of those must throw rather than
// return a corrupt value or read out of bounds.

#include <absl/algorithm/container.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <deque>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "basics/serializer.h"

namespace {

using sdb::basics::ReadTuple;
using sdb::basics::WriteTuple;

template<typename T, typename Arg = sdb::basics::detail::Empty>
void RoundTrip(const T& in, const Arg& arg = {}) {
  duckdb::MemoryStream stream;
  {
    duckdb::BinarySerializer sink{stream};
    WriteTuple(sink, in, arg);
  }
  stream.Rewind();
  T out{};
  duckdb::BinaryDeserializer source{stream};
  ReadTuple(source, out, arg);
  EXPECT_EQ(in, out);
}

// Serialize `wire` (any shape) and try to read it back as a `Target`. Returns
// the thrown message, or "" if the read unexpectedly succeeded.
template<typename Target, typename Wire>
std::string ReadError(const Wire& wire) {
  duckdb::MemoryStream stream;
  {
    duckdb::BinarySerializer sink{stream};
    WriteTuple(sink, wire);
  }
  stream.Rewind();
  duckdb::BinaryDeserializer source{stream};
  Target out{};
  try {
    ReadTuple(source, out);
  } catch (const std::exception& e) {
    return e.what();
  }
  return {};
}

template<typename Target, typename Wire>
bool ReadFails(const Wire& wire) {
  return !ReadError<Target>(wire).empty();
}

// ===========================================================================
// Shared types
// ===========================================================================

template<typename T>
struct Box {
  T v{};
  bool operator==(const Box&) const = default;
};

enum class Color : uint8_t { Red, Green, Blue };
enum class Signed : int16_t { Neg = -3, Zero = 0, Pos = 7 };
// int64 underlying type, but enumerators stay inside magic_enum's reflectable
// range -- the wide underlying type is what is exercised on the wire.
enum class Wide : int64_t { Lo = -100, Hi = 100 };

struct Inner {
  int32_t x{};
  bool operator==(const Inner&) const = default;
};

struct Outer {
  Inner inner;
  int32_t y{};
  bool operator==(const Outer&) const = default;
};

struct Everything {
  bool b{};
  int8_t i8{};
  uint8_t u8{};
  int16_t i16{};
  uint16_t u16{};
  int32_t i32{};
  uint32_t u32{};
  int64_t i64{};
  uint64_t u64{};
  float f{};
  double d{};
  std::string s;
  Color color{};
  std::optional<int32_t> opt;
  std::vector<int32_t> vec;
  std::map<std::string, int32_t> map;
  bool operator==(const Everything&) const = default;
};

struct Pointers {
  std::unique_ptr<int32_t> up;
  std::shared_ptr<std::string> sp;
  std::vector<std::unique_ptr<int32_t>> vec;
  bool operator==(const Pointers& r) const {
    auto eq = [](const auto& a, const auto& b) {
      return (!a && !b) || (a && b && *a == *b);
    };
    return eq(up, r.up) && eq(sp, r.sp) && absl::c_equal(vec, r.vec, eq);
  }
};

// User-defined SerdeRead/SerdeWrite dispatch (sink/source protocol directly).
class MyCustomInt {
 public:
  MyCustomInt() = default;
  explicit MyCustomInt(int64_t value) : _value{value} {}
  int64_t Value() const { return _value; }
  bool operator==(const MyCustomInt&) const = default;

  template<typename Context>
  friend void SerdeRead(Context ctx, MyCustomInt& v) {
    v._value = ctx.io().ReadSignedInt64();
  }
  template<typename Context>
  friend void SerdeWrite(Context ctx, MyCustomInt v) {
    ctx.io().WriteValue(v._value);
  }

 private:
  int64_t _value{};
};

// ===========================================================================
// Primitive round-trips over every supported scalar type. The binary format
// is byte-exact for floating point, so extreme values are fair game here.
// ===========================================================================

template<typename T>
std::vector<T> Samples() {
  if constexpr (std::is_same_v<T, bool>) {
    return {true, false};
  } else if constexpr (std::is_floating_point_v<T>) {
    return {T(0),
            T(1),
            T(-1),
            T(0.5),
            T(-0.25),
            T(123.5),
            std::numeric_limits<T>::min(),
            std::numeric_limits<T>::max(),
            std::numeric_limits<T>::lowest()};
  } else {
    std::vector<T> v{T(0), T(1), std::numeric_limits<T>::min(),
                     std::numeric_limits<T>::max()};
    if constexpr (std::is_signed_v<T>) {
      v.push_back(T(-1));
    }
    return v;
  }
}

template<typename T>
class BinPrim : public ::testing::Test {};

// `char` is intentionally absent: WriteTuple serializes it (its arithmetic
// branch lists `char`), but ReadTuple has no `char` case and misreads the
// byte as a list header. See BinCharGap below.
using PrimTypes =
  ::testing::Types<bool, int8_t, uint8_t, int16_t, uint16_t, int32_t, uint32_t,
                   int64_t, uint64_t, float, double>;
TYPED_TEST_SUITE(BinPrim, PrimTypes);

TYPED_TEST(BinPrim, BareRoundTrip) {
  for (TypeParam v : Samples<TypeParam>()) {
    duckdb::MemoryStream stream;
    {
      duckdb::BinarySerializer sink{stream};
      WriteTuple(sink, v);
    }
    stream.Rewind();
    TypeParam out{};
    duckdb::BinaryDeserializer source{stream};
    ReadTuple(source, out);
    EXPECT_EQ(out, v);
  }
}

TYPED_TEST(BinPrim, BoxedRoundTrip) {
  for (TypeParam v : Samples<TypeParam>()) {
    RoundTrip(Box<TypeParam>{v});
  }
}

TYPED_TEST(BinPrim, VectorRoundTrip) {
  // std::vector<bool> is a proxy container the serializer does not model.
  if constexpr (!std::is_same_v<TypeParam, bool>) {
    RoundTrip(Box<std::vector<TypeParam>>{Samples<TypeParam>()});
  }
}

// Documented asymmetry: WriteTuple accepts `char` (it is listed in the
// arithmetic static_assert and written via WriteValue(char)), but ReadTuple
// has no `char` branch, so it falls through to the list-reading path and
// misreads the single byte. `char` is therefore not round-trippable through
// the tuple/binary format. If ReadTuple gains a `char` case, drop this test
// and add `char` back to BinPrim's PrimTypes.
TEST(BinCharGap, char_tuple_read_is_unsupported) {
  duckdb::MemoryStream stream;
  {
    duckdb::BinarySerializer sink{stream};
    WriteTuple(sink, char{'A'});
  }
  stream.Rewind();
  duckdb::BinaryDeserializer source{stream};
  char out = 0;
  bool threw = false;
  try {
    ReadTuple(source, out);
  } catch (...) {
    threw = true;
  }
  EXPECT_TRUE(threw || out != char{'A'});
}

// ===========================================================================
// Strings -- binary strings are length-prefixed, so any byte sequence works.
// ===========================================================================

TEST(BinString, empty) { RoundTrip(Box<std::string>{""}); }
TEST(BinString, simple) { RoundTrip(Box<std::string>{"hello"}); }
TEST(BinString, embedded_nul) {
  RoundTrip(Box<std::string>{std::string{"a\0b\0c", 5}});
}
TEST(BinString, all_bytes) {
  std::string s;
  for (int c = 0; c < 256; ++c) {
    s.push_back(static_cast<char>(c));
  }
  RoundTrip(Box<std::string>{s});
}
TEST(BinString, long_string) {
  RoundTrip(Box<std::string>{std::string(50000, 'q')});
}
TEST(BinString, unicode) { RoundTrip(Box<std::string>{"héllo ☃ 漢字"}); }
TEST(BinString, vector_of_strings) {
  RoundTrip(Box<std::vector<std::string>>{{"", "a", std::string{"x\0y", 3}}});
}

// ===========================================================================
// Enums.
// ===========================================================================

TEST(BinEnum, color_each) {
  RoundTrip(Box<Color>{Color::Red});
  RoundTrip(Box<Color>{Color::Green});
  RoundTrip(Box<Color>{Color::Blue});
}
TEST(BinEnum, signed_each) {
  RoundTrip(Box<Signed>{Signed::Neg});
  RoundTrip(Box<Signed>{Signed::Zero});
  RoundTrip(Box<Signed>{Signed::Pos});
}
TEST(BinEnum, wide_each) {
  RoundTrip(Box<Wide>{Wide::Lo});
  RoundTrip(Box<Wide>{Wide::Hi});
}
TEST(BinEnum, vector_of_enum) {
  RoundTrip(Box<std::vector<Color>>{{Color::Red, Color::Blue, Color::Green}});
}
TEST(BinEnum, map_enum_key) {
  RoundTrip(Box<std::map<Color, int32_t>>{{{Color::Red, 1}, {Color::Blue, 3}}});
}

// ===========================================================================
// Optionals / pointers.
// ===========================================================================

TEST(BinOpt, present) { RoundTrip(Box<std::optional<int32_t>>{42}); }
TEST(BinOpt, absent) { RoundTrip(Box<std::optional<int32_t>>{}); }
TEST(BinOpt, optional_string) {
  RoundTrip(Box<std::optional<std::string>>{"x"});
  RoundTrip(Box<std::optional<std::string>>{});
}
TEST(BinOpt, optional_struct) {
  RoundTrip(Box<std::optional<Inner>>{Inner{.x = 9}});
  RoundTrip(Box<std::optional<Inner>>{});
}
TEST(BinOpt, vector_of_optional) {
  RoundTrip(Box<std::vector<std::optional<int32_t>>>{{1, std::nullopt, 3}});
}

TEST(BinPtr, unique_set_and_null) {
  Pointers p;
  p.up = std::make_unique<int32_t>(7);
  p.sp = std::make_shared<std::string>("hi");
  p.vec.push_back(std::make_unique<int32_t>(1));
  p.vec.push_back(nullptr);
  p.vec.push_back(std::make_unique<int32_t>(2));
  RoundTrip(p);
}
TEST(BinPtr, all_null) { RoundTrip(Pointers{}); }

// ===========================================================================
// Containers.
// ===========================================================================

TEST(BinContainer, vector_empty) { RoundTrip(Box<std::vector<int32_t>>{}); }
TEST(BinContainer, vector_many) {
  RoundTrip(Box<std::vector<int32_t>>{{1, 2, 3, 4, 5, 6, 7}});
}
TEST(BinContainer, list) { RoundTrip(Box<std::list<int32_t>>{{1, 2, 3}}); }
TEST(BinContainer, deque) { RoundTrip(Box<std::deque<int32_t>>{{4, 5, 6}}); }
TEST(BinContainer, set) { RoundTrip(Box<std::set<int32_t>>{{3, 1, 2}}); }
TEST(BinContainer, unordered_set) {
  RoundTrip(Box<std::unordered_set<int32_t>>{{10, 20, 30}});
}
TEST(BinContainer, map_string) {
  RoundTrip(Box<std::map<std::string, int32_t>>{{{"a", 1}, {"b", 2}}});
}
TEST(BinContainer, map_int) {
  RoundTrip(Box<std::map<int32_t, std::string>>{{{1, "a"}, {2, "b"}}});
}
TEST(BinContainer, unordered_map) {
  RoundTrip(
    Box<std::unordered_map<std::string, int32_t>>{{{"x", 1}, {"y", 2}}});
}
TEST(BinContainer, vector_of_vector) {
  RoundTrip(Box<std::vector<std::vector<int32_t>>>{{{1, 2}, {}, {3}}});
}
TEST(BinContainer, vector_of_struct) {
  RoundTrip(Box<std::vector<Inner>>{{{.x = 1}, {.x = 2}}});
}
TEST(BinContainer, map_of_vector) {
  RoundTrip(Box<std::map<std::string, std::vector<int32_t>>>{
    {{"a", {1, 2}}, {"b", {}}}});
}

// ===========================================================================
// Tuples / pairs / arrays.
// ===========================================================================

TEST(BinTuple, pair) {
  RoundTrip(Box<std::pair<int32_t, std::string>>{{7, "z"}});
}
TEST(BinTuple, triple) {
  RoundTrip(Box<std::tuple<int32_t, std::string, bool>>{{1, "a", true}});
}
TEST(BinTuple, array_ints) {
  RoundTrip(Box<std::array<int32_t, 3>>{{{1, 2, 3}}});
}
TEST(BinTuple, array_strings) {
  RoundTrip(Box<std::array<std::string, 2>>{{{"a", "bb"}}});
}
TEST(BinTuple, nested_array) {
  RoundTrip(Box<std::array<std::array<int32_t, 2>, 2>>{{{{{1, 2}}, {{3, 4}}}}});
}

// ===========================================================================
// Variants.
// ===========================================================================

using Var3 = std::variant<int32_t, std::string, double>;

TEST(BinVariant, alt0) { RoundTrip(Box<Var3>{Var3{int32_t{5}}}); }
TEST(BinVariant, alt1) { RoundTrip(Box<Var3>{Var3{std::string{"hi"}}}); }
TEST(BinVariant, alt2) { RoundTrip(Box<Var3>{Var3{2.5}}); }
TEST(BinVariant, bare) { RoundTrip(Var3{std::string{"bare"}}); }
TEST(BinVariant, vector_of_variant) {
  RoundTrip(Box<std::vector<Var3>>{{int32_t{1}, std::string{"a"}, 3.5}});
}

// ===========================================================================
// Complex aggregates and custom dispatch.
// ===========================================================================

TEST(BinComplex, everything_default) { RoundTrip(Everything{}); }
TEST(BinComplex, everything_populated) {
  RoundTrip(Everything{
    .b = true,
    .i8 = -8,
    .u8 = 200,
    .i16 = -300,
    .u16 = 40000,
    .i32 = -123456,
    .u32 = 4000000000u,
    .i64 = -5000000000LL,
    .u64 = 18000000000000000000ull,
    .f = 1.5f,
    .d = 3.141592653589793,
    .s = std::string{"em\0bed", 6},
    .color = Color::Blue,
    .opt = 99,
    .vec = {1, 2, 3},
    .map = {{"k", 7}},
  });
}
TEST(BinComplex, nested) { RoundTrip(Outer{.inner = {.x = 5}, .y = 6}); }
TEST(BinComplex, vector_of_outer) {
  RoundTrip(Box<std::vector<Outer>>{
    {{.inner = {.x = 1}, .y = 2}, {.inner = {.x = 3}, .y = 4}}});
}

TEST(BinCustom, custom_serde) {
  struct Test {
    int32_t a{};
    MyCustomInt id{54};
    bool operator==(const Test&) const = default;
  };
  RoundTrip(Test{.a = 1, .id = MyCustomInt{2}});
}

// ===========================================================================
// FAILURE MODES: element-count mismatch between writer and reader shapes.
// ===========================================================================

struct F1 {
  int32_t a{};
  bool operator==(const F1&) const = default;
};
struct F2 {
  int32_t a{};
  int32_t b{};
  bool operator==(const F2&) const = default;
};
struct F3 {
  int32_t a{};
  int32_t b{};
  int32_t c{};
  bool operator==(const F3&) const = default;
};

TEST(BinFailCount, struct_too_few_fields) {
  const std::string m = ReadError<F2>(F1{.a = 42});
  EXPECT_NE(m.find("element"), std::string::npos) << m;
}
TEST(BinFailCount, struct_too_many_fields) {
  const std::string m = ReadError<F1>(F2{.a = 1, .b = 2});
  EXPECT_NE(m.find("element"), std::string::npos) << m;
}
TEST(BinFailCount, struct_one_vs_three) {
  EXPECT_TRUE((ReadFails<F3>(F1{.a = 1})));
  EXPECT_TRUE((ReadFails<F1>(F3{.a = 1, .b = 2, .c = 3})));
}
TEST(BinFailCount, array_too_short) {
  EXPECT_TRUE(
    (ReadFails<std::array<int32_t, 2>>(std::array<int32_t, 3>{1, 2, 3})));
}
TEST(BinFailCount, array_too_long) {
  EXPECT_TRUE(
    (ReadFails<std::array<int32_t, 3>>(std::array<int32_t, 2>{1, 2})));
}
TEST(BinFailCount, tuple_size_mismatch) {
  EXPECT_TRUE((ReadFails<std::tuple<int32_t, int32_t>>(
    std::tuple<int32_t, int32_t, int32_t>{1, 2, 3})));
  EXPECT_TRUE((ReadFails<std::tuple<int32_t, int32_t, int32_t>>(
    std::tuple<int32_t, int32_t>{1, 2})));
}

// Nested count mismatch is detected and surfaces while reading the inner
// element.
struct NInner2 {
  int32_t a{};
  int32_t b{};
  bool operator==(const NInner2&) const = default;
};
struct NOuterWire {
  int32_t x{};
  NInner2 inner;
  bool operator==(const NOuterWire&) const = default;
};
struct NInner1 {
  int32_t a{};
  bool operator==(const NInner1&) const = default;
};
struct NOuterTarget {
  int32_t x{};
  NInner1 inner;
  bool operator==(const NOuterTarget&) const = default;
};

TEST(BinFailCount, nested_struct_field_count_mismatch) {
  const std::string m =
    ReadError<NOuterTarget>(NOuterWire{.x = 1, .inner = {.a = 2, .b = 3}});
  EXPECT_FALSE(m.empty()) << m;
}

// ===========================================================================
// FAILURE MODES: out-of-range enum values.
// ===========================================================================

enum class EI32 : int32_t { A = 1, B = 2 };
enum class EU8 : uint8_t { X = 1, Y = 2 };
enum class EI64 : int64_t { L = 1 };

TEST(BinFailEnum, int32_underlying_out_of_range) {
  const std::string m = ReadError<Box<EI32>>(Box<int32_t>{999});
  EXPECT_NE(m.find("enum"), std::string::npos) << m;
}
TEST(BinFailEnum, uint8_underlying_out_of_range) {
  EXPECT_TRUE((ReadFails<Box<EU8>>(Box<uint8_t>{200})));
}
TEST(BinFailEnum, int64_underlying_out_of_range) {
  EXPECT_TRUE((ReadFails<Box<EI64>>(Box<int64_t>{77})));
}
TEST(BinFailEnum, enum_in_map_value_out_of_range) {
  EXPECT_TRUE((ReadFails<std::map<std::string, EI32>>(
    std::map<std::string, int32_t>{{"k", 0}})));
}
TEST(BinFailEnum, enum_in_vector_out_of_range) {
  EXPECT_TRUE((ReadFails<std::vector<EU8>>(std::vector<uint8_t>{1, 250})));
}

// ===========================================================================
// FAILURE MODES: variant index out of range.
// ===========================================================================

using Wide6 =
  std::variant<int32_t, int32_t, int32_t, int32_t, int32_t, int32_t>;
using Narrow2 = std::variant<int32_t, int32_t>;

TEST(BinFailVariant, index_out_of_range) {
  Wide6 w;
  w.emplace<5>(123);
  const std::string m = ReadError<Narrow2>(w);
  EXPECT_NE(m.find("out of range"), std::string::npos) << m;
}
TEST(BinFailVariant, index_out_of_range_boxed) {
  Box<Wide6> w;
  w.v.emplace<4>(7);
  EXPECT_TRUE((ReadFails<Box<Narrow2>>(w)));
}

// ===========================================================================
// Deeper nesting and additional composite shapes.
// ===========================================================================

struct L3b {
  int32_t c{};
  bool operator==(const L3b&) const = default;
};
struct L2b {
  int32_t b{};
  L3b l3;
  bool operator==(const L2b&) const = default;
};
struct L1b {
  int32_t a{};
  L2b l2;
  bool operator==(const L1b&) const = default;
};

TEST(BinNest, three_level_struct) {
  RoundTrip(L1b{.a = 1, .l2 = {.b = 2, .l3 = {.c = 3}}});
}
TEST(BinNest, vector_cubed) {
  RoundTrip(Box<std::vector<std::vector<std::vector<int32_t>>>>{
    {{{1, 2}, {3}}, {}, {{4, 5, 6}}}});
}
TEST(BinNest, box_in_box) {
  Box<Box<Box<int32_t>>> b;
  b.v.v.v = 7;
  RoundTrip(b);
}
TEST(BinNest, map_negative_int_keys) {
  Box<std::map<int32_t, int32_t>> b;
  b.v = std::map<int32_t, int32_t>{{-5, 1}, {3, 2}};
  RoundTrip(b);
}
TEST(BinNest, map_of_struct) {
  Box<std::map<std::string, Inner>> b;
  b.v = std::map<std::string, Inner>{{"a", {.x = 1}}, {"b", {.x = 2}}};
  RoundTrip(b);
}
TEST(BinNest, optional_vector_of_struct) {
  Box<std::optional<std::vector<Inner>>> present;
  present.v = std::vector<Inner>{{.x = 1}, {.x = 2}};
  RoundTrip(present);
  RoundTrip(Box<std::optional<std::vector<Inner>>>{});
}
TEST(BinNest, pair_of_struct) {
  RoundTrip(Box<std::pair<Inner, Inner>>{{{.x = 1}, {.x = 2}}});
}

}  // namespace
