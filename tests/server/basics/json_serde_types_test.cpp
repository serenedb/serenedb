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

// Exhaustive coverage of the JSON sink adapter for basics/serializer.h:
//   WriteObject -> JsonSink (simdjson string_builder)
//   ReadObject  <- JsonSource (simdjson ondemand)
//
// The bulk of the cases are failure modes where the JSON payload does not
// match the C++ schema it is read into. A type mismatch must surface as a
// clean throw -- never a silent wrong value, an out-of-bounds read, or a
// corrupt result.
//
// JsonSource reads the document via document::get_value(), which simdjson only
// allows for a top-level array or object. Reading therefore always starts from
// an object or array; scalar round-trips are wrapped in a single-field struct
// (Box) so the document root is an object.

#include <gtest/gtest.h>
#include <simdjson.h>

#include <array>
#include <cstdint>
#include <deque>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "basics/serializer.h"
#include "basics/simdjson_sink.h"

namespace {

using sdb::basics::JsonSink;
using sdb::basics::JsonSource;
using sdb::basics::ReadObject;
using sdb::basics::WriteObject;

template<typename T>
std::string ToJson(const T& value) {
  simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::builder::string_builder sb(256);
  {
    JsonSink sink{sb};
    WriteObject(sink, value);
  }
  std::string_view body;
  EXPECT_EQ(sb.view().get(body), simdjson::SUCCESS);
  return std::string{body};
}

template<typename T>
T FromJson(std::string_view json) {
  simdjson::padded_string padded{json};
  simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::parser parser;
  simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::document doc;
  EXPECT_EQ(parser.iterate(padded).get(doc), simdjson::SUCCESS);
  T out{};
  JsonSource source{doc};
  ReadObject(source, out);
  return out;
}

// Write then read back; the value must survive unchanged. The top-level type
// must be an object or array (see file comment).
template<typename T>
void RoundTrip(const T& in) {
  EXPECT_EQ(FromJson<T>(ToJson(in)), in);
}

// Returns the exception message raised while reading well-formed `json` into a
// `T`, or "" if the read unexpectedly succeeded.
template<typename T>
std::string ReadError(std::string_view json) {
  try {
    (void)FromJson<T>(json);
  } catch (const std::exception& e) {
    return e.what();
  }
  return {};
}

// True when reading `json` into `T` either fails to parse or throws.
template<typename T>
bool ReadFails(std::string_view json) {
  simdjson::padded_string padded{json};
  simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::parser parser;
  simdjson::SIMDJSON_BUILTIN_IMPLEMENTATION::ondemand::document doc;
  if (parser.iterate(padded).get(doc) != simdjson::SUCCESS) {
    return true;
  }
  try {
    T out{};
    JsonSource source{doc};
    ReadObject(source, out);
  } catch (...) {
    return true;
  }
  return false;
}

// ===========================================================================
// Shared types
// ===========================================================================

template<typename T>
struct Box {
  T v{};
  bool operator==(const Box&) const = default;
};

struct Flat {
  int32_t i{};
  std::string s;
  bool operator==(const Flat&) const = default;
};

struct Inner {
  int32_t x{};
  bool operator==(const Inner&) const = default;
};

struct Outer {
  Inner inner;
  int32_t y{};
  bool operator==(const Outer&) const = default;
};

struct Deep3 {
  Outer outer;
  std::string tag;
  bool operator==(const Deep3&) const = default;
};

enum class Color : uint8_t { Red, Green, Blue };
enum class Signed : int16_t { Neg = -3, Zero = 0, Pos = 7 };

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

// Owns pointers, so it needs a hand-written comparison.
struct Pointers {
  std::unique_ptr<int32_t> up;
  std::shared_ptr<std::string> sp;
  bool operator==(const Pointers& r) const {
    auto eq = [](const auto& a, const auto& b) {
      return (!a && !b) || (a && b && *a == *b);
    };
    return eq(up, r.up) && eq(sp, r.sp);
  }
};

// ===========================================================================
// Primitive round-trips, multiplied over every supported scalar type. Scalars
// are wrapped (object root); a top-level array exercises the read path too.
// ===========================================================================

template<typename T>
std::vector<T> Samples() {
  if constexpr (std::is_same_v<T, bool>) {
    return {true, false};
  } else if constexpr (std::is_floating_point_v<T>) {
    return {T(0), T(1), T(-1), T(0.5), T(-0.25), T(123.5), T(-4096.0)};
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
class JsonPrim : public ::testing::Test {};

using PrimTypes =
  ::testing::Types<bool, char, int8_t, uint8_t, int16_t, uint16_t, int32_t,
                   uint32_t, int64_t, uint64_t, float, double>;
TYPED_TEST_SUITE(JsonPrim, PrimTypes);

TYPED_TEST(JsonPrim, BoxedRoundTrip) {
  for (TypeParam v : Samples<TypeParam>()) {
    Box<TypeParam> b{v};
    EXPECT_EQ(FromJson<Box<TypeParam>>(ToJson(b)), b);
  }
}

TYPED_TEST(JsonPrim, VectorRoundTrip) {
  // std::vector<bool> is a proxy container the serializer does not model.
  if constexpr (!std::is_same_v<TypeParam, bool>) {
    RoundTrip(Samples<TypeParam>());
  }
}

// ===========================================================================
// Exact JSON output (write side). Bare scalars write fine even though they
// cannot be read back as a top-level document.
// ===========================================================================

TEST(JsonOut, bool_true) { EXPECT_EQ(ToJson(true), "true"); }
TEST(JsonOut, bool_false) { EXPECT_EQ(ToJson(false), "false"); }
TEST(JsonOut, small_int) { EXPECT_EQ(ToJson(int32_t{5}), "5"); }
TEST(JsonOut, negative_int) { EXPECT_EQ(ToJson(int32_t{-7}), "-7"); }
TEST(JsonOut, zero) { EXPECT_EQ(ToJson(int32_t{0}), "0"); }

TEST(JsonOut, int64_min) {
  EXPECT_EQ(ToJson(std::numeric_limits<int64_t>::min()),
            "-9223372036854775808");
}
TEST(JsonOut, int64_max) {
  EXPECT_EQ(ToJson(std::numeric_limits<int64_t>::max()), "9223372036854775807");
}
TEST(JsonOut, uint64_max) {
  EXPECT_EQ(ToJson(std::numeric_limits<uint64_t>::max()),
            "18446744073709551615");
}

TEST(JsonOut, string_simple) {
  EXPECT_EQ(ToJson(std::string{"hi"}), R"("hi")");
}
TEST(JsonOut, string_empty) { EXPECT_EQ(ToJson(std::string{}), R"("")"); }
TEST(JsonOut, string_view) {
  EXPECT_EQ(ToJson(std::string_view{"abc"}), R"("abc")");
}
TEST(JsonOut, enum_name) { EXPECT_EQ(ToJson(Color::Green), R"("Green")"); }

TEST(JsonOut, flat_object) {
  EXPECT_EQ(ToJson(Flat{.i = 1, .s = "a"}), R"({"i":1,"s":"a"})");
}
TEST(JsonOut, nested_object) {
  EXPECT_EQ(ToJson(Outer{.inner = {.x = 7}, .y = 9}),
            R"({"inner":{"x":7},"y":9})");
}
TEST(JsonOut, deep_object) {
  EXPECT_EQ(ToJson(Deep3{.outer = {.inner = {.x = 1}, .y = 2}, .tag = "t"}),
            R"({"outer":{"inner":{"x":1},"y":2},"tag":"t"})");
}
TEST(JsonOut, scalar_array) {
  EXPECT_EQ(ToJson(std::vector<int32_t>{1, 2, 3}), "[1,2,3]");
}
TEST(JsonOut, empty_array) { EXPECT_EQ(ToJson(std::vector<int32_t>{}), "[]"); }
TEST(JsonOut, empty_object) {
  EXPECT_EQ(ToJson(std::map<std::string, int32_t>{}), "{}");
}
TEST(JsonOut, map_sorted) {
  EXPECT_EQ(ToJson(std::map<std::string, int32_t>{{"b", 2}, {"a", 1}}),
            R"({"a":1,"b":2})");
}
TEST(JsonOut, enum_map) {
  EXPECT_EQ(ToJson(std::map<Color, int32_t>{{Color::Red, 0}, {Color::Blue, 2}}),
            R"({"Red":0,"Blue":2})");
}
TEST(JsonOut, int_map) {
  EXPECT_EQ(ToJson(std::map<int32_t, int32_t>{{1, 10}, {2, 20}}),
            R"({"1":10,"2":20})");
}
TEST(JsonOut, pair_is_array) {
  EXPECT_EQ(ToJson(std::pair<int32_t, std::string>{3, "x"}), R"([3,"x"])");
}
TEST(JsonOut, tuple_is_array) {
  EXPECT_EQ(ToJson(std::tuple<int32_t, int32_t, int32_t>{1, 2, 3}), "[1,2,3]");
}
TEST(JsonOut, variant_index0) {
  EXPECT_EQ(ToJson(std::variant<int32_t, std::string>{42}), R"([0,42])");
}
TEST(JsonOut, variant_index1) {
  EXPECT_EQ(ToJson(std::variant<int32_t, std::string>{std::string{"hi"}}),
            R"([1,"hi"])");
}
TEST(JsonOut, optional_present_is_bare_value) {
  EXPECT_EQ(ToJson(std::optional<int32_t>{5}), "5");
}
TEST(JsonOut, optional_absent_is_null) {
  EXPECT_EQ(ToJson(std::optional<int32_t>{}), "null");
}
TEST(JsonOut, null_pointer_is_null) {
  EXPECT_EQ(ToJson(std::unique_ptr<int32_t>{}), "null");
}
TEST(JsonOut, boxed_optional_absent_is_empty_object) {
  EXPECT_EQ(ToJson(Box<std::optional<int32_t>>{}), "{}");
}

// ===========================================================================
// Strings -- escaping, unicode, length. JSON strings are valid UTF-8 only;
// arbitrary byte payloads are exercised by the binary adapter, not here.
// ===========================================================================

TEST(JsonString, quote_escape) {
  RoundTrip(Box<std::string>{R"(he said "hi")"});
}
TEST(JsonString, backslash_escape) { RoundTrip(Box<std::string>{R"(a\b\c)"}); }
TEST(JsonString, newline_tab) {
  RoundTrip(Box<std::string>{"line1\nline2\ttab"});
}
TEST(JsonString, control_chars) {
  RoundTrip(Box<std::string>{"\x01\x02\x1f end"});
}
TEST(JsonString, unicode_utf8) {
  RoundTrip(Box<std::string>{"héllo wörld ☃ 漢字"});
}
TEST(JsonString, only_spaces) { RoundTrip(Box<std::string>{"   "}); }
TEST(JsonString, long_string) {
  RoundTrip(Box<std::string>{std::string(10000, 'z')});
}
TEST(JsonString, slash_is_preserved) { RoundTrip(Box<std::string>{"a/b/c"}); }
TEST(JsonString, empty) { RoundTrip(Box<std::string>{""}); }
TEST(JsonString, vector_of_strings) {
  RoundTrip(std::vector<std::string>{"", "a", R"(q"q)", "long enough string"});
}

// ===========================================================================
// Enums.
// ===========================================================================

TEST(JsonEnum, each_value_roundtrip) {
  RoundTrip(Box<Color>{Color::Red});
  RoundTrip(Box<Color>{Color::Green});
  RoundTrip(Box<Color>{Color::Blue});
}
TEST(JsonEnum, signed_underlying_roundtrip) {
  RoundTrip(Box<Signed>{Signed::Neg});
  RoundTrip(Box<Signed>{Signed::Zero});
  RoundTrip(Box<Signed>{Signed::Pos});
}
TEST(JsonEnum, read_is_case_insensitive) {
  EXPECT_EQ(FromJson<Box<Color>>(R"({"v":"green"})"),
            (Box<Color>{Color::Green}));
  EXPECT_EQ(FromJson<Box<Color>>(R"({"v":"BLUE"})"), (Box<Color>{Color::Blue}));
  EXPECT_EQ(FromJson<Box<Color>>(R"({"v":"rEd"})"), (Box<Color>{Color::Red}));
}
TEST(JsonEnum, vector_of_enum) {
  RoundTrip(std::vector<Color>{Color::Red, Color::Blue, Color::Green});
}
TEST(JsonEnum, invalid_name_throws) {
  const std::string msg = ReadError<Box<Color>>(R"({"v":"Purple"})");
  EXPECT_NE(msg.find("Invalid enum value"), std::string::npos) << msg;
  EXPECT_NE(msg.find("Purple"), std::string::npos) << msg;
}
TEST(JsonEnum, empty_name_throws) {
  EXPECT_TRUE(ReadFails<Box<Color>>(R"({"v":""})"));
}
TEST(JsonEnum, number_instead_of_name_throws) {
  const std::string msg = ReadError<Box<Color>>(R"({"v":1})");
  EXPECT_NE(msg.find("expected string"), std::string::npos) << msg;
}

// ===========================================================================
// Optionals.
// ===========================================================================

struct OptMid {
  int32_t a{};
  std::optional<int32_t> b;
  int32_t c{};
  bool operator==(const OptMid&) const = default;
};

TEST(JsonOpt, present) { RoundTrip(Box<std::optional<int32_t>>{42}); }
TEST(JsonOpt, absent) { RoundTrip(Box<std::optional<int32_t>>{}); }
TEST(JsonOpt, present_string) {
  RoundTrip(Box<std::optional<std::string>>{"x"});
}
TEST(JsonOpt, absent_in_struct_is_omitted) {
  EXPECT_EQ(ToJson(OptMid{.a = 1, .b = std::nullopt, .c = 3}),
            R"({"a":1,"c":3})");
}
TEST(JsonOpt, present_in_struct) {
  EXPECT_EQ(ToJson(OptMid{.a = 1, .b = 2, .c = 3}), R"({"a":1,"b":2,"c":3})");
  RoundTrip(OptMid{.a = 1, .b = 2, .c = 3});
}
TEST(JsonOpt, missing_reads_as_nullopt) {
  EXPECT_EQ(FromJson<OptMid>(R"({"a":1,"c":3})"),
            (OptMid{.a = 1, .b = std::nullopt, .c = 3}));
}
TEST(JsonOpt, explicit_null_reads_as_nullopt) {
  EXPECT_EQ(FromJson<Box<std::optional<int32_t>>>(R"({"v":null})"),
            (Box<std::optional<int32_t>>{}));
}
TEST(JsonOpt, optional_struct) {
  RoundTrip(Box<std::optional<Inner>>{Inner{.x = 9}});
  RoundTrip(Box<std::optional<Inner>>{});
}
TEST(JsonOpt, optional_vector) {
  RoundTrip(Box<std::optional<std::vector<int32_t>>>{{{1, 2, 3}}});
  RoundTrip(Box<std::optional<std::vector<int32_t>>>{});
}
TEST(JsonOpt, vector_of_optional) {
  RoundTrip(std::vector<std::optional<int32_t>>{1, std::nullopt, 3});
}
TEST(JsonOpt, map_of_optional) {
  RoundTrip(std::map<std::string, std::optional<int32_t>>{{"a", 1},
                                                          {"b", std::nullopt}});
}

// ===========================================================================
// Pointers.
// ===========================================================================

TEST(JsonPtr, boxed_optional_default_from_empty_object) {
  EXPECT_EQ(FromJson<Box<std::optional<int32_t>>>("{}"),
            (Box<std::optional<int32_t>>{}));
}
TEST(JsonPtr, struct_with_pointers) {
  Pointers p;
  p.up = std::make_unique<int32_t>(7);
  p.sp = std::make_shared<std::string>("hello");
  RoundTrip(p);
}
TEST(JsonPtr, struct_with_null_pointers) { RoundTrip(Pointers{}); }
TEST(JsonPtr, shared_only) {
  Pointers p;
  p.sp = std::make_shared<std::string>("only shared");
  RoundTrip(p);
}

// ===========================================================================
// Containers (top-level array / object).
// ===========================================================================

TEST(JsonContainer, vector_empty) { RoundTrip(std::vector<int32_t>{}); }
TEST(JsonContainer, vector_one) { RoundTrip(std::vector<int32_t>{42}); }
TEST(JsonContainer, vector_many) {
  RoundTrip(std::vector<int32_t>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
}
TEST(JsonContainer, list) { RoundTrip(std::list<int32_t>{5, 6, 7}); }
TEST(JsonContainer, deque) { RoundTrip(std::deque<int32_t>{9, 8, 7}); }
TEST(JsonContainer, set) { RoundTrip(std::set<int32_t>{3, 1, 2}); }
TEST(JsonContainer, unordered_set) {
  RoundTrip(std::unordered_set<int32_t>{10, 20, 30});
}
TEST(JsonContainer, map_string_key) {
  RoundTrip(std::map<std::string, int32_t>{{"a", 1}, {"b", 2}});
}
TEST(JsonContainer, map_int_key) {
  RoundTrip(std::map<int32_t, int32_t>{{1, 100}, {2, 200}});
}
TEST(JsonContainer, map_int64_key) {
  RoundTrip(std::map<int64_t, std::string>{{1, "a"}, {-2, "b"}});
}
TEST(JsonContainer, map_uint64_key) {
  RoundTrip(std::map<uint64_t, int32_t>{{1, 10}, {2, 20}});
}
TEST(JsonContainer, map_enum_key) {
  RoundTrip(std::map<Color, int32_t>{{Color::Red, 1}, {Color::Blue, 3}});
}
TEST(JsonContainer, unordered_map) {
  RoundTrip(std::unordered_map<std::string, int32_t>{{"x", 1}, {"y", 2}});
}
TEST(JsonContainer, vector_of_vector) {
  RoundTrip(std::vector<std::vector<int32_t>>{{1, 2}, {}, {3}});
}
TEST(JsonContainer, map_of_vector) {
  RoundTrip(
    std::map<std::string, std::vector<int32_t>>{{"a", {1, 2}}, {"b", {}}});
}
TEST(JsonContainer, vector_of_struct) {
  RoundTrip(std::vector<Inner>{{.x = 1}, {.x = 2}, {.x = 3}});
}
TEST(JsonContainer, vector_of_map) {
  RoundTrip(
    std::vector<std::map<std::string, int32_t>>{{{"a", 1}}, {{"b", 2}}});
}
TEST(JsonContainer, map_of_struct) {
  RoundTrip(std::map<std::string, Inner>{{"a", {.x = 1}}, {"b", {.x = 2}}});
}

// ===========================================================================
// Tuples / pairs / fixed arrays (top-level array).
// ===========================================================================

TEST(JsonTuple, pair) { RoundTrip(std::pair<int32_t, std::string>{7, "z"}); }
TEST(JsonTuple, triple) {
  RoundTrip(std::tuple<int32_t, std::string, bool>{1, "a", true});
}
TEST(JsonTuple, tuple_with_container) {
  RoundTrip(std::tuple<std::vector<int32_t>, std::string>{{1, 2, 3}, "k"});
}
TEST(JsonTuple, array_ints) { RoundTrip(std::array<int32_t, 3>{1, 2, 3}); }
TEST(JsonTuple, array_strings) {
  RoundTrip(std::array<std::string, 2>{"a", "bb"});
}
TEST(JsonTuple, array_exact_length) {
  EXPECT_EQ((FromJson<std::array<int32_t, 2>>("[1,2]")),
            (std::array<int32_t, 2>{1, 2}));
}
TEST(JsonTuple, array_short_pads_default) {
  EXPECT_EQ((FromJson<std::array<int32_t, 3>>("[9]")),
            (std::array<int32_t, 3>{9, 0, 0}));
}
TEST(JsonTuple, array_empty_all_default) {
  EXPECT_EQ((FromJson<std::array<int32_t, 2>>("[]")),
            (std::array<int32_t, 2>{0, 0}));
}
TEST(JsonTuple, nested_array) {
  RoundTrip(std::array<std::array<int32_t, 2>, 2>{{{1, 2}, {3, 4}}});
}
TEST(JsonTuple, array_of_struct) {
  RoundTrip(std::array<Inner, 2>{{{.x = 1}, {.x = 2}}});
}

// ===========================================================================
// Variants (top-level array [index, value]).
// ===========================================================================

using Var3 = std::variant<int32_t, std::string, double>;

TEST(JsonVariant, alt0) { RoundTrip(Var3{int32_t{5}}); }
TEST(JsonVariant, alt1) { RoundTrip(Var3{std::string{"hi"}}); }
TEST(JsonVariant, alt2) { RoundTrip(Var3{2.5}); }
TEST(JsonVariant, in_struct) { RoundTrip(Box<Var3>{Var3{std::string{"q"}}}); }
TEST(JsonVariant, vector_of_variant) {
  RoundTrip(std::vector<Var3>{int32_t{1}, std::string{"a"}, 3.5});
}
TEST(JsonVariant, variant_of_struct) {
  RoundTrip(std::variant<Inner, int32_t>{Inner{.x = 8}});
}

// ===========================================================================
// Complex / nested aggregates.
// ===========================================================================

TEST(JsonComplex, everything_default) { RoundTrip(Everything{}); }
TEST(JsonComplex, everything_populated) {
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
    .s = "mixed",
    .color = Color::Blue,
    .opt = 99,
    .vec = {1, 2, 3},
    .map = {{"k", 7}},
  });
}
TEST(JsonComplex, deep_nesting) {
  RoundTrip(Deep3{.outer = {.inner = {.x = 5}, .y = 6}, .tag = "deep"});
}
TEST(JsonComplex, vector_of_outer) {
  RoundTrip(std::vector<Outer>{{.inner = {.x = 1}, .y = 2},
                               {.inner = {.x = 3}, .y = 4}});
}
TEST(JsonComplex, map_string_to_vector_of_struct) {
  RoundTrip(std::map<std::string, std::vector<Inner>>{
    {"a", {{.x = 1}, {.x = 2}}}, {"b", {}}});
}
TEST(JsonComplex, double_pi_roundtrip) {
  RoundTrip(Box<double>{3.141592653589793});
}

// ===========================================================================
// By-name dispatch: order independence, unknown fields, missing fields.
// ===========================================================================

TEST(JsonDispatch, any_order) {
  EXPECT_EQ(FromJson<Flat>(R"({"s":"z","i":7})"), (Flat{.i = 7, .s = "z"}));
}
TEST(JsonDispatch, any_order_nested) {
  EXPECT_EQ(FromJson<Outer>(R"({"y":9,"inner":{"x":7}})"),
            (Outer{.inner = {.x = 7}, .y = 9}));
}
TEST(JsonDispatch, skip_unknown_scalar) {
  EXPECT_EQ(FromJson<Flat>(R"({"i":1,"extra":99,"s":"x"})"),
            (Flat{.i = 1, .s = "x"}));
}
TEST(JsonDispatch, skip_unknown_object) {
  EXPECT_EQ(FromJson<Flat>(R"({"extra":{"nested":[1,2]},"i":1,"s":"x"})"),
            (Flat{.i = 1, .s = "x"}));
}
TEST(JsonDispatch, skip_unknown_array) {
  EXPECT_EQ(FromJson<Flat>(R"({"i":1,"s":"x","extra":[1,[2,3],{"a":4}]})"),
            (Flat{.i = 1, .s = "x"}));
}
TEST(JsonDispatch, all_missing_is_default) {
  EXPECT_EQ(FromJson<Flat>("{}"), (Flat{}));
}
TEST(JsonDispatch, partial_missing) {
  EXPECT_EQ(FromJson<Flat>(R"({"s":"only"})"), (Flat{.i = 0, .s = "only"}));
}
TEST(JsonDispatch, nested_partial) {
  EXPECT_EQ(FromJson<Outer>(R"({"inner":{}})"),
            (Outer{.inner = {.x = 0}, .y = 0}));
}

// ===========================================================================
// FAILURE MODES: scalar type mismatches.
//
// A non-optional scalar field given the wrong JSON kind must throw with a
// message naming both the expected and the actual kind.
// ===========================================================================

using IntBox = Box<int32_t>;
using UIntBox = Box<uint64_t>;
using DblBox = Box<double>;
using BoolBox = Box<bool>;
using StrBox = Box<std::string>;

TEST(JsonFailScalar, int_from_string) {
  const std::string m = ReadError<IntBox>(R"({"v":"nope"})");
  EXPECT_NE(m.find("expected number"), std::string::npos) << m;
  EXPECT_NE(m.find("found string"), std::string::npos) << m;
}
TEST(JsonFailScalar, int_from_bool) {
  const std::string m = ReadError<IntBox>(R"({"v":true})");
  EXPECT_NE(m.find("expected number"), std::string::npos) << m;
  EXPECT_NE(m.find("found boolean"), std::string::npos) << m;
}
TEST(JsonFailScalar, int_from_array) {
  const std::string m = ReadError<IntBox>(R"({"v":[1,2]})");
  EXPECT_NE(m.find("expected number"), std::string::npos) << m;
  EXPECT_NE(m.find("found array"), std::string::npos) << m;
}
TEST(JsonFailScalar, int_from_object) {
  const std::string m = ReadError<IntBox>(R"({"v":{"a":1}})");
  EXPECT_NE(m.find("expected number"), std::string::npos) << m;
  EXPECT_NE(m.find("found object"), std::string::npos) << m;
}
TEST(JsonFailScalar, int_from_null) {
  const std::string m = ReadError<IntBox>(R"({"v":null})");
  EXPECT_NE(m.find("expected number"), std::string::npos) << m;
  EXPECT_NE(m.find("found null"), std::string::npos) << m;
}
TEST(JsonFailScalar, int_from_decimal) {
  EXPECT_TRUE(ReadFails<IntBox>(R"({"v":3.5})"));
}
TEST(JsonFailScalar, int_from_too_large) {
  EXPECT_TRUE(ReadFails<IntBox>(R"({"v":99999999999999999999})"));
}

TEST(JsonFailScalar, uint_from_negative) {
  EXPECT_TRUE(ReadFails<UIntBox>(R"({"v":-1})"));
}
TEST(JsonFailScalar, uint_from_string) {
  EXPECT_TRUE(ReadFails<UIntBox>(R"({"v":"x"})"));
}
TEST(JsonFailScalar, uint_from_bool) {
  EXPECT_TRUE(ReadFails<UIntBox>(R"({"v":false})"));
}

TEST(JsonFailScalar, double_from_string) {
  const std::string m = ReadError<DblBox>(R"({"v":"x"})");
  EXPECT_NE(m.find("expected number"), std::string::npos) << m;
  EXPECT_NE(m.find("found string"), std::string::npos) << m;
}
TEST(JsonFailScalar, double_from_bool) {
  EXPECT_TRUE(ReadFails<DblBox>(R"({"v":true})"));
}
TEST(JsonFailScalar, double_from_array) {
  EXPECT_TRUE(ReadFails<DblBox>(R"({"v":[]})"));
}
TEST(JsonFailScalar, double_from_object) {
  EXPECT_TRUE(ReadFails<DblBox>(R"({"v":{}})"));
}
TEST(JsonFailScalar, double_from_null) {
  EXPECT_TRUE(ReadFails<DblBox>(R"({"v":null})"));
}
TEST(JsonFailScalar, double_from_integer_is_ok) {
  EXPECT_EQ(FromJson<DblBox>(R"({"v":5})"), (DblBox{5.0}));
}

TEST(JsonFailScalar, bool_from_number) {
  const std::string m = ReadError<BoolBox>(R"({"v":1})");
  EXPECT_NE(m.find("expected boolean"), std::string::npos) << m;
  EXPECT_NE(m.find("found number"), std::string::npos) << m;
}
TEST(JsonFailScalar, bool_from_string) {
  EXPECT_TRUE(ReadFails<BoolBox>(R"({"v":"true"})"));
}
TEST(JsonFailScalar, bool_from_array) {
  EXPECT_TRUE(ReadFails<BoolBox>(R"({"v":[true]})"));
}
TEST(JsonFailScalar, bool_from_object) {
  EXPECT_TRUE(ReadFails<BoolBox>(R"({"v":{}})"));
}
TEST(JsonFailScalar, bool_from_null) {
  EXPECT_TRUE(ReadFails<BoolBox>(R"({"v":null})"));
}

TEST(JsonFailScalar, string_from_number) {
  const std::string m = ReadError<StrBox>(R"({"v":1})");
  EXPECT_NE(m.find("expected string"), std::string::npos) << m;
  EXPECT_NE(m.find("found number"), std::string::npos) << m;
}
TEST(JsonFailScalar, string_from_bool) {
  EXPECT_TRUE(ReadFails<StrBox>(R"({"v":true})"));
}
TEST(JsonFailScalar, string_from_array) {
  EXPECT_TRUE(ReadFails<StrBox>(R"({"v":["a"]})"));
}
TEST(JsonFailScalar, string_from_object) {
  EXPECT_TRUE(ReadFails<StrBox>(R"({"v":{}})"));
}
TEST(JsonFailScalar, string_from_null) {
  EXPECT_TRUE(ReadFails<StrBox>(R"({"v":null})"));
}

// ===========================================================================
// FAILURE MODES: container / object kind mismatches.
// ===========================================================================

using VecBox = Box<std::vector<int32_t>>;
using MapBox = Box<std::map<std::string, int32_t>>;

TEST(JsonFailKind, array_from_number) {
  const std::string m = ReadError<VecBox>(R"({"v":42})");
  EXPECT_NE(m.find("expected array"), std::string::npos) << m;
  EXPECT_NE(m.find("found number"), std::string::npos) << m;
}
TEST(JsonFailKind, array_from_string) {
  EXPECT_TRUE(ReadFails<VecBox>(R"({"v":"x"})"));
}
TEST(JsonFailKind, array_from_object) {
  const std::string m = ReadError<VecBox>(R"({"v":{"a":1}})");
  EXPECT_NE(m.find("expected array"), std::string::npos) << m;
  EXPECT_NE(m.find("found object"), std::string::npos) << m;
}
TEST(JsonFailKind, array_from_bool) {
  EXPECT_TRUE(ReadFails<VecBox>(R"({"v":true})"));
}
TEST(JsonFailKind, object_from_number) {
  const std::string m = ReadError<Outer>(R"({"inner":42})");
  EXPECT_NE(m.find("expected object"), std::string::npos) << m;
  EXPECT_NE(m.find("found number"), std::string::npos) << m;
}
TEST(JsonFailKind, object_from_array) {
  const std::string m = ReadError<Outer>(R"({"inner":[1,2]})");
  EXPECT_NE(m.find("expected object"), std::string::npos) << m;
  EXPECT_NE(m.find("found array"), std::string::npos) << m;
}
TEST(JsonFailKind, object_from_string) {
  EXPECT_TRUE(ReadFails<Outer>(R"({"inner":"x"})"));
}
TEST(JsonFailKind, map_from_array) {
  const std::string m = ReadError<MapBox>(R"({"v":[1,2]})");
  EXPECT_NE(m.find("expected object"), std::string::npos) << m;
}
TEST(JsonFailKind, map_from_number) {
  EXPECT_TRUE(ReadFails<MapBox>(R"({"v":7})"));
}
TEST(JsonFailKind, top_level_struct_from_number) {
  EXPECT_TRUE(ReadFails<Flat>("42"));
}
TEST(JsonFailKind, top_level_struct_from_array) {
  EXPECT_TRUE(ReadFails<Flat>("[1,2]"));
}
TEST(JsonFailKind, top_level_vector_from_object) {
  EXPECT_TRUE(ReadFails<std::vector<int32_t>>(R"({"a":1})"));
}

// ===========================================================================
// FAILURE MODES: element-level mismatches inside containers.
// ===========================================================================

TEST(JsonFailElem, vector_element_wrong_type) {
  const std::string m = ReadError<VecBox>(R"({"v":[1,"x",3]})");
  EXPECT_NE(m.find("expected number"), std::string::npos) << m;
  EXPECT_NE(m.find("found string"), std::string::npos) << m;
}
TEST(JsonFailElem, vector_element_null) {
  EXPECT_TRUE(ReadFails<VecBox>(R"({"v":[1,null]})"));
}
TEST(JsonFailElem, vector_of_struct_bad_element) {
  EXPECT_TRUE(ReadFails<Box<std::vector<Inner>>>(R"({"v":[{"x":1},5]})"));
}
TEST(JsonFailElem, set_element_wrong_type) {
  EXPECT_TRUE(ReadFails<Box<std::set<int32_t>>>(R"({"v":[1,"two"]})"));
}
TEST(JsonFailElem, map_value_wrong_type) {
  EXPECT_TRUE(ReadFails<MapBox>(R"({"v":{"a":1,"b":"x"}})"));
}
TEST(JsonFailElem, nested_vector_element_wrong_type) {
  EXPECT_TRUE(ReadFails<Box<std::vector<std::vector<int32_t>>>>(
    R"({"v":[[1,2],["x"]]})"));
}
TEST(JsonFailElem, deeply_nested_mismatch) {
  EXPECT_TRUE(ReadFails<Deep3>(R"({"outer":{"inner":{"x":"bad"},"y":2}})"));
}

// ===========================================================================
// FAILURE MODES: map keys.
// ===========================================================================

TEST(JsonFailKey, int_key_not_a_number) {
  const std::string m =
    ReadError<Box<std::map<int32_t, int32_t>>>(R"({"v":{"notanum":1}})");
  EXPECT_NE(m.find("integral map key"), std::string::npos) << m;
}
TEST(JsonFailKey, int_key_overflow) {
  EXPECT_TRUE(
    (ReadFails<Box<std::map<int32_t, int32_t>>>(R"({"v":{"99999999999":1}})")));
}
TEST(JsonFailKey, enum_key_invalid) {
  const std::string m =
    ReadError<Box<std::map<Color, int32_t>>>(R"({"v":{"Mauve":1}})");
  EXPECT_NE(m.find("enum map key"), std::string::npos) << m;
}

// ===========================================================================
// FAILURE MODES: fixed arrays.
// ===========================================================================

TEST(JsonFailArray, too_many_elements) {
  const std::string m = ReadError<std::array<int32_t, 2>>("[1,2,3]");
  EXPECT_NE(m.find("more elements"), std::string::npos) << m;
}
TEST(JsonFailArray, way_too_many_elements) {
  EXPECT_TRUE((ReadFails<std::array<int32_t, 1>>("[1,2,3,4,5]")));
}
TEST(JsonFailArray, element_wrong_type) {
  EXPECT_TRUE((ReadFails<std::array<int32_t, 2>>(R"([1,"x"])")));
}
TEST(JsonFailArray, not_an_array) {
  EXPECT_TRUE((ReadFails<std::array<int32_t, 2>>("42")));
}

// ===========================================================================
// FAILURE MODES: variants.
// ===========================================================================

TEST(JsonFailVariant, index_out_of_range) {
  const std::string m = ReadError<Var3>("[5,0]");
  EXPECT_NE(m.find("out of range"), std::string::npos) << m;
}
TEST(JsonFailVariant, index_far_out_of_range) {
  EXPECT_TRUE(ReadFails<Var3>("[99,0]"));
}
TEST(JsonFailVariant, index_not_a_number) {
  EXPECT_TRUE(ReadFails<Var3>(R"(["zero",0])"));
}
TEST(JsonFailVariant, payload_wrong_type) {
  // index 0 selects int32_t, but the payload is a string.
  EXPECT_TRUE(ReadFails<Var3>(R"([0,"not-an-int"])"));
}
TEST(JsonFailVariant, in_struct_index_out_of_range) {
  EXPECT_TRUE(ReadFails<Box<Var3>>(R"({"v":[7,0]})"));
}

// ===========================================================================
// FAILURE MODES: tuples / pairs.
// ===========================================================================

TEST(JsonFailTuple, element_wrong_type) {
  EXPECT_TRUE((ReadFails<std::pair<int32_t, std::string>>(R"(["x","y"])")));
}
TEST(JsonFailTuple, second_element_wrong_type) {
  EXPECT_TRUE((ReadFails<std::pair<int32_t, std::string>>("[1,2]")));
}
TEST(JsonFailTuple, not_an_array) {
  EXPECT_TRUE((ReadFails<std::tuple<int32_t, int32_t>>(R"({"a":1})")));
}

// ===========================================================================
// Number formats: exponent / sign forms, and integer-vs-float coercion.
// ===========================================================================

TEST(JsonNumber, exponent_into_double) {
  EXPECT_EQ(FromJson<DblBox>(R"({"v":1e3})"), (DblBox{1000.0}));
  EXPECT_EQ(FromJson<DblBox>(R"({"v":1.5e2})"), (DblBox{150.0}));
  EXPECT_EQ(FromJson<DblBox>(R"({"v":-2.5E-1})"), (DblBox{-0.25}));
}
TEST(JsonNumber, exponent_into_int_throws) {
  EXPECT_TRUE(ReadFails<IntBox>(R"({"v":1e3})"));
}
TEST(JsonNumber, negative_zero_into_int) {
  EXPECT_EQ(FromJson<IntBox>(R"({"v":-0})"), (IntBox{0}));
}
TEST(JsonNumber, integer_into_double_ok) {
  EXPECT_EQ(FromJson<DblBox>(R"({"v":-123})"), (DblBox{-123.0}));
}

// ===========================================================================
// FAILURE MODES: explicit null where a non-nullable composite is expected.
// ===========================================================================

TEST(JsonNullField, null_into_vector_throws) {
  const std::string m = ReadError<VecBox>(R"({"v":null})");
  EXPECT_NE(m.find("expected array"), std::string::npos) << m;
  EXPECT_NE(m.find("found null"), std::string::npos) << m;
}
TEST(JsonNullField, null_into_map_throws) {
  EXPECT_TRUE(ReadFails<MapBox>(R"({"v":null})"));
}
TEST(JsonNullField, null_into_nested_object_throws) {
  const std::string m = ReadError<Outer>(R"({"inner":null})");
  EXPECT_NE(m.find("expected object"), std::string::npos) << m;
  EXPECT_NE(m.find("found null"), std::string::npos) << m;
}

// ===========================================================================
// Maps of structs: round-trip and value-type mismatch.
// ===========================================================================

using MapInnerBox = Box<std::map<std::string, Inner>>;
using MapIntBox = Box<std::map<int32_t, int32_t>>;

TEST(JsonMapValue, struct_value_from_number_throws) {
  EXPECT_TRUE(ReadFails<MapInnerBox>(R"({"v":{"a":5}})"));
}
TEST(JsonMapValue, map_of_struct_roundtrip) {
  MapInnerBox b;
  b.v = std::map<std::string, Inner>{{"a", {.x = 1}}, {"b", {.x = 2}}};
  RoundTrip(b);
}
TEST(JsonMapValue, map_negative_int_keys) {
  MapIntBox b;
  b.v = std::map<int32_t, int32_t>{{-5, 1}, {3, 2}};
  RoundTrip(b);
}

// ===========================================================================
// Deeper nesting.
// ===========================================================================

TEST(JsonDeepNest, vector_cubed) {
  RoundTrip(std::vector<std::vector<std::vector<int32_t>>>{
    {{1, 2}, {3}}, {}, {{4, 5, 6}}});
}
TEST(JsonDeepNest, box_in_box) {
  Box<Box<Box<int32_t>>> b;
  b.v.v.v = 7;
  RoundTrip(b);
}
TEST(JsonDeepNest, optional_vector_of_struct) {
  Box<std::optional<std::vector<Inner>>> present;
  present.v = std::vector<Inner>{{.x = 1}, {.x = 2}};
  RoundTrip(present);
  RoundTrip(Box<std::optional<std::vector<Inner>>>{});
}

// ===========================================================================
// More variant / tuple shapes.
// ===========================================================================

TEST(JsonVariant2, payload_wrong_type_index1) {
  // alt 1 is std::string, payload is a number.
  EXPECT_TRUE(ReadFails<Var3>("[1,5]"));
}
TEST(JsonVariant2, payload_wrong_type_index2) {
  // alt 2 is double, payload is a string.
  EXPECT_TRUE(ReadFails<Var3>(R"([2,"x"])"));
}
TEST(JsonVariant2, pair_of_struct_roundtrip) {
  RoundTrip(std::pair<Inner, Inner>{{.x = 1}, {.x = 2}});
}

// ===========================================================================
// Empty composite fields read back as empty.
// ===========================================================================

TEST(JsonEmpty, empty_vector_field) {
  EXPECT_EQ(FromJson<VecBox>(R"({"v":[]})"), (VecBox{}));
}
TEST(JsonEmpty, empty_map_field) {
  EXPECT_EQ(FromJson<MapBox>(R"({"v":{}})"), (MapBox{}));
}

}  // namespace
