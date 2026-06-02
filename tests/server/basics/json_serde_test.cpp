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

#include <gtest/gtest.h>
#include <simdjson.h>

#include <array>
#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <variant>
#include <vector>

#include "basics/serializer.h"
#include "basics/simdjson_sink.h"

namespace {

template<typename T>
std::string ToJson(const T& value) {
  simdjson::builder::string_builder sb(256);
  {
    sdb::basics::JsonSink sink{sb};
    sdb::basics::WriteObject(sink, value);
  }
  std::string_view body;
  EXPECT_EQ(sb.view().get(body), simdjson::SUCCESS);
  return std::string{body};
}

template<typename T>
T FromJson(std::string_view json) {
  simdjson::padded_string padded{json};
  simdjson::ondemand::parser parser;
  simdjson::ondemand::document doc;
  EXPECT_EQ(parser.iterate(padded).get(doc), simdjson::SUCCESS);
  T out{};
  sdb::basics::JsonSource source{doc};
  sdb::basics::ReadObject(source, out);
  return out;
}

struct Flat {
  int i{};
  std::string s;
  bool operator==(const Flat&) const = default;
};

struct WithOpt {
  int a{};
  std::optional<int> b;
  int c{};
  bool operator==(const WithOpt&) const = default;
};

struct OptFirst {
  std::optional<int> a;
  int b{};
  bool operator==(const OptFirst&) const = default;
};

struct OptLast {
  int a{};
  std::optional<int> b;
  bool operator==(const OptLast&) const = default;
};

struct AllOpt {
  std::optional<int> a;
  std::optional<int> b;
  bool operator==(const AllOpt&) const = default;
};

struct Inner {
  int x{};
  bool operator==(const Inner&) const = default;
};

struct Outer {
  Inner inner;
  int y{};
  bool operator==(const Outer&) const = default;
};

struct VecObj {
  int x{};
  bool operator==(const VecObj&) const = default;
};

struct WithVec {
  std::vector<VecObj> items;
  bool operator==(const WithVec&) const = default;
};

struct WithMap {
  std::map<std::string, int> m;
  bool operator==(const WithMap&) const = default;
};

struct WithVar {
  std::variant<int, std::string> v;
  bool operator==(const WithVar&) const = default;
};

enum class Color { Red, Green, Blue };

struct WithColor {
  Color c{};
  bool operator==(const WithColor&) const = default;
};

struct WithIntMap {
  std::map<int, int> m;
  bool operator==(const WithIntMap&) const = default;
};

struct WithBool {
  bool b = false;
  bool operator==(const WithBool&) const = default;
};

struct WithEnumMap {
  std::map<Color, int> m;
  bool operator==(const WithEnumMap&) const = default;
};

// ---------------------------------------------------------------------------
// Exact-output: the separator placement must produce byte-valid JSON, with no
// leading / trailing / doubled commas around skipped (absent-optional) members.
// ---------------------------------------------------------------------------

TEST(JsonSerde, flat_object) {
  EXPECT_EQ(ToJson(Flat{.i = 1, .s = "a"}), R"({"i":1,"s":"a"})");
}

TEST(JsonSerde, absent_optional_in_middle_is_omitted) {
  EXPECT_EQ(ToJson(WithOpt{.a = 1, .b = std::nullopt, .c = 3}),
            R"({"a":1,"c":3})");
}

TEST(JsonSerde, present_optional) {
  EXPECT_EQ(ToJson(WithOpt{.a = 1, .b = 2, .c = 3}), R"({"a":1,"b":2,"c":3})");
}

TEST(JsonSerde, absent_optional_first_no_leading_comma) {
  EXPECT_EQ(ToJson(OptFirst{.a = std::nullopt, .b = 2}), R"({"b":2})");
}

TEST(JsonSerde, absent_optional_last_no_trailing_comma) {
  EXPECT_EQ(ToJson(OptLast{.a = 1, .b = std::nullopt}), R"({"a":1})");
}

TEST(JsonSerde, all_absent_is_empty_object) {
  EXPECT_EQ(ToJson(AllOpt{}), R"({})");
}

TEST(JsonSerde, nested_object) {
  EXPECT_EQ(ToJson(Outer{.inner = {.x = 7}, .y = 9}),
            R"({"inner":{"x":7},"y":9})");
}

TEST(JsonSerde, scalar_array) {
  EXPECT_EQ(ToJson(std::vector<int>{1, 2, 3}), R"([1,2,3])");
}

TEST(JsonSerde, empty_array) { EXPECT_EQ(ToJson(std::vector<int>{}), R"([])"); }

TEST(JsonSerde, array_of_objects) {
  EXPECT_EQ(ToJson(WithVec{.items = {{.x = 1}, {.x = 2}}}),
            R"({"items":[{"x":1},{"x":2}]})");
}

TEST(JsonSerde, map_object) {
  EXPECT_EQ(ToJson(WithMap{.m = {{"k1", 1}, {"k2", 2}}}),
            R"({"m":{"k1":1,"k2":2}})");
}

TEST(JsonSerde, variant_pair) {
  EXPECT_EQ(ToJson(WithVar{.v = 42}), R"({"v":[0,42]})");
  EXPECT_EQ(ToJson(WithVar{.v = std::string{"hi"}}), R"({"v":[1,"hi"]})");
}

// ---------------------------------------------------------------------------
// Round-trip: write then read back yields an equal value.
// ---------------------------------------------------------------------------

template<typename T>
void RoundTrip(const T& in) {
  EXPECT_EQ(FromJson<T>(ToJson(in)), in);
}

TEST(JsonSerde, roundtrip) {
  RoundTrip(Flat{.i = -5, .s = "hello"});
  RoundTrip(WithOpt{.a = 1, .b = std::nullopt, .c = 3});
  RoundTrip(WithOpt{.a = 1, .b = 2, .c = 3});
  RoundTrip(OptFirst{.a = std::nullopt, .b = 2});
  RoundTrip(AllOpt{});
  RoundTrip(Outer{.inner = {.x = 7}, .y = 9});
  RoundTrip(WithVec{.items = {{.x = 1}, {.x = 2}, {.x = 3}}});
  RoundTrip(WithVec{.items = {}});
  RoundTrip(WithMap{.m = {{"k1", 1}, {"k2", 2}}});
  RoundTrip(WithVar{.v = 42});
  RoundTrip(WithVar{.v = std::string{"hi"}});
  RoundTrip(std::vector<int>{1, 2, 3});
}

// ---------------------------------------------------------------------------
// Single-pass by-name dispatch: must tolerate fields in any order and skip
// unknown fields (cases the round-trip, which always emits declaration order,
// can't reach).
// ---------------------------------------------------------------------------

TEST(JsonSerde, reads_fields_in_any_order) {
  EXPECT_EQ(FromJson<Flat>(R"({"s":"z","i":7})"), (Flat{.i = 7, .s = "z"}));
  EXPECT_EQ(FromJson<Outer>(R"({"y":9,"inner":{"x":7}})"),
            (Outer{.inner = {.x = 7}, .y = 9}));
}

TEST(JsonSerde, skips_unknown_fields) {
  EXPECT_EQ(FromJson<Flat>(R"({"i":1,"extra":99,"s":"x"})"),
            (Flat{.i = 1, .s = "x"}));
  EXPECT_EQ(FromJson<Flat>(R"({"extra":{"nested":[1,2]},"i":1,"s":"x"})"),
            (Flat{.i = 1, .s = "x"}));
}

// ---------------------------------------------------------------------------
// Non-conforming input: a missing field is tolerated (every field is read as
// optional, left default). Type / shape mismatches must fail cleanly by
// throwing -- never corrupt memory or read past a fixed-size target.
// ---------------------------------------------------------------------------

// Returns the message of the error raised while reading `json` into a `T`, or
// an empty string if the read unexpectedly succeeded.
template<typename T>
std::string ReadError(std::string_view json) {
  try {
    FromJson<T>(json);
  } catch (const std::exception& e) {
    return e.what();
  }
  return {};
}

TEST(JsonSerde, missing_field_is_left_default) {
  EXPECT_EQ(FromJson<Flat>(R"({"s":"a"})"), (Flat{.i = 0, .s = "a"}));
}

TEST(JsonSerde, scalar_type_mismatch_reports_expected_and_actual) {
  const std::string msg = ReadError<Flat>(R"({"i":"not-an-int","s":"a"})");
  EXPECT_NE(msg.find("expected number"), std::string::npos) << msg;
  EXPECT_NE(msg.find("found string"), std::string::npos) << msg;
}

TEST(JsonSerde, container_kind_mismatch_reports_expected_and_actual) {
  const std::string msg = ReadError<WithVec>(R"({"items":42})");
  EXPECT_NE(msg.find("expected array"), std::string::npos) << msg;
  EXPECT_NE(msg.find("found number"), std::string::npos) << msg;
}

TEST(JsonSerde, object_kind_mismatch_reports_expected_and_actual) {
  const std::string msg = ReadError<Outer>(R"({"inner":42})");
  EXPECT_NE(msg.find("expected object"), std::string::npos) << msg;
  EXPECT_NE(msg.find("found number"), std::string::npos) << msg;
}

TEST(JsonSerde, invalid_enum_value_throws) {
  const std::string msg = ReadError<WithColor>(R"({"c":"purple"})");
  EXPECT_NE(msg.find("Invalid enum value"), std::string::npos) << msg;
  EXPECT_NE(msg.find("purple"), std::string::npos) << msg;
}

TEST(JsonSerde, bad_integral_map_key_throws) {
  const std::string msg = ReadError<WithIntMap>(R"({"m":{"notanum":1}})");
  EXPECT_NE(msg.find("integral map key"), std::string::npos) << msg;
}

TEST(JsonSerde, fixed_array_length_mismatch) {
  using Arr = std::array<int, 2>;
  EXPECT_EQ(FromJson<Arr>("[1,2]"), (Arr{1, 2}));
  EXPECT_EQ(FromJson<Arr>("[9]"),
            (Arr{9, 0}));  // short: trailing slots default
  const std::string msg = ReadError<Arr>("[1,2,3]");  // long: bounded, no OOB
  EXPECT_NE(msg.find("more elements"), std::string::npos) << msg;
}

TEST(JsonSerde, variant_index_out_of_range_throws) {
  const std::string msg = ReadError<WithVar>(R"({"v":[5,0]})");
  EXPECT_NE(msg.find("out of range"), std::string::npos) << msg;
}

// --- per-primitive type mismatches ---

TEST(JsonSerde, bool_type_mismatch_reports_expected_and_actual) {
  const std::string msg = ReadError<WithBool>(R"({"b":42})");
  EXPECT_NE(msg.find("expected boolean"), std::string::npos) << msg;
  EXPECT_NE(msg.find("found number"), std::string::npos) << msg;
}

TEST(JsonSerde, string_type_mismatch_reports_expected_and_actual) {
  const std::string msg = ReadError<Flat>(R"({"i":1,"s":42})");
  EXPECT_NE(msg.find("expected string"), std::string::npos) << msg;
  EXPECT_NE(msg.find("found number"), std::string::npos) << msg;
}

TEST(JsonSerde, non_integer_number_into_int_throws) {
  // simdjson rejects rather than truncating a fractional number into an int.
  const std::string msg = ReadError<Flat>(R"({"i":1.5,"s":"a"})");
  EXPECT_FALSE(msg.empty());
}

// --- element / value type mismatches inside containers ---

TEST(JsonSerde, vector_element_type_mismatch_throws) {
  // items is a vector<object>; a scalar element fails when read as an object.
  const std::string msg = ReadError<WithVec>(R"({"items":[42]})");
  EXPECT_NE(msg.find("expected object"), std::string::npos) << msg;
  EXPECT_NE(msg.find("found number"), std::string::npos) << msg;
}

TEST(JsonSerde, map_value_type_mismatch_throws) {
  const std::string msg = ReadError<WithMap>(R"({"m":{"k":"x"}})");
  EXPECT_NE(msg.find("expected number"), std::string::npos) << msg;
  EXPECT_NE(msg.find("found string"), std::string::npos) << msg;
}

TEST(JsonSerde, map_kind_mismatch_throws) {
  const std::string msg = ReadError<WithMap>(R"({"m":42})");
  EXPECT_NE(msg.find("expected object"), std::string::npos) << msg;
  EXPECT_NE(msg.find("found number"), std::string::npos) << msg;
}

TEST(JsonSerde, invalid_enum_map_key_throws) {
  const std::string msg = ReadError<WithEnumMap>(R"({"m":{"purple":1}})");
  EXPECT_NE(msg.find("Invalid enum map key"), std::string::npos) << msg;
}

// --- variant sub-failures (array shape, index, payload) ---

TEST(JsonSerde, variant_not_an_array_throws) {
  const std::string msg = ReadError<WithVar>(R"({"v":42})");
  EXPECT_NE(msg.find("expected array"), std::string::npos) << msg;
}

TEST(JsonSerde, variant_index_not_a_number_throws) {
  const std::string msg = ReadError<WithVar>(R"({"v":["x",0]})");
  EXPECT_NE(msg.find("expected number"), std::string::npos) << msg;
}

TEST(JsonSerde, variant_payload_type_mismatch_throws) {
  // arm 0 is int; a string payload fails when read as an int.
  const std::string msg = ReadError<WithVar>(R"({"v":[0,"hi"]})");
  EXPECT_NE(msg.find("expected number"), std::string::npos) << msg;
  EXPECT_NE(msg.find("found string"), std::string::npos) << msg;
}

TEST(JsonSerde, variant_empty_array_throws) {
  const std::string msg = ReadError<WithVar>(R"({"v":[]})");
  EXPECT_NE(msg.find("expected variant"), std::string::npos) << msg;
}

TEST(JsonSerde, variant_missing_payload_throws) {
  const std::string msg = ReadError<WithVar>(R"({"v":[0]})");
  EXPECT_NE(msg.find("missing its value"), std::string::npos) << msg;
}

// --- tuple length (mirrors the fixed-array contract) ---

TEST(JsonSerde, tuple_exact_short_long) {
  using T = std::tuple<int, std::string>;
  EXPECT_EQ(FromJson<T>(R"([1,"a"])"), (T{1, "a"}));
  EXPECT_EQ(FromJson<T>("[1]"), (T{1, ""}));  // short: trailing defaults
  const std::string msg = ReadError<T>(R"([1,"a",99])");  // long: rejected
  EXPECT_NE(msg.find("more elements"), std::string::npos) << msg;
}

// --- writing a value outside the enum throws (matches the read side) ---

TEST(JsonSerde, write_invalid_enum_throws) {
  EXPECT_ANY_THROW(ToJson(WithColor{static_cast<Color>(99)}));
}

}  // namespace
