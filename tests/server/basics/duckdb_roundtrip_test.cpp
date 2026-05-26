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

// Struct round-trips through the duckdb-binary serializer/deserializer.

#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <list>
#include <magic_enum/magic_enum.hpp>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

#include "basics/serializer.h"

namespace {

// Round-trip a value through the duckdb-binary tuple pipeline and assert
// that the deserialised result compares equal to the original.
template<typename T, typename Arg = sdb::basics::detail::Empty>
void RoundTrip(const T& in, const Arg& arg = {}) {
  duckdb::MemoryStream stream;
  {
    duckdb::BinarySerializer sink{stream};
    sdb::basics::WriteTuple(sink, in, arg);
  }
  stream.Rewind();
  T out{};
  duckdb::BinaryDeserializer source{stream};
  sdb::basics::ReadTuple(source, out, arg);
  EXPECT_EQ(in, out);
}

// Round-trip in-place: deserialised value is read into `out` (caller-owned)
// for cases where the test wants to inspect the read-back value separately.
template<typename T, typename Arg = sdb::basics::detail::Empty>
void RoundTripInto(const T& in, T& out, const Arg& arg = {}) {
  duckdb::MemoryStream stream;
  {
    duckdb::BinarySerializer sink{stream};
    sdb::basics::WriteTuple(sink, in, arg);
  }
  stream.Rewind();
  duckdb::BinaryDeserializer source{stream};
  sdb::basics::ReadTuple(source, out, arg);
}

// ------------------------------------------------------------------
// Primitives wrapped in a single-field aggregate so the templated
// WriteTuple/ReadTuple has an aggregate to walk.
// ------------------------------------------------------------------

struct Wrap1Int {
  int v{};
  bool operator==(const Wrap1Int&) const = default;
};
struct Wrap1Double {
  double v{};
  bool operator==(const Wrap1Double&) const = default;
};
struct Wrap1Bool {
  bool v{};
  bool operator==(const Wrap1Bool&) const = default;
};
struct Wrap1String {
  std::string v;
  bool operator==(const Wrap1String&) const = default;
};
struct Wrap1U64 {
  uint64_t v{};
  bool operator==(const Wrap1U64&) const = default;
};

TEST(DuckRoundTrip, int) {
  RoundTrip(Wrap1Int{.v = 42});
  RoundTrip(Wrap1Int{.v = -42});
  RoundTrip(Wrap1Int{.v = 0});
}

TEST(DuckRoundTrip, double) {
  RoundTrip(Wrap1Double{.v = 123.456});
  RoundTrip(Wrap1Double{.v = -0.5});
}

TEST(DuckRoundTrip, bool) {
  RoundTrip(Wrap1Bool{.v = true});
  RoundTrip(Wrap1Bool{.v = false});
}

TEST(DuckRoundTrip, string) {
  RoundTrip(Wrap1String{.v = "foobar"});
  RoundTrip(Wrap1String{.v = ""});
}

TEST(DuckRoundTrip, uint64) { RoundTrip(Wrap1U64{.v = 0xdeadbeefcafebabeULL}); }

// ------------------------------------------------------------------
// Plain aggregates.
// ------------------------------------------------------------------

struct Dummy {
  int i{};
  double d{};
  bool b{};
  std::string s;
  bool operator==(const Dummy&) const = default;
};

struct Nested {
  Dummy dummy;
  bool operator==(const Nested&) const = default;
};

struct TypedInt {
  int value{};
  bool operator==(const TypedInt&) const = default;
  auto operator<=>(const TypedInt&) const = default;
};

struct Container {
  TypedInt i{};
  bool operator==(const Container&) const = default;
  auto operator<=>(const Container&) const = default;
};

TEST(DuckRoundTrip, flat_object) {
  RoundTrip(Dummy{.i = 42, .d = 123.456, .b = true, .s = "foobar"});
}

TEST(DuckRoundTrip, nested_object) {
  RoundTrip(Nested{.dummy = {.i = 42, .d = 123.456, .b = true, .s = "foobar"}});
}

TEST(DuckRoundTrip, typed_int) { RoundTrip(TypedInt{.value = 4}); }

TEST(DuckRoundTrip, container) { RoundTrip(Container{.i = {.value = 7}}); }

// ------------------------------------------------------------------
// Sequence containers.
// ------------------------------------------------------------------

struct ListThings {
  std::vector<TypedInt> vec;
  std::list<int> list;
  bool operator==(const ListThings&) const = default;
};

TEST(DuckRoundTrip, list_of_aggregates) {
  RoundTrip(ListThings{.vec = {{1}, {2}, {3}}, .list = {4, 5}});
}

TEST(DuckRoundTrip, empty_sequence_containers) { RoundTrip(ListThings{}); }

TEST(DuckRoundTrip, reads_into_populated_container) {
  // The resize path reads into existing slots; a pre-populated, larger target
  // must still end up with exactly the serialized contents.
  const ListThings in{.vec = {{1}, {2}}, .list = {7}};
  ListThings out{.vec = {{9}, {9}, {9}, {9}}, .list = {1, 2, 3}};
  RoundTripInto(in, out);
  EXPECT_EQ(in, out);
}

// ------------------------------------------------------------------
// Associative containers (ordered + unordered).
// ------------------------------------------------------------------

struct MapThings {
  std::map<std::string, TypedInt> map;
  std::unordered_map<std::string, int> unordered;
  bool operator==(const MapThings&) const = default;
};

struct IntKeyMap {
  std::map<int, TypedInt> map;
  bool operator==(const IntKeyMap&) const = default;
};

TEST(DuckRoundTrip, ordered_and_unordered_map) {
  RoundTrip(MapThings{.map = {{"1", {1}}, {"2", {2}}, {"3", {3}}},
                      .unordered = {{"4", 4}, {"5", 5}}});
}

TEST(DuckRoundTrip, int_keyed_map) {
  RoundTrip(IntKeyMap{.map = {{1, {1}}, {2, {2}}, {3, {3}}}});
}

// ------------------------------------------------------------------
// Sets (ordered + unordered). std::unordered_set's iteration order is
// implementation-defined, so we read back into a fresh struct and
// compare via operator== (which is content-based, not order-based).
// ------------------------------------------------------------------

struct SetThings {
  std::set<TypedInt> set;
  std::unordered_set<int> unordered;
  bool operator==(const SetThings&) const = default;
};

TEST(DuckRoundTrip, sets) {
  RoundTrip(SetThings{.set = {{1}, {2}, {3}}, .unordered = {4, 5}});
}

// ------------------------------------------------------------------
// Tuples, pairs, std::array.
// ------------------------------------------------------------------

struct TupleThings {
  std::tuple<std::string, int, double> tuple;
  std::pair<int, std::string> pair;
  std::array<std::string, 2> array1;
  std::array<int, 3> array2;
  bool operator==(const TupleThings&) const = default;
};

TEST(DuckRoundTrip, tuple_pair_array) {
  RoundTrip(TupleThings{.tuple = {"foo", 42, 12.34},
                        .pair = {987, "bar"},
                        .array1 = {"a", "b"},
                        .array2 = {1, 2, 3}});
}

// ------------------------------------------------------------------
// Nullables: std::optional, std::unique_ptr, std::shared_ptr.
// ------------------------------------------------------------------

struct OptBag {
  std::optional<int> some;
  std::optional<int> none;
  std::optional<std::string> str_some;
  std::optional<std::string> str_none;
  std::vector<std::optional<int>> vec;
  bool operator==(const OptBag&) const = default;
};

TEST(DuckRoundTrip, optional) {
  RoundTrip(OptBag{
    .some = 7,
    .none = std::nullopt,
    .str_some = "hi",
    .str_none = std::nullopt,
    .vec = {1, std::nullopt, 3},
  });
}

struct UniqueBag {
  std::unique_ptr<int> set;
  std::unique_ptr<int> unset;
  bool operator==(const UniqueBag& r) const {
    auto eq = [](const auto& a, const auto& b) {
      return (a == nullptr && b == nullptr) ||
             (a != nullptr && b != nullptr && *a == *b);
    };
    return eq(set, r.set) && eq(unset, r.unset);
  }
};

TEST(DuckRoundTrip, unique_ptr) {
  RoundTrip(UniqueBag{
    .set = std::make_unique<int>(13),
    .unset = nullptr,
  });
}

struct SharedBag {
  std::shared_ptr<int> set;
  std::shared_ptr<int> unset;
  bool operator==(const SharedBag& r) const {
    auto eq = [](const auto& a, const auto& b) {
      return (a == nullptr && b == nullptr) ||
             (a != nullptr && b != nullptr && *a == *b);
    };
    return eq(set, r.set) && eq(unset, r.unset);
  }
};

TEST(DuckRoundTrip, shared_ptr) {
  RoundTrip(SharedBag{
    .set = std::make_shared<int>(42),
    .unset = nullptr,
  });
}

// ------------------------------------------------------------------
// Enums (string, int, mixed). Wrapped in an aggregate; multiple values
// per case to ensure no implicit ordering hides bugs.
// ------------------------------------------------------------------

enum class MyStringEnum {
  Value1,
  Value2,
};

enum class MyIntEnum : int32_t {
  Value1 = 1,
  Value2 = 2,
};

enum class MyMixedEnum : uint8_t {
  Value1,
  Value2,
};

struct EnumBag {
  std::vector<MyStringEnum> str_enums;
  std::vector<MyIntEnum> int_enums;
  std::vector<MyMixedEnum> mixed_enums;
  bool operator==(const EnumBag&) const = default;
};

TEST(DuckRoundTrip, enums) {
  RoundTrip(EnumBag{
    .str_enums = {MyStringEnum::Value1, MyStringEnum::Value2},
    .int_enums = {MyIntEnum::Value1, MyIntEnum::Value2},
    .mixed_enums = {MyMixedEnum::Value1, MyMixedEnum::Value2},
  });
}

TEST(DuckRoundTrip, write_invalid_enum_throws) {
  struct EnumField {
    MyIntEnum v{};
  };
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer sink{stream};
  EXPECT_ANY_THROW(
    sdb::basics::WriteTuple(sink, EnumField{static_cast<MyIntEnum>(999)}));
}

// ------------------------------------------------------------------
// Context-driven read. The Arg-flavoured overload of WriteTuple/ReadTuple
// threads a Context through every nested SerdeRead. Here the context
// supplies the default values the reader installs irrespective of what
// the wire bytes say — matches the legacy `WithContext` test from
// vpack_load_test.cpp.
// ------------------------------------------------------------------

struct WithContext {
  int i{};
  std::string s;
  bool operator==(const WithContext&) const = default;
};

struct Ctx {
  int default_int;
  std::string default_string;
};

template<typename Context>
void SerdeWrite(Context ctx, const WithContext& v) {
  ctx.io().OnListBegin(2);
  ctx.io().WriteValue(static_cast<uint64_t>(v.i));
  sdb::basics::detail::WriteString(ctx.io(), v.s);
  ctx.io().OnListEnd();
}

template<typename Context>
void SerdeRead(Context ctx, WithContext& v) {
  // Discard wire bytes; install context defaults instead.
  [[maybe_unused]] auto count = ctx.io().OnListBegin();
  [[maybe_unused]] auto raw_i = ctx.io().ReadUnsignedInt64();
  [[maybe_unused]] const std::string raw_s = ctx.io().ReadString();
  ctx.io().OnListEnd();

  v.i = ctx.arg().default_int;
  v.s = ctx.arg().default_string;
}

TEST(DuckRoundTrip, context_driven_read) {
  Ctx ctx{.default_int = 42, .default_string = "from-ctx"};

  WithContext in{.i = 1, .s = "wire-value"};
  WithContext out{};
  RoundTripInto(in, out, ctx);

  EXPECT_EQ(42, out.i);
  EXPECT_EQ("from-ctx", out.s);
}

// ------------------------------------------------------------------
// Failure cases. Truncated payloads must raise on the read side; bad
// enum discriminants are tested elsewhere (serializer_test.cpp).
// ------------------------------------------------------------------

struct ErrorTTest {
  std::string s;
  size_t id;
  bool operator==(const ErrorTTest&) const = default;
};

TEST(DuckRoundTrip, error_t_test) {
  RoundTrip(ErrorTTest{.s = "ReturnNode", .id = 3});
}

TEST(DuckRoundTrip, truncated_payload_throws) {
  // Write a narrow shape, attempt to read into a wider one ⇒ ReadTuple
  // raises when the stream runs out before all fields are populated.
  struct Narrow {
    int i{};
    bool operator==(const Narrow&) const = default;
  };
  struct Wide {
    int i{};
    double d{};
    bool b{};
    std::string s;
    bool operator==(const Wide&) const = default;
  };

  duckdb::MemoryStream stream;
  {
    duckdb::BinarySerializer sink{stream};
    sdb::basics::WriteTuple(sink, Narrow{.i = 42});
  }
  stream.Rewind();
  duckdb::BinaryDeserializer source{stream};
  Wide out{};
  EXPECT_ANY_THROW(sdb::basics::ReadTuple(source, out));
}

// Serializes `in` as a `Src`, then reads it back as a `Dst`; returns the error
// message raised when the shapes don't match, or empty on unexpected success.
template<typename Dst, typename Src>
std::string ReadTupleError(const Src& in) {
  duckdb::MemoryStream stream;
  {
    duckdb::BinarySerializer sink{stream};
    sdb::basics::WriteTuple(sink, in);
  }
  stream.Rewind();
  duckdb::BinaryDeserializer source{stream};
  Dst out{};
  try {
    sdb::basics::ReadTuple(source, out);
  } catch (const std::exception& e) {
    return e.what();
  }
  return {};
}

TEST(DuckRoundTrip, field_count_mismatch_reports_counts) {
  struct OneField {
    int a{};
  };
  struct TwoFields {
    int a{};
    int b{};
  };
  const std::string msg = ReadTupleError<TwoFields>(OneField{.a = 1});
  EXPECT_NE(msg.find("serialized data has 1"), std::string::npos) << msg;
  EXPECT_NE(msg.find("expected 2"), std::string::npos) << msg;
}

TEST(DuckRoundTrip, invalid_enum_value_throws) {
  struct RawInt {
    int32_t v{};
  };
  struct EnumField {
    MyIntEnum v{};
  };
  const std::string msg = ReadTupleError<EnumField>(RawInt{.v = 999});
  EXPECT_NE(msg.find("Invalid enum value"), std::string::npos) << msg;
  EXPECT_NE(msg.find("999"), std::string::npos) << msg;
}

TEST(DuckRoundTrip, variant_index_out_of_range_throws) {
  struct RawIndex {
    uint64_t v{};
  };
  struct VarField {
    std::variant<int, std::string> v;
  };
  const std::string msg = ReadTupleError<VarField>(RawIndex{.v = 5});
  EXPECT_NE(msg.find("out of range"), std::string::npos) << msg;
}

TEST(DuckRoundTrip, element_error_reports_index) {
  // A bad value at field index 1 must be wrapped with that index, not 0.
  struct TwoInts {
    int32_t a{};
    int32_t b{};
  };
  struct IntThenEnum {
    int32_t a{};
    MyIntEnum b{};
  };
  const std::string msg =
    ReadTupleError<IntThenEnum>(TwoInts{.a = 1, .b = 999});
  EXPECT_NE(msg.find("element 1"), std::string::npos) << msg;
  EXPECT_NE(msg.find("Invalid enum value"), std::string::npos) << msg;
}

TEST(DuckRoundTrip, fixed_array_size_mismatch_throws) {
  struct A3 {
    std::array<int, 3> v{};
  };
  struct A2 {
    std::array<int, 2> v{};
  };
  const std::string msg = ReadTupleError<A2>(A3{});
  EXPECT_NE(msg.find("serialized data has 3"), std::string::npos) << msg;
  EXPECT_NE(msg.find("expected 2"), std::string::npos) << msg;
}

}  // namespace
