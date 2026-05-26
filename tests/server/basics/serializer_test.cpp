////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

// Round-trips structs through the duckdb-binary serializer/deserializer.

#include <absl/algorithm/container.h>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <gtest/gtest.h>
#include "basics/serializer.h"

#include <array>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

// Round-trip a value through the duckdb-binary pipeline and assert it
// survives.
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

// Serialize `in` (which may be of a different type than `T`) and assert
// that deserializing into a fresh `T` throws. Used by the failure-mode
// tests: the input is shaped so the read hits a parser error (truncated
// stream, out-of-range enum, etc.).
template<typename T, typename Input>
void ExpectReadFails(const Input& in) {
  duckdb::MemoryStream stream;
  {
    duckdb::BinarySerializer sink{stream};
    sdb::basics::WriteTuple(sink, in);
  }
  stream.Rewind();
  duckdb::BinaryDeserializer source{stream};
  T out{};
  EXPECT_ANY_THROW(sdb::basics::ReadTuple(source, out));
}

// ------------------------------------------------------------------
// Shared types — mirror the original serializer_test.cpp type matrix.
// All carry operator== so RoundTrip's value comparison works.
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

struct List {
  std::vector<Container> vec{{{32}}, {{13}}, {{67}}};
  std::list<int> list{1, 2, 3};
  bool operator==(const List&) const = default;
};

struct Map {
  std::map<std::string, Container> map{{"test", {}}};
  std::unordered_map<std::string, int> unordered{{"string", 5}};
  bool operator==(const Map&) const = default;
};

struct Set {
  std::set<Container> set{{{1}}, {{2}}, {{4}}, {{5}}, {{6}}};
  std::unordered_set<int> unordered{5, 100, 42};
  bool operator==(const Set&) const = default;
};

struct Tuple {
  std::tuple<std::string, int, double> tuple{"123", 5, 42.};
  std::pair<int, std::string> pair{123, "dfals"};
  std::array<std::string, 2> array1{"", "foobar"};
  std::array<int, 3> array2{1, 4, 56};
  bool operator==(const Tuple&) const = default;
};

struct Optional {
  std::optional<int> a;
  std::optional<int> b{5};
  std::optional<int> x;
  std::optional<std::string> y;
  std::vector<std::optional<int>> vec{5, {}};
  std::map<std::string, std::optional<int>> map{{"key", {}}};
  bool operator==(const Optional&) const = default;
};

struct Pointer {
  std::shared_ptr<int> a;
  std::shared_ptr<int> b;
  std::unique_ptr<int> c{std::make_unique<int>(42)};
  std::vector<std::unique_ptr<int>> vec{};

  bool operator==(const Pointer& r) const {
    auto eq = [](const auto& l, const auto& r) {
      return (l == nullptr && r == nullptr) ||
             (l != nullptr && r != nullptr && *l == *r);
    };
    return eq(a, r.a) && eq(b, r.b) && eq(c, r.c) &&
           absl::c_equal(vec, r.vec, eq);
  }
};

struct Fallback {
  int i{};
  std::string s;
  Dummy d = {.i = 1, .d = 4.2, .b = true, .s = "2"};
  int dynamic{};
  bool operator==(const Fallback&) const = default;
};

struct Invariant {
  int i{};
  std::string s;
  bool operator==(const Invariant&) const = default;
};

struct NestedInvariant {
  Invariant i;
  Invariant o;
  bool operator==(const NestedInvariant&) const = default;
};

enum class SomeEnum { Value1 = 1, Value2 = 5 };

struct Null {
  bool operator==(const Null&) const = default;
};

// ------------------------------------------------------------------
// User-defined SerdeRead / SerdeWrite dispatch.
//
// A custom type whose persistent shape is just an int64. The overloads
// use the sink/source protocol directly so the same overload works for
// any sink the templated machinery hands it.
// ------------------------------------------------------------------

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

// Same shape as MyCustomInt, but the Context's user-supplied arg shifts
// the value on its way in and out.
class MyCustomIntWithArg {
 public:
  MyCustomIntWithArg() = default;
  explicit MyCustomIntWithArg(int64_t value) : _value{value} {}

  int64_t Value() const { return _value; }
  bool operator==(const MyCustomIntWithArg&) const = default;

  template<typename Context>
  friend void SerdeRead(Context ctx, MyCustomIntWithArg& v) {
    v._value = ctx.io().ReadSignedInt64() - ctx.arg().value;
  }

  template<typename Context>
  friend void SerdeWrite(Context ctx, MyCustomIntWithArg v) {
    ctx.io().WriteValue(
      static_cast<int64_t>(v._value + ctx.arg().value));
  }

 private:
  int64_t _value{};
};

// ==================================================================
// Tests
// ==================================================================

TEST(SerializerTest, testRange) {
  // std::views::transform a tiny array, write the view, then read back
  // into a fresh array and verify the same transform.
  auto func = [](auto v) { return v * 2; };
  std::array<int, 3> values{1, 2, 3};
  auto view = values | std::views::transform(func);

  duckdb::MemoryStream stream;
  {
    duckdb::BinarySerializer sink{stream};
    sdb::basics::WriteTuple(sink, view);
  }
  stream.Rewind();
  std::array<int, 3> read_values{};
  duckdb::BinaryDeserializer source{stream};
  sdb::basics::ReadTuple(source, read_values);

  std::array<int, 3> expected = values;
  absl::c_transform(expected, expected.begin(), func);
  EXPECT_EQ(expected, read_values);
}

TEST(SerializerTest, testCustom) {
  struct Test {
    int a{};
    int b{0};
    MyCustomInt id{54};
    bool operator==(const Test&) const = default;
  };
  RoundTrip(Test{.a = 0, .b = 0, .id = MyCustomInt{2}});
}

TEST(SerializerTest, testCustomWithArg) {
  struct Test {
    int a{};
    int b{0};
    MyCustomIntWithArg id{54};
    bool operator==(const Test&) const = default;
  };
  struct Arg {
    int64_t value{};
  };
  // Wire emits (_value + arg.value), read subtracts arg.value back ⇒
  // user-observed value round-trips for any arg.
  RoundTrip(Test{.id = MyCustomIntWithArg{2}}, Arg{.value = 2});
  RoundTrip(Test{.id = MyCustomIntWithArg{42}}, Arg{.value = -10});
}

TEST(SerializerTest, testMandatory) {
  // Two-field mandatory tuple. Object-format and slice-parser error
  // variants from the original test don't translate; only the duckdb-
  // applicable cases survive: positive round-trip + stream underflow on
  // a too-short payload.
  struct Test {
    int a{};
    int b{0};
    bool operator==(const Test&) const = default;
  };
  RoundTrip(Test{42, 43});

  // Stream underflow: a struct with a single field written, then read
  // into the wide two-field shape ⇒ ReadTuple throws on field `b`.
  struct Narrow {
    int a{};
    bool operator==(const Narrow&) const = default;
  };
  ExpectReadFails<Test>(Narrow{.a = 42});
}

TEST(SerializerTest, testEnum) {
  using Map = std::map<std::string, SomeEnum>;
  struct Holder {
    Map values;
    bool operator==(const Holder&) const = default;
  };

  RoundTrip(Holder{.values = {{"first", SomeEnum::Value1},
                              {"second", SomeEnum::Value2}}});

  // Wire shape: same as Holder but with raw int (so writing skips the
  // enum_cast check); the int 0 isn't a SomeEnum member, so the read
  // hits magic_enum's invalid-value path and throws.
  struct WireShape {
    std::map<std::string, int> values;
    bool operator==(const WireShape&) const = default;
  };
  ExpectReadFails<Holder>(
    WireShape{.values = {{"first", 0}, {"second", 5}}});
}

// testVariable — original used `vpack::Builder` to wrap a map inside an
// outer object. There's no duckdb-binary equivalent for a "bare map at
// the top level inside an object"; wrap the map in a struct and
// round-trip it directly.
TEST(SerializerTest, testVariable) {
  struct Holder {
    std::map<std::string, int> map{{"first", 1}, {"second", 2}};
    bool operator==(const Holder&) const = default;
  };
  RoundTrip(Holder{});
}

TEST(SerializerTest, testComplexObject) {
  struct ComplexObject {
    Dummy dummy{1, false};
    std::optional<Dummy> optDummy;
    std::optional<int> optInt{444};
    int intValue{42};
    std::string stringValue{"fff"};
    SomeEnum enumValue{SomeEnum::Value1};
    std::pair<int, std::string> pair{2, "sdfab"};
    std::tuple<int, int, size_t> tuple{2, 3, 42};
    std::vector<int> vector{1, 2, 3};
    [[no_unique_address]] Null null;
    std::unordered_map<int, std::string> hashMap{{1, "jjj"}, {3, "kkkkk"}};
    std::unordered_set<std::string> hashSet{"55", "664"};
    std::map<int, std::string> map{{1, "jjj"}, {3, "kkkkk"}};
    std::set<int> set{6, 7, 8};
    std::shared_ptr<int> sharedPtr;

    bool operator==(const ComplexObject&) const = default;
  };

  RoundTrip(ComplexObject{});
}

TEST(SerializerTest, testComplexObject2) {
  struct ComplexObject2 {
    Dummy dummy{1, false};
    std::optional<Dummy> optionalDummy;
    std::optional<int> optInt{444};
    int intValue{42};
    std::string string{"fff"};
    SomeEnum enumValue{SomeEnum::Value1};
    std::pair<int, std::string> pair{2, "sdfab"};
    std::tuple<int, int, size_t> tuple{2, 3, 42};
    std::vector<int> vector{1, 2, 3};
    std::vector<Dummy> vectorWithMeta{Dummy{}, Dummy{}};
    [[no_unique_address]] Null null;

    bool operator==(const ComplexObject2&) const = default;
  };

  RoundTrip(ComplexObject2{});
}

TEST(SerializerTest, testDummy) { RoundTrip(Dummy{}); }
TEST(SerializerTest, testNested) { RoundTrip(Nested{}); }
TEST(SerializerTest, testTypedInt) { RoundTrip(TypedInt{}); }
TEST(SerializerTest, testContainer) { RoundTrip(Container{}); }
TEST(SerializerTest, testList) { RoundTrip(List{}); }
TEST(SerializerTest, testMap) { RoundTrip(Map{}); }
TEST(SerializerTest, testSet) { RoundTrip(Set{}); }
TEST(SerializerTest, testTuple) { RoundTrip(Tuple{}); }
TEST(SerializerTest, testOptional) { RoundTrip(Optional{}); }
TEST(SerializerTest, testPointer) {
  Pointer p{
    .a = nullptr,
    .b = std::make_shared<int>(7),
    .c = std::make_unique<int>(13),
    .vec = {},
  };
  p.vec.push_back(std::make_unique<int>(1));
  p.vec.push_back(nullptr);
  p.vec.push_back(std::make_unique<int>(2));
  RoundTrip(p);
}
TEST(SerializerTest, testFallback) { RoundTrip(Fallback{}); }
TEST(SerializerTest, testInvariant) { RoundTrip(Invariant{}); }
TEST(SerializerTest, testNestedInvariant) { RoundTrip(NestedInvariant{}); }

}  // namespace
