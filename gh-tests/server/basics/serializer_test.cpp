#include <absl/algorithm/container.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <frozen/unordered_map.h>
#include <gtest/gtest.h>
#include <vpack/builder.h>
#include <vpack/literal.h>
#include <vpack/options.h>
#include <vpack/slice.h>
#include <vpack/vpack_helper.h>

#include <basics/empty.hpp>
#include <ranges>
#include <tuple>
#include <type_traits>
#include <unordered_map>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/identifiers/object_id.h"
#include "vpack/serializer.h"

namespace {

void ExpectEqualSlices(vpack::Slice lhs, vpack::Slice rhs, const char* where) {
  SCOPED_TRACE(rhs.toString());
  SCOPED_TRACE(rhs.toHex());
  SCOPED_TRACE("----ACTUAL----");
  SCOPED_TRACE(lhs.toString());
  SCOPED_TRACE(lhs.toHex());
  SCOPED_TRACE("---EXPECTED---");
  SCOPED_TRACE(where);
  EXPECT_EQ(0, sdb::basics::VPackHelper::compare(lhs, rhs));
  EXPECT_TRUE(sdb::basics::VPackHelper::equals(lhs, rhs));
}

#define EXPECT_EQUAL_SLICES_STRINGIFY(x) #x
#define EXPECT_EQUAL_SLICES_EXPANDER(leftSlice, rightSlice, file, line) \
  ExpectEqualSlices(leftSlice, rightSlice,                              \
                    file ":" EXPECT_EQUAL_SLICES_STRINGIFY(line))
#define EXPECT_EQUAL_SLICES(leftSlice, rightSlice) \
  EXPECT_EQUAL_SLICES_EXPANDER(leftSlice, rightSlice, __FILE__, __LINE__)
void ExpectEqualSlices(vpack::Slice lhs, vpack::Slice rhs, const char* where);

using namespace sdb;
using namespace vpack;

class MyCustomInt {
 public:
  MyCustomInt(int value) : _value{value} {}

  int Value() const { return _value; }

  bool operator==(const MyCustomInt&) const = default;

  template<typename Context>
  friend void VPackRead(Context ctx, MyCustomInt& v) {
    if constexpr (std::is_same_v<vpack::TupleFormat,
                                 typename Context::Format>) {
      v._value = ctx.vpack().template getNumber<int>();
    } else {
      std::ignore = absl::SimpleAtoi(ctx.vpack().stringView(), &v._value);
    }
  }

  template<typename Context>
  friend void VPackWrite(Context ctx, MyCustomInt v) {
    if constexpr (std::is_same_v<vpack::TupleFormat,
                                 typename Context::Format>) {
      ctx.vpack().add(v._value);
    } else {
      ctx.vpack().add(std::to_string(v._value));
    }
  }

 private:
  int _value;
};

class MyCustomIntWithArg {
 public:
  MyCustomIntWithArg(int value) : _value{value} {}

  int Value() const { return _value; }

  bool operator==(const MyCustomIntWithArg&) const = default;

  template<typename Context>
  friend void VPackRead(Context ctx, MyCustomIntWithArg& v) {
    if constexpr (std::is_same_v<vpack::TupleFormat,
                                 typename Context::Format>) {
      v._value = ctx.vpack().template getNumber<int>();
    } else {
      std::ignore = absl::SimpleAtoi(ctx.vpack().stringView(), &v._value);
    }
    v._value -= ctx.arg().value;
  }

  template<typename Context>
  friend void VPackWrite(Context ctx, MyCustomIntWithArg v) {
    if constexpr (std::is_same_v<vpack::TupleFormat,
                                 typename Context::Format>) {
      ctx.vpack().add(v._value + ctx.arg().value);
    } else {
      ctx.vpack().add(std::to_string(v._value + ctx.arg().value));
    }
  }

 private:
  int _value;
};

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
  bool operator==(const TypedInt& r) const = default;
  auto operator<=>(const TypedInt& r) const = default;
};

struct Container {
  TypedInt i{};
  bool operator==(const Container& r) const = default;
  auto operator<=>(const Container& r) const = default;
};

struct List {
  std::vector<Container> vec{{32}, {13}, {67}};
  std::list<int> list{1, 2, 3};

  bool operator==(const List& r) const = default;
};

struct Map {
  std::map<std::string, Container> map{{"test", {}}};
  std::unordered_map<std::string, int> unordered{{"string", 5}};

  bool operator==(const Map& r) const = default;
};

struct Set {
  std::set<Container> set{{1}, {2}, {4}, {5}, {6}};
  std::unordered_set<int> unordered{5, 100, 42};

  bool operator==(const Set& r) const = default;
};

struct Tuple {
  std::tuple<std::string, int, double> tuple{"123", 5, 42.};
  std::pair<int, std::string> pair{123, "dfals"};
  std::array<std::string, 2> array1{"", "foobar"};
  std::array<int, 3> array2{1, 4, 56};

  bool operator==(const Tuple& r) const = default;
};

struct Optional {
  std::optional<int> a;
  std::optional<int> b{5};
  std::optional<int> x;
  std::optional<std::string> y;
  std::vector<std::optional<int>> vec{5, {}};
  std::map<std::string, std::optional<int>> map{{"key", {}}};

  bool operator==(const Optional& r) const = default;
};

struct Pointer {
  std::shared_ptr<int> a;
  std::shared_ptr<int> b;
  std::unique_ptr<int> c{std::make_unique<int>(42)};
  std::unique_ptr<Container> d{std::make_unique<Container>()};
  std::vector<std::unique_ptr<int>> vec{};
  std::shared_ptr<int> x{std::make_shared<int>(43)};
  std::shared_ptr<int> y;

  static bool comparePointers(auto& l, auto& r) {
    if (l != nullptr && r != nullptr) {
      return *l == *r;
    }

    return l == nullptr && r == nullptr;
  }

  bool operator==(const Pointer& r) const {
    return comparePointers(a, r.a) && comparePointers(b, r.b) &&
           comparePointers(c, r.c) && comparePointers(d, r.d) &&
           comparePointers(y, r.y) && comparePointers(x, r.x) &&
           absl::c_equal(vec, r.vec, [](auto& l, auto& r) {
             return comparePointers(l, r);
           });
  }
};

struct Fallback {
  int i{};
  std::string s;
  Dummy d = {.i = 1, .d = 4.2, .b = true, .s = "2"};
  int dynamic{};

  bool operator==(const Fallback& r) const = default;
};

struct Invariant {
  int i{};
  std::string s;

  bool operator==(const Invariant& r) const = default;
};

struct NestedInvariant {
  Invariant i;
  Invariant o;

  bool operator==(const NestedInvariant& r) const = default;
};

enum class SomeEnum { Value1 = 1, Value2 = 5 };

struct Null {
  bool operator==(const Null&) const = default;
};

template<typename T, typename Arg = irs::utils::Empty>
void CheckTupleFormat(const T& in, vpack::Slice expected = {},
                      const Arg& arg = {}) {
  vpack::Builder builder;
  vpack::WriteTuple(builder, in, arg);

  if (!expected.isNone()) {
    EXPECT_EQUAL_SLICES(expected, builder.slice());
  }

  T out;
  vpack::ReadTuple(builder.slice(), out, arg);

  ASSERT_EQ(in, out);
}

template<typename T, typename Arg = irs::utils::Empty>
void CheckObjectFormat(const T& in, vpack::Slice expected = {},
                       const Arg& arg = {}) {
  vpack::Builder builder;
  vpack::WriteObject(builder, in, arg);

  if (!expected.isNone()) {
    EXPECT_EQUAL_SLICES(expected, builder.slice());
  }

  T out;
  vpack::ReadObject(builder.slice(), out, {.skip_unknown = false}, arg);

  ASSERT_EQ(in, out);
}

template<typename T>
void CheckObjectEquality(const T& in, vpack::Slice source) {
  T out;
  vpack::ReadObject(source, out, {.skip_unknown = false});

  ASSERT_EQ(in, out);
}

template<typename T>
void CheckErrorIncompatibleObject(vpack::Slice source) {
  T out;
  try {
    vpack::ReadObject(source, out, {.skip_unknown = false});
    FAIL();
  } catch (const basics::Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
    auto message = std::string_view{e.what()};
    EXPECT_TRUE(message.contains("Incompatible")) << message;
  }
}

template<typename T>
void CheckErrorIncompatibleTuple(std::string_view field, vpack::Slice source) {
  T out;
  try {
    vpack::ReadTuple(source, out);
    FAIL();
  } catch (const basics::Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
    auto message = std::string_view{e.what()};
    EXPECT_TRUE(message.contains("incompatible")) << message;
  }
}

template<typename T>
void CheckErrorUnexpectedField(std::string_view field, vpack::Slice source) {
  T out;
  try {
    vpack::ReadObject(source, out, {.skip_unknown = false});
    FAIL();
  } catch (const basics::Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
    auto message = std::string_view{e.what()};
    EXPECT_TRUE(message.contains(field)) << message;
    EXPECT_TRUE(message.contains("unexpected")) << message;
  }
}

template<typename T>
void CheckErrorInvalidTypeInObject(std::string_view field,
                                   std::string_view type, vpack::Slice source) {
  T out;
  try {
    vpack::ReadObject(source, out, {.skip_unknown = false});
    FAIL();
  } catch (const basics::Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
    auto message = std::string_view{e.what()};
    EXPECT_TRUE(message.contains(field)) << message;
    EXPECT_TRUE(message.contains(type)) << message;
  }
}

template<typename T>
void CheckErrorInvalidEnumInTuple(uint64_t value, std::string_view type,
                                  vpack::Slice source) {
  T out;
  try {
    vpack::ReadTuple(source, out);
    FAIL();
  } catch (const basics::Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
    auto message = std::string_view{e.what()};
    EXPECT_TRUE(message.contains(std::to_string(value))) << message;
    EXPECT_TRUE(message.contains(type)) << message;
  }
}

template<typename T>
void CheckErrorInvalidEnumInObject(std::string_view value,
                                   std::string_view type, vpack::Slice source) {
  T out;
  try {
    vpack::ReadObject(source, out, {.skip_unknown = false});
    FAIL();
  } catch (const basics::Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
    auto message = std::string_view{e.what()};
    EXPECT_TRUE(message.contains(value)) << message;
    EXPECT_TRUE(message.contains(type)) << message;
  }
}

template<typename T>
void CheckErrorInvalidTypeInTuple(size_t index, std::string type,
                                  vpack::Slice source) {
  T out;
  try {
    vpack::ReadTuple(source, out);
    FAIL();
  } catch (const basics::Exception& e) {
    EXPECT_EQ(e.code(), sdb::ERROR_BAD_PARAMETER);
    auto message = std::string_view{e.what()};
    EXPECT_TRUE(message.contains(std::to_string(index))) << message;
    EXPECT_TRUE(message.contains(type)) << message;
  }
}

TEST(SerializerTest, testRange) {
  auto func = [](auto v) { return v * 2; };

  std::array values{1, 2, 3};
  auto view = values | std::views::transform(func);

  vpack::Builder b;
  vpack::WriteTuple(b, view);

  absl::c_transform(values, values.begin(), func);
  std::array<int, values.size()> read_values;
  vpack::ReadTuple(b.slice(), read_values);

  ASSERT_EQ(values, read_values);
}

TEST(SerializerTest, testCustom) {
  struct Test {
    int a{};
    int b{0};
    MyCustomInt id{54};

    bool operator==(const Test&) const = default;
  };

  CheckTupleFormat(Test{.id = MyCustomInt{2}}, R"([0, 0, 2])"_vpack.slice());
  CheckObjectFormat(Test{.id = MyCustomInt{42}},
                    R"({"a":0, "b": 0, "id": "42"})"_vpack.slice());
}

TEST(SerializerTest, testCustomWithArg) {
  struct Test {
    int a{};
    int b{0};
    MyCustomIntWithArg id{54};

    bool operator==(const Test&) const = default;
  };

  struct Arg {
    int value{};
  };

  CheckTupleFormat(Test{.id = MyCustomIntWithArg{2}},
                   R"([0, 0, 4])"_vpack.slice(), Arg{2});
  CheckObjectFormat(Test{.id = MyCustomIntWithArg{42}},
                    R"({"a":0, "b": 0, "id": "44"})"_vpack.slice(), Arg{2});
}

TEST(SerializerTest, testMandatory) {
  struct Test {
    int a{};
    int b{0};

    bool operator==(const Test&) const = default;
  };

  CheckObjectFormat(Test{42, 43}, R"({ "a": 42, "b": 43 })"_vpack);
  CheckTupleFormat(Test{42, 43}, R"([ 42, 43 ])"_vpack);
  CheckErrorUnexpectedField<Test>(
    "unexpected", R"({ "a": 42, "b": 43, "unexpected": 0 })"_vpack);
  CheckErrorIncompatibleObject<Test>(R"({ "a": 42 })"_vpack);
  CheckErrorInvalidTypeInObject<Test>("a", "Int", R"({ "a": "42" })"_vpack);
  CheckErrorIncompatibleObject<Test>(R"({  })"_vpack);
  CheckErrorIncompatibleTuple<Test>("b", R"([ 42 ])"_vpack);
  CheckErrorIncompatibleTuple<Test>("b", R"([ ])"_vpack);
  CheckErrorIncompatibleTuple<Test>("b", R"([ 42, 43, 0 ])"_vpack);
  CheckErrorInvalidTypeInTuple<Test>(0, "Int", R"([ "42", 43 ])"_vpack);
  CheckErrorInvalidTypeInTuple<Test>(1, "Int", R"([ 42, "43" ])"_vpack);
}

TEST(SerializerTest, testEnum) {
  using Map = std::map<std::string_view, SomeEnum>;
  const Map values{
    {"first", SomeEnum::Value1},
    {"second", SomeEnum::Value2},
  };

  CheckObjectFormat(values,
                    R"({"first": "Value1", "second": "Value2"})"_vpack.slice());
  CheckErrorInvalidEnumInObject<Map>(
    "InvalidValue", "SomeEnum",
    R"({"first": "InvalidValue", "second": "Value2"})"_vpack.slice());
  CheckTupleFormat(values, R"([["first", 1], ["second", 5]])"_vpack.slice());
  CheckErrorInvalidEnumInTuple<Map>(
    0, "SomeEnum", R"([["first", 0], ["second", 5]])"_vpack.slice());
  CheckErrorIncompatibleTuple<Map>("b", R"([["first", 0, "something"]])"_vpack);
  CheckErrorIncompatibleTuple<Map>("b", R"([["first"]])"_vpack);
}

TEST(SerializerTest, testVariable) {
  const std::map<std::string_view, int> values{
    {"first", 1},
    {"second", 2},
  };

  {
    vpack::Builder b;
    b.openObject();
    b.add("map");
    vpack::WriteTuple(b, values);
    b.close();
    EXPECT_EQUAL_SLICES(
      R"({ "map": [ ["first", 1], ["second", 2] ] })"_vpack.slice(), b.slice());

    std::map<std::string_view, int> read_values;
    vpack::ReadTuple(b.slice().get("map"), read_values);
    EXPECT_EQ(values, read_values);
  }

  {
    vpack::Builder b;
    b.openObject();
    b.add("map");
    vpack::WriteObject(b, values);
    b.close();
    EXPECT_EQUAL_SLICES(
      R"({ "map": { "first": 1, "second": 2 } })"_vpack.slice(), b.slice());

    std::map<std::string_view, int> read_values;
    vpack::ReadObject(b.slice().get("map"), read_values,
                      {.skip_unknown = false});
    EXPECT_EQ(values, read_values);
  }
}

TEST(SerializerTest, testFieldObject) {
  struct Object {
    // NOLINTBEGIN
    vpack::Skip<int> skipField1{1};
    vpack::NameOptional<"optionalField", int> optField{};
    vpack::Skip<int> skipField2{2};
    vpack::Name<"mandatoryField", int> field{};
    vpack::Skip<int> skipField3{3};
    // NOLINTEND

    bool operator==(const Object&) const = default;
  };

  CheckObjectFormat(
    Object{.optField = 42, .field = 43},
    R"({"optionalField":42, "mandatoryField": 43})"_vpack.slice());
  CheckObjectEquality(Object{.optField = 0, .field = 43},
                      R"({"mandatoryField": 43})"_vpack.slice());
  CheckErrorUnexpectedField<Object>(
    "unknownField",
    R"({"unknownField": 42, "mandatoryField": 43})"_vpack.slice());
  CheckErrorInvalidTypeInObject<Object>(
    "mandatoryField", "Int", R"({"mandatoryField": "43"})"_vpack.slice());
  // Failed because no manadatory field, only optional
  CheckErrorIncompatibleObject<Object>(
    R"({"optionalField": 42})"_vpack.slice());
}

TEST(SerializerTest, testFieldTuple) {
  struct Object {
    // NOLINTBEGIN
    // Our length check obliviously doesn't work for skip fields :(
    // vpack::Skip<int> skipField1{1};
    vpack::NameOptional<"optionalField", int> optField{};
    // vpack::Skip<int> skipField2{2};
    vpack::Name<"mandatoryField", int> field{};
    // vpack::Skip<int> skipField3{3};
    // NOLINTEND

    bool operator==(const Object&) const = default;
  };

  CheckTupleFormat(Object{.optField = 42, .field = 43},
                   R"([42, 43])"_vpack.slice());
}

TEST(SerializerTest, testComplexObject) {
  struct ComplexObject {
    // NOLINTBEGIN
    Dummy dummy{1, false};
    std::optional<Dummy> optDummy;
    std::optional<int> optInt{444};
    int intValue{42};
    vpack::Name<"override", int> intValueWithMeta{42};
    std::string stringValue{"fff"};
    SomeEnum enumValue{SomeEnum::Value1};
    std::string_view stringView{"fff"};
    std::pair<int, std::string> pair{2, "sdfab"};
    std::tuple<int, int, size_t> tuple{2, 3, 42};
    std::vector<int> vector{1, 2, 3};
    [[no_unique_address]] Null null;
    std::unordered_map<int, std::string> hashMap{
      {1, "jjj"},
      {3, "kkkkk"},
    };
    std::unordered_set<std::string> hashSet{"55", "664"};
    std::map<int, std::string> map{
      {1, "jjj"},
      {3, "kkkkk"},
    };
    std::set<int> set{6, 7, 8};
    std::shared_ptr<int> sharedPtr;
    // NOLINTEND

    bool operator==(const ComplexObject&) const = default;
  };

#pragma clang diagnostic ignored "-Winvalid-offsetof"
  {
    const auto& offsets =
      vpack::detail::MakeFieldMap<ComplexObject, irs::utils::Empty>().first;
    EXPECT_EQ(offsetof(ComplexObject, dummy), offsets.at("dummy").first);
    EXPECT_EQ(offsetof(ComplexObject, optDummy), offsets.at("optDummy").first);
    EXPECT_EQ(offsetof(ComplexObject, optInt), offsets.at("optInt").first);
    EXPECT_EQ(offsetof(ComplexObject, intValue), offsets.at("intValue").first);
    EXPECT_EQ(offsetof(ComplexObject, intValueWithMeta),
              offsets.at("override").first);
    EXPECT_EQ(offsetof(ComplexObject, stringValue),
              offsets.at("stringValue").first);
    EXPECT_EQ(offsetof(ComplexObject, enumValue),
              offsets.at("enumValue").first);
    EXPECT_EQ(offsetof(ComplexObject, tuple), offsets.at("tuple").first);
    EXPECT_EQ(offsetof(ComplexObject, null), offsets.at("null").first);
    EXPECT_EQ(offsetof(ComplexObject, hashSet), offsets.at("hashSet").first);
    EXPECT_EQ(offsetof(ComplexObject, hashMap), offsets.at("hashMap").first);
    EXPECT_EQ(offsetof(ComplexObject, stringView),
              offsets.at("stringView").first);
    EXPECT_EQ(offsetof(ComplexObject, pair), offsets.at("pair").first);
    EXPECT_EQ(offsetof(ComplexObject, vector), offsets.at("vector").first);
    EXPECT_EQ(offsetof(ComplexObject, map), offsets.at("map").first);
    EXPECT_EQ(offsetof(ComplexObject, set), offsets.at("set").first);
    EXPECT_EQ(offsetof(ComplexObject, sharedPtr),
              offsets.at("sharedPtr").first);
  }

  CheckTupleFormat(ComplexObject{});
  CheckObjectFormat(ComplexObject{});
}

TEST(SerializerTest, testComplexObject2) {
  struct ComplexObject2 {
    // NOLINTBEGIN
    Dummy dummy{1, false};
    std::optional<Dummy> optionalDummy;
    std::optional<int> optInt{444};
    int intValue{42};
    vpack::NameOptional<"hhh", int> intValueWithMeta{42};
    std::string string{"fff"};
    SomeEnum enumValue{SomeEnum::Value1};
    std::string_view stringView{"fff"};
    std::pair<int, std::string> pair{2, "sdfab"};
    std::tuple<int, int, size_t> tuple{2, 3, 42};
    std::vector<int> vector{1, 2, 3};
    vpack::NameOptional<"hierere", std::vector<Dummy>> vectorWithMeta{Dummy{},
                                                                      Dummy{}};
    [[no_unique_address]] Null null;
    // NOLINTEND

    bool operator==(const ComplexObject2&) const = default;
  };

  CheckTupleFormat(ComplexObject2{});
  CheckObjectFormat(ComplexObject2{});
}

TEST(Serializer, testDummy) {
  {
    const auto& offsets =
      vpack::detail::MakeFieldMap<Dummy, irs::utils::Empty>();
    EXPECT_TRUE(offsetof(Dummy, i) == offsets.first.at("i").first);
    EXPECT_TRUE(offsetof(Dummy, d) == offsets.first.at("d").first);
    EXPECT_TRUE(offsetof(Dummy, b) == offsets.first.at("b").first);
    EXPECT_TRUE(offsetof(Dummy, s) == offsets.first.at("s").first);
  }

  CheckTupleFormat(Dummy{});
  CheckObjectFormat(Dummy{});
}

TEST(SerializerTest, testNested) {
  CheckTupleFormat(Nested{});
  CheckObjectFormat(Nested{});
}

TEST(SerializerTest, testTypedInt) {
  CheckTupleFormat(TypedInt{});
  CheckObjectFormat(TypedInt{});
}

TEST(SerializerTest, testContainer) {
  CheckTupleFormat(Container{});
  CheckObjectFormat(Container{});
}

TEST(SerializerTest, testList) {
  CheckTupleFormat(List{});
  CheckObjectFormat(List{});
}

TEST(SerializerTest, testMap) {
  CheckTupleFormat(Map{});
  CheckObjectFormat(Map{});
}

TEST(SerializerTest, testSet) {
  CheckTupleFormat(Set{});
  CheckObjectFormat(Set{});
}

TEST(SerializerTest, testTuple) {
  CheckTupleFormat(Tuple{});
  CheckObjectFormat(Tuple{});
}

TEST(SerializerTest, testOptional) {
  CheckTupleFormat(Optional{});
  CheckObjectFormat(Optional{});
}

TEST(SerializerTest, testFallback) {
  CheckTupleFormat(Fallback{});
  CheckObjectFormat(Fallback{});
}

TEST(SerializerTest, testInvariant) {
  CheckTupleFormat(Invariant{});
  CheckObjectFormat(Invariant{});
}

TEST(SerializerTest, testNestedInvariant) {
  CheckTupleFormat(NestedInvariant{});
  CheckObjectFormat(NestedInvariant{});
}

struct CustomFields {
  vpack::Optional<int> some_int;
  vpack::Optional<std::vector<int>> some_vector;
  // unique_ptr works because our patch
  // https://github.com/boostorg/pfr/issues/208
  vpack::Optional<std::unique_ptr<int>> some_unique_ptr;
  vpack::Optional<std::shared_ptr<int>> some_shared_ptr;
  // atomic doesn't work see the issue
  // https://github.com/boostorg/pfr/issues/208
  // vpack::Optional<std::atomic<int>> some_atomic;

  bool operator==(const CustomFields&) const = default;
};

TEST(SerializerTest, testCustomFields) {
  CheckTupleFormat(CustomFields{});
  CheckObjectFormat(CustomFields{});
}

}  // namespace
