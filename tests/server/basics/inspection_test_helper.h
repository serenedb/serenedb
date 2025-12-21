////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2022 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#include <list>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <variant>
#include <vector>

#include "vpack/serializer.h"

namespace {

struct Dummy {
  int i{};
  double d{};
  bool b{};
  std::string s;

  bool operator==(const Dummy&) const = default;
};

struct Nested {
  Dummy dummy;
};

struct TypedInt {
  int value{};
  int getValue() { return value; }
  bool operator==(const TypedInt& r) const { return value == r.value; };
  bool operator<(const TypedInt& r) const { return value < r.value; };
};

struct Container {
  TypedInt i{.value = 0};
  bool operator==(const Container& r) const { return i == r.i; };
  bool operator<(const Container& r) const { return i < r.i; };
};

struct List {
  std::vector<TypedInt> vec;
  std::list<int> list;
};

struct Map {
  std::map<std::string, TypedInt> map;
  std::unordered_map<std::string, int> unordered;
};

struct TransformedMap {
  std::map<int, TypedInt> map;
};

struct Set {
  std::set<TypedInt> set;
  std::unordered_set<int> unordered;
};

struct Tuple {
  std::tuple<std::string, int, double> tuple;
  std::pair<int, std::string> pair;
  std::array<std::string, 2> array1;
  std::array<int, 3> array2;
};

struct Optional {
  std::optional<int> a;
  std::optional<int> b;
  std::optional<int> x;
  std::optional<std::string> y;
  std::vector<std::optional<int>> vec;
  std::map<std::string, std::optional<int>> map;
};

struct Pointer {
  std::shared_ptr<int> a;
  std::shared_ptr<int> b;
  std::unique_ptr<int> c;
  std::unique_ptr<TypedInt> d;
  std::vector<std::unique_ptr<int>> vec;
  std::shared_ptr<int> x = std::make_shared<int>(123);
  std::shared_ptr<int> y = std::make_shared<int>(456);
};

struct Fallback {
  int i{};
  std::string s;
  Dummy d = {.i = 1, .d = 4.2, .b = true, .s = "2"};
  int dynamic{};
};

struct Invariant {
  int i{};
  std::string s;
};

struct InvariantWithResult {
  int i{};
  std::string s;
};

struct InvariantAndFallback {
  int i{};
  std::string s;
};

struct ObjectInvariant {
  int i{};
  std::string s;
};

struct NestedInvariant {
  Invariant i;
  ObjectInvariant o;
};

struct FallbackReference {
  int x{};
  int y{};
};

struct OptionalFieldTransform {
  std::optional<int> x;
  std::optional<int> y;
  std::optional<int> z;
};

struct Specialization {
  int i{};
  std::string s;
};

enum class AnEnumClass { Option1, Option2, Option3 };

template<typename Enum>
struct EnumStorage {
  using MemoryType = Enum;

  std::underlying_type_t<Enum> code{};
  std::string message;

  explicit EnumStorage(Enum e)
    : code(std::to_underlying(e)), message(to_string(e)) {}
  explicit EnumStorage() {}

  operator Enum() const { return Enum(code); }
};

struct AnEmptyObject {};

struct NonDefaultConstructibleIntLike {
  NonDefaultConstructibleIntLike() = default;
  explicit NonDefaultConstructibleIntLike(uint64_t value) : value(value) {}

  friend auto operator==(NonDefaultConstructibleIntLike,
                         NonDefaultConstructibleIntLike) -> bool = default;

  uint64_t value{};
};

void VPackRead(auto ctx, NonDefaultConstructibleIntLike& v) {
  v.value = ctx.vpack().template getNumber<uint64_t>();
}

void VPackWrite(auto ctx, NonDefaultConstructibleIntLike v) {
  ctx.vpack().add(v.value);
}

}  // namespace

namespace {

struct ExplicitIgnore {
  std::string s;
};

struct Unsafe {
  std::string_view view;
  vpack::Slice slice;
};

struct Struct1 {
  int v{};
};
struct Struct2 {
  int v{};
};
struct Struct3 {
  int a{};
  int b{};
};

struct MyQualifiedVariant
  : std::variant<std::string, int, Struct1, Struct2, std::monostate> {};

struct QualifiedVariant {
  MyQualifiedVariant a;
  MyQualifiedVariant b;
  MyQualifiedVariant c;
  MyQualifiedVariant d;
  MyQualifiedVariant e;
};

struct MyUnqualifiedVariant
  : std::variant<std::string, int, Struct1, Struct2, std::monostate> {};

struct UnqualifiedVariant {
  MyUnqualifiedVariant a;
  MyUnqualifiedVariant b;
  MyUnqualifiedVariant c;
  MyUnqualifiedVariant d;
  MyUnqualifiedVariant e;
};

struct MyEmbeddedVariant : std::variant<Struct1, Struct2, Struct3, bool> {};

struct EmbeddedVariant {
  MyEmbeddedVariant a;
  MyEmbeddedVariant b;
  MyEmbeddedVariant c;
  MyEmbeddedVariant d;
};

struct MyInlineVariant
  : std::variant<std::string, Struct1, std::vector<int>, TypedInt,
                 std::tuple<std::string, int, bool>> {};

struct InlineVariant {
  MyInlineVariant a;
  MyInlineVariant b;
  MyInlineVariant c;
  MyInlineVariant d;
  MyInlineVariant e;
};

enum class MyStringEnum {
  Value1,
  Value2,
  Value3 = Value2,
};

}  // namespace

namespace {
enum class MyIntEnum {
  Value1 = 1,
  Value2,
  Value3 = Value2,
};

enum class MyMixedEnum {
  Value1,
  Value2,
};

struct Embedded {
  int a{};
  InvariantAndFallback inner;
  int b{};
};

struct EmbeddedObjectInvariant {
  int a{};
  ObjectInvariant inner;
  int b{};
};

struct WithContext {
  int i{};
  std::string s;
};

struct Context {
  int default_int;
  int min_int;
  std::string default_string;
};

template<typename Context>
void VPackRead(Context ctx, WithContext& v) {
  auto& arg = ctx.arg();
  v.i = std::max(arg.default_int, arg.min_int);
  v.s = arg.default_string;
}

}  // namespace

namespace magic_enum {

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<MyMixedEnum>(MyMixedEnum value) noexcept {
  switch (value) {
    case MyMixedEnum::Value1:
      return "value1";
    case MyMixedEnum::Value2:
      return "value2";
  }
  return invalid_tag;
}

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<MyStringEnum>(MyStringEnum value) noexcept {
  switch (value) {
    case MyStringEnum::Value1:
      return "value1";
    case MyStringEnum::Value2:
      return "value2";
  }
  return invalid_tag;
}

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<MyIntEnum>(MyIntEnum value) noexcept {
  switch (value) {
    case MyIntEnum::Value1:
      return "value1";
    case MyIntEnum::Value2:
      return "value2";
  }
  return invalid_tag;
}

}  // namespace magic_enum
