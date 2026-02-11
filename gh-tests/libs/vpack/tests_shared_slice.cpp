////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Tobias Gödderz
////////////////////////////////////////////////////////////////////////////////

#include <memory>
#include <variant>

#include "gtest/gtest.h"
#include "tests-common.h"
#include "vpack/builder.h"
#include "vpack/slice.h"

using namespace vpack;

void InitTestCases(std::vector<Builder>& test_cases) {
  {
    auto& builder = test_cases.emplace_back();
    builder.add(Slice::noneSlice());
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.add(Slice::nullSlice());
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.add(Slice::trueSlice());
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.add(Slice::falseSlice());
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.add(Slice::zeroSlice());
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.add(Slice::emptyStringSlice());
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.add(Slice::emptyArraySlice());
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.add(Slice::emptyObjectSlice());
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.add(UINT64_MAX);
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.add(42);
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.add(-42);
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.openArray();
    builder.add(42);
    builder.close();
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.openArray();
    builder.add("42");
    builder.close();
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.openObject();
    builder.add("foo", 42);
    builder.close();
  }
  {
    auto& builder = test_cases.emplace_back();
    builder.openObject();
    builder.add("bar", "42");
    builder.close();
  }
}

const std::vector<Builder>& TestCases() {
  static std::vector<Builder> gTestCases{};

  if (gTestCases.empty()) {
    InitTestCases(gTestCases);
  }

  return gTestCases;
}

/*
// Iterates over all testcases. Takes a callback `F(Slice, SharedSlice)`.
template<typename F>
  requires std::is_invocable_v<F, Slice>
void ForAllTestCases(F f) {
  for (const auto& builder : TestCases()) {
    ASSERT_TRUE(builder.isClosed());
    auto slice = builder.slice();
    // auto sharedSlice = SharedSlice(builder.buffer());
    auto shared_slice = builder.sharedSlice();
    // both should point to equal data
    ASSERT_EQ(slice.byteSize(), shared_slice.slice().byteSize());
    ASSERT_EQ(
      0, memcmp(slice.start(), shared_slice.slice().start(), slice.byteSize()));
    // Now that we know the data is equal, make slice point to the exact same
    // location.
    // This makes comparisons in the tests easier, because they can be reduced
    // to pointer comparisons.
    slice = shared_slice.slice();
    ASSERT_EQ(slice.start(), shared_slice.slice().start());
    using namespace std::string_literals;
    auto slice_string = [&]() {
      try {
        return slice.toString();
      } catch (const Exception&) {
        return slice.toHex();
      }
    }();
    auto msg = "When testing slice "s + slice_string;
    SCOPED_TRACE(msg.c_str());
    f(slice, shared_slice);
  }
}

// Iterates over all testcases. Takes a callback `F(SharedSlice&&)`
// (or `F(SharedSlice)`, the slice will be moved).
// Used for refcount tests, so the SharedSlice will be the only owner of its
// buffer.
template<typename F>
  requires std::is_invocable_v<F, SharedSlice&&>
void ForAllTestCases(F f) {
  for (const auto& builder : TestCases()) {
    Buffer buffer = builder.bufferRef();
    ASSERT_TRUE(builder.isClosed());
    auto shared_slice = SharedSlice{std::move(buffer)};
    ASSERT_EQ(1, shared_slice.buffer().use_count());
    using namespace std::string_literals;
    auto slice_string = [&]() {
      try {
        return shared_slice.toString();
      } catch (const Exception&) {
        return shared_slice.toHex();
      }
    }();
    auto msg = "When testing slice "s + slice_string;
    SCOPED_TRACE(msg.c_str());
    f(std::move(shared_slice));
  }
}
*/

/**
 * @brief Can hold either a value or an exception, and is constructed by
 *        executing a callback. Holds either the value the callback returns,
 * or the vpack::Exception it throws. Can then be used to compare the
 * result/exception with another instance.
 *
 *        Also allows to compare a Slice with a SharedSlice by comparing the
 *        data pointers.
 */
template<typename V>
class ResultOrException {
 public:
  template<typename F>
  explicit ResultOrException(F f) {
    try {
      variant.template emplace<V>(f());
    } catch (const Exception& e) {
      variant.template emplace<Exception>(e);
    }
  }

  template<typename W>
  bool operator==(const ResultOrException<W>& other) const {
    using LeftType = std::decay_t<decltype(std::get<0>(variant))>;
    using RightType = std::decay_t<decltype(std::get<0>(other.variant))>;
    static_assert(std::is_same_v<std::decay_t<V>, LeftType>);
    static_assert(std::is_same_v<std::decay_t<W>, RightType>);
    constexpr bool kLeftIsSlice = std::is_same_v<Slice, LeftType>;
    constexpr bool kRightIsSharedSlice = false;
    // We only allow comparisons of either equal types V == W, or comparing
    // a Slice (left) with a SharedSlice (right).
    static_assert(kLeftIsSlice == kRightIsSharedSlice);
    auto constexpr kComparingSliceWithSharedSlice =
      kLeftIsSlice && kRightIsSharedSlice;
    static_assert(std::is_same_v<V, W> || kComparingSliceWithSharedSlice);

    if (variant.index() != other.variant.index()) {
      return false;
    }

    switch (variant.index()) {
      case 0: {
        const auto& left = std::get<0>(variant);
        const auto& right = std::get<0>(other.variant);
        if constexpr (kComparingSliceWithSharedSlice) {
          // Compare slice with shared slice by pointer equality
          return left.start() == right.buffer().get();
        } else {
          // Compare other values with operator==()
          return left == right;
        }
      }
      case 1: {
        const auto& left = std::get<1>(variant);
        const auto& right = std::get<1>(other.variant);
        return left.errorCode() == right.errorCode();
      }
      case std::variant_npos:
        throw std::exception();
    }

    throw std::exception();
  }

  friend std::ostream& operator<<(std::ostream& out,
                                  const ResultOrException& that) {
    const auto& variant = that.variant;
    switch (variant.index()) {
      case 0: {
        const auto& value = std::get<0>(variant);
        return out << ::testing::PrintToString(value);
      }
      case 1: {
        const auto& ex = std::get<1>(variant);
        return out << ex;
      }
      case std::variant_npos:
        throw std::exception();
    }

    throw std::exception();
  }

  std::variant<V, Exception> variant;
};

template<typename F>
ResultOrException(F) -> ResultOrException<std::invoke_result_t<F>>;

#define R(a) ResultOrException([&]() { return a; })

// Compare either the results, or the exceptions if thrown.
// I'd actually prefer to write
// ASSERT_EQ(R(left), R(right))
// inline everywhere, but we'd need googletest 1.10 for that to work.
#define ASSERT_EQ_EX(left, right) \
  do {                            \
    auto l = R(left);             \
    auto r = R(right);            \
    ASSERT_EQ(l, r);              \
  } while (false)

/*
TEST(SharedSliceAgainstSliceTest, start) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.start(), shared_slice.start().get());
  });
}

TEST(SharedSliceAgainstSliceTest, startAs) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.startAs<void*>(), shared_slice.startAs<void*>().get());
  });
}

TEST(SharedSliceAgainstSliceTest, head) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.head(), shared_slice.head());
  });
}

TEST(SharedSliceAgainstSliceTest, begin) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.begin(), shared_slice.begin().get());
  });
}

TEST(SharedSliceAgainstSliceTest, end) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.end(), shared_slice.end().get());
  });
}

TEST(SharedSliceAgainstSliceTest, type) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.type(), shared_slice.type());
  });
}

TEST(SharedSliceAgainstSliceTest, typeName) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.typeName(), shared_slice.typeName());
  });
}

TEST(SharedSliceAgainstSliceTest, volatileHash) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.volatileHash(), shared_slice.volatileHash());
  });
}

TEST(SharedSliceAgainstSliceTest, hash) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.hash(), shared_slice.hash());
  });
}

TEST(SharedSliceAgainstSliceTest, hashSlow) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.hashSlow(), shared_slice.hashSlow());
  });
}

TEST(SharedSliceAgainstSliceTest, normalizedHash) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.normalizedHash(), shared_slice.normalizedHash());
  });
}

TEST(SharedSliceAgainstSliceTest, hashString) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    if (slice.isString()) {
      ASSERT_EQ(slice.hashString(), shared_slice.hashString());
    }
  });
}

TEST(SharedSliceAgainstSliceTest, isType) {
  const auto types = std::vector<ValueType>{
    ValueType::None,  ValueType::None,     ValueType::Null,   ValueType::Bool,
    ValueType::Array, ValueType::Object,   ValueType::Double, ValueType::None,
    ValueType::None,  ValueType::None,     ValueType::None,   ValueType::Int,
    ValueType::UInt,  ValueType::SmallInt, ValueType::String, ValueType::None,
    ValueType::None,  ValueType::None,     ValueType::None,
  };

  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto type : types) {
      using namespace std::string_literals;
      SCOPED_TRACE("When testing type "s + std::string{valueTypeName(type)});
      ASSERT_EQ(slice.type() == type, shared_slice.type() == type);
    }
  });
}

TEST(SharedSliceAgainstSliceTest, isNone) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isNone(), shared_slice.isNone());
  });
}

TEST(SharedSliceAgainstSliceTest, isIllegal) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isNone(), shared_slice.isNone());
  });
}

TEST(SharedSliceAgainstSliceTest, isNull) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isNull(), shared_slice.isNull());
  });
}

TEST(SharedSliceAgainstSliceTest, isBool) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isBool(), shared_slice.isBool());
  });
}

TEST(SharedSliceAgainstSliceTest, isTrue) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isTrue(), shared_slice.isTrue());
  });
}

TEST(SharedSliceAgainstSliceTest, isFalse) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isFalse(), shared_slice.isFalse());
  });
}

TEST(SharedSliceAgainstSliceTest, isArray) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isArray(), shared_slice.isArray());
  });
}

TEST(SharedSliceAgainstSliceTest, isObject) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isObject(), shared_slice.isObject());
  });
}

TEST(SharedSliceAgainstSliceTest, isDouble) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isDouble(), shared_slice.isDouble());
  });
}

TEST(SharedSliceAgainstSliceTest, isMinKey) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isNone(), shared_slice.isNone());
  });
}

TEST(SharedSliceAgainstSliceTest, isMaxKey) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isNone(), shared_slice.isNone());
  });
}

TEST(SharedSliceAgainstSliceTest, isInt) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isInt(), shared_slice.isInt());
  });
}

TEST(SharedSliceAgainstSliceTest, isUInt) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isUInt(), shared_slice.isUInt());
  });
}

TEST(SharedSliceAgainstSliceTest, isSmallInt) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isSmallInt(), shared_slice.isSmallInt());
  });
}

TEST(SharedSliceAgainstSliceTest, isString) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isString(), shared_slice.isString());
  });
}

TEST(SharedSliceAgainstSliceTest, isInteger) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isInteger(), shared_slice.isInteger());
  });
}

TEST(SharedSliceAgainstSliceTest, isNumber) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isNumber(), shared_slice.isNumber());
  });
}

TEST(SharedSliceAgainstSliceTest, isNumberT) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isNumber<uint64_t>(), shared_slice.isNumber<uint64_t>());
    ASSERT_EQ(slice.isNumber<int64_t>(), shared_slice.isNumber<int64_t>());
    ASSERT_EQ(slice.isNumber<uint8_t>(), shared_slice.isNumber<uint8_t>());
  });
}

TEST(SharedSliceAgainstSliceTest, isSorted) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ(slice.isSorted(), shared_slice.isSorted());
  });
}

TEST(SharedSliceAgainstSliceTest, getBool) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.getBool(), shared_slice.getBool());
  });
}

TEST(SharedSliceAgainstSliceTest, getDouble) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.getDouble(), shared_slice.getDouble());
  });
}

TEST(SharedSliceAgainstSliceTest, at) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.at(0), shared_slice.at(0));
    ASSERT_EQ_EX(slice.at(1), shared_slice.at(1));
  });
}

TEST(SharedSliceAgainstSliceTest, length) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.length(), shared_slice.length());
  });
}

TEST(SharedSliceAgainstSliceTest, keyAt) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.keyAt(0), shared_slice.keyAt(0));
    ASSERT_EQ_EX(slice.keyAt(1), shared_slice.keyAt(1));
  });
}

TEST(SharedSliceAgainstSliceTest, valueAt) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.valueAt(0), shared_slice.valueAt(0));
    ASSERT_EQ_EX(slice.valueAt(1), shared_slice.valueAt(1));
  });
}

TEST(SharedSliceAgainstSliceTest, getPCharVectorCharLen) {
  using namespace std::string_literals;
  auto paths =
    std::vector<std::pair<const char*, size_t>>{{"foo", 3}, {"bar", 3}};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& path : paths) {
      ASSERT_EQ_EX(slice.get(std::string_view{path.first, path.second}),
                   shared_slice.get(std::string_view{path.first, path.second}));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, getPVector) {
  using namespace std::string_literals;
  auto paths = std::vector<std::vector<std::string>>{{"foo"s}, {"bar"s}};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& path : paths) {
      ASSERT_EQ_EX(slice.get(path), shared_slice.get(path));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, getPStringRef) {
  using namespace std::string_literals;
  auto attrs = std::vector<std::string>{"foo"s, "bar"s};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& attr : attrs) {
      ASSERT_EQ_EX(slice.get(std::string_view(attr)),
                   shared_slice.get(std::string_view(attr)));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, getPString) {
  using namespace std::string_literals;
  auto attrs = std::vector<std::string>{"foo"s, "bar"s};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& attr : attrs) {
      ASSERT_EQ_EX(slice.get(attr), shared_slice.get(attr));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, getPCharPtr) {
  using namespace std::string_literals;
  auto attrs = std::vector<std::string>{"foo"s, "bar"s};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& attr : attrs) {
      ASSERT_EQ_EX(slice.get(attr.c_str()), shared_slice.get(attr.c_str()));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, operatorIndexPStringRef) {
  using namespace std::string_literals;
  auto attrs = std::vector<std::string>{"foo"s, "bar"s};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& attr : attrs) {
      ASSERT_EQ_EX(slice.get(std::string_view(attr)),
                   shared_slice.get(std::string_view(attr)));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, operatorIndexPString) {
  using namespace std::string_literals;
  auto attrs = std::vector<std::string>{"foo"s, "bar"s};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& attr : attrs) {
      ASSERT_EQ_EX(slice.get(attr), shared_slice.get(attr));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, findDataOffset) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    if ((slice.isArray() && !slice.isEmptyArray()) ||
        (slice.isObject() && !slice.isEmptyObject())) {
      ASSERT_EQ_EX(slice.findDataOffset(slice.head()),
                   shared_slice.findDataOffset(slice.head()));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, hasKeyPStringRef) {
  using namespace std::string_literals;
  auto attrs = std::vector<std::string>{"foo"s, "bar"s};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& attr : attrs) {
      ASSERT_EQ_EX(slice.hasKey(std::string_view(attr)),
                   !shared_slice.get(std::string_view(attr)).isNone());
    }
  });
}

TEST(SharedSliceAgainstSliceTest, hasKeyPString) {
  using namespace std::string_literals;
  auto attrs = std::vector<std::string>{"foo"s, "bar"s};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& attr : attrs) {
      ASSERT_EQ_EX(slice.hasKey(attr), !shared_slice.get(attr).isNone());
    }
  });
}

TEST(SharedSliceAgainstSliceTest, hasKeyPCharPtr) {
  using namespace std::string_literals;
  auto attrs = std::vector<std::string>{"foo"s, "bar"s};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& attr : attrs) {
      ASSERT_EQ_EX(slice.hasKey(attr.c_str()),
                   !shared_slice.get(attr.c_str()).isNone());
    }
  });
}

TEST(SharedSliceAgainstSliceTest, hasKeyPVector) {
  using namespace std::string_literals;
  auto paths = std::vector<std::vector<std::string>>{{"foo"s}, {"bar"s}};
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    for (const auto& path : paths) {
      ASSERT_EQ_EX(slice.get(path).isNone(), shared_slice.get(path).isNone());
    }
  });
}

TEST(SharedSliceAgainstSliceTest, isEmptyArray) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.isEmptyArray(), shared_slice.isEmptyArray());
  });
}

TEST(SharedSliceAgainstSliceTest, isEmptyObject) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.isEmptyObject(), shared_slice.isEmptyObject());
  });
}

TEST(SharedSliceAgainstSliceTest, getInt) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.getInt(), shared_slice.getInt());
  });
}

TEST(SharedSliceAgainstSliceTest, getUInt) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.getUInt(), shared_slice.getUInt());
  });
}

TEST(SharedSliceAgainstSliceTest, getNumber) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.getNumber<uint64_t>(),
                 shared_slice.getNumber<uint64_t>());
    ASSERT_EQ_EX(slice.getNumber<int64_t>(), shared_slice.getNumber<int64_t>());
    ASSERT_EQ_EX(slice.getNumber<uint8_t>(), shared_slice.getNumber<uint8_t>());
  });
}

TEST(SharedSliceAgainstSliceTest, getStringUnchecked) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    if (slice.isString()) {
      ASSERT_EQ(slice.stringViewUnchecked(), shared_slice.stringView());
    }
  });
}

TEST(SharedSliceAgainstSliceTest, copyString) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.copyString(), shared_slice.copyString());
  });
}

TEST(SharedSliceAgainstSliceTest, stringView) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.stringView(), shared_slice.stringView());
  });
}

TEST(SharedSliceAgainstSliceTest, byteSize) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.byteSize(), shared_slice.byteSize());
  });
}

TEST(SharedSliceAgainstSliceTest, getNthOffset) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    if (slice.isArray() || slice.isObject()) {  // avoid assertion
      ASSERT_EQ_EX(slice.getNthOffset(0), shared_slice.getNthOffset(0));
      ASSERT_EQ_EX(slice.getNthOffset(1), shared_slice.getNthOffset(1));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, compareStringPStringRef) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.compareString("42"), shared_slice.compareString("42"));
    ASSERT_EQ_EX(slice.compareString("foo"), shared_slice.compareString("foo"));
    ASSERT_EQ_EX(slice.compareString("bar"), shared_slice.compareString("bar"));
  });
}

TEST(SharedSliceAgainstSliceTest, compareStringPString) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    using namespace std::string_literals;
    ASSERT_EQ_EX(slice.compareString("42"s), shared_slice.compareString("42"s));
    ASSERT_EQ_EX(slice.compareString("foo"s),
                 shared_slice.compareString("foo"s));
    ASSERT_EQ_EX(slice.compareString("bar"s),
                 shared_slice.compareString("bar"s));
  });
}

TEST(SharedSliceAgainstSliceTest, compareStringPCharPtr) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.compareString("42"), shared_slice.compareString("42"));
    ASSERT_EQ_EX(slice.compareString("foo"), shared_slice.compareString("foo"));
    ASSERT_EQ_EX(slice.compareString("bar"), shared_slice.compareString("bar"));
  });
}

TEST(SharedSliceAgainstSliceTest, compareStringUncheckedPStringRef) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    if (slice.isString()) {
      ASSERT_EQ_EX(slice.compareStringUnchecked("42"),
                   shared_slice.compareStringUnchecked("42"));
      ASSERT_EQ_EX(slice.compareStringUnchecked("foo"),
                   shared_slice.compareStringUnchecked("foo"));
      ASSERT_EQ_EX(slice.compareStringUnchecked("bar"),
                   shared_slice.compareStringUnchecked("bar"));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, compareStringUncheckedPString) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    if (slice.isString()) {
      using namespace std::string_literals;
      ASSERT_EQ_EX(slice.compareStringUnchecked("42"s),
                   shared_slice.compareStringUnchecked("42"s));
      ASSERT_EQ_EX(slice.compareStringUnchecked("foo"s),
                   shared_slice.compareStringUnchecked("foo"s));
      ASSERT_EQ_EX(slice.compareStringUnchecked("bar"s),
                   shared_slice.compareStringUnchecked("bar"s));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, compareStringUncheckedPCharPtr) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    if (slice.isString()) {
      ASSERT_EQ_EX(slice.compareStringUnchecked("42"),
                   shared_slice.compareStringUnchecked("42"));
      ASSERT_EQ_EX(slice.compareStringUnchecked("foo"),
                   shared_slice.compareStringUnchecked("foo"));
      ASSERT_EQ_EX(slice.compareStringUnchecked("bar"),
                   shared_slice.compareStringUnchecked("bar"));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, isEqualStringPStringRef) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.isEqualString("42"), shared_slice.isEqualString("42"));
    ASSERT_EQ_EX(slice.isEqualString("foo"), shared_slice.isEqualString("foo"));
    ASSERT_EQ_EX(slice.isEqualString("bar"), shared_slice.isEqualString("bar"));
  });
}

TEST(SharedSliceAgainstSliceTest, isEqualStringPString) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    using namespace std::string_literals;
    ASSERT_EQ_EX(slice.isEqualString("42"s), shared_slice.isEqualString("42"s));
    ASSERT_EQ_EX(slice.isEqualString("foo"s),
                 shared_slice.isEqualString("foo"s));
    ASSERT_EQ_EX(slice.isEqualString("bar"s),
                 shared_slice.isEqualString("bar"s));
  });
}

TEST(SharedSliceAgainstSliceTest, isEqualStringUncheckedPStringRef) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    if (slice.isString()) {
      ASSERT_EQ_EX(slice.isEqualStringUnchecked("42"),
                   shared_slice.isEqualStringUnchecked("42"));
      ASSERT_EQ_EX(slice.isEqualStringUnchecked("foo"),
                   shared_slice.isEqualStringUnchecked("foo"));
      ASSERT_EQ_EX(slice.isEqualStringUnchecked("bar"),
                   shared_slice.isEqualStringUnchecked("bar"));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, isEqualStringUncheckedPString) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    if (slice.isString()) {
      using namespace std::string_literals;
      ASSERT_EQ_EX(slice.isEqualStringUnchecked("42"s),
                   shared_slice.isEqualStringUnchecked("42"s));
      ASSERT_EQ_EX(slice.isEqualStringUnchecked("foo"s),
                   shared_slice.isEqualStringUnchecked("foo"s));
      ASSERT_EQ_EX(slice.isEqualStringUnchecked("bar"s),
                   shared_slice.isEqualStringUnchecked("bar"s));
    }
  });
}

TEST(SharedSliceAgainstSliceTest, binaryEquals) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.binaryEquals(slice), shared_slice.binaryEquals(slice));
    ASSERT_EQ_EX(slice.binaryEquals(slice),
                 shared_slice.binaryEquals(shared_slice));
    ASSERT_EQ_EX(slice.binaryEquals(shared_slice.slice()),
                 shared_slice.binaryEquals(slice));
    ASSERT_EQ_EX(slice.binaryEquals(shared_slice.slice()),
                 shared_slice.binaryEquals(shared_slice));
  });
}

TEST(SharedSliceAgainstSliceTest, toHex) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.toHex(), shared_slice.toHex());
  });
}

TEST(SharedSliceAgainstSliceTest, toJson) {
  const auto pretty = []() {
    auto opts = Options{};
    opts.pretty_print = true;
    return opts;
  }();
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.toJson(), shared_slice.toJson());
    ASSERT_EQ_EX(slice.toJson(&pretty), shared_slice.toJson(&pretty));
  });
}

TEST(SharedSliceAgainstSliceTest, toString) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.toString(), shared_slice.toString());
    // TODO add a check with an options argument, that actually results in a
    // different return value for some input test case.
  });
}

TEST(SharedSliceAgainstSliceTest, hexType) {
  ForAllTestCases([&](Slice slice, SharedSlice shared_slice) {
    ASSERT_EQ_EX(slice.hexType(), shared_slice.hexType());
  });
}

TEST(SharedSliceRefcountTest, createFromEmpty) {
  std::shared_ptr<const uint8_t> empty;

  SharedSlice shared_slice{empty};
  ASSERT_TRUE(shared_slice.isNone());
}

TEST(SharedSliceRefcountTest, createFromEmptyBuffer) {
  Builder b;

  SharedSlice shared_slice = std::move(b).sharedSlice();
  ASSERT_TRUE(shared_slice.isNone());
}

TEST(SharedSliceRefcountTest, createFromOpenBuffer) {
  Builder b;
  b.openObject();

  SharedSlice s;
  ASSERT_VPACK_EXCEPTION(s = std::move(b).sharedSlice(),
                         Exception::kBuilderNotSealed);
}

TEST(SharedSliceRefcountTest, createFromUInt8) {
  std::shared_ptr<uint8_t> data(new uint8_t[7],
                                std::default_delete<uint8_t[]>());
  char* p = reinterpret_cast<char*>(data.get());
  *p++ = '\x86';
  *p++ = 'f';
  *p++ = 'o';
  *p++ = 'o';
  *p++ = 'b';
  *p++ = 'a';
  *p++ = 'r';

  SharedSlice shared_slice{std::const_pointer_cast<const uint8_t>(data)};
  ASSERT_TRUE(shared_slice.isString());
  ASSERT_EQ("foobar", shared_slice.copyString());
}

TEST(SharedSliceRefcountTest, createFromUInt8Move) {
  std::shared_ptr<uint8_t> data(new uint8_t[7],
                                std::default_delete<uint8_t[]>());
  char* p = reinterpret_cast<char*>(data.get());
  *p++ = '\x86';
  *p++ = 'f';
  *p++ = 'o';
  *p++ = 'o';
  *p++ = 'b';
  *p++ = 'a';
  *p++ = 'r';

  SharedSlice shared_slice{std::const_pointer_cast<const uint8_t>(data)};
  ASSERT_TRUE(shared_slice.isString());
  ASSERT_EQ("foobar", shared_slice.copyString());
}

TEST(SharedSliceRefcountTest, copyConstructor) {
  ForAllTestCases([&](SharedSlice&& shared_slice_ref) {
    // We assume to be the only owner of the referenced buffer
    ASSERT_EQ(1, shared_slice_ref.buffer().use_count());

    // Execute copy constructor
    SharedSlice shared_slice{shared_slice_ref};

    // Use count for both should be two
    ASSERT_GE(2, shared_slice_ref.buffer().use_count());
    ASSERT_GE(2, shared_slice.buffer().use_count());

    // Both should share ownership
    ASSERT_TRUE(haveSameOwnership(shared_slice_ref, shared_slice));

    // Both should share the same buffer
    ASSERT_EQ(shared_slice_ref.buffer(), shared_slice.buffer());
  });
}

TEST(SharedSliceRefcountTest, copyAssignment) {
  SharedSlice shared_slice;
  ForAllTestCases([&](SharedSlice&& shared_slice_ref) {
    // We assume to be the only owner of the referenced buffer
    ASSERT_EQ(1, shared_slice_ref.buffer().use_count());

    // Execute copy assignment
    shared_slice = shared_slice_ref;

    // Use count for both should be two
    ASSERT_GE(2, shared_slice_ref.buffer().use_count());
    ASSERT_GE(2, shared_slice.buffer().use_count());

    // Both should share ownership
    ASSERT_TRUE(haveSameOwnership(shared_slice_ref, shared_slice));

    // Both should share the same buffer
    ASSERT_EQ(shared_slice_ref.buffer(), shared_slice.buffer());
  });
}

TEST(SharedSliceRefcountTest, aliasingCopyConstructor) {
  ForAllTestCases([&](SharedSlice&& shared_slice_ref) {
    // We assume to be the only owner of the referenced buffer
    ASSERT_EQ(1, shared_slice_ref.buffer().use_count());

    Builder b;
    b.add(-7);

    const auto slice = b.slice();

    // Execute aliasing copy constructor (nonsensical here - usually, slice
    // should point into the same memory block)
    SharedSlice shared_slice{shared_slice_ref, slice};

    // Use count for both should be two
    ASSERT_GE(2, shared_slice_ref.buffer().use_count());
    ASSERT_GE(2, shared_slice.buffer().use_count());

    // Both should share ownership
    ASSERT_TRUE(haveSameOwnership(shared_slice_ref, shared_slice));

    // The aliased copy should point to different memory than the originating
    // shared slice...
    ASSERT_NE(shared_slice_ref.buffer(), shared_slice.buffer());
    // ... but to the same memory the originating (raw) slice did, instead.
    ASSERT_EQ(slice.start(), shared_slice.start().get());
  });
}

TEST(SharedSliceRefcountTest, moveConstructor) {
  ForAllTestCases([&](SharedSlice&& shared_slice_ref) {
    // We assume to be the only owner of the referenced buffer
    ASSERT_EQ(1, shared_slice_ref.buffer().use_count());
    const auto orig_pointer = shared_slice_ref.buffer().get();

    // Execute move constructor
    SharedSlice shared_slice{std::move(shared_slice_ref)};

    // The passed slice should now point to a valid None slice
    ASSERT_LE(0, shared_slice_ref.buffer().use_count());

    ASSERT_TRUE(shared_slice_ref.isNone());
    // The underlying buffers should be different
    ASSERT_NE(shared_slice_ref.buffer(), shared_slice.buffer());

    // The slices should not share ownership
    ASSERT_FALSE(haveSameOwnership(shared_slice_ref, shared_slice));

    // The local sharedSlice should be the only owner of its buffer
    ASSERT_EQ(1, shared_slice.buffer().use_count());

    // sharedSlice should point to the same buffer as the sharedSliceRef did
    // originally
    ASSERT_EQ(orig_pointer, shared_slice.buffer().get());
  });
}

TEST(SharedSliceRefcountTest, aliasingMoveConstructor) {
  ForAllTestCases([&](SharedSlice&& shared_slice_ref) {
    // We assume to be the only owner of the referenced buffer
    ASSERT_EQ(1, shared_slice_ref.buffer().use_count());
    const auto orig_pointer = shared_slice_ref.buffer().get();

    Builder b;
    b.add(-7);

    const auto slice = b.slice();

    // Execute aliasing move constructor (nonsensical here - usually, slice
    // should point into the same memory block)
    SharedSlice shared_slice{std::move(shared_slice_ref), slice};

    // The passed slice should now point to a valid None slice
    ASSERT_LE(0, shared_slice_ref.buffer().use_count());
    ASSERT_TRUE(shared_slice_ref.isNone());
    // The underlying buffers should be different
    ASSERT_NE(shared_slice_ref.buffer(), shared_slice.buffer());

    // The slices should not share ownership
    ASSERT_FALSE(haveSameOwnership(shared_slice_ref, shared_slice));

    // The local sharedSlice should be the only owner of its buffer
    ASSERT_EQ(1, shared_slice.buffer().use_count());

    // The aliased copy should point to different memory than the originating
    // shared slice...
    ASSERT_NE(orig_pointer, shared_slice.buffer().get());
    // ... but to the same memory the originating (raw) slice did, instead.
    ASSERT_EQ(slice.start(), shared_slice.start().get());
  });
}

TEST(SharedSliceRefcountTest, moveAssignment) {
  SharedSlice shared_slice;
  ForAllTestCases([&](SharedSlice&& shared_slice_ref) {
    // We assume to be the only owner of the referenced buffer
    ASSERT_EQ(1, shared_slice_ref.buffer().use_count());
    const auto orig_pointer = shared_slice_ref.buffer().get();

    // Execute move assignment
    shared_slice = std::move(shared_slice_ref);

    // The passed slice should now point to a valid None slice
    ASSERT_LE(0, shared_slice_ref.buffer().use_count());
    ASSERT_TRUE(shared_slice_ref.isNone());
    // The underlying buffers should be different
    ASSERT_NE(shared_slice_ref.buffer(), shared_slice.buffer());

    // The slices should not share ownership
    ASSERT_FALSE(haveSameOwnership(shared_slice_ref, shared_slice));

    // The local sharedSlice should be the only owner of its buffer
    ASSERT_EQ(1, shared_slice.buffer().use_count());

    // sharedSlice should point to the same buffer as the sharedSliceRef did
    // originally
    ASSERT_EQ(orig_pointer, shared_slice.buffer().get());
  });
}

TEST(SharedSliceRefcountTest, destructor) {
  ForAllTestCases([&](SharedSlice&& shared_slice_ref) {
    std::weak_ptr<const uint8_t> weak_ptr;
    {
      SharedSlice shared_slice{std::move(shared_slice_ref)};
      // We assume to be the only owner of the referenced buffer
      ASSERT_EQ(1, shared_slice.buffer().use_count());
      weak_ptr = decltype(weak_ptr)(shared_slice.buffer());
      ASSERT_EQ(1, weak_ptr.use_count());
    }
    ASSERT_EQ(0, weak_ptr.use_count());
  });
}

TEST(SharedSliceRefcountTest, start) {
  ForAllTestCases([&](SharedSlice shared_slice) {
    ASSERT_EQ(1, shared_slice.buffer().use_count());
    auto result = shared_slice.start();
    ASSERT_EQ(2, shared_slice.buffer().use_count());
    ASSERT_EQ(2, result.use_count());
    ASSERT_TRUE(haveSameOwnership(shared_slice.buffer(), result));
  });
}

TEST(SharedSliceRefcountTest, startAs) {
  ForAllTestCases([&](SharedSlice shared_slice) {
    ASSERT_EQ(1, shared_slice.buffer().use_count());
    auto result = shared_slice.startAs<void*>();
    ASSERT_EQ(2, shared_slice.buffer().use_count());
    ASSERT_EQ(2, result.use_count());
    ASSERT_TRUE(haveSameOwnership(shared_slice.buffer(), result));
  });
}

TEST(SharedSliceRefcountTest, begin) {
  ForAllTestCases([&](SharedSlice shared_slice) {
    ASSERT_EQ(1, shared_slice.buffer().use_count());
    auto result = shared_slice.begin();
    ASSERT_EQ(2, shared_slice.buffer().use_count());
    ASSERT_EQ(2, result.use_count());
    ASSERT_TRUE(haveSameOwnership(shared_slice.buffer(), result));
  });
}

TEST(SharedSliceRefcountTest, end) {
  ForAllTestCases([&](SharedSlice shared_slice) {
    ASSERT_EQ(1, shared_slice.buffer().use_count());
    auto result = shared_slice.end();
    ASSERT_EQ(2, shared_slice.buffer().use_count());
    ASSERT_EQ(2, result.use_count());
    ASSERT_TRUE(haveSameOwnership(shared_slice.buffer(), result));
  });
}

*/
