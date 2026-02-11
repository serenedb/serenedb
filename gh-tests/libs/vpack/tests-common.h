
#include "basics/buffer.h"
#include "basics/sink.h"
#include "gtest/gtest.h"
#include "vpack/builder.h"
#include "vpack/collection.h"
#include "vpack/common.h"
#include "vpack/dumper.h"
#include "vpack/exception.h"
#include "vpack/hex_dump.h"
#include "vpack/iterator.h"
#include "vpack/options.h"
#include "vpack/parser.h"
#include "vpack/slice.h"
#include "vpack/validator.h"
#include "vpack/value.h"
#include "vpack/value_type.h"

using namespace vpack;

// helper for catching VPack-specific exceptions
#define ASSERT_VPACK_EXCEPTION(operation, code) \
  try {                                         \
    (operation);                                \
    ASSERT_FALSE(true);                         \
  } catch (Exception const& ex) {               \
    ASSERT_EQ(code, ex.errorCode());            \
  } catch (...) {                               \
    ASSERT_FALSE(true);                         \
  }

// don't complain if this function is not called
static void DumpDouble(double, uint8_t*) VPACK_UNUSED;

static void DumpDouble(double x, uint8_t* p) {
  uint64_t u;
  memcpy(&u, &x, sizeof(double));
  for (size_t i = 0; i < 8; i++) {
    p[i] = u & 0xff;
    u >>= 8;
  }
}

// don't complain if this function is not called
static void CheckDump(Slice, const std::string&) VPACK_UNUSED;

static void CheckDump(Slice s, const std::string& known_good) {
  sdb::basics::StrSink sink;
  Dumper dumper(&sink);
  dumper.Dump(s);
  ASSERT_EQ(known_good, sink.Impl());
}

// don't complain if this function is not called
static void CheckBuild(Slice, ValueType, ValueLength) VPACK_UNUSED;

// With the following function we check type determination and size
// of the produced VPack value:
static void CheckBuild(Slice s, ValueType t, ValueLength byte_size) {
  ASSERT_EQ(t, s.type());
  ValueType other =
    (t == ValueType::String) ? ValueType::Int : ValueType::String;
  ASSERT_NE(other, s.type());

  ASSERT_EQ(byte_size, s.byteSize());

  switch (t) {
    case ValueType::None:
      ASSERT_TRUE(s.isNone());
      ASSERT_FALSE(s.isNull());
      ASSERT_FALSE(s.isBool());
      ASSERT_FALSE(s.isFalse());
      ASSERT_FALSE(s.isTrue());
      ASSERT_FALSE(s.isDouble());
      ASSERT_FALSE(s.isArray());
      ASSERT_FALSE(s.isObject());
      ASSERT_FALSE(s.isInt());
      ASSERT_FALSE(s.isUInt());
      ASSERT_FALSE(s.isSmallInt());
      ASSERT_FALSE(s.isString());
      ASSERT_FALSE(s.isNumber());
      break;
    case ValueType::Null:
      ASSERT_FALSE(s.isNone());
      ASSERT_TRUE(s.isNull());
      ASSERT_FALSE(s.isBool());
      ASSERT_FALSE(s.isFalse());
      ASSERT_FALSE(s.isTrue());
      ASSERT_FALSE(s.isDouble());
      ASSERT_FALSE(s.isArray());
      ASSERT_FALSE(s.isObject());
      ASSERT_FALSE(s.isInt());
      ASSERT_FALSE(s.isUInt());
      ASSERT_FALSE(s.isSmallInt());
      ASSERT_FALSE(s.isString());
      ASSERT_FALSE(s.isNumber());
      break;
    case ValueType::Bool:
      ASSERT_FALSE(s.isNone());
      ASSERT_FALSE(s.isNull());
      ASSERT_TRUE(s.isBool());
      ASSERT_TRUE(s.isFalse() || s.isTrue());
      ASSERT_FALSE(s.isDouble());
      ASSERT_FALSE(s.isArray());
      ASSERT_FALSE(s.isObject());
      ASSERT_FALSE(s.isInt());
      ASSERT_FALSE(s.isUInt());
      ASSERT_FALSE(s.isSmallInt());
      ASSERT_FALSE(s.isString());
      ASSERT_FALSE(s.isNumber());
      break;
    case ValueType::Double:
      ASSERT_FALSE(s.isNone());
      ASSERT_FALSE(s.isNull());
      ASSERT_FALSE(s.isBool());
      ASSERT_FALSE(s.isFalse());
      ASSERT_FALSE(s.isTrue());
      ASSERT_TRUE(s.isDouble());
      ASSERT_FALSE(s.isArray());
      ASSERT_FALSE(s.isObject());
      ASSERT_FALSE(s.isInt());
      ASSERT_FALSE(s.isUInt());
      ASSERT_FALSE(s.isSmallInt());
      ASSERT_FALSE(s.isString());
      ASSERT_TRUE(s.isNumber());
      break;
    case ValueType::Array:
      ASSERT_FALSE(s.isNone());
      ASSERT_FALSE(s.isNull());
      ASSERT_FALSE(s.isBool());
      ASSERT_FALSE(s.isFalse());
      ASSERT_FALSE(s.isTrue());
      ASSERT_FALSE(s.isDouble());
      ASSERT_TRUE(s.isArray());
      ASSERT_FALSE(s.isObject());
      ASSERT_FALSE(s.isInt());
      ASSERT_FALSE(s.isUInt());
      ASSERT_FALSE(s.isSmallInt());
      ASSERT_FALSE(s.isString());
      ASSERT_FALSE(s.isNumber());
      break;
    case ValueType::Object:
      ASSERT_FALSE(s.isNone());
      ASSERT_FALSE(s.isNull());
      ASSERT_FALSE(s.isBool());
      ASSERT_FALSE(s.isFalse());
      ASSERT_FALSE(s.isTrue());
      ASSERT_FALSE(s.isDouble());
      ASSERT_FALSE(s.isArray());
      ASSERT_TRUE(s.isObject());
      ASSERT_FALSE(s.isInt());
      ASSERT_FALSE(s.isUInt());
      ASSERT_FALSE(s.isSmallInt());
      ASSERT_FALSE(s.isString());
      ASSERT_FALSE(s.isNumber());
      break;
    case ValueType::Int:
      ASSERT_FALSE(s.isNone());
      ASSERT_FALSE(s.isNull());
      ASSERT_FALSE(s.isBool());
      ASSERT_FALSE(s.isFalse());
      ASSERT_FALSE(s.isTrue());
      ASSERT_FALSE(s.isDouble());
      ASSERT_FALSE(s.isArray());
      ASSERT_FALSE(s.isObject());
      ASSERT_TRUE(s.isInt());
      ASSERT_FALSE(s.isUInt());
      ASSERT_FALSE(s.isSmallInt());
      ASSERT_FALSE(s.isString());
      ASSERT_TRUE(s.isNumber());
      break;
    case ValueType::UInt:
      ASSERT_FALSE(s.isNone());
      ASSERT_FALSE(s.isNull());
      ASSERT_FALSE(s.isBool());
      ASSERT_FALSE(s.isFalse());
      ASSERT_FALSE(s.isTrue());
      ASSERT_FALSE(s.isDouble());
      ASSERT_FALSE(s.isArray());
      ASSERT_FALSE(s.isObject());
      ASSERT_FALSE(s.isInt());
      ASSERT_TRUE(s.isUInt());
      ASSERT_FALSE(s.isSmallInt());
      ASSERT_FALSE(s.isString());
      ASSERT_TRUE(s.isNumber());
      break;
    case ValueType::SmallInt:
      ASSERT_FALSE(s.isNone());
      ASSERT_FALSE(s.isNull());
      ASSERT_FALSE(s.isBool());
      ASSERT_FALSE(s.isFalse());
      ASSERT_FALSE(s.isTrue());
      ASSERT_FALSE(s.isDouble());
      ASSERT_FALSE(s.isArray());
      ASSERT_FALSE(s.isObject());
      ASSERT_FALSE(s.isInt());
      ASSERT_FALSE(s.isUInt());
      ASSERT_TRUE(s.isSmallInt());
      ASSERT_FALSE(s.isString());
      ASSERT_TRUE(s.isNumber());
      break;
    case ValueType::String:
      ASSERT_FALSE(s.isNone());
      ASSERT_FALSE(s.isNull());
      ASSERT_FALSE(s.isBool());
      ASSERT_FALSE(s.isFalse());
      ASSERT_FALSE(s.isTrue());
      ASSERT_FALSE(s.isDouble());
      ASSERT_FALSE(s.isArray());
      ASSERT_FALSE(s.isObject());
      ASSERT_FALSE(s.isInt());
      ASSERT_FALSE(s.isUInt());
      ASSERT_FALSE(s.isSmallInt());
      ASSERT_TRUE(s.isString());
      ASSERT_FALSE(s.isNumber());
      break;
  }
}

template<typename T, typename U>
bool HaveSameOwnership(const std::shared_ptr<T>& left,
                       const std::shared_ptr<U>& right) {
  static thread_local auto gOwnerLess = std::owner_less<void>{};
  return !gOwnerLess(left, right) && !gOwnerLess(right, left);
}
