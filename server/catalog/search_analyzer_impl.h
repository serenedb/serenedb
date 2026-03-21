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

#pragma once

#include <vpack/builder.h>
#include <vpack/slice.h>

#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/utils/string.hpp>

#include "basics/bit_utils.hpp"
#include "basics/exceptions.h"
#include "basics/object_pool.hpp"
#include "catalog/function.h"

namespace sdb::search {

class SearchAnalyzerFeature;

enum class FunctionValueType : uint8_t {
  Invalid = 0,
  String = 1 << 0,
  Number = 1 << 1,  // json number: f64, i64, u64
  Bool = 1 << 2,
  Null = 1 << 3,  // json null
  Array = 1 << 4,
  Object = 1 << 5,
  Collection = 1 << 6,  // collection name

  JsonPrimitive = String | Number | Bool | Null,
  JsonCompound = Array | Object,
  Json = JsonPrimitive | JsonCompound,
};

ENABLE_BITMASK_ENUM(FunctionValueType);

struct AnalyzerReturnTypeAttr final : irs::Attribute {
  static constexpr std::string_view type_name() noexcept {
    return "return_type";
  }
  FunctionValueType value = FunctionValueType::Invalid;
};

class Features final {
 public:
  constexpr Features(
    irs::IndexFeatures index_features = irs::IndexFeatures::None) noexcept
    : _index_features{index_features} {}

  // Adds feature by name. Properly resolves field/index features
  // Return true if feature found, false otherwise
  bool Add(std::string_view name);

  Result FromVPack(vpack::Slice s);
  void ToVPack(vpack::Builder& b) const;

  std::string ToStr() const {
    vpack::Builder builder;
    ToVPack(builder);
    return builder.toJson();
  }

  void Clear() noexcept { _index_features = irs::IndexFeatures::None; }

  constexpr irs::IndexFeatures GetIndexFeatures() const noexcept {
    return _index_features;
  }

  // Validate that features are supported by serened an ensure that
  // their dependencies are met.
  Result Validate(std::string_view type = {}) const;

  void Visit(std::function<void(std::string_view)> visitor) const;

  bool HasFeatures(irs::IndexFeatures features) const noexcept {
    return (_index_features & features) == features;
  }

  constexpr bool operator==(const Features& rhs) const noexcept = default;
  constexpr auto operator<=>(const Features& rhs) const noexcept = default;

 private:
  irs::IndexFeatures _index_features;
};

void VPackWrite(auto ctx, const Features& features) {
  features.ToVPack(ctx.vpack());
}
void VPackRead(auto ctx, Features& features) {
  Result res = features.FromVPack(ctx.vpack());
  if (res.fail()) {
    SDB_THROW(std::move(res));
  }
}

bool IsGeoAnalyzer(std::string_view type) noexcept;

// TODO(mbkkt) rewrite this, I think we have such requirements:
// 1. We should parse analyzer definition only once
// 2. We don't need to cache allocations
// 3. We shouldn't copy/have multiple immutable parts of analyzer definition,
//    e.g. stopwords dictionary
// Thread-safe analyzer pool
class AnalyzerImpl final {
 public:
  using StoreFunc = irs::bytes_view (*)(irs::Tokenizer*, vpack::Slice);

  // type tags for primitive token streams
  struct StringStreamTag {};
  struct NumberStreamTag {};
  struct BoolStreamTag {};
  struct NullStreamTag {};

  struct Builder {
    using ptr = irs::analysis::Analyzer::ptr;

    static ptr make(StringStreamTag);
    static ptr make(NumberStreamTag);
    static ptr make(BoolStreamTag);
    static ptr make(NullStreamTag);
    static ptr make(std::string_view type, vpack::Slice properties);
  };

  using CacheType = irs::UnboundedObjectPool<Builder>;

  // nullptr == error creating analyzer
  CacheType::ptr Get() const noexcept;

  std::string_view GetType() const noexcept { return _type; }
  vpack::Slice GetProperties() const noexcept { return _properties; }
  Features GetFeatures() const noexcept { return _features; }
  StoreFunc GetStoreFunc() const noexcept { return _store_func; }
  char GetFieldMarker() const noexcept { return _field_marker; }

  void ToVPack(vpack::Builder& builder) const;

  static Result FromVPack(ObjectId database, vpack::Slice slice,
                          std::unique_ptr<AnalyzerImpl>& implementation);

  Result init(std::string_view type, vpack::Slice properties, Features features,
              FunctionValueType input_type = FunctionValueType::Invalid,
              FunctionValueType return_type = FunctionValueType::Invalid);

  FunctionValueType GetInputType() const noexcept { return _input_type; }

  FunctionValueType GetReturnType() const noexcept { return _return_type; }

  bool Accepts(FunctionValueType arg) const noexcept {
    return (_input_type & arg) == arg;
  }

 private:
  // cache size is 8, it's chosen in legacy code without clarification
  mutable CacheType _cache{8};
  std::string_view _type;
  vpack::Slice _properties;
  Features _features;
  StoreFunc _store_func{};
  char _field_marker{};
  std::string _config;  // non-null type + non-null properties

  FunctionValueType _input_type{FunctionValueType::Invalid};
  FunctionValueType _return_type{FunctionValueType::Invalid};
};

bool AnalyzerEquals(const AnalyzerImpl& analyzer, std::string_view type,
                    vpack::Slice properties, Features features);

}  // namespace sdb::search
namespace magic_enum {

template<>
[[maybe_unused]] constexpr customize::customize_t
customize::enum_name<sdb::search::FunctionValueType>(
  sdb::search::FunctionValueType value) noexcept {
  switch (value) {
    using enum sdb::search::FunctionValueType;
    case String:
      return "string";
    case Number:
      return "number";
    case Bool:
      return "bool";
    case Null:
      return "null";
    case Array:
      return "array";
    case Object:
      return "object";
    case JsonPrimitive:
      return "json-primitive";
    case JsonCompound:
      return "json-compound";
    case Json:
      return "json";
    default:
      return invalid_tag;
  }
}

}  // namespace magic_enum
