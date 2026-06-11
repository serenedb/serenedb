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

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/utils/string.hpp>
#include <magic_enum/magic_enum.hpp>
#include <utility>

#include "basics/object_pool.hpp"
#include "basics/result.h"

namespace sdb::search {

class SearchAnalyzerFeature;

class Features final {
 public:
  constexpr Features(
    irs::IndexFeatures index_features = irs::IndexFeatures::None) noexcept
    : _index_features{index_features} {}

  // Adds feature by name. Properly resolves field/index features
  // Return true if feature found, false otherwise
  bool Add(std::string_view name);

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

  void Serialize(duckdb::Serializer& serializer) const {
    serializer.WriteValue(std::to_underlying(_index_features));
  }

  static Features Deserialize(duckdb::Deserializer& deserializer) {
    return Features{
      static_cast<irs::IndexFeatures>(deserializer.ReadUnsignedInt8())};
  }

 private:
  irs::IndexFeatures _index_features;
};

bool IsGeoAnalyzer(std::string_view type) noexcept;

class AnalyzerImpl final {
 public:
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
    static ptr make(std::string_view bytes);
  };

  using CacheType = irs::UnboundedObjectPool<Builder>;
};

}  // namespace sdb::search
