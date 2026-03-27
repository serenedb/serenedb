////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
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

#pragma once

#include <array>
#include <string_view>

#include "basics/system-compiler.h"

namespace sdb::search {

/// the delimiter used to separate jSON nesting levels when
/// generating
///        flat iResearch field names
inline constexpr const char kNestingLevelDelimiter = '.';

/// the prefix used to denote start of jSON list offset when generating
///        flat iResearch field names
inline constexpr const char kNestingListOffsetPrefix = '[';

/// the suffix used to denote end of jSON list offset when generating
///        flat iResearch field names
inline constexpr const char kNestingListOffsetSuffix = ']';

[[maybe_unused]] inline constexpr std::string_view kIresearchInvertedIndexType =
  "inverted";

/// defines the implementation version of the iresearch view interface
///        e.g. which how data is stored in iresearch
enum class ViewVersion : uint32_t {
  Min = 0,
  Max = 0,  // the latest
};

/// defines the implementation version of the iresearch index interface
///        e.g. which how data is stored in iresearch
enum class LinkVersion : uint32_t {
  Min = 0,
  Max = 0,  // the latest
};

/// Return format identifier according to a specified index version
constexpr std::string_view GetFormat(LinkVersion version) noexcept {
  constexpr std::array<std::string_view, 1> kFormats{
    "1_5simd"  // the current storage format used with Search index
  };
  return kFormats[static_cast<uint32_t>(version)];
}

inline constexpr std::string_view kPkColumn = "#";

struct StaticStrings {
  static constexpr std::string_view kEngineDirRoot = "engine_search";

  /// the name of the field in the Search View definition denoting the
  /// corresponding link definitions
  static constexpr std::string_view kVersionField = "version";

  /// attribute name for storing link/inverted index errors
  static constexpr std::string_view kLinkError = "error";
  static constexpr std::string_view kLinkErrorOutOfSync = "outOfSync";
  static constexpr std::string_view kLinkErrorFailed = "failed";

  /// the name of the field in the Search Link definition denoting the
  /// referenced analyzer definitions
  static constexpr std::string_view kAnalyzerDefinitionsField =
    "analyzerDefinitions";

  /// the name of the field in the analyzer definition denoting the
  /// corresponding analyzer properties
  static constexpr std::string_view kAnalyzerPropertiesField = "properties";

  /// the name of the field in the analyzer definition denoting the
  /// corresponding analyzer features
  static constexpr std::string_view kAnalyzerFeaturesField = "features";

  static constexpr std::string_view kAnalyzerInputTypeField = "inputType";
  static constexpr std::string_view kAnalyzerReturnTypeField = "returnType";

  /// the name of the field in the Search Link definition denoting the
  ///        stored values
  static constexpr std::string_view kStoredValuesField = "storedValues";

  /// the name of the field in the Search Link definition denoting the
  ///        corresponding collection name in cluster (not shard name!)
  static constexpr std::string_view kTableNameField = "collectionName";

  // enables caching for field
  static constexpr std::string_view kCacheField = "cache";

  // enables caching for primary key column
  static constexpr std::string_view kCachePrimaryKeyField = "primaryKeyCache";

  static constexpr std::string_view kOptimizeTopField = "optimizeTop";

  /// the name of the field in the Search View definition denoting the
  ///        time in Ms between running consolidations
  static constexpr std::string_view kConsolidationIntervalMsec =
    "consolidationIntervalMsec";

  /// the name of the field in the Search View definition denoting the
  ///        time in Ms between running commits
  static constexpr std::string_view kCommitIntervalMsec = "commitIntervalMsec";

  /// the name of the field in the Search View definition denoting the
  ///        number of completed consolidtions before cleanup is run
  static constexpr std::string_view kCleanupIntervalStep =
    "cleanupIntervalStep";

  /// the name of the field in the Search View definition denoting the
  ///         consolidation policy properties
  static constexpr std::string_view kConsolidationPolicy =
    "consolidationPolicy";

  /// the name of the field in the Search View definition denoting the
  ///        maximum number of concurrent active writers (segments) that perform
  ///        a transaction. Other writers (segments) wait till current active
  ///        writers (segments) finish.
  static constexpr std::string_view kWritebufferActive = "writebufferActive";

  /// the name of the field in the Search View definition denoting the
  ///        maximum number of writers (segments) cached in the pool.
  static constexpr std::string_view kWritebufferIdle = "writebufferIdle";

  /// the name of the field in the Search View definition denoting the
  ///        maximum memory byte size per writer (segment) before a writer
  ///        (segment) flush is triggered
  static constexpr std::string_view kWritebufferSizeMax = "writebufferSizeMax";
};

}  // namespace sdb::search
