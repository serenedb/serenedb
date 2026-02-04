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

#include "inverted_index_shard_meta.h"

#include <absl/strings/str_cat.h>
#include <vpack/builder.h>
#include <vpack/iterator.h>
#include <vpack/parser.h>

#include <iresearch/utils/index_utils.hpp>

#include "catalog/search_common.h"
#include "catalog/vpack_helper.h"
#include "vpack/vpack_helper.h"

namespace {

using namespace sdb;
using namespace sdb::search;

constexpr std::string_view kPolicyTier = "tier";

template<typename T>
InvertedIndexShardMeta::ConsolidationPolicy CreateConsolidationPolicy(
  vpack::Slice slice, std::string& error_field);

template<>
InvertedIndexShardMeta::ConsolidationPolicy
CreateConsolidationPolicy<irs::index_utils::ConsolidateTier>(
  vpack::Slice slice, std::string& error_field) {
  irs::index_utils::ConsolidateTier options;
  vpack::Builder properties;

  {
    constexpr std::string_view kFieldName = "segmentsBytesFloor";

    auto field = slice.get(kFieldName);
    if (!field.isNone()) {
      if (!field.isNumber<size_t>()) {
        error_field = kFieldName;

        return {};
      }

      options.floor_segment_bytes = field.getNumber<size_t>();
    }
  }

  {
    constexpr std::string_view kFieldName = "segmentsBytesMax";

    auto field = slice.get(kFieldName);
    if (!field.isNone()) {
      if (!field.isNumber<size_t>()) {
        error_field = kFieldName;

        return {};
      }

      options.max_segments_bytes = field.getNumber<size_t>();
    }
  }

  {
    constexpr std::string_view kFieldName = "segmentsMax";

    auto field = slice.get(kFieldName);
    if (!field.isNone()) {
      if (!field.isNumber<size_t>()) {
        error_field = kFieldName;

        return {};
      }

      options.max_segments = field.getNumber<size_t>();
    }
  }

  {
    constexpr std::string_view kFieldName = "segmentsMin";

    auto field = slice.get(kFieldName);
    if (!field.isNone()) {
      if (!field.isNumber<size_t>()) {
        error_field = kFieldName;

        return {};
      }

      options.min_segments = field.getNumber<size_t>();
    }
  }

  {
    constexpr std::string_view kFieldName = "minScore";

    auto field = slice.get(kFieldName);
    if (!field.isNone()) {
      if (!field.isNumber<double>()) {
        error_field = kFieldName;

        return {};
      }

      options.min_score = field.getNumber<double>();
    }
  }

  properties.openObject();
  properties.add("type", kPolicyTier);
  properties.add("segmentsBytesFloor", options.floor_segment_bytes);
  properties.add("segmentsBytesMax", options.max_segments_bytes);
  properties.add("segmentsMax", options.max_segments);
  properties.add("segmentsMin", options.min_segments);
  properties.add("minScore", options.min_score);
  properties.close();

  return {irs::index_utils::MakePolicy(options), std::move(properties)};
}

}  // namespace

namespace sdb::search {
InvertedIndexShardMeta::Mask::Mask(bool mask /*=false*/) noexcept
  : cleanup_interval_step(mask),
    commit_interval_msec(mask),
    consolidation_interval_msec(mask),
    consolidation_policy(mask),
    version(mask),
    writebuffer_active(mask),
    writebuffer_idle(mask),
    writebuffer_size_max(mask) {}

InvertedIndexShardMeta::InvertedIndexShardMeta()
  : cleanup_interval_step(2),
    commit_interval_msec(1000),
    consolidation_interval_msec(1000),
    version(static_cast<uint32_t>(ViewVersion::Max)),
    writebuffer_active(0),
    writebuffer_idle(64),
    writebuffer_size_max(32 * (size_t(1) << 20)) {  // 32MB
  std::string error_field;

  // cppcheck-suppress useInitializationList
  consolidation_policy =
    CreateConsolidationPolicy<irs::index_utils::ConsolidateTier>(
      vpack::Parser::fromJson("{ \"type\": \"tier\" }")->slice(), error_field);
  SDB_ASSERT(consolidation_policy.policy());  // ensure above syntax is correct
}

void InvertedIndexShardMeta::storeFull(const InvertedIndexShardMeta& other) {
  if (this == &other) {
    return;
  }
  cleanup_interval_step = other.cleanup_interval_step;
  commit_interval_msec = other.commit_interval_msec;
  consolidation_interval_msec = other.consolidation_interval_msec;
  consolidation_policy = other.consolidation_policy;
  version = other.version;
  writebuffer_active = other.writebuffer_active;
  writebuffer_idle = other.writebuffer_idle;
  writebuffer_size_max = other.writebuffer_size_max;
}

void InvertedIndexShardMeta::storeFull(InvertedIndexShardMeta&& other) noexcept {
  if (this == &other) {
    return;
  }
  cleanup_interval_step = other.cleanup_interval_step;
  commit_interval_msec = other.commit_interval_msec;
  consolidation_interval_msec = other.consolidation_interval_msec;
  consolidation_policy = std::move(other.consolidation_policy);
  version = other.version;
  writebuffer_active = other.writebuffer_active;
  writebuffer_idle = other.writebuffer_idle;
  writebuffer_size_max = other.writebuffer_size_max;
}

void InvertedIndexShardMeta::storePartial(InvertedIndexShardMeta&& other) noexcept {
  if (this == &other) {
    return;
  }
  cleanup_interval_step = other.cleanup_interval_step;
  commit_interval_msec = other.commit_interval_msec;
  consolidation_interval_msec = other.consolidation_interval_msec;
  consolidation_policy = std::move(other.consolidation_policy);
}

bool InvertedIndexShardMeta::json(vpack::Builder& builder,
                         const InvertedIndexShardMeta* ignore_equal /*= nullptr*/,
                         const InvertedIndexShardMeta::Mask* mask /*= nullptr*/) const {
  if (!builder.isOpenObject()) {
    return false;
  }

  if ((!ignore_equal ||
       cleanup_interval_step != ignore_equal->cleanup_interval_step) &&
      (!mask || mask->cleanup_interval_step)) {
    builder.add(StaticStrings::kCleanupIntervalStep, cleanup_interval_step);
  }

  if ((!ignore_equal ||
       commit_interval_msec != ignore_equal->commit_interval_msec) &&
      (!mask || mask->commit_interval_msec)) {
    builder.add(StaticStrings::kCommitIntervalMsec, commit_interval_msec);
  }

  if ((!ignore_equal || consolidation_interval_msec !=
                          ignore_equal->consolidation_interval_msec) &&
      (!mask || mask->consolidation_interval_msec)) {
    builder.add(StaticStrings::kConsolidationIntervalMsec,
                consolidation_interval_msec);
  }

  if ((!ignore_equal || !basics::VPackHelper::equals(
                          consolidation_policy.properties(),
                          ignore_equal->consolidation_policy.properties())) &&
      (!mask || mask->consolidation_policy)) {
    builder.add(StaticStrings::kConsolidationPolicy,
                consolidation_policy.properties());
  }

  if ((!ignore_equal || version != ignore_equal->version) &&
      (!mask || mask->version)) {
    builder.add(StaticStrings::kVersionField, version);
  }

  if ((!ignore_equal ||
       writebuffer_active != ignore_equal->writebuffer_active) &&
      (!mask || mask->writebuffer_active)) {
    builder.add(StaticStrings::kWritebufferActive, writebuffer_active);
  }

  if ((!ignore_equal || writebuffer_idle != ignore_equal->writebuffer_idle) &&
      (!mask || mask->writebuffer_idle)) {
    builder.add(StaticStrings::kWritebufferIdle, writebuffer_idle);
  }

  if ((!ignore_equal ||
       writebuffer_size_max != ignore_equal->writebuffer_size_max) &&
      (!mask || mask->writebuffer_size_max)) {
    builder.add(StaticStrings::kWritebufferSizeMax, writebuffer_size_max);
  }

  return true;
}

bool InvertedIndexShardMeta::init(vpack::Slice slice, std::string& error_field,
                         const InvertedIndexShardMeta& defaults, Mask* mask) noexcept {
  if (!slice.isObject()) {
    error_field = "Object is expected";
    return false;
  }

  Mask tmp_mask;

  if (!mask) {
    mask = &tmp_mask;
  }

  {
    constexpr std::string_view kFieldName{StaticStrings::kVersionField};

    auto field = slice.get(kFieldName);
    mask->version = !field.isNone();

    if (!mask->version) {
      version = defaults.version;
    } else {
      if (!GetNumber(version, field)) {
        error_field = kFieldName;
        return false;
      }
    }
  }

  {
    constexpr std::string_view kFieldName{StaticStrings::kCleanupIntervalStep};

    auto field = slice.get(kFieldName);
    mask->cleanup_interval_step = !field.isNone();

    if (!mask->cleanup_interval_step) {
      cleanup_interval_step = defaults.cleanup_interval_step;
    } else {
      if (!GetNumber(cleanup_interval_step, field)) {
        error_field = kFieldName;
        return false;
      }
    }
  }

  {
    constexpr std::string_view kFieldName{StaticStrings::kCommitIntervalMsec};

    auto field = slice.get(kFieldName);
    mask->commit_interval_msec = !field.isNone();

    if (!mask->commit_interval_msec) {
      commit_interval_msec = defaults.commit_interval_msec;
    } else {
      if (!GetNumber(commit_interval_msec, field)) {
        error_field = kFieldName;
        return false;
      }
    }
  }

  {
    constexpr std::string_view kFieldName{
      StaticStrings::kConsolidationIntervalMsec};

    auto field = slice.get(kFieldName);
    mask->consolidation_interval_msec = !field.isNone();

    if (!mask->consolidation_interval_msec) {
      consolidation_interval_msec = defaults.consolidation_interval_msec;
    } else {
      if (!GetNumber(consolidation_interval_msec, field)) {
        error_field = kFieldName;
        return false;
      }
    }
  }

  {
    constexpr std::string_view kFieldName{StaticStrings::kConsolidationPolicy};
    std::string error_sub_field;

    auto field = slice.get(kFieldName);
    mask->consolidation_policy = !field.isNone();

    if (!mask->consolidation_policy) {
      consolidation_policy = defaults.consolidation_policy;
    } else {
      if (!field.isObject()) {
        error_field = kFieldName;
        return false;
      }

      constexpr std::string_view kTypeFieldName("type");

      auto type_field = field.get(kTypeFieldName);

      if (!type_field.isString()) {
        error_field = absl::StrCat(kFieldName, ".", kTypeFieldName);
        return false;
      }

      const auto type = type_field.stringView();

      if (kPolicyTier == type) {
        consolidation_policy =
          CreateConsolidationPolicy<irs::index_utils::ConsolidateTier>(
            field, error_sub_field);
      } else {
        error_field = absl::StrCat(kFieldName, ".", kTypeFieldName);
        return false;
      }

      if (!consolidation_policy.policy()) {
        if (error_sub_field.empty()) {
          error_field = kFieldName;
        } else {
          error_field = absl::StrCat(kFieldName, ".", error_sub_field);
        }
        return false;
      }
    }
  }

  {
    constexpr std::string_view kFieldName(StaticStrings::kWritebufferActive);

    auto field = slice.get(kFieldName);
    mask->writebuffer_active = !field.isNone();

    if (!mask->writebuffer_active) {
      writebuffer_active = defaults.writebuffer_active;
    } else {
      if (!GetNumber(writebuffer_active, field)) {
        error_field = kFieldName;
        return false;
      }
    }
  }

  {
    constexpr std::string_view kFieldName(StaticStrings::kWritebufferIdle);

    auto field = slice.get(kFieldName);
    mask->writebuffer_idle = !field.isNone();

    if (!mask->writebuffer_idle) {
      writebuffer_idle = defaults.writebuffer_idle;
    } else {
      if (!GetNumber(writebuffer_idle, field)) {
        error_field = kFieldName;
        return false;
      }
    }
  }

  {
    constexpr std::string_view kFieldName(StaticStrings::kWritebufferSizeMax);

    auto field = slice.get(kFieldName);
    mask->writebuffer_size_max = !field.isNone();

    if (!mask->writebuffer_size_max) {
      writebuffer_size_max = defaults.writebuffer_size_max;
    } else {
      if (!GetNumber(writebuffer_size_max, field)) {
        error_field = kFieldName;
        return false;
      }
    }
  }
  return true;
}

bool InvertedIndexShardMeta::operator==(const InvertedIndexShardMeta& other) const noexcept {
  if (consolidation_interval_msec != other.consolidation_interval_msec ||
      cleanup_interval_step != other.cleanup_interval_step ||
      commit_interval_msec != other.commit_interval_msec ||
      version != other.version ||
      writebuffer_active != other.writebuffer_active ||
      writebuffer_idle != other.writebuffer_idle ||
      writebuffer_size_max != other.writebuffer_size_max) {
    return false;
  }

  try {
    if (!basics::VPackHelper::equals(consolidation_policy.properties(),
                                     other.consolidation_policy.properties())) {
      return false;
    }
  } catch (...) {
    return false;
  }
  return true;
}

}  // namespace sdb::search
