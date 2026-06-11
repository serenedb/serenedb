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

#include <memory>
#include <vector>

#include "iresearch/search/filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs::optimizer {

template<typename T>
std::unique_ptr<T> MakeBoolean(std::vector<Filter::ptr> children,
                               ScoreMergeType merge_type) {
  auto node = std::make_unique<T>();
  node->merge_type(merge_type);
  for (auto& child : children) {
    node->add(std::move(child));
  }
  return node;
}

inline bool TryFoldBoost(Filter& survivor, score_t boost,
                         const Scorer* scorer) {
  if (boost == kNoBoost || scorer == nullptr) {
    return true;
  }
  auto* boostable = dynamic_cast<FilterWithBoost*>(&survivor);
  if (boostable == nullptr) {
    return false;
  }
  boostable->boost(boostable->Boost() * boost);
  return true;
}

}  // namespace irs::optimizer
