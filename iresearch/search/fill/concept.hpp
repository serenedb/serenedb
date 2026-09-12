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

#include <concepts>
#include <cstdint>

#include "iresearch/types.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::fill {

template<typename T>
concept Producer = requires(T& t, doc_id_t min, doc_id_t max, uint64_t* mask) {
  { t.FillOr(min, max, mask) } -> std::same_as<doc_id_t>;
};

template<typename T>
concept Type =
  Producer<T> && requires(T& t, doc_id_t min, doc_id_t max, uint64_t* mask) {
    { t.FillAnd(min, max, mask) } -> std::same_as<doc_id_t>;
    { t.FillAndNot(min, max, mask) } -> std::same_as<doc_id_t>;
  };

template<typename T>
concept ConstantProducer =
  Producer<T> && requires(T& t, doc_id_t min, doc_id_t max, uint64_t* mask,
                          score_t* scores, score_t value) {
    { t.FillSum(min, max, mask, scores, value) } -> std::same_as<doc_id_t>;
    { t.FillMax(min, max, mask, scores, value) } -> std::same_as<doc_id_t>;
  };

template<typename T>
concept ScoredType =
  requires(T& t, doc_id_t min, doc_id_t max, uint64_t* mask, score_t* scores) {
    { t.Fill(min, max, mask, scores) } -> std::same_as<doc_id_t>;
  };

}  // namespace irs::fill
