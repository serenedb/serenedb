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

#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::lead {

template<typename T>
concept Type = requires(T& t, doc_id_t target) {
  { t.Value() } noexcept -> std::same_as<doc_id_t>;
  { t.Advance() } -> std::same_as<doc_id_t>;
  { t.Seek(target) } -> std::same_as<doc_id_t>;
};

template<typename T>
concept ScoredType = Type<T> && requires(T& t, uint32_t slot) {
  { t.FetchScoreArgs(slot) };
  { t.PrepareScore() } -> std::same_as<ScoreFunction>;
};

}  // namespace irs::lead
