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

#include <string>

namespace sdb::search::functions {

inline constexpr std::string_view kPhrase = "phrase";
inline constexpr std::string_view kTermEq = "term_eq";
inline constexpr std::string_view kTermLt = "term_lt";
inline constexpr std::string_view kTermLe = "term_lte";
inline constexpr std::string_view kTermGe = "term_gte";
inline constexpr std::string_view kTermGt = "term_gt";
inline constexpr std::string_view kTermIn = "term_in";
inline constexpr std::string_view kTermLike = "term_like";
inline constexpr std::string_view kNgramMatch = "ngram_match";
inline constexpr std::string_view kLevenshteinMatch = "levenshtein_match";

void RegisterSearchFunctions(const std::string& prefix);

}  // namespace sdb::search::functions
