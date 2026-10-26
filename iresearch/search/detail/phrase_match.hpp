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

#include <cstdint>

namespace irs::detail {

enum class PhraseMatch : uint8_t {
  Plain,
  Intervals,
  Slop,
};

template<typename Query>
PhraseMatch MatchOf(const Query& query) noexcept {
  if (query.slop != 0) {
    return PhraseMatch::Slop;
  }
  return query.has_intervals ? PhraseMatch::Intervals : PhraseMatch::Plain;
}

template<typename Query, typename F>
auto ResolveMatch(const Query& query, F&& f) {
  switch (MatchOf(query)) {
    case PhraseMatch::Slop:
      return f.template operator()<PhraseMatch::Slop>();
    case PhraseMatch::Intervals:
      return f.template operator()<PhraseMatch::Intervals>();
    case PhraseMatch::Plain:
      break;
  }
  return f.template operator()<PhraseMatch::Plain>();
}

}  // namespace irs::detail
