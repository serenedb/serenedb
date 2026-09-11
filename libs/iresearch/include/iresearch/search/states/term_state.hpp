////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/common/score_args.hpp"

namespace irs {

struct TermReader;

struct TermState {
  TermState(const TermReader* reader, const PostingMeta& cookie)
    : reader{reader}, cookie{cookie} {}

  const TermReader* reader;
  PostingMeta cookie;
};

namespace search {

struct PostingClause {
  TermState state;
  score_t boost = kNoBoost;
  StatsRecord stats{};
};

struct AllDocsClause {
  score_t boost = kNoBoost;
  StatsRecord stats;
};

}  // namespace search
}  // namespace irs
