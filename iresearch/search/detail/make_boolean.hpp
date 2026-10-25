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

#include "iresearch/search/detail/make_query.hpp"

#define IRS_MAKE_BOOLEAN(Family, Ctx)                                 \
  template Family::Result MakeQuery<Family, Ctx>(const BooleanQuery&, \
                                                 const Ctx&)

#define IRS_MAKE_BOOLEAN_REQUIRED(Family, Ctx)                                 \
  template Family::Result builder::MakeRequired<Family, Ctx>(                  \
    std::span<const PostingClause>, std::span<const QueryBuilder::ptr>,        \
    std::span<const PostingClause>, std::span<const QueryBuilder::ptr>, Terms, \
    uint32_t, const SubReader&, const Ctx&, ScoreMergeType, score_t)

#define IRS_MAKE_BOOLEAN_EXCLUSION(Family, Ctx)                \
  template Family::Result builder::MakeExclusion<Family, Ctx>( \
    const BooleanQuery&, const Ctx&, ScoreMergeType, score_t)

#define IRS_MAKE_BOOLEAN_EXCLUSION_WINDOW(Family, Ctx)               \
  template Family::Result builder::MakeExclusionWindow<Family, Ctx>( \
    const BooleanQuery&, const Ctx&, ScoreMergeType, score_t, uint64_t)

#define IRS_MAKE_BOOLEAN_NEGATION(Family, Ctx)                          \
  template Family::Result builder::MakeNegation<Family, Ctx>(           \
    std::span<const PostingClause>, std::span<const QueryBuilder::ptr>, \
    const SubReader&, uint64_t, const Ctx&)
