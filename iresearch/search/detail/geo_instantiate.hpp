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

#include "iresearch/search/detail/geo_parsers.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/filters/geo_filter.hpp"
#include "iresearch/search/queries/geo_query.hpp"

#define IRS_GEO_PARSERS(F, ...)     \
  F(SourceJsonParser, __VA_ARGS__)  \
  F(SourceWkbParser, __VA_ARGS__)   \
  F(SourcePointParser, __VA_ARGS__) \
  F(S2ShapeParser, __VA_ARGS__)     \
  F(S2PointParser, __VA_ARGS__)

#define IRS_GEO_DISTANCE_ACCEPTORS(F, ...)                                \
  IRS_GEO_PARSERS(F, __VA_ARGS__, GeoDistanceAcceptor<false>)             \
  IRS_GEO_PARSERS(F, __VA_ARGS__, GeoDistanceAcceptor<true>)              \
  IRS_GEO_PARSERS(F, __VA_ARGS__, GeoDistanceRangeAcceptor<false, false>) \
  IRS_GEO_PARSERS(F, __VA_ARGS__, GeoDistanceRangeAcceptor<false, true>)  \
  IRS_GEO_PARSERS(F, __VA_ARGS__, GeoDistanceRangeAcceptor<true, false>)  \
  IRS_GEO_PARSERS(F, __VA_ARGS__, GeoDistanceRangeAcceptor<true, true>)

#define IRS_MAKE_GEO_DOCS(Parser, Family, ...) \
  IRS_MAKE_DOCS(Family, GeoKind, GeoQuery<Parser, __VA_ARGS__>);

#define IRS_MAKE_GEO_SCORED(Parser, Family, ...) \
  IRS_MAKE_SCORED(Family, GeoKind, GeoQuery<Parser, __VA_ARGS__>);
