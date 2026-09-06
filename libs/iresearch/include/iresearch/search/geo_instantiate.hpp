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

#include "iresearch/search/geo_filter.hpp"
#include "iresearch/search/geo_parsers.hpp"

#define IRS_GEO_PARSERS(F, ...)     \
  F(SourceJsonParser, __VA_ARGS__)  \
  F(SourceWkbParser, __VA_ARGS__)   \
  F(SourcePointParser, __VA_ARGS__) \
  F(S2ShapeParser, __VA_ARGS__)     \
  F(S2PointParser, __VA_ARGS__)

#define IRS_GEO_DISTANCE_ACCEPTORS(F)                        \
  IRS_GEO_PARSERS(F, GeoDistanceAcceptor<false>)             \
  IRS_GEO_PARSERS(F, GeoDistanceAcceptor<true>)              \
  IRS_GEO_PARSERS(F, GeoDistanceRangeAcceptor<false, false>) \
  IRS_GEO_PARSERS(F, GeoDistanceRangeAcceptor<false, true>)  \
  IRS_GEO_PARSERS(F, GeoDistanceRangeAcceptor<true, false>)  \
  IRS_GEO_PARSERS(F, GeoDistanceRangeAcceptor<true, true>)
