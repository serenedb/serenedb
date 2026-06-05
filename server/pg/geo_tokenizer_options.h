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

#include <iresearch/analysis/geo_analyzer.hpp>

#include "pg/option_help.h"

namespace sdb::pg::tokenizer_options {

using namespace std::string_view_literals;

// Geo (shared S2 options)

inline constexpr OptionInfo kGeoMaxCells{"maxcells", 20,
                                         "Maximum number of S2 cells"};
inline constexpr OptionInfo kGeoMinLevel{"minlevel", 4,
                                         "Minimum S2 cell level (0-30)"};
inline constexpr OptionInfo kGeoMaxLevel{
  "maxlevel", 23, "Maximum S2 cell level (0-30), ~1m precision at level 23"};
inline constexpr OptionInfo kGeoLevelMod{"levelmod", 1,
                                         "S2 level modifier (1, 2, or 3)"};
inline constexpr OptionInfo kGeoOptimizeForSpace{
  "optimizeforspace", false, "Optimize S2 index for space rather than speed"};

// GeoPoint

inline constexpr OptionInfo kGeoLatitude{
  "latitude", ""sv,
  "Slash-separated path to latitude field (e.g., 'lat' or 'loc/lat'); "
  "empty (the default) treats the indexed JSON value as a [lat, lng] "
  "array directly. latitude and longitude must both be set or both left "
  "empty."};
inline constexpr OptionInfo kGeoLongitude{
  "longitude", ""sv,
  "Slash-separated path to longitude field (e.g., 'lng' or 'loc/lng'); "
  "see latitude for empty-default semantics."};

// GeoJson

void CheckGeoJsonType(std::string_view option, std::string_view value);
void CheckGeoJsonCoding(std::string_view option, std::string_view value);
inline constexpr OptionInfo kGeoJsonType{
  "type", "shape"sv, "GeoJson shape type: shape, centroid, point",
  CheckGeoJsonType};
inline constexpr OptionInfo kGeoJsonCoding{
  "coding", "source"sv,
  "Encoding format: source, s2point, s2latlngf64, s2latlngu32",
  CheckGeoJsonCoding};

inline constexpr OptionInfo kGeoS2Options[] = {
  kGeoMaxCells, kGeoMinLevel, kGeoMaxLevel, kGeoLevelMod, kGeoOptimizeForSpace};

inline constexpr OptionInfo kGeoPointOptions[] = {kGeoLatitude, kGeoLongitude};

inline constexpr OptionInfo kGeoJsonOptions[] = {kGeoJsonType, kGeoJsonCoding};

inline constexpr OptionGroup kGeoS2Subgroup{"options", kGeoS2Options, {}};
inline constexpr OptionGroup kGeoPointSubgroups[] = {kGeoS2Subgroup};
inline constexpr OptionGroup kGeoJsonSubgroups[] = {kGeoS2Subgroup};
inline constexpr OptionGroup kGeoPointGroup{
  irs::analysis::GeoPointAnalyzer::type_name(),
  kGeoPointOptions,
  kGeoPointSubgroups,
};
inline constexpr OptionGroup kGeoJsonGroup{
  irs::analysis::GeoJsonAnalyzer::type_name(),
  kGeoJsonOptions,
  kGeoJsonSubgroups,
};

}  // namespace sdb::pg::tokenizer_options
