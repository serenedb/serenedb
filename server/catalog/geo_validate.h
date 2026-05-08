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

#include <duckdb/common/types/geometry_crs.hpp>
#include <string_view>

#include "basics/errors.h"
#include "basics/result.h"

namespace sdb::catalog {

// CRS84 covers GeoJSON / S2 / our index encoding. Compare by identifier
// rather than attempting semantic CRS equivalence (which would need a
// PROJ-style library): PROJJSON / WKT2 CRS84 definitions that don't hand
// us a matching short identifier are rejected -- users should declare
// with the short form.
inline bool IsCRS84Identifier(std::string_view id) noexcept {
  return id == "OGC:CRS84" || id == "EPSG:4326" || id == "4326";
}

// Validate that a GEOMETRY-typed value or column declares a CRS84-compatible
// CRS. The error message is intentionally subject-free ("GEOMETRY type ...")
// so callers can prepend their own context (e.g. "Column 'foo': ...",
// "Centroid argument: ..."). Single point of truth for the CRS contract:
// future enhancements (proper PROJJSON-aware comparison, override knobs,
// etc.) live here.
inline Result ValidateGeometryCRS84(const duckdb::LogicalType& type) noexcept {
  if (!duckdb::GeoType::HasCRS(type)) {
    return {ERROR_BAD_PARAMETER,
            "GEOMETRY type has no CRS attached; declare it with "
            "::GEOMETRY('OGC:CRS84')"};
  }
  const auto& crs = duckdb::GeoType::GetCRS(type);
  if (!IsCRS84Identifier(crs.GetIdentifier())) {
    return {ERROR_BAD_PARAMETER, "GEOMETRY type has invalid CRS '",
            crs.GetIdentifier(),
            "'; only CRS84 is supported (EPSG:4326, OGC:CRS84, 4326)"};
  }
  return {};
}

}  // namespace sdb::catalog
