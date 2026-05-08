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

#include "pg/geo_tokenizer_options.h"

#include <iresearch/analysis/geo_analyzer.hpp>

#include "magic_enum/magic_enum.hpp"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg::tokenizer_options {

void CheckGeoJsonType(std::string_view option, std::string_view value) {
  using Type = irs::analysis::GeoJsonAnalyzer::Type;
  if (!magic_enum::enum_cast<Type>(value, magic_enum::case_insensitive)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid value in \"", option, "\" parameter"),
                    ERR_HINT(kGeoJsonType.description));
  }
}

void CheckGeoJsonCoding(std::string_view option, std::string_view value) {
  using Coding = irs::analysis::GeoJsonAnalyzer::Coding;
  // The iresearch analyzer still implements VPack for tests/internal use, but
  // SereneDB doesn't expose it: VPack stores the original GeoJSON text which
  // GEOMETRY columns don't carry, and on JSON columns has strings not VPACKs.
  // So VPACK is useless at it does not gives "sore what we index" property and
  // takes more space that S2 encodings.
  const auto coding =
    magic_enum::enum_cast<Coding>(value, magic_enum::case_insensitive);
  if (!coding || *coding == Coding::VPack) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid value in \"", option, "\" parameter"),
                    ERR_HINT(kGeoJsonCoding.description));
  }
}

}  // namespace sdb::pg::tokenizer_options
