////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/tokenizer_options.h"

#include <filesystem>
#include <iresearch/analysis/tokenizer.hpp>

#include "magic_enum/magic_enum.hpp"
#include "pg/sql_exception_macro.h"

LIBPG_QUERY_INCLUDES_BEGIN
#include "postgres.h"
LIBPG_QUERY_INCLUDES_END

namespace sdb::pg::tokenizer_options {

void CheckFileExists(std::string_view name) {
  if (!std::filesystem::exists(name)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("File \"", name, "\" does not exist"));
  }
}

void CheckCase(std::string_view value) {
  if (!magic_enum::enum_cast<irs::Case>(value, magic_enum::case_insensitive)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid value in \"", kCase.name, "\" parameter"),
                    ERR_HINT(kCase.description));
  }
}

void CheckThreshold(double value) {
  if (value < 0. || value > 1.) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("invalid value in \"", kThreshold.name,
                            "\" parameter. Should be in [0, 1]"),
                    ERR_HINT(kThreshold.description));
  }
}

void CheckTemplate(std::string_view value) {
  for (const auto& group : kTokenizerSubgroups) {
    if (group.name == value) {
      return;
    }
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("Invalid type of text search dictionary"));
}

void CheckNumHashes(int value) {
  if (value <= 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("The number of hashes should be positive number"));
  }
}

}  // namespace sdb::pg::tokenizer_options
