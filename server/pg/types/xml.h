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
#include <duckdb/function/cast/cast_function_set.hpp>
#include <duckdb/main/client_context.hpp>
#include <iresearch/utils/pg/sql_error.hpp>
#include <optional>
#include <string>
#include <string_view>

#include "pg/types/function.h"

namespace sdb::pg {

enum class XmlOption : uint8_t {
  Content,
  Document,
};

XmlOption CurrentXmlOption(duckdb::ClientContext& context);

std::optional<irs::pg::SqlErrorData> CheckXml(std::string_view text,
                                              XmlOption option);

struct XmlText {
  std::string declaration;
  std::string_view body;
};

XmlText XmlOut(std::string_view xml);

void RegisterXml(duckdb::CastFunctionSet& casts, Functions& functions);

}  // namespace sdb::pg
