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

#include <duckdb/common/enums/compression_type.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/function/compression_function.hpp>
#include <optional>

#include "iresearch/formats/column/codecs/string_choice.hpp"

namespace irs::codecs {

struct ColCodecs {
  static const duckdb::CompressionFunction* Get(duckdb::CompressionType type,
                                                duckdb::PhysicalType physical);

  static std::optional<StringChoice> Choice(duckdb::CompressionType type);

  static duckdb::CompressionType TypeOf(StringChoice choice) noexcept;
};

}  // namespace irs::codecs
