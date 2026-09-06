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

#include <duckdb/common/types/string_type.hpp>
#include <string_view>

namespace irs {

// Term encodings for typed columns: ingest gathers these directly into
// keyword/constant blocks, no tokenizer involved.
constexpr std::string_view kTrueTerm{"\xFF", 1};
constexpr std::string_view kFalseTerm{"\x00", 1};

constexpr std::string_view BooleanTerm(bool value) noexcept {
  return value ? kTrueTerm : kFalseTerm;
}

inline duckdb::string_t BoolTerm(bool value) noexcept {
  const auto term = BooleanTerm(value);
  return {term.data(), static_cast<uint32_t>(term.size())};
}

// data pointer != nullptr or IRS_ASSERT failure in bytes_hash::insert(...)
constexpr std::string_view kNullTerm{"\x00", 0};

inline duckdb::string_t NullTerm() noexcept {
  return {kNullTerm.data(), static_cast<uint32_t>(kNullTerm.size())};
}

}  // namespace irs
