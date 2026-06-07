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

#pragma once

#include <functional>
#include <iresearch/search/filter.hpp>
#include <ostream>
#include <string>
#include <string_view>

#include "catalog/table_options.h"

namespace irs {

// Decodes a binary-encoded field name (uint64 column id + mangle byte)
// into a human-readable string.
std::string FieldToString(std::string_view field);

// Prints filter with raw bytes for field names (identity transform).
std::string ToString(const Filter& f);

// Prints filter resolving each iresearch `field_id` via the caller-supplied
// callback. The id may be a column's main field_id, a per-kind sub-field id
// (numeric/bool/null), or a synthetic expression id -- the caller does the
// catalog lookup and typically returns "name(kind)".
std::string ToStringDemangled(
  const Filter& f, const std::function<std::string(field_id)>& field_to_string);

template<typename Sink>
void AbslStringify(Sink& sink, const Filter& filter) {
  sink.Append(ToString(filter));
}

}  // namespace irs
