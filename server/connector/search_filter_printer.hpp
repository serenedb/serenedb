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

#include <iresearch/search/filter.hpp>
#include <ostream>
#include <string>
#include <string_view>

namespace irs {

// Decodes a binary-encoded field name (uint64 column id + mangle byte)
// into a human-readable string.
std::string FieldToString(std::string_view field);

// Prints filter with raw bytes for field names (identity transform).
std::string ToString(const Filter& f);

// Prints filter with decoded field names (uses FieldToString).
std::string ToStringDemangled(const Filter& f);

template<typename Sink>
void AbslStringify(Sink& sink, const Filter& filter) {
  sink.Append(ToString(filter));
}

inline std::ostream& operator<<(std::ostream& os, const Filter& filter) {
  return os << ToString(filter);
}

}  // namespace irs
