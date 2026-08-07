////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <cstring>

#include "string.hpp"

namespace irs {

enum class WildcardType {
  TermEscaped = 0,  // f\%o
  Term,             // foo
  PrefixEscaped,    // fo\%
  Prefix,           // foo%
  Wildcard,         // f_o%
};

WildcardType ComputeWildcardType(bytes_view pattern) noexcept;

enum WildcardMatch : uint8_t {
  kAnyStr = '%',   // match any number of arbitrary characters
  kAnyChr = '_',   // match a single arbitrary character
  kEscape = '\\',  // escape control symbol, e.g. "\%" issues literal "%"
};

}  // namespace irs
