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

#include <type_traits>

namespace irs {
namespace fill {

class Erased;

}  // namespace fill
namespace lead {

class Erased;

}  // namespace lead
namespace probe {

class Erased;

}  // namespace probe
namespace search {

template<typename T>
inline constexpr bool kIsErased =
  std::is_same_v<T, fill::Erased> || std::is_same_v<T, lead::Erased> ||
  std::is_same_v<T, probe::Erased>;

}  // namespace search
}  // namespace irs
