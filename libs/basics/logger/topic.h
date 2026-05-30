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
//
// Topic constants for SDB_LOG / SDB_INFO / etc. The SDB_* macros prepend
// `::sdb::log::` to the topic argument, so call sites write
// `SDB_INFO(GENERAL, "...")` and get `::sdb::log::GENERAL` resolved at
// expansion. Each constant is a `constexpr inline std::string_view`; the
// runtime entry point takes it by string_view.

#pragma once

#include <string_view>

namespace sdb::log {

inline constexpr std::string_view GENERAL{"general"};
inline constexpr std::string_view STARTUP{"startup"};
inline constexpr std::string_view HTTP{"http"};
inline constexpr std::string_view SSL{"ssl"};
inline constexpr std::string_view STORAGE{"storage"};
inline constexpr std::string_view SEARCH{"search"};
inline constexpr std::string_view IRESEARCH{"iresearch"};
inline constexpr std::string_view CRASH{"crash"};

}  // namespace sdb::log
