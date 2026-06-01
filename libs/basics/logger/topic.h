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

// Topic constants for SDB_LOG / SDB_INFO / etc. The SDB_* macros prepend
// `::sdb::log::` to the topic argument, so call sites write
// `SDB_INFO(GENERAL, "...")` and get `::sdb::log::GENERAL` resolved at
// expansion. Each constant is a `constexpr inline std::string_view`; the
// runtime entry point takes it by string_view.
//
// String values map to DuckDB log type names. They are PascalCase because
// DuckDB matches enabled/disabled_log_types CASE-SENSITIVELY. `GENERAL` maps
// to the empty string -- DuckDB's `DefaultLogType` -- so plain SDB_INFO()
// stays in the unfiltered default bucket.
//
// INVARIANT: every constant below MUST be initialized from a string literal
// so that `.data()` returns a NUL-terminated pointer. ShouldLog() / Write()
// in log_types.cpp pass topic.data() directly to duckdb APIs that expect a
// null-terminated string -- never construct one of these from a non-literal
// source.

#include <string_view>

namespace sdb::log {

inline constexpr std::string_view GENERAL{""};
inline constexpr std::string_view STARTUP{"Startup"};
inline constexpr std::string_view HTTP{"HTTP"};
inline constexpr std::string_view SSL{"SSL"};
inline constexpr std::string_view STORAGE{"Storage"};
inline constexpr std::string_view SEARCH{"Search"};
inline constexpr std::string_view IRESEARCH{"IResearch"};

}  // namespace sdb::log
