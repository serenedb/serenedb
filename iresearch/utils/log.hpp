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

#include <absl/strings/str_cat.h>

#include <duckdb/logging/logging.hpp>
#include <string>
#include <string_view>

#include "iresearch/utils/application_exit.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/topic.hpp"

namespace duckdb {

class Logger;

}  // namespace duckdb
namespace irs::log {

void SetLogger(duckdb::Logger* logger) noexcept;

void Log(duckdb::LogLevel level, std::string_view topic,
         const std::string& message) noexcept;

IRS_NO_INLINE void LogCrash(std::string_view message) noexcept;

bool IsEnabled(duckdb::LogLevel level, std::string_view topic) noexcept;

}  // namespace irs::log

#define SDB_LOG_INTERNAL(LEVEL, TOPIC, ...)                    \
  do {                                                         \
    constexpr ::duckdb::LogLevel kSdbLevel = (LEVEL);          \
    if (::irs::log::IsEnabled(kSdbLevel, ::irs::log::TOPIC)) { \
      ::irs::log::Log(kSdbLevel, ::irs::log::TOPIC,            \
                      ::absl::StrCat(__VA_ARGS__));            \
    }                                                          \
  } while (0)

#define SDB_LOG_INTERNAL_IF(LEVEL, TOPIC, COND, ...) \
  do {                                               \
    if ((COND)) {                                    \
      SDB_LOG_INTERNAL(LEVEL, TOPIC, __VA_ARGS__);   \
    }                                                \
  } while (0)

#define SDB_TRACE(TOPIC, ...) \
  SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_TRACE, TOPIC, __VA_ARGS__)
#define SDB_DEBUG(TOPIC, ...) \
  SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_DEBUG, TOPIC, __VA_ARGS__)
#define SDB_INFO(TOPIC, ...) \
  SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_INFO, TOPIC, __VA_ARGS__)
#define SDB_WARN(TOPIC, ...) \
  SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_WARNING, TOPIC, __VA_ARGS__)
#define SDB_ERROR(TOPIC, ...) \
  SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_ERROR, TOPIC, __VA_ARGS__)

#define SDB_FATAL(TOPIC, ...)                                            \
  do {                                                                   \
    SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_FATAL, TOPIC, __VA_ARGS__); \
    ::irs::FatalErrorExit();                                             \
  } while (0)

#define SDB_TRACE_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(::duckdb::LogLevel::LOG_TRACE, TOPIC, COND, __VA_ARGS__)
#define SDB_DEBUG_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(::duckdb::LogLevel::LOG_DEBUG, TOPIC, COND, __VA_ARGS__)
#define SDB_INFO_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(::duckdb::LogLevel::LOG_INFO, TOPIC, COND, __VA_ARGS__)
#define SDB_WARN_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(::duckdb::LogLevel::LOG_WARNING, TOPIC, COND, __VA_ARGS__)
#define SDB_ERROR_IF(TOPIC, COND, ...) \
  SDB_LOG_INTERNAL_IF(::duckdb::LogLevel::LOG_ERROR, TOPIC, COND, __VA_ARGS__)
#define SDB_FATAL_IF(TOPIC, COND, ...)                                     \
  do {                                                                     \
    if ((COND)) {                                                          \
      SDB_LOG_INTERNAL(::duckdb::LogLevel::LOG_FATAL, TOPIC, __VA_ARGS__); \
      ::irs::FatalErrorExit();                                             \
    }                                                                      \
  } while (0)

#ifdef SDB_DEV
#define SDB_PRINT_LEVEL ::duckdb::LogLevel::LOG_ERROR
#else
#define SDB_PRINT_LEVEL ::duckdb::LogLevel::LOG_TRACE
#endif

#define SDB_PRINT(...) \
  SDB_LOG_INTERNAL(SDB_PRINT_LEVEL, GENERAL, "###### ", __VA_ARGS__)
#define SDB_PRINT_IF(COND, ...) \
  SDB_LOG_INTERNAL_IF(SDB_PRINT_LEVEL, GENERAL, (COND), "###### ", __VA_ARGS__)
