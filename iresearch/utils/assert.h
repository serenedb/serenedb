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

#include <source_location>

#include "iresearch/utils/crash_handler.h"

#define SDB_VERIFY(expr, ...)                                                \
  do {                                                                       \
    if (!(expr)) [[unlikely]] {                                              \
      irs::CrashHandler::assertionFailure(std::source_location::current(),   \
                                          #expr, absl::StrCat(__VA_ARGS__)); \
    }                                                                        \
  } while (false)

#ifdef SDB_DEV

#define SDB_ASSERT(expr, ...) /*GCOVR_EXCL_LINE*/ \
  SDB_VERIFY((expr), __VA_ARGS__)

#else

#define SDB_ASSERT(expr, ...) /*GCOVR_EXCL_LINE*/           \
  do {                                                      \
    static_cast<void>(sizeof((expr)));                      \
    static_cast<void>(sizeof((absl::StrCat(__VA_ARGS__)))); \
  } while (false)

#endif
