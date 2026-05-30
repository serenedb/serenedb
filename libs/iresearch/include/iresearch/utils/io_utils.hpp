////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/strings/str_cat.h>

#include "basics/logger/logger.h"
#include "basics/memory.hpp"
#include "iresearch/utils/string.hpp"

#define IRS_MAKE_IO_PTR(method)                                                \
  template<typename T>                                                         \
  struct Auto##method : std::default_delete<T> {                               \
    using ptr = std::unique_ptr<T, Auto##method<T>>;                           \
    void operator()(T* p) const noexcept {                                     \
      try {                                                                    \
        p->method();                                                           \
        std::default_delete<T>::operator()(p);                                 \
      } catch (const std::exception& e) {                                      \
        SDB_ERROR(IRESEARCH,                             \
                  absl::StrCat("caught exception while closing i/o stream: '", \
                               e.what(), "'"));                                \
      } catch (...) {                                                          \
        SDB_ERROR(IRESEARCH,                             \
                  "caught an unspecified exception while closing i/o stream"); \
      }                                                                        \
    }                                                                          \
  }

namespace irs::io_utils {

IRS_MAKE_IO_PTR(Close);
IRS_MAKE_IO_PTR(unlock);

}  // namespace irs::io_utils

#define IRS_USING_IO_PTR(type, method) \
  using ptr = irs::io_utils::Auto##method<type>::ptr
