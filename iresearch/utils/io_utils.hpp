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

#include "iresearch/utils/log.hpp"
#include "iresearch/utils/memory.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::io_utils {

template<typename T>
struct AutoClose : std::default_delete<T> {
  using ptr = std::unique_ptr<T, AutoClose<T>>;

  void operator()(T* p) const noexcept {
    try {
      p->Close();
    } catch (const std::exception& e) {
      SDB_ERROR(IRESEARCH,
                absl::StrCat("caught exception while closing i/o stream: '",
                             e.what(), "'"));
    } catch (...) {
      SDB_ERROR(IRESEARCH,
                "caught an unspecified exception while closing i/o stream");
    }
    std::default_delete<T>::operator()(p);
  }
};

}  // namespace irs::io_utils
