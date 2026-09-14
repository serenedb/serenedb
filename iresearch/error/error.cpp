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

#include "error.hpp"

#include "iresearch/utils/shared.hpp"

namespace irs {

const char* ErrorBase::what() const noexcept {
  return "An unspecified error has occured.";
}

const char* NotSupported::what() const noexcept {
  return "Operation not supported.";
}

LockObtainFailed::LockObtainFailed(std::string_view filename /*= "" */
                                   )
  : _error("Lock obtain timed out") {
  if (IsNull(filename)) {
    _error += ".";
  } else {
    _error += ": ";
    _error += filename;
  }
}

const char* LockObtainFailed::what() const noexcept { return _error.c_str(); }

FileNotFound::FileNotFound(std::string_view filename /*= "" */
                           )
  : _error("File not found") {
  if (IsNull(filename)) {
    _error += ".";
  } else {
    _error += ": ";
    _error.append(filename);
  }
}

const char* FileNotFound::what() const noexcept { return _error.c_str(); }

const char* IndexNotFound::what() const noexcept {
  return "No segments* file found.";
}

const char* NotImplError::what() const noexcept { return "Not implemented."; }

}  // namespace irs
