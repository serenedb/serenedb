////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "basics/file_result.h"
#include "basics/result.h"

namespace sdb {

class FileResultString : public FileResult {
 public:
  explicit FileResultString(const std::string& result)
    : FileResult(), _message(result) {}

  FileResultString(int sys_error_number, const std::string& result)
    : FileResult(sys_error_number), _message(result) {}

  explicit FileResultString(int sys_error_number)
    : FileResult(sys_error_number), _message() {}

 public:
  const std::string& result() const { return _message; }

 protected:
  const std::string _message;
};

}  // namespace sdb
