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

#include <string>
#include <vector>

#include "basics/common.h"

namespace sdb {

class GlobalContext {
 public:
  static GlobalContext* gContext;

 public:
  GlobalContext(int argc, char* argv[], const char* install_directory);
  ~GlobalContext();

 public:
  const std::string& binaryName() const { return _binary_name; }
  const std::string& runRoot() const { return _run_root; }
  void normalizePath(std::vector<std::string>& path, const char* which_path,
                     bool fatal);
  void normalizePath(std::string& path, const char* which_path, bool fatal);
  const std::string& getBinaryPath() const { return _binary_path; }
  int exit(int ret);
  void installHup();

 private:
  const std::string _binary_name;
  const std::string _binary_path;
  const std::string _run_root;
  int _ret;
};

}  // namespace sdb
