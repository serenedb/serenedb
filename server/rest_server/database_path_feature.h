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

#include "rest_server/serened.h"

namespace sdb {

class DatabasePathFeature final {
 public:
  static constexpr std::string_view name() { return "DatabasePath"; }

  inline static DatabasePathFeature* gInstance = nullptr;
  static DatabasePathFeature& instance() noexcept { return *gInstance; }

  DatabasePathFeature();
  ~DatabasePathFeature();

  void start();
  void stop() {}

  const std::string& directory() const { return _directory; }
  std::string subdirectoryName(std::string_view sub_directory) const;
  void setDirectory(const std::string& path) {
    // This is only needed in the catch tests, where we initialize the
    // feature with no CLI parsed (and therefore no flag-derived
    // _directory). Please do not use it from other code.
    _directory = path;
  }

 private:
  std::string _directory;
};

}  // namespace sdb
