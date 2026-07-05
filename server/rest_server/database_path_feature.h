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
#include <string_view>

namespace sdb {

class DatabasePathFeature final {
 public:
  inline static DatabasePathFeature* gInstance = nullptr;
  static DatabasePathFeature& instance() noexcept { return *gInstance; }

  DatabasePathFeature();
  ~DatabasePathFeature();

  const std::string& directory() const { return _directory; }
  std::string subdirectoryName(std::string_view sub_directory) const;

  const std::string& hbaConfigFile() const { return _hba_config_file; }

 private:
  std::string _directory;
  std::string _hba_config_file;
};

}  // namespace sdb
