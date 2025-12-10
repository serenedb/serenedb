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

#include <span>

#include "rest_server/serened.h"

namespace sdb {

class InitDatabaseFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "InitDatabase"; }

  InitDatabaseFeature(Server& server,
                      std::span<const size_t> non_server_features);

  const std::string& defaultPassword() const { return _password; }
  bool isInitDatabase() const { return _init_database; }
  bool restoreAdmin() const { return _restore_admin; }

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() final;

 private:
  void checkEmptyDatabase();
  std::string readPassword(const std::string&);

  bool _seen_password = false;
  bool _init_database = false;
  bool _restore_admin = false;
  std::string _password;
  std::span<const size_t> _non_server_features;
};

}  // namespace sdb
