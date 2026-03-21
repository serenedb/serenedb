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

#include <sys/types.h>

#include <memory>
#include <string>

#include "basics/operating-system.h"
#include "rest_server/serened.h"

namespace sdb {
namespace options {

class ProgramOptions;
}

class PrivilegeFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Privilege"; }

  explicit PrivilegeFeature(Server& server);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() final;

  std::string _uid;  // NOLINT
  std::string _gid;  // NOLINT

  void dropPrivilegesPermanently();

 private:
  void extractPrivileges();

#ifdef SERENEDB_HAVE_SETUID
  uid_t _numeric_uid{};
#endif
#ifdef SERENEDB_HAVE_SETGID
  gid_t _numeric_gid{};
#endif
};

}  // namespace sdb
