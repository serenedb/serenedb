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

#include "app/options/program_options.h"
#include "basics/debugging.h"
#include "catalog/identifiers/server_id.h"
#include "rest_server/serened.h"

namespace sdb {

class ServerIdFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "ServerId"; }

  explicit ServerIdFeature(Server& server);
  ~ServerIdFeature() final;

  void start() final;

  bool GetIsInitiallyEmpty() const { return _is_initially_empty; }

  static ServerId GetId() {
    SDB_ASSERT(gServerid.isSet());
    return gServerid;
  }

  // fake the server id from the outside. used for testing only
  static void SetId(ServerId server_id) { gServerid = server_id; }

 private:
  /// generates a new server id
  void generateId();

  /// reads server id from file
  ErrorCode readId();

  /// writes server id to file
  ErrorCode writeId();

  /// read / create the server id on startup
  ErrorCode determineId(bool check_version);

  std::string _id_filename;

  bool _is_initially_empty = false;

  static ServerId gServerid;
};

}  // namespace sdb
