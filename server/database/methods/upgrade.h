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

#include "basics/result.h"
#include "database/methods/version.h"

namespace sdb {

class UpgradeFeature;

namespace catalog {

class Database;
}

namespace methods {

struct UpgradeResult {
  Result result;
  VersionResult::StatusCode type = VersionResult::kInvalid;
};

struct Upgrade {
  friend class sdb::UpgradeFeature;

  enum Flags : uint32_t {
    kDatabaseSystem = (1u << 0),
    kDatabaseAll = (1u << 1),
    kDatabaseExceptSystem = (1u << 2),
    // =============
    kDatabaseInit = (1u << 3),
    kDatabaseUpgrade = (1u << 4),
    kDatabaseExisting = (1u << 5),
    kDatabaseOnlyOnce = (1u << 6),  // hint that task should be run on
                                    // database only once. New databases
                                    // should assume this task as executed
    // =============
    kClusterNone = (1u << 7),
    kClusterLocal = (1u << 8),  // agency
    kClusterCoordinatorGlobal = (1u << 9),
    kClusterDbServerLocal = (1u << 10)
  };

  using TaskFunction = std::function<Result(catalog::Database&, vpack::Slice)>;

  struct Task {
    std::string name;
    std::string description;
    uint32_t system_flag;
    uint32_t cluster_flags;
    uint32_t database_flags;
    TaskFunction action;
  };

 public:
  static UpgradeResult clusterBootstrap(catalog::Database& system);

  static UpgradeResult startup(catalog::Database& database, bool upgrade,
                               bool ignore_file_errors);

  static UpgradeResult startupCoordinator(catalog::Database& database);

 private:
  // register tasks, only run once on startup
  static void registerTasks(UpgradeFeature&);

  static UpgradeResult runTasks(catalog::Database& database,
                                VersionResult& vinfo, vpack::Slice params,
                                uint32_t cluster_flag, uint32_t db_flag);
};

}  // namespace methods
}  // namespace sdb
