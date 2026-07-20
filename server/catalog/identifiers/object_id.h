////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "basics/identifier.h"

namespace sdb {

class ObjectId : public basics::Identifier {
 public:
  using Identifier::Identifier;

  static constexpr ObjectId none() { return ObjectId{0}; }

  bool isSet() const { return id() != 0; }
};

static_assert(sizeof(ObjectId) == sizeof(ObjectId::BaseType));

namespace id {  // system IDs

inline constexpr ObjectId kInvalid{};
inline constexpr ObjectId kMinSystem{1'000'000};
inline constexpr ObjectId kMaxSystem{2'000'000};
inline constexpr ObjectId kRootUser{kMinSystem};

// Database IDs
inline constexpr ObjectId kInstance{kMinSystem.id() + 4};
inline constexpr ObjectId kSystemDB{kMinSystem.id() + 2};
inline constexpr ObjectId kTombstoneDatabase{kMinSystem.id() + 1};

// Schema IDs
inline constexpr ObjectId kPgCatalogSchema{11};
inline constexpr ObjectId kPgInformationSchema{kMinSystem.id() + 3};

// Type IDs
inline constexpr ObjectId kVariant{kMinSystem.id() + 100};
inline constexpr ObjectId kVariantArray{kMinSystem.id() + 101};

// OpClasses
inline constexpr ObjectId kPgOpclassIvf{kMinSystem.id() + 200};
inline constexpr ObjectId kPgOpclassIncluded{kMinSystem.id() + 201};

// Access methods
inline constexpr ObjectId kPgAmInverted{kMinSystem.id() + 300};
inline constexpr ObjectId kPgAmIresearch{kMinSystem.id() + 301};
inline constexpr ObjectId kPgAmSecondary{kMinSystem.id() + 303};

// Predefined (bootstrap) roles -- NOLOGIN/INHERIT, seeded on startup, fixed ids
// so membership edges persist. Enforced via RoleClosure where serened has the
// matching gate (read/write_all_data, maintain, read/write_server_files);
// others are present for GRANT/membership parity + the pg_monitor chain.
inline constexpr ObjectId kPgReadAllData{kMinSystem.id() + 400};
inline constexpr ObjectId kPgWriteAllData{kMinSystem.id() + 401};
inline constexpr ObjectId kPgReadAllSettings{kMinSystem.id() + 402};
inline constexpr ObjectId kPgReadAllStats{kMinSystem.id() + 403};
inline constexpr ObjectId kPgStatScanTables{kMinSystem.id() + 404};
inline constexpr ObjectId kPgMonitor{kMinSystem.id() + 405};
inline constexpr ObjectId kPgDatabaseOwner{kMinSystem.id() + 406};
inline constexpr ObjectId kPgSignalBackend{kMinSystem.id() + 407};
inline constexpr ObjectId kPgReadServerFiles{kMinSystem.id() + 408};
inline constexpr ObjectId kPgWriteServerFiles{kMinSystem.id() + 409};
inline constexpr ObjectId kPgExecuteServerProgram{kMinSystem.id() + 410};
inline constexpr ObjectId kPgCheckpoint{kMinSystem.id() + 411};
inline constexpr ObjectId kPgMaintain{kMinSystem.id() + 412};
inline constexpr ObjectId kPgUseReservedConnections{kMinSystem.id() + 413};
inline constexpr ObjectId kPgCreateSubscription{kMinSystem.id() + 414};
inline constexpr ObjectId kPgSignalAutovacuumWorker{kMinSystem.id() + 415};

}  // namespace id
}  // namespace sdb
