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

#include "general_server/state.h"

#include <absl/strings/internal/ostringstream.h>
#include <vpack/iterator.h>
#include <vpack/vpack_helper.h>

#include <algorithm>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <iomanip>
#include <regex>
#include <utility>

#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "basics/physical_memory.h"
#include "basics/result_or.h"
#include "basics/string_utils.h"
#include "basics/write_locker.h"
#include "database/ticks.h"
#include "rest/common_defines.h"
#include "rest/version.h"
#include "rest_server/database_path_feature.h"
#include "storage_engine/engine_feature.h"

using namespace sdb;
using namespace sdb::basics;

namespace {

// whenever the format of the generated UUIDs changes, please make sure to
// adjust this regex too!
const std::regex kUuidRegex(
  "^(SNGL|CRDN|PRMR|AGNT)-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-"
  "f0-9]{12}$");

ServerState* gInstance = nullptr;

}  // namespace

ServerState::ServerState() {
  SDB_ASSERT(gInstance == nullptr);
  gInstance = this;
  SetRole(Role::Undefined);
}

ServerState* ServerState::instance() noexcept {
  SDB_ASSERT(gInstance);
  return gInstance;
}

bool ServerState::IsCoordinatorId(std::string_view id) {
  // intended to be a cheap validation, and intentionally not using
  return id.starts_with("CRDN-") &&
         std::regex_match(id.begin(), id.end(), ::kUuidRegex);
}

bool ServerState::IsDBServerId(std::string_view id) {
  return id.starts_with("PRMR-") &&
         std::regex_match(id.begin(), id.end(), ::kUuidRegex);
}

std::string ServerState::RoleToStr(Role role) {
  switch (role) {
    case Role::Undefined:
      return "UNDEFINED";
    case Role::Single:
      return "SINGLE";
    case Role::DBServer:
      return "PRIMARY";
    case Role::Coordinator:
      return "COORDINATOR";
    case Role::Agent:
      return "AGENT";
  }

  SDB_ASSERT(false);
  return "";
}

std::string ServerState::RoleToShortStr(Role role) {
  // whenever anything here changes, please make sure to
  // adjust ::uuidRegex too!
  switch (role) {
    case Role::Undefined:
      return "NONE";
    case Role::Single:
      return "SNGL";
    case Role::DBServer:
      return "PRMR";
    case Role::Coordinator:
      return "CRDN";
    case Role::Agent:
      return "AGNT";
  }

  SDB_ASSERT(false);
  return "";
}

ServerState::Role ServerState::StrToRole(std::string_view value) noexcept {
  if (value == "SINGLE") {
    return Role::Single;
  } else if (value == "PRIMARY" || value == "DBSERVER") {
    // note: DBSERVER is an alias for PRIMARY
    // internally and in all API values returned we will still use PRIMARY
    // for compatibility reasons
    return Role::DBServer;
  } else if (value == "COORDINATOR") {
    return Role::Coordinator;
  } else if (value == "AGENT") {
    return Role::Agent;
  }

  return Role::Undefined;
}

std::string ServerState::StateToStr(State state) {
  // TODO MAX: cleanup
  switch (state) {
    case State::Undefined:
      return "UNDEFINED";
    case State::Startup:
      return "STARTUP";
    case State::Serving:
      return "SERVING";
    case State::Shutdown:
      return "SHUTDOWN";
  }

  SDB_ASSERT(false);
  return "";
}

ServerState::State ServerState::StringToState(std::string_view value) noexcept {
  if (value == "STARTUP") {
    return State::Startup;
  } else if (value == "SERVING") {
    return State::Serving;
  } else if (value == "SHUTDOWN") {
    return State::Shutdown;
  }

  return State::Undefined;
}

std::string ServerState::ModeToStr(Mode mode) {
  switch (mode) {
    case Mode::Default:
      return "default";
    case Mode::Startup:
      return "startup";
    case Mode::Maintenance:
      return "maintenance";
    case Mode::Invalid:
      return "invalid";
  }

  SDB_ASSERT(false);
  return "";
}

ServerState::Mode ServerState::StrToMode(std::string_view value) noexcept {
  if (value == "default") {
    return Mode::Default;
  } else if (value == "startup") {
    return Mode::Startup;
  } else if (value == "maintenance") {
    return Mode::Maintenance;
  }
  return Mode::Invalid;
}

ServerState::Mode ServerState::GetMode() noexcept {
  return _mode.load(std::memory_order_acquire);
}

ServerState::Mode ServerState::SetMode(ServerState::Mode value) noexcept {
  if (_mode.load(std::memory_order_acquire) != value) {
    return _mode.exchange(value, std::memory_order_release);
  }
  return value;
}

bool ServerState::IsStartupOrMaintenance() noexcept {
  Mode value = GetMode();
  return value == Mode::Startup || value == Mode::Maintenance;
}

bool ServerState::ReadOnly() noexcept {
  return _read_only.load(std::memory_order_acquire);
}

/// set server read-only
bool ServerState::SetReadOnly(ReadOnlyMode ro) noexcept {
  auto ret = ReadOnly();
  if (ro == ReadOnlyMode::ApiFalse) {
    _read_only.store(false, std::memory_order_release);
  } else if (ro == ReadOnlyMode::ApiTrue) {
    _read_only.store(true, std::memory_order_release);
  }
  return ret;
}

/// whether or not "value" is a server UUID
bool ServerState::IsUUID(std::string_view value) const {
  // whenever the format of the generated UUIDs changes, please make sure to
  // adjust ::uuidRegex too!
  return std::regex_match(value.begin(), value.end(), ::kUuidRegex);
}

std::string ServerState::RoleToAgencyListKey(ServerState::Role role) {
  return RoleToAgencyKey(role) + "s";
}

std::string ServerState::RoleToAgencyKey(ServerState::Role role) {
  switch (role) {
    case Role::DBServer:
      return "DBServer";
    case Role::Coordinator:
      return "Coordinator";
    case Role::Single:
      return "Single";
    case Role::Agent:
      return "Agent";
    case Role::Undefined: {
      return "Undefined";
    }
  }
  return "INVALID_CLUSTER_ROLE";
}

std::string ServerState::GetUUIDFilename() const {
  auto& dbpath = DatabasePathFeature::instance();
  return file_utils::BuildFilename(dbpath.directory(), "UUID");
}

bool ServerState::HasPersistedId() {
  std::string uuid_filename = GetUUIDFilename();
  return file_utils::Exists(uuid_filename);
}

bool ServerState::WritePersistedId(std::string_view id) {
  std::string uuid_filename = GetUUIDFilename();
  // try to create underlying directory
  auto error = ERROR_OK;
  file_utils::CreateDirectory(
    std::string{file_utils::Dirname(uuid_filename)}.c_str(), &error);

  try {
    sdb::basics::file_utils::Spit(uuid_filename, id, true);
  } catch (const sdb::basics::Exception& ex) {
    SDB_FATAL(GENERAL, "Cannot write UUID file '", uuid_filename,
              "': ", ex.what());
  }

  return true;
}

std::string ServerState::GeneratePersistedId(Role role) {
  // whenever the format of the generated UUID changes, please make sure to
  // adjust ::uuidRegex too!
  std::string id =
    RoleToShortStr(role) + "-" + to_string(boost::uuids::random_generator()());
  WritePersistedId(id);
  return id;
}

std::string ServerState::GetShortName() const {
  if (_role == Role::Agent) {
    return GetId().substr(0, 13);
  }
  auto num = GetShortId();
  if (num == 0) {
    return std::string{};  // not yet known
  }
  size_t width = std::max(std::to_string(num).size(), static_cast<size_t>(4));
  std::string ss_str;
  absl::strings_internal::OStringStream ss{&ss_str};  // ShortName
  ss << RoleToAgencyKey(GetRole()) << std::setw(width) << std::setfill('0')
     << num;
  return ss_str;
}

void ServerState::SetRole(ServerState::Role role) {
  _role.store(role, std::memory_order_release);
}

std::string ServerState::GetId() const {
  std::lock_guard guard(_id_lock);
  return _id;
}

void ServerState::SetId(std::string_view id) {
  if (!id.empty()) {
    std::lock_guard guard(_id_lock);
    _id = id;
  }
}

uint32_t ServerState::GetShortId() const {
  return _short_id.load(std::memory_order_relaxed);
}

void ServerState::SetShortId(uint32_t id) {
  if (id != 0) {
    _short_id.store(id, std::memory_order_relaxed);
  }
}

ServerState::State ServerState::GetState() {
  absl::ReaderMutexLock read_locker{&_lock};
  return _state;
}

void ServerState::SetState(State state) {
  bool result = false;

  absl::WriterMutexLock write_locker{&_lock};

  if (state == _state) {
    return;
  }

  auto role = GetRole();
  if (role == Role::DBServer) {
    result = CheckPrimaryState(state);
  } else if (role == Role::Coordinator) {
    result = CheckCoordinatorState(state);
  } else if (role == Role::Single) {
    result = true;
  }

  if (result) {
    SDB_DEBUG(STARTUP, "changing state of ", ServerState::RoleToStr(role),
              " server from ", ServerState::StateToStr(_state), " to ",
              ServerState::StateToStr(state));

    _state = state;
  } else {
    SDB_ERROR(STARTUP, "invalid state transition for ",
              ServerState::RoleToStr(role), " server from ",
              ServerState::StateToStr(_state), " to ",
              ServerState::StateToStr(state));
  }
}

bool ServerState::CheckPrimaryState(State state) {
  if (state == State::Startup) {
    // startup state can only be set once
    return (_state == State::Undefined);
  } else if (state == State::Serving) {
    return (_state == State::Startup);
  } else if (state == State::Shutdown) {
    return (_state == State::Startup || _state == State::Serving);
  }

  // anything else is invalid
  return false;
}

bool ServerState::CheckCoordinatorState(State state) {
  if (state == State::Startup) {
    // startup state can only be set once
    return (_state == State::Undefined);
  } else if (state == State::Serving) {
    return (_state == State::Startup);
  } else if (state == State::Shutdown) {
    return (_state == State::Startup || _state == State::Serving);
  }

  // anything else is invalid
  return false;
}

void ServerState::Reset() { gInstance = nullptr; }

#ifdef SDB_GTEST
bool ServerState::IsGTest() const noexcept { return _is_gtest; }

void ServerState::SetGTest(bool is_gtest) noexcept { _is_gtest = is_gtest; }
#endif
