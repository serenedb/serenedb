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

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>

#include "basics/common.h"
#include "basics/read_write_lock.h"
#include "basics/reboot_id.h"
#include "basics/result_or.h"
#include "catalog/types.h"
#include "rest_server/serened.h"

namespace sdb {

class AgencyComm;
class Result;

class ServerState {
 public:
  /// an enum describing the roles a server can have
  enum class Role : uint8_t {
    Undefined = 0,  // initial value
    Single,         // is set when cluster feature is off
    DBServer,
    Coordinator,
    Agent,
  };

  /// an enum describing the possible states a server can have
  enum class State : uint8_t {
    Undefined = 0,  // initial value
    Startup,        // used by all roles
    Serving,        // used by all roles
    Shutdown,       // used by all roles
  };

  enum class ReadOnlyMode : uint8_t {
    ApiTrue,  // Set from outside via API
    ApiFalse,
  };

  enum class Mode : uint8_t {
    Default = 0,
    Startup = 1,
    /// reject almost all requests
    Maintenance = 2,

    Invalid = 255,  // this mode is used to indicate shutdown
  };

  ServerState();

  static ServerState* instance() noexcept;

  static bool IsCoordinatorId(std::string_view id);

  static bool IsDBServerId(std::string_view id);

  static std::string RoleToStr(Role role);

  static std::string RoleToShortStr(Role role);

  /// get the key for lists of a role in the agency
  static std::string RoleToAgencyListKey(Role role);

  /// get the key for a role in the agency
  static std::string RoleToAgencyKey(Role role);

  static Role StrToRole(std::string_view str) noexcept;

  static std::string StateToStr(State state);

  static State StringToState(std::string_view str) noexcept;

  static std::string ModeToStr(Mode mode);

  static Mode StrToMode(std::string_view str) noexcept;

  Mode GetMode() noexcept;

  /// sets server mode, returns previously held
  /// value (performs atomic read-modify-write operation)
  Mode SetMode(Mode mode) noexcept;

  bool IsStartupOrMaintenance() noexcept;

  /// should not allow DDL operations / transactions
  bool ReadOnly() noexcept;

  bool SetReadOnly(ReadOnlyMode mode) noexcept;

  static void Reset();

  bool IsSingle() const noexcept { return IsSingle(GetRole()); }

  static bool IsSingle(Role role) noexcept {
    SDB_ASSERT(role != Role::Undefined);
    return role == Role::Single;
  }

  bool IsCoordinator() const noexcept { return IsCoordinator(GetRole()); }

  static bool IsCoordinator(Role role) noexcept {
    SDB_ASSERT(role != Role::Undefined);
    return role == Role::Coordinator;
  }

  bool IsDBServer() const noexcept { return IsDBServer(GetRole()); }

  static bool IsDBServer(Role role) noexcept {
    SDB_ASSERT(role != Role::Undefined);
    return role == Role::DBServer;
  }

  static bool IsClusterNode(Role role) noexcept {
    return IsCoordinator(role) || IsDBServer(role);
  }

  bool IsClusterNode() const noexcept { return IsClusterNode(GetRole()); }

  bool IsAgent() const noexcept { return IsAgent(GetRole()); }

  static bool IsAgent(Role role) noexcept {
    SDB_ASSERT(role != Role::Undefined);
    return role == Role::Agent;
  }

  bool IsClientNode() const noexcept { return IsClientNode(GetRole()); }

  static bool IsClientNode(Role role) noexcept {
    return IsCoordinator(role) || IsSingle(role);
  }

  bool IsDataNode() const noexcept { return IsDataNode(GetRole()); }

  static bool IsDataNode(Role role) noexcept {
    return IsDBServer(role) || IsSingle(role);
  }

  Role GetRole() const noexcept {
    return _role.load(std::memory_order_acquire);
  }

  /// register with agency, create / load server ID
  bool IntegrateIntoCluster(Role role, std::string_view my_addr,
                            std::string_view my_adv_endpoint);

  /// unregister this server with the agency, removing it from
  /// the cluster setup.
  /// the timeout can be used as the max wait time for the agency to
  /// acknowledge the unregister action.
  bool Unregister(double timeout);

  /// mark this server as shut down in the agency, without removing it
  /// from the cluster setup.
  /// the timeout can be used as the max wait time for the agency to
  /// acknowledge the logoff action.
  bool Logoff(double timeout);

  void SetRole(Role);

  std::string GetId() const;

  void SetId(std::string_view id);

  uint32_t GetShortId() const;

  void SetShortId(uint32_t id);

  std::string GetShortName() const;

  RebootId GetRebootId() const;

  void SetRebootId(RebootId reboot_id);

  std::string GetEndpoint();

  std::string GetAdvertisedEndpoint();

  /// find a host identification string
  void FindHost(std::string_view fallback);

  /// get a string to identify the host we are running on
  std::string GetHost() { return _host; }

  State GetState();

  void SetState(State state);

  std::string GetPersistedId();
  bool HasPersistedId();
  bool WritePersistedId(std::string_view id);
  std::string GeneratePersistedId(Role role);

  /// sets server mode and propagates new mode to agency
  Result PropagateClusterReadOnly(bool);

  /// file where the server persists its UUID
  std::string GetUUIDFilename() const;

#ifdef SDB_GTEST
  [[nodiscard]] bool IsGTest() const noexcept;
  void SetGTest(bool is_gtest) noexcept;
#else
  [[nodiscard]] constexpr bool IsGTest() const noexcept { return false; }
#endif

 private:
  /// validate a state transition for a primary server
  bool CheckPrimaryState(State state);

  /// validate a state transition for a coordinator server
  bool CheckCoordinatorState(State state);

  /// try to read the rebootID from the Agency
  ResultOr<uint64_t> ReadRebootIdFromAgency(AgencyComm& comm);

  /// check whether the agency has been initalized
  bool CheckIfAgencyInitialized(AgencyComm& comm, Role role);

  /// register at agency, might already be done
  bool RegisterAtAgencyPhase1(AgencyComm& comm, Role role);

  /// write the Current/ServersRegistered entry
  bool RegisterAtAgencyPhase2(AgencyComm& comm, bool had_persisted_id);

  /// whether or not "value" is a server UUID
  bool IsUUID(std::string_view value) const;

#ifdef SDB_GTEST
  bool _is_gtest = false;
#endif
  std::atomic<Role> _role = Role::Undefined;

  /// the current state
  State _state = State::Undefined;

  std::atomic_bool _read_only = false;

  std::atomic<Mode> _mode = Mode::Default;

  /// the server's short id, can be set just once
  std::atomic<uint32_t> _short_id = 0;

  /// the server's rebootId.
  ///
  /// A server
  ///   * ~boots~ if it is started on a new database directory without a UUID
  ///   persisted
  ///   * ~reboots~ if it is started on a pre-existing database directory with a
  ///   UUID present
  ///
  /// when integrating into a cluster (via IntegrateIntoCluster), the server
  /// tries to increment the agency key Current/KnownServers/_id/rebootId; if
  /// this key did not exist it is created with the value 1, so a valid rebootId
  /// is always >= 1, and if the server booted must be 1.
  ///
  /// Changes of rebootIds (i.e. server reboots) are noticed in ClusterInfo and
  /// can be used through a notification architecture from there
  std::atomic<RebootId::value_type> _reboot_id = 0;

  /// r/w lock for state
  mutable absl::Mutex _lock;

  /// lock for writing and reading server id
  mutable absl::Mutex _id_lock;

  /// the server's id, can be set just once, use GetId and SetId, do not
  /// access directly
  std::string _id;

  /// the server's own endpoint, can be set just once
  std::string _my_endpoint;

  /// the server's own advertised endpoint, can be set just once,
  /// if empty, we advertise _address
  std::string _advertised_endpoint;

  /// an identification string for the host a server is running on
  std::string _host;
};

}  // namespace sdb
