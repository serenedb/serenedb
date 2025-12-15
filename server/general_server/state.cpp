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

#include <algorithm>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <iomanip>
#include <regex>
#include <utility>

#ifdef SDB_CLUSTER
#include "agency/async_agency_comm.h"
#endif
#include "app/app_server.h"
#include "basics/application-exit.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "basics/physical_memory.h"
#include "basics/read_locker.h"
#include "basics/result_or.h"
#include "basics/string_utils.h"
#include "basics/time_string.h"
#include "basics/write_locker.h"
#include "database/ticks.h"
#include "rest/common_defines.h"
#include "rest/version.h"
#include "rest_server/database_path_feature.h"
#include "rest_server/serened.h"
#include "storage_engine/engine_selector_feature.h"
#include "vpack/vpack_helper.h"

using namespace sdb;
using namespace sdb::basics;

namespace {

// whenever the format of the generated UUIDs changes, please make sure to
// adjust this regex too!
const std::regex kUuidRegex(
  "^(SNGL|CRDN|PRMR|AGNT)-[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-"
  "f0-9]{12}$");

[[maybe_unused]] constexpr const char* kCurrentServersRegisteredPref =
  "/Current/ServersRegistered/";

ServerState* gInstance = nullptr;

}  // namespace

ServerState::ServerState() {
  SDB_ASSERT(gInstance == nullptr);
  gInstance = this;
  SetRole(Role::Undefined);
}

void ServerState::FindHost(std::string_view fallback) {
  // Compute a string identifying the host on which we are running, note
  // that this is more complicated than immediately obvious, because we
  // could sit in a container which is deployed by Kubernetes or Mesos or
  // some other orchestration framework:

  // the following is set by Mesos or by an administrator:
  char* p = getenv("HOST");
  if (p != nullptr) {
    _host = p;
    return;
  }

  // the following is set by Kubernetes when using the downward API:
  p = getenv("NODE_NAME");
  if (p != nullptr) {
    _host = p;
    return;
  }

  // Now look at the contents of the file /etc/machine-id, if it exists:
  std::string name = "/etc/machine-id";
  if (sdb::basics::file_utils::Exists(name)) {
    try {
      _host = sdb::basics::file_utils::Slurp(name);
      while (!_host.empty() && (_host.back() == '\r' || _host.back() == '\n' ||
                                _host.back() == ' ')) {
        _host.erase(_host.size() - 1);
      }
      if (!_host.empty()) {
        return;
      }
    } catch (...) {
    }
  }

  // Finally, as a last resort, take the fallback, coming from
  // the ClusterFeature with the value of --cluster.my-address
  // or by the AgencyFeature with the value of --agency.my-address:
  _host = fallback;
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

#ifdef SDB_CLUSTER

/// unregister this server with the agency
bool ServerState::Unregister(double timeout) {
  SDB_ASSERT(!GetId().empty());
  SDB_ASSERT(AsyncAgencyCommManager::isEnabled());

  const std::string agency_list_key = RoleToAgencyListKey(GetRole());
  const std::string& id = GetId();
  std::vector<AgencyOperation> operations;
  operations.reserve(6);
  operations.emplace_back("Current/" + agency_list_key + "/" + id,
                          AgencySimpleOperationType::DeleteOp);
  operations.emplace_back("Current/ServersKnown/" + id,
                          AgencySimpleOperationType::DeleteOp);
  operations.emplace_back("Current/ServersRegistered/" + id,
                          AgencySimpleOperationType::DeleteOp);
  operations.emplace_back("Current/Version",
                          AgencySimpleOperationType::IncrementOp);
  operations.emplace_back("Plan/" + agency_list_key + "/" + id,
                          AgencySimpleOperationType::DeleteOp);
  operations.emplace_back("Plan/Version",
                          AgencySimpleOperationType::IncrementOp);

  AgencyWriteTransaction unregister_transaction(operations);
  AgencyComm comm(SerenedServer::Instance());
  auto r = comm.sendTransactionWithFailover(unregister_transaction, timeout);
  return r.successful();
}

/// log off this server from the agency
bool ServerState::Logoff(double timeout) {
  SDB_ASSERT(!GetId().empty());
  SDB_ASSERT(AsyncAgencyCommManager::isEnabled());

  const std::string agency_list_key = RoleToAgencyListKey(GetRole());
  const std::string& id = GetId();
  std::vector<AgencyOperation> operations;
  operations.reserve(3);
  operations.emplace_back("Current/" + agency_list_key + "/" + id,
                          AgencySimpleOperationType::DeleteOp);
  operations.emplace_back("Current/ServersRegistered/" + id,
                          AgencySimpleOperationType::DeleteOp);
  operations.emplace_back("Current/Version",
                          AgencySimpleOperationType::IncrementOp);

  AgencyWriteTransaction unregister_transaction(operations);
  AgencyComm comm(SerenedServer::Instance());

  // Try only once to unregister because maybe the agencycomm
  // is shutting down as well...
  int max_tries = static_cast<int>(timeout / 3.0);
  int tries = 0;
  while (true) {
    auto res = comm.sendTransactionWithFailover(unregister_transaction, 3.0);

    if (res.successful()) {
      return true;
    }

    if (res.httpCode() == rest::ResponseCode::ServiceUnavailable ||
        !res.connected()) {
      SDB_INFO("xxxxx", Logger::CLUSTER,
               "unable to unregister server from agency, because agency is in "
               "shutdown");
      return false;
    }

    if (++tries < max_tries) {
      // try again
      SDB_WARN("xxxxx", Logger::CLUSTER,
               "unable to unregister server from agency (attempt ", tries,
               " of ", max_tries, "): ", res.errorMessage());
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    } else {
      // give up
      SDB_ERROR(
        "xxxxx", Logger::CLUSTER,
        "giving up unregistering server from agency: ", res.errorMessage());
      return false;
    }
  }
}

ResultOr<uint64_t> ServerState::ReadRebootIdFromAgency(AgencyComm& comm) {
  SDB_ASSERT(!_id.empty());
  std::string reboot_id_path = "Current/ServersKnown/" + _id + "/rebootId";
  auto result = comm.getValues(reboot_id_path);

  if (!result.successful()) {
    SDB_WARN("xxxxx", Logger::CLUSTER, "Could not read back ", reboot_id_path);

    return std::unexpected<Result>(std::in_place, ERROR_INTERNAL,
                                   "could not read rebootId from agency");
  }

  auto slice_path = AgencyCommHelper::slicePath(reboot_id_path);
  auto value_slice = result.slice().at(0).get(slice_path);

  if (!value_slice.isInteger()) {
    SDB_WARN("xxxxx", Logger::CLUSTER, "rebootId is not an integer");

    return std::unexpected<Result>(std::in_place, ERROR_INTERNAL,
                                   "rebootId is not an integer");
  }

  return value_slice.getNumber<uint64_t>();
}

bool ServerState::IntegrateIntoCluster(Role role, std::string_view my_endpoint,
                                       std::string_view adv_endpoint) {
  absl::WriterMutexLock write_locker{&_lock};

  AgencyComm comm(SerenedServer::Instance());

  std::string id;
  bool had_persisted_id = HasPersistedId();
  if (!had_persisted_id) {
    id = GeneratePersistedId(role);

    SDB_INFO("xxxxx", Logger::CLUSTER, "Fresh start. Persisting new UUID ", id);
  } else {
    id = GetPersistedId();
    SDB_DEBUG("xxxxx", Logger::CLUSTER, "Restarting with persisted UUID ", id);
  }
  SetId(id);
  _my_endpoint = my_endpoint;
  _advertised_endpoint = adv_endpoint;
  SDB_ASSERT(!_my_endpoint.empty());

  if (!RegisterAtAgencyPhase1(comm, role)) {
    return false;
  }

  log::SetRole(RoleToStr(role)[0]);
  _role.store(role, std::memory_order_release);

  SDB_DEBUG("xxxxx", Logger::CLUSTER, "We successfully announced ourselves as ",
            RoleToStr(role), " and our id is ", id);

  // now overwrite the entry in /Current/ServersRegistered/<myId>
  bool registered = RegisterAtAgencyPhase2(comm, had_persisted_id);
  if (!registered) {
    return false;
  }

  // now check the configuration of the different servers for duplicate
  // endpoints
  auto result = comm.getValues(::kCurrentServersRegisteredPref);

  if (result.successful()) {
    auto slice_path =
      AgencyCommHelper::slicePath(::kCurrentServersRegisteredPref);
    auto value_slice = result.slice().at(0).get(slice_path);

    if (value_slice.isObject()) {
      // map from server UUID to endpoint
      containers::FlatHashMap<std::string_view, std::string_view> endpoints;
      for (auto [it_key, it_value] : vpack::ObjectIterator(value_slice)) {
        const auto server_id = it_key.stringView();

        if (!IsUUID(server_id)) {
          continue;
        }
        if (!it_value.isObject()) {
          continue;
        }
        vpack::Slice endpoint_slice = it_value.get("endpoint");
        if (!endpoint_slice.isString()) {
          continue;
        }
        const auto [idIter, emplaced] =
          endpoints.try_emplace(endpoint_slice.stringView(), server_id);
        if (!emplaced && idIter->first != server_id) {
          // duplicate entry!
          SDB_WARN(
            "xxxxx", Logger::CLUSTER,
            "found duplicate server entry for endpoint '",
            endpoint_slice.stringView(),
            "' when processing endpoints configuration for server ", server_id,
            ": already used by other server ", idIter->second,
            ". it looks like this is a (mis)configuration issue. ",
            "full servers registered configuration: ", value_slice.toJson());
          // anyway, continue with startup
        }
      }
    }
  }

  return true;
}

bool ServerState::CheckIfAgencyInitialized(AgencyComm& comm, Role role) {
  const std::string agency_list_key = RoleToAgencyListKey(role);
  auto result = comm.getValues("Plan/" + agency_list_key, 3.0);
  if (!result.successful()) {
    SDB_WARN("xxxxx", Logger::STARTUP, "Couldn't fetch Plan/", agency_list_key,
             " from agency. ", " Agency is not initialized? ",
             result.errorMessage());
    return false;
  }

  vpack::Slice servers = result.slice().at(0).get(std::vector<std::string>(
    {AgencyCommHelper::path(), "Plan", agency_list_key}));
  if (!servers.isObject()) {
    SDB_WARN("xxxxx", Logger::STARTUP, "Plan/", agency_list_key,
             " in agency is no object, but ", servers.typeName(),
             ". Agency not initialized?");
    return false;
  }
  return true;
}

bool ServerState::RegisterAtAgencyPhase1(AgencyComm& comm, Role role) {
  // if the agency is not initialized, we'll give the bunch a little time to get
  // their act together before calling it a day.

  using namespace std::chrono;
  using Clock = steady_clock;
  auto registration_timeout = Clock::now() + seconds(300);
  auto backoff = milliseconds(75);
  do {
    if (CheckIfAgencyInitialized(comm, role)) {
      break;
    } else if (Clock::now() >= registration_timeout) {
      return false;
    }
    std::this_thread::sleep_for(backoff);
    if (backoff < seconds(1)) {
      backoff += backoff;
    }
  } while (!SerenedServer::Instance().isStopping());

  const std::string agency_list_key = RoleToAgencyListKey(role);
  const std::string latest_id_key = "Latest" + RoleToAgencyKey(role) + "Id";

  vpack::Builder builder;
  builder.add("none");

  std::string plan_url = "Plan/" + agency_list_key + "/" + _id;
  std::string current_url = "Current/" + agency_list_key + "/" + _id;

  AgencyWriteTransaction preg(
    {AgencyOperation(plan_url, AgencyValueOperationType::Set, builder.slice()),
     AgencyOperation("Plan/Version", AgencySimpleOperationType::IncrementOp)},
    AgencyPrecondition(plan_url, AgencyPrecondition::Type::Empty, true));
  // ok to fail..if it failed we are already registered
  auto preg_result = comm.sendTransactionWithFailover(preg, 0.0);
  if (!preg_result.successful()) {
    SDB_TRACE("xxxxx", Logger::CLUSTER,
              "unable to initially register in agency. ",
              preg_result.errorMessage());
  }

  AgencyWriteTransaction creg(
    {AgencyOperation(current_url, AgencyValueOperationType::Set,
                     builder.slice()),
     AgencyOperation("Current/Version",
                     AgencySimpleOperationType::IncrementOp)},
    AgencyPrecondition(current_url, AgencyPrecondition::Type::Empty, true));
  // ok to fail..if it failed we are already registered
  auto creg_result = comm.sendTransactionWithFailover(creg, 0.0);
  if (!creg_result.successful()) {
    SDB_TRACE("xxxxx", Logger::CLUSTER,
              "unable to initially register in agency. ",
              creg_result.errorMessage());
  }

  // coordinator is already/still registered from an previous unclean shutdown;
  // must establish a new short ID
  bool force_change_short_id = IsCoordinator(role);

  std::string target_id_path = "Target/" + latest_id_key;
  std::string target_url = "Target/MapUniqueToShortID/" + _id;

  size_t attempts{0};
  while (attempts++ < 300) {
    AgencyReadTransaction read_value_trx(
      std::vector<std::string>{AgencyCommHelper::path(target_id_path),
                               AgencyCommHelper::path(target_url)});
    auto result = comm.sendTransactionWithFailover(read_value_trx, 0.0);

    if (!result.successful()) {
      SDB_WARN("xxxxx", Logger::CLUSTER, "Couldn't fetch ", target_id_path,
               " and ", target_url);
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    vpack::Slice map_slice = result.slice().at(0).get(std::vector<std::string>(
      {AgencyCommHelper::path(), "Target", "MapUniqueToShortID", _id}));

    // already registered
    if (!map_slice.isNone() && !force_change_short_id) {
      vpack::Slice s = map_slice.get("TransactionID");
      if (s.isNumber()) {
        uint32_t short_id = s.getNumber<uint32_t>();
        SetShortId(short_id);
        SDB_DEBUG("xxxxx", Logger::CLUSTER, "restored short id ", short_id,
                  " from agency");
      } else {
        SDB_WARN("xxxxx", Logger::CLUSTER,
                 "unable to restore short id from agency");
      }
      return true;
    }

    vpack::Slice latest_id_slice =
      result.slice().at(0).get(std::vector<std::string>(
        {AgencyCommHelper::path(), "Target", latest_id_key}));

    uint32_t num = 0;
    std::unique_ptr<AgencyPrecondition> latest_id_precondition;
    vpack::Builder latest_id_builder;
    if (latest_id_slice.isNumber()) {
      num = latest_id_slice.getNumber<uint32_t>();
      latest_id_builder.add(num);
      latest_id_precondition = std::make_unique<AgencyPrecondition>(
        target_id_path, AgencyPrecondition::Type::Value,
        latest_id_builder.slice());
    } else {
      latest_id_precondition = std::make_unique<AgencyPrecondition>(
        target_id_path, AgencyPrecondition::Type::Empty, true);
    }

    vpack::Builder local_id_builder;
    {
      vpack::ObjectBuilder b(&local_id_builder);
      local_id_builder.add("TransactionID", num + 1);
      size_t width =
        std::max(std::to_string(num + 1).size(), static_cast<size_t>(4));
      std::string ss_str;
      absl::strings_internal::OStringStream ss{&ss_str};  // ShortName
      ss << RoleToAgencyKey(role) << std::setw(width) << std::setfill('0')
         << num + 1;
      local_id_builder.add("ShortName", ss_str);
    }

    std::vector<AgencyOperation> operations;
    std::vector<AgencyPrecondition> preconditions;

    operations.push_back(
      AgencyOperation(target_id_path, AgencySimpleOperationType::IncrementOp));
    operations.push_back(AgencyOperation(
      target_url, AgencyValueOperationType::Set, local_id_builder.slice()));

    preconditions.push_back(*(latest_id_precondition.get()));
    preconditions.push_back(AgencyPrecondition(
      target_url, AgencyPrecondition::Type::Empty, map_slice.isNone()));

    AgencyWriteTransaction trx(operations, preconditions);
    result = comm.sendTransactionWithFailover(trx, 0.0);

    if (result.successful()) {
      SetShortId(num +
                 1);  // save short ID for generating server-specific ticks
      return true;
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  SDB_FATAL("xxxxx", Logger::STARTUP, "Couldn't register shortname for ", _id);
  return false;
}

bool ServerState::RegisterAtAgencyPhase2(AgencyComm& comm,
                                         const bool had_persisted_id) {
  SDB_ASSERT(!_id.empty() && !_my_endpoint.empty());

  const std::string server_registration_path =
    ::kCurrentServersRegisteredPref + _id;
  const std::string reboot_id_path =
    "/Current/ServersKnown/" + _id + "/rebootId";

  // If we generated a new UUID, this *must not* exist in the Agency, so we
  // should fail to register.
  std::vector<AgencyPrecondition> pre;
  if (!had_persisted_id) {
    pre.emplace_back(AgencyPrecondition(reboot_id_path,
                                        AgencyPrecondition::Type::Empty, true));
  }

  constexpr int64_t kMinDelay = 50;    // ms
  constexpr int64_t kMaxDelay = 1000;  // ms
  int64_t delay = kMinDelay;           // ms

  while (!SerenedServer::Instance().isStopping()) {
    vpack::Builder builder;
    {
      vpack::ObjectBuilder b(&builder);
      builder.add("endpoint", _my_endpoint);
      builder.add("advertisedEndpoint", _advertised_endpoint);
      builder.add("host", GetHost());
      builder.add("physicalMemory", physical_memory::GetValue());
      builder.add("numberOfCores", number_of_cores::GetValue());
      builder.add("version", rest::Version::getNumericServerVersion());
      builder.add("versionString", rest::Version::getServerVersion());
      builder.add("timestamp",
                  TimepointToString(std::chrono::system_clock::now()));
    }

    AgencyWriteTransaction trx(
      {AgencyOperation(server_registration_path, AgencyValueOperationType::Set,
                       builder.slice()),
       AgencyOperation(reboot_id_path, AgencySimpleOperationType::IncrementOp),
       AgencyOperation("Current/Version",
                       AgencySimpleOperationType::IncrementOp)},
      pre);

    auto result = comm.sendTransactionWithFailover(trx, 0.0);

    if (result.successful()) {
      break;  // Continue below to read back the rebootId
    }

    SDB_WARN("xxxxx", Logger::CLUSTER,
             "failed to register server in agency: http code: ",
             result.httpCode(), ", body: '", result.body(), "', retrying ...");

    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    // use exponential backoff, but limit the maximum delay
    delay *= 2;
    delay = std::clamp(delay, kMinDelay, kMaxDelay);
  }

  // if we left the above retry loop because the server is stopping
  // we'll skip this and return false right away.
  delay = kMinDelay;
  while (!SerenedServer::Instance().isStopping()) {
    auto result = ReadRebootIdFromAgency(comm);

    if (result) {
      SetRebootId(RebootId{*result});
      return true;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(delay));
    // use exponential backoff, but limit the maximum delay
    delay *= 2;
    delay = std::clamp(delay, kMinDelay, kMaxDelay);
  }

  return false;
}

Result ServerState::PropagateClusterReadOnly(bool mode) {
  // Agency enabled will work for single server replication as well as cluster
  if (AsyncAgencyCommManager::isEnabled()) {
    std::vector<AgencyOperation> operations;
    vpack::Builder builder;
    builder.add(mode);
    operations.push_back(AgencyOperation(
      "Readonly", AgencyValueOperationType::Set, builder.slice()));

    AgencyWriteTransaction readonly_mode(operations);
    AgencyComm comm(SerenedServer::Instance());
    // TODO(mbkkt) make async
    auto r = comm.sendTransactionWithFailover(readonly_mode);
    if (!r.successful()) {
      return Result(ERROR_CLUSTER_AGENCY_COMMUNICATION_FAILED,
                    r.errorMessage());
    }
    // This is propagated to all servers via the heartbeat, which happens
    // once per second. So to ensure that every server has taken note of
    // the change, we delay here for 2 seconds.
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }
  SetReadOnly(mode ? ReadOnlyMode::ApiTrue : ReadOnlyMode::ApiFalse);
  return {};
}

#endif

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
  auto& dbpath = SerenedServer::Instance().getFeature<DatabasePathFeature>();
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
    SDB_FATAL("xxxxx", sdb::Logger::FIXME, "Cannot write UUID file '",
              uuid_filename, "': ", ex.what());
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

std::string ServerState::GetPersistedId() {
  std::string uuid_filename = GetUUIDFilename();
  if (HasPersistedId()) {
    try {
      auto uuid_buf = basics::file_utils::Slurp(uuid_filename);
      basics::string_utils::TrimInPlace(uuid_buf);
      if (!uuid_buf.empty()) {
        return uuid_buf;
      }
    } catch (const basics::Exception& ex) {
      SDB_FATAL("xxxxx", Logger::CLUSTER, "Couldn't read UUID file '",
                uuid_filename, "' - ", ex.what());
    }
  }

  SDB_FATAL("xxxxx", Logger::STARTUP, "Couldn't open UUID file '",
            uuid_filename, "'");
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
  log::SetRole(RoleToStr(role)[0]);
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

RebootId ServerState::GetRebootId() const {
  const RebootId reboot_id{_reboot_id.load(std::memory_order_relaxed)};
  SDB_ASSERT(reboot_id.initialized());
  return reboot_id;
}

void ServerState::SetRebootId(RebootId reboot_id) {
  SDB_ASSERT(reboot_id.initialized());
  _reboot_id.store(reboot_id.value(), std::memory_order_relaxed);
}

std::string ServerState::GetEndpoint() {
  absl::ReaderMutexLock read_locker{&_lock};
  return _my_endpoint;
}

std::string ServerState::GetAdvertisedEndpoint() {
  absl::ReaderMutexLock read_locker{&_lock};
  return _advertised_endpoint;
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
    SDB_DEBUG("xxxxx", Logger::CLUSTER, "changing state of ",
              ServerState::RoleToStr(role), " server from ",
              ServerState::StateToStr(_state), " to ",
              ServerState::StateToStr(state));

    _state = state;
  } else {
    SDB_ERROR("xxxxx", Logger::CLUSTER, "invalid state transition for ",
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
