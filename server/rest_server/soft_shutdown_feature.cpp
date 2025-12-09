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

#include "rest_server/soft_shutdown_feature.h"

#include "app/app_server.h"
#include "app/logger_feature.h"
#include "basics/logger/logger.h"
#include "general_server/general_server_feature.h"
#include "general_server/scheduler.h"
#include "general_server/scheduler_feature.h"

#ifdef SDB_CLUSTER
#include "aql/query_registry_feature.h"
#include "transaction/manager.h"
#include "transaction/manager_feature.h"
#endif

using namespace sdb::options;

namespace {

void QueueShutdownChecker(absl::Mutex& mutex,
                          sdb::Scheduler::WorkHandle& work_item,
                          std::function<void(bool)>& check_func) {
  sdb::Scheduler* scheduler = sdb::SchedulerFeature::gScheduler;
  std::lock_guard guard(mutex);
  work_item =
    scheduler->queueDelayed("soft-shutdown", sdb::RequestLane::ClusterInternal,
                            std::chrono::seconds(2), check_func);
}

}  // namespace

namespace sdb {

SoftShutdownFeature::SoftShutdownFeature(Server& server)
  : SerenedFeature{server, name()} {
  setOptional(true);

  // We do not yet know if we are a coordinator, so just in case,
  // create a SoftShutdownTracker, it will not hurt if it is not used:
  _soft_shutdown_tracker = std::make_shared<SoftShutdownTracker>(server);
}

void SoftShutdownFeature::beginShutdown() {
  _soft_shutdown_tracker->cancelChecker();
}

void SoftShutdownTracker::cancelChecker() {
  if (_soft_shutdown_ongoing.load(std::memory_order_relaxed)) {
    // This is called when an actual shutdown happens. We then want to
    // delete the WorkItem of the soft shutdown checker, such that the
    // Scheduler does not have any cron jobs any more:
    std::lock_guard guard(_work_item_mutex);
    _work_item.reset();
  }
}

SoftShutdownTracker::SoftShutdownTracker(SerenedServer& server)
  : _server(server), _soft_shutdown_ongoing(false) {
  _check_func = [this](bool /*cancelled*/) {
    if (_server.isStopping()) {
      return;  // already stopping, do nothing, and in particular
               // let's not schedule ourselves again!
    }
    if (!this->checkAndShutdownIfAllClear()) {
      // Rearm ourselves:
      QueueShutdownChecker(this->_work_item_mutex, this->_work_item,
                           this->_check_func);
    }
  };
}

void SoftShutdownTracker::initiateSoftShutdown() {
  bool prev = _soft_shutdown_ongoing.exchange(true, std::memory_order_relaxed);
  if (prev) {
    // Make behaviour idempotent!
    SDB_INFO("xxxxx", Logger::STARTUP,
             "Received second soft shutdown request, ignoring it...");
    return;
  }

  SDB_INFO("xxxxx", Logger::STARTUP, "Initiating soft shutdown...");

  // Tell GeneralServerFeature, which will forward to all features which
  // overload the initiateSoftShutdown method:
  _server.initiateSoftShutdown();
  // Currently, these are:
  //   - the GeneralServerFeature for its JobManager

  // And initiate our checker to watch numbers:
  QueueShutdownChecker(_work_item_mutex, _work_item, _check_func);
}

bool SoftShutdownTracker::checkAndShutdownIfAllClear() const {
  Status status = getStatus();
  if (!status.allClear()) {
    vpack::Builder builder;
    toVPack(builder, status);
    // FIXME: Set to DEBUG level
    SDB_INFO("xxxxx", Logger::STARTUP,
             "Soft shutdown check said 'not all clear': ",
             builder.slice().toJson(), ".");
    return false;
  }
  SDB_INFO("xxxxx", Logger::STARTUP,
           "Goal reached for soft shutdown, all ongoing tasks are terminated"
           ", will now trigger the actual shutdown...");
  initiateActualShutdown();
  return true;
}

void SoftShutdownTracker::initiateActualShutdown() const {
  Scheduler* scheduler = SchedulerFeature::gScheduler;
  auto self = shared_from_this();
  scheduler->queue(RequestLane::ClusterInternal, [self = shared_from_this()] {
    // Give the server 2 seconds to finish stuff
    std::this_thread::sleep_for(std::chrono::seconds(2));
    self->_server.beginShutdown();
  });
}

void SoftShutdownTracker::toVPack(vpack::Builder& builder,
                                  const SoftShutdownTracker::Status& status) {
  vpack::ObjectBuilder guard(&builder);
  builder.add("softShutdownOngoing", status.soft_shutdown_ongoing);
  builder.add("AQLcursors", status.aql_cursors);
  builder.add("transactions", status.transactions);
  builder.add("pendingJobs", status.pending_jobs);
  builder.add("doneJobs", status.done_jobs);
  builder.add("lowPrioOngoingRequests", status.low_prio_ongoing_requests);
  builder.add("lowPrioQueuedRequests", status.low_prio_queued_requests);
  builder.add("allClear", status.allClear());
}

SoftShutdownTracker::Status SoftShutdownTracker::getStatus() const {
  Status status(_soft_shutdown_ongoing.load(std::memory_order_relaxed));

#ifdef SDB_CLUSTER
  // Get number of active AQL cursors from each database:
  status.aql_cursors += GetCursorCount();

  // Get number of active transactions from Manager:
  auto& manager_feature = _server.getFeature<transaction::ManagerFeature>();
  auto* manager = manager_feature.manager();
  if (manager != nullptr) {
    status.transactions = manager->getActiveTransactionCount();
  }
#endif

  // Get numbers of pending and done asynchronous jobs:
  auto& general_server_feature = _server.getFeature<GeneralServerFeature>();
  auto& job_manager = general_server_feature.jobManager();
  std::tie(status.pending_jobs, status.done_jobs) =
    job_manager.getNrPendingAndDone();

  // Get number of ongoing and queued requests from scheduler:
  std::tie(status.low_prio_ongoing_requests, status.low_prio_queued_requests) =
    SchedulerFeature::gScheduler->getNumberLowPrioOngoingAndQueued();

  return status;
}

}  // namespace sdb
