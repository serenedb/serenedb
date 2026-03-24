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

#include <cstdint>
#include <iosfwd>

namespace sdb {

enum class RequestLane {
  // For requests that do not block or wait for something.
  // This ignores blocks that can occur when delivering
  // a file from, for example, an NFS mount.
  ClientFast,

  // For requests that execute an AQL query or are tightly
  // related like simple queries,
  // that do AQL requests, user administrator that
  // internally uses AQL.
  ClientAql,

  // For requests that might block or wait for something,
  // which are not CLIENT_AQL.
  ClientSlow,

  // Used for all requests sent by the web interface
  ClientUi,

  // For requests between agents. These are basically the
  // requests used to implement RAFT.
  AgencyInternal,

  // For requests from the DBserver or Coordinator accessing
  // the agency.
  AgencyCluster,

  // For requests from the DBserver to the Coordinator or
  // from the Coordinator to the DBserver. But not having high priority.
  ClusterInternal,

  // Internal AQL requests, or continuations. Low priority.
  ClusterAql,

  // For requests from the DBserver to the Coordinator, and continuations on the
  // Coordinator.
  // These have medium priority. Because client requests made against the
  // RestCursorHandler (with lane CLIENT_AQL) might block and need these to
  // finish. Ongoing low priority requests can also prevent low priority lanes
  // from being worked on, having the same effect.
  ClusterAqlInternalCoordinator,

  // Shutdown request for AQL queries, i.e. /_api/aql/finish/<id> on the
  // DBserver. This calls have slightly higher priority than normal AQL requests
  // because the query shutdown can release resources and unblock other
  // operations.
  ClusterAqlShutdown,

  // DOCUMENT() requests inside cluster AQL queries, executed on DBservers.
  // These requests will read a locally available document and do not depend
  // on other requests. They can always make progress. They will be initiated
  // on coordinators and handling them quickly may unblock the coordinator
  // part of an AQL query.
  ClusterAqlDocument,

  // For requests from the DBserver to the Coordinator or
  // from the Coordinator to the DBserver for administration
  // or diagnostic purpose. Should not block.
  ClusterAdmin,

  // For requests used between leader and follower for
  // replication to compare the local states of data.
  ServerReplication,

  // For requests used between leader and follower for
  // replication to go the final mile and get back to
  // in-sync mode (wal tailing)
  ServerReplicationCatchup,

  // For synchronous replication requests on the follower
  ServerSynchronousReplication,

  // Internal tasks with low priority
  InternalLow,

  // Default continuation lane for requests (e.g. after returning from a network
  // call). Some requests, such as CLUSTER_AQL, will have a different
  // continuation lane for more fine-grained control.
  Continuation,

  // Not yet used:
  // For requests which go from the agency back to coordinators or
  // DBservers to report about changes in the agency. They are fast
  // and should have high prio. Will never block.
  // AGENCY_CALLBACK`

  // Used by futures that have been delayed using Scheduler::delay.
  DelayedFuture,

  // undefined request lane, used only in the beginning
  Undefined,
};

enum class RequestPriority : int8_t {
  Low = -2,
  Med = -1,
  High = 0,
  Maintenance = 1,
};

constexpr RequestPriority PriorityRequestLane(RequestLane lane) {
  switch (lane) {
    case RequestLane::ClientFast:
      return RequestPriority::Maintenance;
    case RequestLane::ClientAql:
      return RequestPriority::Low;
    case RequestLane::ClientSlow:
      return RequestPriority::Low;
    case RequestLane::AgencyInternal:
      return RequestPriority::High;
    case RequestLane::AgencyCluster:
      return RequestPriority::Low;
    case RequestLane::ClusterInternal:
      return RequestPriority::High;
    case RequestLane::ClusterAql:
      return RequestPriority::Med;
    case RequestLane::ClusterAqlInternalCoordinator:
      return RequestPriority::Med;
    case RequestLane::ClusterAqlShutdown:
      return RequestPriority::Med;
    case RequestLane::ClusterAqlDocument:
      return RequestPriority::Med;
    case RequestLane::ClusterAdmin:
      return RequestPriority::High;
    case RequestLane::ServerReplicationCatchup:
      return RequestPriority::Med;
    case RequestLane::ServerReplication:
      return RequestPriority::Low;
    case RequestLane::InternalLow:
      return RequestPriority::Low;
    case RequestLane::ClientUi:
      return RequestPriority::High;
    case RequestLane::DelayedFuture:
      return RequestPriority::High;
    case RequestLane::ServerSynchronousReplication:
      return RequestPriority::High;
    case RequestLane::Continuation:
      return RequestPriority::Med;
    case RequestLane::Undefined:
      // assume low priority for UNDEFINED. we should never get
      // here under normal circumstances. if we do, returning the
      // default shouldn't do any harm.
      return RequestPriority::Low;
  }
  return RequestPriority::Low;
}

}  // namespace sdb
