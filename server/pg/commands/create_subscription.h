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

#include <duckdb/common/named_parameter_map.hpp>
#include <optional>
#include <string>
#include <vector>

#include "catalog/subscription.h"
#include "pg/connection_context.h"

namespace sdb::pg {

// Create a subscription from an assembled config. An empty `cfg.slot_name`
// defaults to the subscription name; `cfg.origin_name` is validated (any/none).
// `connect = false` creates it disabled (no connect/copy until enabled), the
// only sense of PG's connect option that applies to our async apply.
void CreateSubscription(ConnectionContext& conn_ctx, std::string_view name,
                        catalog::Subscription::Config cfg, bool connect);

// DROP SUBSCRIPTION: removes the catalog entry, stops the apply worker and
// releases the publisher's replication slot. `missing_ok` mirrors IF EXISTS.
void DropSubscription(ConnectionContext& conn_ctx, std::string_view name,
                      bool missing_ok);

// ALTER SUBSCRIPTION ... ENABLE|DISABLE: toggles the enabled flag and starts or
// stops the apply worker accordingly.
void AlterSubscriptionEnabled(ConnectionContext& conn_ctx,
                              std::string_view name, bool enabled);

// The options settable via ALTER SUBSCRIPTION ... SET (...); only the present
// (engaged) optionals are applied.
struct AlterSubscriptionOptions {
  std::optional<bool> binary;
  std::optional<std::string> origin;
  std::optional<bool> disable_on_error;
  std::optional<std::string> synchronous_commit;
  std::optional<bool> password_required;
  std::optional<bool> run_as_owner;
  std::optional<bool> failover;
};

// ALTER SUBSCRIPTION ... SET (option = ...): update the given options (only the
// present ones change) and restart the running apply worker so it reconnects
// with the new config.
void AlterSubscriptionSet(ConnectionContext& conn_ctx, std::string_view name,
                          const AlterSubscriptionOptions& opts);

// ALTER SUBSCRIPTION ... CONNECTION '...': change the publisher endpoint and
// reconnect.
void AlterSubscriptionConnection(ConnectionContext& conn_ctx,
                                 std::string_view name,
                                 std::string_view conninfo);

// ALTER SUBSCRIPTION ... {SET|ADD|DROP} PUBLICATION ...: change the replicated
// publication set and reconnect. `mode` is 'set' | 'add' | 'drop'.
void AlterSubscriptionPublication(ConnectionContext& conn_ctx,
                                  std::string_view name, std::string_view mode,
                                  std::vector<std::string>&& publications);

// ALTER SUBSCRIPTION ... RENAME TO / OWNER TO: rename or reassign ownership.
// Neither restarts the apply worker (transparent to the running stream).
void AlterSubscriptionRename(ConnectionContext& conn_ctx, std::string_view name,
                             std::string_view new_name);
void AlterSubscriptionOwner(ConnectionContext& conn_ctx, std::string_view name,
                            std::string_view owner);

}  // namespace sdb::pg
