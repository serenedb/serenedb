////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "create_subscription.h"

#include <absl/strings/match.h>

#include <algorithm>
#include <functional>

#include "catalog/catalog.h"
#include "catalog/role.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "replication/pg_replication_client.h"
#include "replication/subscription_engine.h"

namespace sdb::pg {
namespace {

// PostgreSQL only lets a superuser disable the password requirement.
void CheckPasswordRequiredPrivilege(ConnectionContext& conn_ctx,
                                    const catalog::Subscription::Config& cfg) {
  if (cfg.password_required) {
    return;
  }
  const ObjectId role_id = conn_ctx.GetRoleId();
  const auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  for (const auto& role : snapshot->GetRoles()) {
    if (role->GetId() == role_id) {
      if (role->IsSuperuser()) {
        return;
      }
      break;
    }
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                  ERR_MSG("password_required=false is superuser-only"));
}

}  // namespace

void CreateSubscription(ConnectionContext& conn_ctx, std::string_view sub_name,
                        catalog::Subscription::Config cfg, bool connect) {
  auto db_id = conn_ctx.GetDatabaseId();
  auto& catalog = catalog::GetCatalog();

  if (!absl::EqualsIgnoreCase(cfg.origin_name, "any") &&
      !absl::EqualsIgnoreCase(cfg.origin_name, "none")) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                    ERR_MSG("unrecognized origin value: \"", cfg.origin_name,
                            "\" (expected \"any\" or \"none\")"));
  }
  // Only a superuser may create a subscription with password_required = false.
  CheckPasswordRequiredPrivilege(conn_ctx, cfg);

  // WITH (create_slot = false) requires an explicit slot_name (the slot must
  // already exist on the publisher -- we won't create it).
  if (!cfg.create_slot && cfg.slot_name.empty()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_SYNTAX_ERROR),
      ERR_MSG("slot_name must be specified when create_slot = false"));
  }
  // by default slot_name is equal subscription name.
  if (cfg.slot_name.empty()) {
    cfg.slot_name = sub_name;
  }
  // WITH (connect = false): create it disabled -- our async apply has no
  // synchronous-connect step, so this is the only meaningful sense of connect.
  if (!connect) {
    cfg.enabled = false;
  }

  const ObjectId role_id = conn_ctx.GetRoleId();

  auto subscription = std::make_shared<catalog::Subscription>(
    role_id, db_id, catalog::NextId(), sub_name, std::move(cfg));

  catalog.CreateSubscription(catalog::AccessContext{role_id}, db_id,
                             subscription);

  // Start streaming immediately (PostgreSQL's default connect=true), unless
  // created disabled. Engine absent only during bootstrap/tests.
  if (subscription->GetConfig().enabled) {
    if (auto* engine = replication::SubscriptionEngine::gInstance) {
      engine->StartSubscription(db_id, subscription);
    }
  }
}

void DropSubscription(ConnectionContext& conn_ctx, std::string_view name,
                      bool missing_ok) {
  const auto db_id = conn_ctx.GetDatabaseId();
  const ObjectId role_id = conn_ctx.GetRoleId();
  auto& catalog = catalog::GetCatalog();

  bool found = false;
  ObjectId sub_id;
  // Captured before the catalog drop so we can still reach the publisher and
  // drop its replication slot (the config is gone once the entry is removed).
  replication::ReplicationTarget target;
  {
    const auto snapshot = catalog.GetCatalogSnapshot();
    for (const auto& sub : snapshot->GetSubscriptions(db_id)) {
      if (sub->GetName() == name) {
        found = true;
        sub_id = sub->GetId();
        const auto& cfg = sub->GetConfig();
        target = replication::ParseConnInfo(cfg.conninfo);
        target.slot_name = cfg.slot_name;
        target.subscription_name = sub->GetName();
        break;
      }
    }
  }

  if (!found) {
    if (missing_ok) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("subscription \"", name, "\" does not exist"));
  }

  // Ownership is enforced inside the catalog (throws on failure); missing_ok is
  // already handled above, so a plain drop suffices here.
  catalog.DropSubscription(catalog::AccessContext{role_id}, db_id, name,
                           /*missing_ok=*/true);

  if (auto* engine = replication::SubscriptionEngine::gInstance) {
    // Stop the streaming client first (releases the slot on the publisher),
    // then drop the now-inactive slot so it doesn't leak and pin WAL.
    engine->StopSubscription(sub_id);
    engine->DropRemoteSlot(std::move(target));
  }
}

void AlterSubscriptionEnabled(ConnectionContext& conn_ctx,
                              std::string_view name, bool enabled) {
  const auto db_id = conn_ctx.GetDatabaseId();
  const ObjectId role_id = conn_ctx.GetRoleId();
  auto& catalog = catalog::GetCatalog();

  catalog.SetSubscriptionEnabled(catalog::AccessContext{role_id}, db_id, name,
                                 enabled);

  auto* engine = replication::SubscriptionEngine::gInstance;
  if (engine == nullptr) {
    return;
  }
  std::shared_ptr<catalog::Subscription> sub;
  {
    const auto snapshot = catalog.GetCatalogSnapshot();
    for (const auto& s : snapshot->GetSubscriptions(db_id)) {
      if (s->GetName() == name) {
        sub = s;
        break;
      }
    }
  }
  if (!sub) {
    return;
  }
  if (enabled) {
    engine->StartSubscription(db_id, sub);
  } else {
    engine->StopSubscription(sub->GetId());
  }
}

namespace {

std::shared_ptr<catalog::Subscription> FindSubscription(
  catalog::Catalog& catalog, ObjectId db_id, std::string_view name) {
  const auto snapshot = catalog.GetCatalogSnapshot();
  for (const auto& s : snapshot->GetSubscriptions(db_id)) {
    if (s->GetName() == name) {
      return s;
    }
  }
  return nullptr;
}

// Read the subscription's current config, apply `mutate`, persist it, and
// restart the running client so it reconnects with the new config.
void ApplyConfigChange(
  ConnectionContext& conn_ctx, std::string_view name,
  const std::function<void(catalog::Subscription::Config&)>& mutate) {
  const auto db_id = conn_ctx.GetDatabaseId();
  const ObjectId role_id = conn_ctx.GetRoleId();
  auto& catalog = catalog::GetCatalog();

  auto sub = FindSubscription(catalog, db_id, name);
  if (!sub) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("subscription \"", name, "\" does not exist"));
  }
  auto cfg = sub->GetConfig();
  mutate(cfg);
  catalog.SetSubscriptionConfig(catalog::AccessContext{role_id}, db_id, name,
                                std::move(cfg));

  if (auto* engine = replication::SubscriptionEngine::gInstance) {
    if (auto updated = FindSubscription(catalog, db_id, name)) {
      engine->RestartSubscription(db_id, updated);
    }
  }
}

}  // namespace

void AlterSubscriptionSet(ConnectionContext& conn_ctx, std::string_view name,
                          const AlterSubscriptionOptions& opts) {
  ApplyConfigChange(conn_ctx, name, [&](catalog::Subscription::Config& cfg) {
    if (opts.binary) {
      cfg.binary = *opts.binary;
    }
    if (opts.disable_on_error) {
      cfg.disable_on_error = *opts.disable_on_error;
    }
    if (opts.origin) {
      if (!absl::EqualsIgnoreCase(*opts.origin, "any") &&
          !absl::EqualsIgnoreCase(*opts.origin, "none")) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_SYNTAX_ERROR),
                        ERR_MSG("unrecognized origin value: \"", *opts.origin,
                                "\" (expected \"any\" or \"none\")"));
      }
      cfg.origin_name = *opts.origin;
    }
    if (opts.synchronous_commit) {
      cfg.synchronous_commit = *opts.synchronous_commit;
    }
    if (opts.run_as_owner) {
      cfg.run_as_owner = *opts.run_as_owner;
    }
    if (opts.failover) {
      cfg.failover = *opts.failover;
    }
    if (opts.password_required) {
      cfg.password_required = *opts.password_required;
      CheckPasswordRequiredPrivilege(conn_ctx, cfg);
    }
  });
}

void AlterSubscriptionConnection(ConnectionContext& conn_ctx,
                                 std::string_view name,
                                 std::string_view conninfo) {
  ApplyConfigChange(conn_ctx, name, [&](catalog::Subscription::Config& cfg) {
    cfg.conninfo = conninfo;
  });
}

void AlterSubscriptionPublication(ConnectionContext& conn_ctx,
                                  std::string_view name, std::string_view mode,
                                  std::vector<std::string>&& publications) {
  ApplyConfigChange(conn_ctx, name, [&](catalog::Subscription::Config& cfg) {
    if (mode == "set") {
      cfg.publications = std::move(publications);
    } else if (mode == "add") {
      for (auto& pub : publications) {
        if (std::find(cfg.publications.begin(), cfg.publications.end(), pub) ==
            cfg.publications.end()) {
          cfg.publications.push_back(std::move(pub));
        }
      }
    } else {  // "drop"
      for (const auto& pub : publications) {
        std::erase(cfg.publications, pub);
      }
    }
    if (cfg.publications.empty()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_SYNTAX_ERROR),
        ERR_MSG("subscription must contain at least one publication"));
    }
  });
}

void AlterSubscriptionRename(ConnectionContext& conn_ctx, std::string_view name,
                             std::string_view new_name) {
  const ObjectId role_id = conn_ctx.GetRoleId();
  catalog::GetCatalog().RenameSubscription(
    catalog::AccessContext{role_id}, conn_ctx.GetDatabaseId(), name, new_name);
}

void AlterSubscriptionOwner(ConnectionContext& conn_ctx, std::string_view name,
                            std::string_view owner) {
  const ObjectId role_id = conn_ctx.GetRoleId();
  auto& catalog = catalog::GetCatalog();
  auto role = catalog.GetCatalogSnapshot()->GetRole(owner);
  if (!role) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("role \"", owner, "\" does not exist"));
  }
  catalog.SetSubscriptionOwner(catalog::AccessContext{role_id},
                               conn_ctx.GetDatabaseId(), name, role->GetId());
}

}  // namespace sdb::pg
