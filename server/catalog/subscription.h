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

#include "catalog/object.h"

namespace sdb::catalog {

class Subscription final : public Object {
 public:
  struct Config {
    std::string conninfo;
    std::vector<std::string> publications;
    std::string slot_name;
    bool enabled = true;
    bool binary = false;
    bool copy_data = true;  // initial-sync existing rows on first connect
    bool disable_on_error = false;  // disable (vs retry) on an apply error
    std::string origin_name = "any";
    bool create_slot =
      true;  // WITH (create_slot=false): expect a pre-made slot
    // apply-worker synchronous_commit setting (stored; PG default 'off')
    std::string synchronous_commit = "off";
    bool password_required =
      true;  // require a password in conninfo (non-super owner)
    bool run_as_owner =
      false;                // apply with the subscription owner's privileges
    bool failover = false;  // publisher slot failover to standbys (stored)
  };

  Subscription(Permissions perm, ObjectId parent_id, ObjectId subscription_id,
               std::string_view sub_name, Config config);

  static std::shared_ptr<Subscription> Deserialize(duckdb::Deserializer& src,
                                                   ReadContext ctx);
  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  const Config& GetConfig() const noexcept { return _config; }

  void SetEnabled(bool enabled) noexcept { _config.enabled = enabled; }
  void SetBinary(bool binary) noexcept { _config.binary = binary; }
  void SetConfig(Config config) noexcept { _config = std::move(config); }

 private:
  Config _config;
};

}  // namespace sdb::catalog
