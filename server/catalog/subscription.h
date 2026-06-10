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
    std::string origin_name = "any";
  };

  Subscription(ObjectId schema_id, ObjectId id, std::string_view name,
               Config config);

  static std::shared_ptr<Subscription> Deserialize(duckdb::Deserializer& src,
                                                   ReadContext ctx);
  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  const Config& GetConfig() const noexcept { return _config; }

 private:
  const Config _config;
};

}  // namespace sdb::catalog
