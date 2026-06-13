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

#include "catalog/subscription.h"

#include "basics/serializer.h"
#include "catalog/persistence/subscription.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {

Subscription::Subscription(ObjectId schema_id, ObjectId id,
                           std::string_view name, Config config)
  : Object{schema_id, id, name, ObjectType::Subscription},
    _config(std::move(config)) {}

std::shared_ptr<Subscription> Subscription::Deserialize(
  duckdb::Deserializer& src, ReadContext ctx) {
  persistence::SubscriptionData data{};
  basics::ReadTuple(src, data);

  return std::make_shared<Subscription>(
    ctx.database_id, ctx.id, data.slot_name,
    Config{
      .conninfo = std::move(data.conninfo),
      .publications = std::move(data.publications),
      .slot_name = data.slot_name,
      .enabled = data.enabled,
      .binary = data.binary,
      .origin_name = data.origin_name,
    });
}

void Subscription::Serialize(duckdb::Serializer& sink) const {
  persistence::SubscriptionData data{
    .conninfo = _config.conninfo,
    .publications = _config.publications,
    .slot_name = _config.slot_name,
    .enabled = _config.enabled,
    .binary = _config.binary,
    .origin_name = _config.origin_name,
  };
  basics::WriteTuple(sink, std::move(data));
}

std::shared_ptr<Object> Subscription::Clone() const {
  THROW_SQL_ERROR(ERR_MSG("Clone not implemented for Subscription"));
}

}  // namespace sdb::catalog
