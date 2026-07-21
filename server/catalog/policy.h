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

#include <string>
#include <vector>

#include "catalog/object.h"
#include "catalog/persistence/policy.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

using persistence::PolicyCommand;

class Policy : public Object {
 public:
  Policy(ObjectId database_id, ObjectId schema_id, ObjectId id,
         ObjectId relation_id, persistence::PolicyData data);

  static std::shared_ptr<Policy> Deserialize(duckdb::Deserializer& src,
                                             ReadContext ctx);
  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  ObjectId GetRelationId() const noexcept { return _relation_id; }

  PolicyCommand Command() const noexcept { return _data.command; }
  bool Permissive() const noexcept { return _data.permissive; }
  const std::vector<ObjectId>& Roles() const noexcept { return _data.roles; }
  bool AppliesToPublic() const noexcept { return _data.roles.empty(); }

  bool HasUsing() const noexcept { return _data.has_using; }
  const std::string& UsingText() const noexcept { return _data.using_text; }
  bool HasCheck() const noexcept { return _data.has_check; }
  const std::string& CheckText() const noexcept { return _data.check_text; }

  persistence::PolicyData& MutableData() noexcept { return _data; }

 private:
  ObjectId _database_id;
  ObjectId _relation_id;
  persistence::PolicyData _data;
};

class RowSecurity : public Object {
 public:
  RowSecurity(ObjectId database_id, ObjectId schema_id, ObjectId id,
              ObjectId relation_id, persistence::RowSecurityData data);

  static std::shared_ptr<RowSecurity> Deserialize(duckdb::Deserializer& src,
                                                  ReadContext ctx);
  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  ObjectId GetRelationId() const noexcept { return _relation_id; }

  bool Enabled() const noexcept { return _data.enabled; }
  bool Forced() const noexcept { return _data.forced; }

 private:
  ObjectId _database_id;
  ObjectId _relation_id;
  persistence::RowSecurityData _data;
};

}  // namespace sdb::catalog
