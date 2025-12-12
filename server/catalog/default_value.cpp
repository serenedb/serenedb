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

#include "default_value.h"

#include <vpack/builder.h>
#include <vpack/slice.h>

#include <memory>
#include <utility>

#include "basics/errors.h"
#include "basics/result.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "pg/sql_collector.h"
#include "pg/sql_parser.h"
#include "pg/sql_resolver.h"
#include "pg/sql_utils.h"
#include "utils/query_string.h"
#include "vpack/vpack_helper.h"

namespace sdb {
namespace {

Result ParseDefaultValue(ObjectId database_id, std::string_view query,
                         pg::Objects& objects,
                         pg::MemoryContextPtr& memory_context,
                         const Node*& expr) {
  return basics::SafeCall([&] -> Result {
    const QueryString query_string{query};
    memory_context = pg::CreateMemoryContext();
    expr = pg::ParseSingleExpression(*memory_context, query_string);
    SDB_ASSERT(expr);
    SDB_ASSERT(objects.getObjects().empty());

    auto database = catalog::GetDatabase(database_id);
    if (!database) {
      return std::move(database).error();
    }

    pg::CollectExpr((*database)->GetName(), *expr, objects);

    return {};
  });
}

}  // namespace

Result DefaultValue::Init(ObjectId database, const Node* expr) {
  if (!expr) {
    return {ERROR_BAD_PARAMETER, "default value expression cannot be null"};
  }
  auto query = pg::Deparse(const_cast<Node*>(expr));
  return Init(database, std::move(query));
}

Result DefaultValue::Init(ObjectId database, std::string query) {
  auto r = ParseDefaultValue(database, query, _objects, _memory_context, _expr);
  if (!r.ok()) {
    return r;
  }
  r = basics::SafeCall([&] {
    pg::Objects objects;
    pg::Disallowed disallowed{};
    pg::Resolve(database, objects);
  });
  if (!r.ok()) {
    return r;
  }
  _query = std::move(query);
  return r;
}

Result DefaultValue::FromVPack(ObjectId database, vpack::Slice slice,
                               std::unique_ptr<DefaultValue>& default_value) {
  auto query = basics::VPackHelper::getString(slice, "query", {});
  if (query.empty()) {
    return {ERROR_BAD_PARAMETER,
            "default value query must be a non-empty string"};
  }
  auto impl = std::make_unique<DefaultValue>();
  auto r = ParseDefaultValue(database, query, impl->_objects,
                             impl->_memory_context, impl->_expr);
  if (!r.ok()) {
    return r;
  }
  impl->_query = std::move(query);
  default_value = std::move(impl);
  return {};
}

void DefaultValue::ToVPack(vpack::Builder& builder) const {
  builder.openObject();
  builder.add("query", _query);
  builder.close();
}

// void VPackWrite(auto ctx, const DefaultValue& default_value) {
// vpack::Builder builder;
// default_value.ToVPack(builder);
// ctx.vpack().add(builder.slice());
// }

// void VPackRead(auto ctx, DefaultValue& default_value) {
// auto vpack = ctx.vpack();
// if (vpack.isObject()) {
//   std::unique_ptr<DefaultValue> impl;
//   auto r = DefaultValue::FromVPack(ObjectId::Invalid(), vpack, impl);
//   if (!r.ok()) {
//     SDB_THROW(r.code(), "Failed to read default value from vpack: {}",
//               r.message());
//   }
//   default_value = std::move(*impl);
// } else {
//   SDB_THROW(sdb::ERROR_BAD_PARAMETER,
//             "Invalid value for default value, expecting an object");
// }
// }

}  // namespace sdb
