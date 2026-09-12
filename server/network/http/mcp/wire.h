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

#pragma once

#include <iresearch/utils/pg/sql_exception_macro.h>
#include <iresearch/utils/serializer.h>
#include <simdjson.h>

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <variant>

#include "server/utils/simdjson_sink.h"

namespace sdb::network::http::mcp {

struct RpcId {
  bool present = false;
  std::variant<std::monostate, int64_t, std::string> value;
};

template<typename Context>
  requires requires(Context ctx) { ctx.io().Type(); }
void SerdeRead(Context ctx, RpcId& id) {
  using JsonType = basics::JsonSource::JsonType;
  id.present = true;
  switch (ctx.io().Type()) {
    case JsonType::string:
      id.value = ctx.io().ReadString();
      return;
    case JsonType::number:
      id.value = ctx.io().ReadSignedInt64();
      return;
    case JsonType::null:
      id.value = std::monostate{};
      return;
    default:
      THROW_SQL_ERROR(ERR_MSG("id must be a string, a number or null"));
  }
}

template<typename Context>
  requires requires(Context ctx) { ctx.io().WriteNull(); }
void SerdeWrite(Context ctx, const RpcId& id) {
  std::visit(
    [&]<typename V>(const V& v) {
      if constexpr (std::is_same_v<V, std::monostate>) {
        ctx.io().WriteNull();
      } else if constexpr (std::is_same_v<V, int64_t>) {
        ctx.io().WriteValue(v);
      } else {
        ctx.io().WriteValue(std::string_view{v});
      }
    },
    id.value);
}

using EmptyObject = std::map<std::string, bool>;

template<typename T>
std::string ToJson(const T& value) {
  simdjson::builder::string_builder sb;
  basics::JsonSink sink{sb};
  basics::WriteObject(sink, value);
  return std::string{sb.view().value()};
}

}  // namespace sdb::network::http::mcp
