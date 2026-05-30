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

#include <absl/strings/str_cat.h>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/identifiers/object_id.h"

namespace sdb {

struct Identifier : ObjectId {
  using ObjectId::ObjectId;
};

void VPackWrite(auto ctx, Identifier value) {
  ctx.vpack().add(absl::AlphaNum(value.id()).Piece());
}

void VPackRead(auto ctx, Identifier& value) {
  uint64_t id;

  if (!absl::SimpleAtoi(ctx.vpack().stringView(), &id)) {
    SDB_THROW(ERROR_BAD_PARAMETER, "Failed to parse identifier from string");
  }

  value = Identifier{id};
}

}  // namespace sdb
