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

#include <folly/json.h>
#include <velox/serializers/UnsafeRowSerializer.h>
#include <velox/type/Type.h>
#include <vpack/vpack.h>

#include "basics/errors.h"
#include "basics/exceptions.h"

namespace facebook::velox {

void VPackWrite(auto ctx, const velox::RowTypePtr& row_type) {
  // TODO(mbkkt): Make it faster.
  if (row_type) {
    const auto object = row_type->serialize();
    const auto json = folly::toJson(object);
    ctx.vpack().add(json);
  } else {
    ctx.vpack().add(vpack::Slice::nullSlice());
  }
}

void VPackRead(auto ctx, velox::RowTypePtr& row_type) {
  // TODO(mbkkt): Make it faster.
  auto vpack = ctx.vpack();
  if (vpack.isString()) {
    const auto json = vpack.stringViewUnchecked();
    const auto object = folly::parseJson(json);
    row_type = velox::ISerializable::deserialize<velox::RowType>(object);
  } else if (vpack.isNull()) {
    row_type = nullptr;
  } else {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "Invalid value for row type, expecting a string or null");
  }
}

void VPackWrite(auto ctx, const velox::TypePtr& type) {
  // TODO(mbkkt): Make it faster.
  if (type) {
    const auto object = type->serialize();
    const auto json = folly::toJson(object);
    ctx.vpack().add(json);
  } else {
    ctx.vpack().add(vpack::Slice::nullSlice());
  }
}

void VPackRead(auto ctx, velox::TypePtr& type) {
  // TODO(mbkkt): Make it faster.
  auto vpack = ctx.vpack();
  if (vpack.isString()) {
    const auto json = vpack.stringViewUnchecked();
    const auto object = folly::parseJson(json);
    type = velox::ISerializable::deserialize<velox::Type>(object);
  } else if (vpack.isNull()) {
    type = nullptr;
  } else {
    SDB_THROW(sdb::ERROR_BAD_PARAMETER,
              "Invalid value for row type, expecting a string or null");
  }
}

}  // namespace facebook::velox
