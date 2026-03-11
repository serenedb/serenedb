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

#include <velox/vector/ComplexVector.h>

#include <yaclib/async/future.hpp>

#include "basics/fwd.h"

namespace sdb::query {

class Query;

class Executor {
 public:
  virtual void Init(Query& query) = 0;
  virtual yaclib::Future<> Execute(velox::RowVectorPtr& batch) = 0;
  virtual yaclib::Future<> RequestCancel() = 0;
  virtual ~Executor() = default;
};

}  // namespace sdb::query
