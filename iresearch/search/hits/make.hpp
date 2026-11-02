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

#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/index/index_meta.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/window_set.hpp"
#include "iresearch/search/hits/root.hpp"
#include "iresearch/search/hits/walk.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {
namespace hits {

template<template<typename...> typename Shape, typename... Parts,
         typename... Args>
Root::ptr MakeShape(const Context&, Args&&... args) {
  return memory::make_managed<Shape<Parts...>>(std::forward<Args>(args)...);
}

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx);

Root::ptr MakeEmpty();

}  // namespace hits
}  // namespace irs
