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

#include <iresearch/search/filters/filter.hpp>

namespace sdb::connector {

struct ScanGlobalState;

// Moves the stored-terms check of wildcard and regexp n-gram filters out of
// the query into a table filter. Only the root and the Must children of a
// root BooleanFilter qualify: a document passes the root only if it passes
// each of them, so checking it after the rest of the tree gives the same set.
void DeferWildcardVerify(irs::Filter& root);

void AddDeferredVerifyFilters(ScanGlobalState& state, const irs::Filter& root);

}  // namespace sdb::connector
