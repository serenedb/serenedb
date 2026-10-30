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

#include "auth/lookup.h"

#include <duckdb/planner/binder.hpp>

namespace sdb::auth {

duckdb::optional_ptr<duckdb::CatalogEntry> Lookup(
  duckdb::ClientContext& context, const duckdb::EntryLookupInfo& lookup,
  duckdb::OnEntryNotFound if_not_found) {
  auto binder = duckdb::Binder::CreateBinder(context);
  binder->SetPrincipal(context.principal);
  return binder->EntryRetriever().GetEntry(lookup, if_not_found);
}

}  // namespace sdb::auth
