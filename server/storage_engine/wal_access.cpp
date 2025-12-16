////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "wal_access.h"

#include "app/app_server.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/table.h"
#include "rest_server/serened.h"

using namespace sdb;

bool WalAccessContext::shouldHandleDB(ObjectId dbid) const {
  return !filter.database.isSet() || filter.database == dbid;
}

bool WalAccessContext::shouldHandleFunction(ObjectId dbid, ObjectId id) const {
  return shouldHandleView(dbid, id);
}

bool WalAccessContext::shouldHandleView(ObjectId dbid, ObjectId id) const {
  if (!dbid.isSet() || !id.isSet() || !shouldHandleDB(dbid)) {
    return false;
  }
  return !filter.database.isSet() ||
         (filter.database == dbid &&
          (!filter.collection.isSet() || filter.collection == id));
}

bool WalAccessContext::shouldHandleCollection(ObjectId dbid, ObjectId cid) {
  if (!dbid.isSet() || !cid.isSet() || !shouldHandleDB(dbid)) {
    return false;
  }
  if (!filter.database.isSet() ||
      (filter.database == dbid &&
       (!filter.collection.isSet() || filter.collection == cid))) {
    return true;
  }
  return false;
}

const catalog::Database* WalAccessContext::LoadDatabase(ObjectId dbid) try {
  SDB_ASSERT(dbid.isSet());
  auto it = databases.try_emplace(dbid).first;
  if (!it->second) {
    it->second = SerenedServer::Instance()
                   .getFeature<catalog::CatalogFeature>()
                   .Local()
                   .GetSnapshot()
                   ->GetDatabase(dbid);
  }
  return it->second.get();
} catch (...) {
  return nullptr;
}

catalog::Table* WalAccessContext::loadCollection(ObjectId dbid,
                                                 ObjectId cid) try {
  SDB_ASSERT(dbid.isSet());
  SDB_ASSERT(cid.isSet());
  auto* database = LoadDatabase(dbid);
  if (!database) {
    return nullptr;
  }

  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Local();
  auto c = catalog.GetSnapshot()->GetObject<catalog::Table>(cid);
  if (!c) {
    return nullptr;
  }

  auto it = collections.try_emplace(cid, std::move(c)).first;
  return it->second.get();
} catch (...) {
  return nullptr;
}
