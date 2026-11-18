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

#include "catalog/secret_store.h"

#include <duckdb/main/secret/secret_manager.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <type_traits>
#include <utility>

#include "catalog/base_catalog.h"

namespace sdb::catalog {
namespace {

class SecretStore final : public duckdb::SecretStorage {
 public:
  SecretStore(ClusterCatalog& cluster, const std::string& name, bool persistent)
    : duckdb::SecretStorage{name, persistent ? LOCAL_FILE_STORAGE_OFFSET
                                             : TEMPORARY_STORAGE_OFFSET},
      _cluster{cluster},
      _set{cluster.Secrets(persistent)} {
    this->persistent = persistent;
  }

  duckdb::unique_ptr<duckdb::SecretEntry> StoreSecret(
    duckdb::unique_ptr<const duckdb::BaseSecret> secret,
    duckdb::OnCreateConflict on_conflict,
    duckdb::optional_ptr<duckdb::CatalogTransaction> transaction) final {
    SDB_ASSERT(transaction);
    const duckdb::Identifier name{secret->GetName()};
    if (_set.Get(*transaction, name)) {
      if (on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
        return nullptr;
      }
      if (on_conflict != duckdb::OnCreateConflict::REPLACE_ON_CONFLICT) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
          ERR_MSG("secret \"", name.GetIdentifierName(), "\" already exists"));
      }
      DeclareModified(*transaction, _cluster,
                      duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
      _set.Drop(*transaction, name);
    }
    auto entry =
      MakeSecretEntry(_cluster, std::move(secret), storage_name, persistent);
    auto result = duckdb::make_uniq<duckdb::SecretEntry>(
      *entry->Cast<duckdb::SecretCatalogEntry>().secret);
    DeclareModified(*transaction, _cluster);
    _set.Create(*transaction, std::move(entry));
    return result;
  }

  duckdb::vector<duckdb::SecretEntry> AllSecrets(
    duckdb::optional_ptr<duckdb::CatalogTransaction> transaction) final {
    return Read(transaction, [&](duckdb::CatalogTransaction view) {
      duckdb::vector<duckdb::SecretEntry> secrets;
      _set.Scan(view, [&](duckdb::CatalogEntry& entry) {
        secrets.push_back(*entry.Cast<duckdb::SecretCatalogEntry>().secret);
      });
      return secrets;
    });
  }

  void DropSecretByName(
    const duckdb::Identifier& name, duckdb::OnEntryNotFound on_entry_not_found,
    duckdb::optional_ptr<duckdb::CatalogTransaction> transaction) final {
    SDB_ASSERT(transaction);
    if (!_set.Get(*transaction, name)) {
      if (on_entry_not_found == duckdb::OnEntryNotFound::THROW_EXCEPTION) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
          ERR_MSG("secret \"", name.GetIdentifierName(), "\" does not exist"));
      }
      return;
    }
    DeclareModified(*transaction, _cluster,
                    duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
    _set.Drop(*transaction, name);
  }

  duckdb::SecretMatch LookupSecret(
    const std::string& path, const std::string& type,
    duckdb::optional_ptr<duckdb::CatalogTransaction> transaction) final {
    return Read(transaction, [&](duckdb::CatalogTransaction view) {
      duckdb::SecretMatch best;
      _set.Scan(view, [&](duckdb::CatalogEntry& entry) {
        auto& secret = *entry.Cast<duckdb::SecretCatalogEntry>().secret;
        if (secret.secret->GetType() == type) {
          best = SelectBestMatch(secret, path, tie_break_offset, best);
        }
      });
      return best;
    });
  }

  duckdb::unique_ptr<duckdb::SecretEntry> GetSecretByName(
    const std::string& name,
    duckdb::optional_ptr<duckdb::CatalogTransaction> transaction) final {
    return Read(transaction, [&](duckdb::CatalogTransaction view) {
      auto entry = _set.Get(view, duckdb::Identifier{name});
      return entry ? duckdb::make_uniq<duckdb::SecretEntry>(
                       *entry->Cast<duckdb::SecretCatalogEntry>().secret)
                   : nullptr;
    });
  }

 private:
  template<typename Body>
  std::invoke_result_t<Body&, duckdb::CatalogTransaction> Read(
    duckdb::optional_ptr<duckdb::CatalogTransaction> transaction, Body&& body) {
    if (transaction) {
      return body(*transaction);
    }
    const CommittedView view;
    return body(view.Transaction(_cluster.GetDatabase()));
  }

  ClusterCatalog& _cluster;
  EntrySet& _set;
};

}  // namespace

duckdb::unique_ptr<duckdb::SecretStorage> CreateSecretStorage(
  duckdb::DatabaseInstance& db, const std::string& name, bool persistent) {
  return duckdb::make_uniq<SecretStore>(ClusterOf(db), name, persistent);
}

duckdb::unique_ptr<duckdb::CatalogEntry> MakeSecretEntry(
  ClusterCatalog& cluster, duckdb::unique_ptr<const duckdb::BaseSecret> secret,
  std::string_view storage, bool persistent) {
  auto entry =
    duckdb::make_uniq<duckdb::SecretCatalogEntry>(std::move(secret), cluster);
  entry->temporary = !persistent;
  entry->secret->storage_mode = storage;
  entry->secret->persist_type = persistent
                                  ? duckdb::SecretPersistType::PERSISTENT
                                  : duckdb::SecretPersistType::TEMPORARY;
  return entry;
}

}  // namespace sdb::catalog
