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

#include "catalog/record.h"

#include <cstring>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/create_trigger_info.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>
#include <string>

#include "catalog/base_catalog.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/search_table.h"
#include "catalog/secret_store.h"
#include "search/log_entry.h"

namespace sdb::catalog {
namespace {

BaseCatalog& Owner(duckdb::CatalogEntry& entry) {
  return entry.ParentCatalog().Cast<BaseCatalog>();
}

std::string SerializeSecret(const duckdb::CatalogEntry& entry) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  entry.Cast<duckdb::SecretCatalogEntry>().secret->secret->Serialize(
    serializer);
  serializer.End();
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

void ReplaySecret(duckdb::ClientContext& context,
                  duckdb::Deserializer& object) {
  const auto kind =
    static_cast<ChangeKind>(object.ReadProperty<uint8_t>(100, "kind"));
  const duckdb::Identifier name{object.ReadProperty<std::string>(101, "name")};
  auto bytes = object.ReadPropertyWithDefault<std::string>(102, "secret");
  auto& cluster = ClusterOf(context);
  auto& set = cluster.Secrets(true);
  const auto transaction = cluster.GetCatalogTransaction(context);
  DeclareModified(transaction, cluster);
  if (kind == ChangeKind::Drop) {
    set.Drop(transaction, name);
    return;
  }
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(bytes.data()), bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  auto secret =
    duckdb::SecretManager::Get(context).DeserializeSecret(deserializer);
  deserializer.End();
  set.Create(
    transaction,
    MakeSecretEntry(cluster, std::move(secret),
                    duckdb::SecretManager::LOCAL_FILE_STORAGE_NAME, true));
}

BaseCatalog& ReplayCatalog(duckdb::ClientContext& context, duckdb::idx_t id) {
  auto& cluster = ClusterOf(context);
  if (cluster.RecordId() == id) {
    return cluster;
  }
  for (const auto& db :
       SereneDBDatabases(duckdb::DatabaseManager::Get(context))) {
    if (auto& catalog = db->GetCatalog().Cast<SereneDBCatalog>();
        catalog.Durable() && catalog.RecordId() == id) {
      return catalog;
    }
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("the log names a missing catalog ", id));
}

duckdb::unique_ptr<duckdb::AlterInfo> DeserializeAlter(
  duckdb::ClientContext& context, std::string& bytes) {
  duckdb::MemoryStream stream{
    reinterpret_cast<duckdb::data_ptr_t>(bytes.data()), bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Set<duckdb::ClientContext&>(context);
  deserializer.Begin();
  auto info = deserializer.ReadProperty<duckdb::unique_ptr<duckdb::ParseInfo>>(
    100, "info");
  deserializer.End();
  return duckdb::unique_ptr_cast<duckdb::ParseInfo, duckdb::AlterInfo>(
    std::move(info));
}

duckdb::TableCatalogEntry& LoggedTable(BaseCatalog& catalog,
                                       duckdb::CatalogTransaction transaction,
                                       duckdb::idx_t oid) {
  auto table = catalog.FindRelation(transaction, oid);
  SDB_ENSURE(table && table->type == duckdb::CatalogType::TABLE_ENTRY,
             "the log changes a missing table ", oid);
  return table->Cast<duckdb::TableCatalogEntry>();
}

void ReplayChange(duckdb::ClientContext& context,
                  duckdb::Deserializer& object) {
  auto& catalog =
    ReplayCatalog(context, object.ReadProperty<duckdb::idx_t>(100, "catalog"));
  const auto kind =
    static_cast<ChangeKind>(object.ReadProperty<uint8_t>(101, "kind"));
  auto info =
    object.ReadPropertyWithDefault<duckdb::unique_ptr<duckdb::CreateInfo>>(
      102, "info");
  auto alter = object.ReadPropertyWithDefault<std::string>(103, "alter");
  const auto oid = object.ReadPropertyWithDefault<duckdb::idx_t>(104, "oid");
  const auto owner =
    object.ReadPropertyWithDefault<duckdb::idx_t>(107, "owner");
  auto dependencies =
    object.ReadPropertyWithExplicitDefault<duckdb::LogicalDependencyList>(
      108, "dependencies", duckdb::LogicalDependencyList{});
  const auto transaction = catalog.GetCatalogTransaction(context);
  switch (kind) {
    case ChangeKind::Create:
      if (owner) {
        LoggedTable(catalog, transaction, owner)
          .CreateTrigger(transaction, info->Cast<duckdb::CreateTriggerInfo>());
        return;
      }
      catalog.ReplayCreate(context, std::move(info));
      return;
    case ChangeKind::Alter: {
      auto info = DeserializeAlter(context, alter);
      info->bind_mode = duckdb::AlterBindMode::SKIP_BINDING;
      catalog.Alter(context, *info);
      if (!dependencies.Set().empty()) {
        auto& table = LoggedTable(catalog, transaction, oid);
        catalog.Oids().RemoveDependents(table);
        table.dependencies = std::move(dependencies);
        catalog.Oids().AddDependents(table);
      }
      return;
    }
    case ChangeKind::Drop: {
      auto entry = catalog.LookupOid(transaction, oid);
      SDB_ENSURE(entry, "the log drops a missing entry ", oid);
      if (owner) {
        LoggedTable(catalog, transaction, owner)
          .DropTrigger(transaction, entry->name, false);
        return;
      }
      catalog.ReplayDrop(context, *entry);
      return;
    }
    case ChangeKind::Rename:
      break;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("the log holds an unknown catalog change"));
}

void ReplaySection(duckdb::ClientContext& context, Lsn begin,
                   duckdb::Deserializer& object) {
  auto& catalog =
    ReplayCatalog(context, object.ReadProperty<duckdb::idx_t>(100, "catalog"));
  const auto oid = object.ReadProperty<duckdb::idx_t>(101, "table");
  auto entries =
    object.ReadProperty<duckdb::vector<std::string>>(102, "entries");
  auto table =
    catalog.FindRelation(catalog.GetCatalogTransaction(context), oid);
  if (!table) {
    return;
  }
  SDB_ENSURE(IsSearchTable(*table), "the log writes a missing table ", oid);
  for (auto& entry : entries) {
    search::ReplayLogEntry(context, table->Cast<SearchTableEntry>(), entry,
                           begin);
  }
}

void WriteSection(duckdb::Serializer::List& list, duckdb::idx_t catalog,
                  duckdb::idx_t table,
                  const duckdb::vector<std::string>& entries) {
  list.WriteObject([&](duckdb::Serializer& object) {
    object.WriteProperty(100, "catalog", catalog);
    object.WriteProperty(101, "table", table);
    object.WriteProperty(102, "entries", entries);
  });
}

void ReplayLsn(duckdb::ClientContext& context, duckdb::Deserializer& object) {
  auto& catalog =
    ReplayCatalog(context, object.ReadProperty<duckdb::idx_t>(100, "catalog"));
  const auto oid = object.ReadProperty<duckdb::idx_t>(101, "oid");
  const auto type = object.ReadProperty<duckdb::CatalogType>(102, "type");
  const auto name = object.ReadPropertyWithDefault<std::string>(103, "name");
  LsnUpdate update{
    .relation = object.ReadPropertyWithDefault<duckdb::idx_t>(104, "relation"),
    .remote_lsn = object.ReadProperty<uint64_t>(105, "remote_lsn"),
    .assign = object.ReadPropertyWithDefault<bool>(106, "assign"),
  };
  const auto transaction = catalog.GetCatalogTransaction(context);
  auto entry = catalog.LookupOid(transaction, oid);
  if (!entry || entry->type != type) {
    entry = catalog.Set(type).Get(transaction, duckdb::Identifier{name});
  }
  if (!entry) {
    return;
  }
  update.entry = &entry->Cast<ReplicationLsnEntry>();
  ApplyLsnUpdate(update);
}

void ReplaySequence(duckdb::ClientContext& context,
                    duckdb::Deserializer& object) {
  auto& catalog =
    ReplayCatalog(context, object.ReadProperty<duckdb::idx_t>(100, "catalog"));
  const auto oid = object.ReadProperty<duckdb::idx_t>(101, "oid");
  const auto usage_count =
    object.ReadPropertyWithDefault<uint64_t>(102, "usage_count");
  const auto counter = object.ReadPropertyWithDefault<int64_t>(103, "counter");
  const auto last_value =
    object.ReadPropertyWithDefault<duckdb::optional<int64_t>>(104,
                                                              "last_value");
  auto entry = catalog.LookupOid(catalog.GetCatalogTransaction(context), oid);
  if (entry && entry->type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    entry->Cast<duckdb::SequenceCatalogEntry>().ReplayValue(
      usage_count, counter, last_value);
  }
}

void WriteSequence(duckdb::Serializer& object, duckdb::idx_t catalog,
                   const duckdb::SequenceValue& value) {
  object.WriteProperty(100, "catalog", catalog);
  object.WriteProperty(101, "oid", value.entry->oid);
  object.WritePropertyWithDefault<uint64_t>(102, "usage_count",
                                            value.usage_count);
  object.WritePropertyWithDefault<int64_t>(103, "counter", value.counter);
  object.WritePropertyWithDefault<duckdb::optional<int64_t>>(104, "last_value",
                                                             value.last_value);
}

void WriteNothing(duckdb::Serializer& serializer, duckdb::field_id_t id,
                  const char* tag) {
  serializer.WriteList(id, tag, 0,
                       [](duckdb::Serializer::List&, duckdb::idx_t) {});
}

std::vector<uint8_t> Join(std::span<const uint8_t> catalog,
                          std::span<const uint8_t> search) {
  const auto size = static_cast<uint32_t>(catalog.size());
  std::vector<uint8_t> payload(sizeof(size) + catalog.size() + search.size());
  std::memcpy(payload.data(), &size, sizeof(size));
  if (!catalog.empty()) {
    std::memcpy(payload.data() + sizeof(size), catalog.data(), catalog.size());
  }
  if (!search.empty()) {
    std::memcpy(payload.data() + sizeof(size) + catalog.size(), search.data(),
                search.size());
  }
  return payload;
}

std::span<const uint8_t> Written(const duckdb::MemoryStream& stream) {
  return {stream.GetData(), stream.GetPosition()};
}

uint32_t CatalogSize(std::span<const uint8_t> payload) {
  uint32_t size = 0;
  SDB_ENSURE(payload.size() >= sizeof(size), "the log holds a cut record");
  std::memcpy(&size, payload.data(), sizeof(size));
  SDB_ENSURE(size <= payload.size() - sizeof(size),
             "the log holds a damaged record");
  return size;
}

}  // namespace

std::vector<uint8_t> SequenceRecord(const BaseCatalog& catalog,
                                    const duckdb::SequenceValue& value) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  WriteNothing(serializer, 100, "changes");
  WriteNothing(serializer, 101, "lsns");
  serializer.WriteList(102, "sequences", 1,
                       [&](duckdb::Serializer::List& list, duckdb::idx_t) {
                         list.WriteObject([&](duckdb::Serializer& object) {
                           WriteSequence(object, catalog.RecordId(), value);
                         });
                       });
  WriteNothing(serializer, 103, "secrets");
  serializer.End();
  return Join(Written(stream), {});
}

std::vector<uint8_t> SearchRecord(duckdb::idx_t catalog, duckdb::idx_t table,
                                  const duckdb::vector<std::string>& entries) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  serializer.WriteList(100, "search", 1,
                       [&](duckdb::Serializer::List& list, duckdb::idx_t) {
                         WriteSection(list, catalog, table, entries);
                       });
  serializer.End();
  return Join({}, Written(stream));
}

std::span<const uint8_t> CatalogPart(std::span<const uint8_t> payload) {
  return payload.subspan(sizeof(uint32_t), CatalogSize(payload));
}

std::string SerializeAlter(const duckdb::AlterInfo& info) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  serializer.WriteProperty(100, "info", &info);
  serializer.End();
  return std::string{reinterpret_cast<const char*>(stream.GetData()),
                     stream.GetPosition()};
}

std::vector<uint8_t> WriteRecord(duckdb::ClientContext& context,
                                 const SessionTransaction& transaction,
                                 search::Writes* writes) {
  std::vector<const Change*> changes;
  std::vector<const Change*> secrets;
  std::vector<duckdb::SequenceCatalogEntry*> sequences;
  for (const auto& change : transaction.Changes()) {
    auto& subject = change.Subject();
    if (subject.type == duckdb::CatalogType::SECRET_ENTRY) {
      if (!subject.temporary) {
        secrets.push_back(&change);
      }
      continue;
    }
    if (change.kind != ChangeKind::Rename &&
        (change.kind != ChangeKind::Alter || !change.alter.empty()) &&
        Owner(subject).Durable()) {
      changes.push_back(&change);
      if (subject.type == duckdb::CatalogType::SEQUENCE_ENTRY &&
          change.kind != ChangeKind::Drop && !subject.temporary) {
        sequences.push_back(&subject.Cast<duckdb::SequenceCatalogEntry>());
      }
    }
  }
  std::vector<search::LogSection> sections;
  if (writes) {
    sections = writes->TakeLog(context, transaction.Snapshot());
    std::erase_if(sections, [](const search::LogSection& section) {
      return section.entries.empty() ||
             !section.table->ParentCatalog().Cast<BaseCatalog>().Durable();
    });
  }
  const auto& lsns = transaction.LsnUpdates();
  duckdb::MemoryStream catalog;
  if (!changes.empty() || !lsns.empty() || !secrets.empty()) {
    duckdb::BinarySerializer serializer{catalog};
    serializer.Begin();
    serializer.WriteList(
      100, "changes", changes.size(),
      [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
        const auto& change = *changes[i];
        auto& subject = change.Subject();
        list.WriteObject([&](duckdb::Serializer& object) {
          object.WriteProperty(100, "catalog", Owner(subject).RecordId());
          object.WriteProperty(101, "kind", static_cast<uint8_t>(change.kind));
          switch (change.kind) {
            case ChangeKind::Create:
              object.WritePropertyWithDefault(102, "info",
                                              subject.GetSerializedInfo());
              break;
            case ChangeKind::Alter:
              object.WritePropertyWithDefault(103, "alter", change.alter);
              object.WriteProperty(104, "oid", subject.oid);
              break;
            case ChangeKind::Drop:
              object.WriteProperty(104, "oid", subject.oid);
              break;
            case ChangeKind::Rename:
              break;
          }
          object.WritePropertyWithDefault<duckdb::idx_t>(107, "owner",
                                                         change.set->Owner());
          if (change.kind == ChangeKind::Alter &&
              subject.type == duckdb::CatalogType::TABLE_ENTRY) {
            object.WriteProperty(
              108, "dependencies",
              subject.Cast<duckdb::TableCatalogEntry>().dependencies);
          }
        });
      });
    serializer.WriteList(
      101, "lsns", lsns.size(),
      [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
        const auto& update = lsns[i];
        list.WriteObject([&](duckdb::Serializer& object) {
          object.WriteProperty(100, "catalog", Owner(*update.entry).RecordId());
          object.WriteProperty(101, "oid", update.entry->oid);
          object.WriteProperty(102, "type", update.entry->type);
          object.WritePropertyWithDefault(
            103, "name", update.entry->name.GetIdentifierName());
          object.WritePropertyWithDefault(104, "relation", update.relation);
          object.WriteProperty(105, "remote_lsn", update.remote_lsn);
          object.WritePropertyWithDefault(106, "assign", update.assign);
        });
      });
    serializer.WriteList(102, "sequences", sequences.size(),
                         [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
                           auto& sequence = *sequences[i];
                           const auto data = sequence.GetData();
                           list.WriteObject([&](duckdb::Serializer& object) {
                             WriteSequence(object, Owner(sequence).RecordId(),
                                           {&sequence, data.usage_count,
                                            data.counter, data.last_value});
                           });
                         });
    serializer.WriteList(
      103, "secrets", secrets.size(),
      [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
        const auto& change = *secrets[i];
        const auto& subject = change.Subject();
        list.WriteObject([&](duckdb::Serializer& object) {
          object.WriteProperty(100, "kind", static_cast<uint8_t>(change.kind));
          object.WriteProperty(101, "name", subject.name.GetIdentifierName());
          if (change.kind == ChangeKind::Create) {
            object.WritePropertyWithDefault(102, "secret",
                                            SerializeSecret(subject));
          }
        });
      });
    serializer.End();
  }
  duckdb::MemoryStream search;
  if (!sections.empty()) {
    duckdb::BinarySerializer serializer{search};
    serializer.Begin();
    serializer.WriteList(
      100, "search", sections.size(),
      [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
        const auto& section = sections[i];
        WriteSection(
          list, section.table->ParentCatalog().Cast<BaseCatalog>().RecordId(),
          section.table->oid, section.entries);
      });
    serializer.End();
  }
  if (catalog.GetPosition() == 0 && search.GetPosition() == 0) {
    return {};
  }
  return Join(Written(catalog), Written(search));
}

void ReplayCatalogPart(duckdb::ClientContext& context,
                       std::span<uint8_t> catalog) {
  duckdb::MemoryStream stream{catalog.data(), catalog.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Set<duckdb::ClientContext&>(context);
  deserializer.Begin();
  deserializer.ReadList(
    100, "changes", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject(
        [&](duckdb::Deserializer& object) { ReplayChange(context, object); });
    });
  deserializer.ReadList(
    101, "lsns", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject(
        [&](duckdb::Deserializer& object) { ReplayLsn(context, object); });
    });
  deserializer.ReadList(
    102, "sequences", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject(
        [&](duckdb::Deserializer& object) { ReplaySequence(context, object); });
    });
  deserializer.ReadList(
    103, "secrets", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject(
        [&](duckdb::Deserializer& object) { ReplaySecret(context, object); });
    });
  deserializer.End();
}

void ReplayRecord(duckdb::ClientContext& context, const Log::Range& range,
                  std::span<uint8_t> payload) {
  const auto size = CatalogSize(payload);
  if (size != 0) {
    ReplayCatalogPart(context, payload.subspan(sizeof(uint32_t), size));
  }
  const auto search = payload.subspan(sizeof(uint32_t) + size);
  if (search.empty()) {
    return;
  }
  duckdb::MemoryStream stream{search.data(), search.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Set<duckdb::ClientContext&>(context);
  deserializer.Begin();
  deserializer.ReadList(100, "search",
                        [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
                          list.ReadObject([&](duckdb::Deserializer& object) {
                            ReplaySection(context, range.begin, object);
                          });
                        });
  deserializer.End();
}

}  // namespace sdb::catalog
