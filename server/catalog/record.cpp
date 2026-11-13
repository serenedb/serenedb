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

#include <absl/algorithm/container.h>
#include <absl/container/btree_map.h>
#include <absl/strings/ascii.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <compare>
#include <cstring>
#include <duckdb/catalog/catalog_entry/sequence_catalog_entry.hpp>
#include <duckdb/common/serializer/binary_deserializer.hpp>
#include <duckdb/common/serializer/binary_serializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/main/secret/secret_manager.hpp>
#include <duckdb/parser/parsed_data/alter_job_info.hpp>
#include <duckdb/parser/parsed_data/alter_sequence_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/comment_on_column_info.hpp>
#include <duckdb/parser/parsed_data/create_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_trigger_info.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "catalog/base_catalog.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/role.h"
#include "catalog/entry/search_table.h"
#include "catalog/entry_set.h"
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

struct ChangeRecord {
  duckdb::idx_t catalog = 0;
  ChangeKind kind = ChangeKind::Create;
  duckdb::idx_t oid = 0;
  duckdb::idx_t owner = 0;
  std::string body;
  duckdb::vector<duckdb::idx_t> references;
  duckdb::vector<std::string> supersedes;
};

struct LsnRecord {
  duckdb::idx_t catalog = 0;
  duckdb::idx_t oid = 0;
  duckdb::CatalogType type = duckdb::CatalogType::INVALID;
  std::string name;
  duckdb::idx_t relation = 0;
  uint64_t remote_lsn = 0;
  bool assign = false;
};

struct SequenceRecordValue {
  duckdb::idx_t catalog = 0;
  duckdb::idx_t oid = 0;
  uint64_t usage_count = 0;
  int64_t counter = 0;
  duckdb::optional<int64_t> last_value;
};

struct SecretRecord {
  ChangeKind kind = ChangeKind::Create;
  std::string name;
  std::string secret;
};

struct CatalogRecord {
  std::vector<ChangeRecord> changes;
  std::vector<LsnRecord> lsns;
  std::vector<SequenceRecordValue> sequences;
  std::vector<SecretRecord> secrets;
  bool shutdown = false;

  bool Empty() const noexcept {
    return changes.empty() && lsns.empty() && sequences.empty() &&
           secrets.empty();
  }
};

constexpr auto kNoLife = std::numeric_limits<size_t>::max();

using Key = std::pair<duckdb::idx_t, duckdb::idx_t>;

struct Life {
  Key key;
  size_t create = kNoLife;
  size_t end = kNoLife;
  bool eliminated = false;
};

struct StateKey {
  size_t life = kNoLife;
  duckdb::idx_t catalog = 0;
  duckdb::idx_t oid = 0;
  duckdb::idx_t relation = 0;

  auto operator<=>(const StateKey&) const = default;
};

template<typename T>
struct Slot {
  size_t record = 0;
  size_t life = kNoLife;
  T value;
};

std::vector<uint8_t> WriteCatalogRecord(const CatalogRecord& record) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  serializer.WriteList(
    100, "changes", record.changes.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& change = record.changes[i];
      list.WriteObject([&](duckdb::Serializer& object) {
        object.WriteProperty(100, "catalog", change.catalog);
        object.WriteProperty(101, "kind", static_cast<uint8_t>(change.kind));
        object.WriteProperty(102, "oid", change.oid);
        object.WritePropertyWithDefault<duckdb::idx_t>(103, "owner",
                                                       change.owner);
        object.WritePropertyWithDefault(104, "body", change.body);
        object.WritePropertyWithDefault(105, "references", change.references);
        object.WritePropertyWithDefault(106, "supersedes", change.supersedes);
      });
    });
  serializer.WriteList(
    101, "lsns", record.lsns.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& update = record.lsns[i];
      list.WriteObject([&](duckdb::Serializer& object) {
        object.WriteProperty(100, "catalog", update.catalog);
        object.WriteProperty(101, "oid", update.oid);
        object.WriteProperty(102, "type", update.type);
        object.WritePropertyWithDefault(103, "name", update.name);
        object.WritePropertyWithDefault(104, "relation", update.relation);
        object.WriteProperty(105, "remote_lsn", update.remote_lsn);
        object.WritePropertyWithDefault(106, "assign", update.assign);
      });
    });
  serializer.WriteList(
    102, "sequences", record.sequences.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& value = record.sequences[i];
      list.WriteObject([&](duckdb::Serializer& object) {
        object.WriteProperty(100, "catalog", value.catalog);
        object.WriteProperty(101, "oid", value.oid);
        object.WritePropertyWithDefault<uint64_t>(102, "usage_count",
                                                  value.usage_count);
        object.WritePropertyWithDefault<int64_t>(103, "counter", value.counter);
        object.WritePropertyWithDefault<duckdb::optional<int64_t>>(
          104, "last_value", value.last_value);
      });
    });
  serializer.WriteList(
    103, "secrets", record.secrets.size(),
    [&](duckdb::Serializer::List& list, duckdb::idx_t i) {
      const auto& secret = record.secrets[i];
      list.WriteObject([&](duckdb::Serializer& object) {
        object.WriteProperty(100, "kind", static_cast<uint8_t>(secret.kind));
        object.WriteProperty(101, "name", secret.name);
        object.WritePropertyWithDefault(102, "secret", secret.secret);
      });
    });
  serializer.WritePropertyWithDefault(104, "shutdown", record.shutdown);
  serializer.End();
  return {stream.GetData(), stream.GetData() + stream.GetPosition()};
}

CatalogRecord ReadCatalogRecord(std::span<uint8_t> bytes) {
  CatalogRecord record;
  duckdb::MemoryStream stream{bytes.data(), bytes.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Begin();
  deserializer.ReadList(
    100, "changes", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::Deserializer& object) {
        record.changes.push_back({
          .catalog = object.ReadProperty<duckdb::idx_t>(100, "catalog"),
          .kind =
            static_cast<ChangeKind>(object.ReadProperty<uint8_t>(101, "kind")),
          .oid = object.ReadProperty<duckdb::idx_t>(102, "oid"),
          .owner = object.ReadPropertyWithDefault<duckdb::idx_t>(103, "owner"),
          .body = object.ReadPropertyWithDefault<std::string>(104, "body"),
          .references =
            object.ReadPropertyWithDefault<duckdb::vector<duckdb::idx_t>>(
              105, "references"),
          .supersedes =
            object.ReadPropertyWithDefault<duckdb::vector<std::string>>(
              106, "supersedes"),
        });
      });
    });
  deserializer.ReadList(
    101, "lsns", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::Deserializer& object) {
        record.lsns.push_back({
          .catalog = object.ReadProperty<duckdb::idx_t>(100, "catalog"),
          .oid = object.ReadProperty<duckdb::idx_t>(101, "oid"),
          .type = object.ReadProperty<duckdb::CatalogType>(102, "type"),
          .name = object.ReadPropertyWithDefault<std::string>(103, "name"),
          .relation =
            object.ReadPropertyWithDefault<duckdb::idx_t>(104, "relation"),
          .remote_lsn = object.ReadProperty<uint64_t>(105, "remote_lsn"),
          .assign = object.ReadPropertyWithDefault<bool>(106, "assign"),
        });
      });
    });
  deserializer.ReadList(
    102, "sequences", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::Deserializer& object) {
        record.sequences.push_back({
          .catalog = object.ReadProperty<duckdb::idx_t>(100, "catalog"),
          .oid = object.ReadProperty<duckdb::idx_t>(101, "oid"),
          .usage_count =
            object.ReadPropertyWithDefault<uint64_t>(102, "usage_count"),
          .counter = object.ReadPropertyWithDefault<int64_t>(103, "counter"),
          .last_value =
            object.ReadPropertyWithDefault<duckdb::optional<int64_t>>(
              104, "last_value"),
        });
      });
    });
  deserializer.ReadList(
    103, "secrets", [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
      list.ReadObject([&](duckdb::Deserializer& object) {
        record.secrets.push_back({
          .kind =
            static_cast<ChangeKind>(object.ReadProperty<uint8_t>(100, "kind")),
          .name = object.ReadProperty<std::string>(101, "name"),
          .secret = object.ReadPropertyWithDefault<std::string>(102, "secret"),
        });
      });
    });
  record.shutdown = deserializer.ReadPropertyWithDefault<bool>(104, "shutdown");
  deserializer.End();
  return record;
}

void ReplaySecret(duckdb::ClientContext& context, ReplayTarget target,
                  SecretRecord& record) {
  auto* owner = target(ClusterCatalog::kRecordId);
  if (!owner) {
    return;
  }
  auto& cluster = owner->Cast<ClusterCatalog>();
  auto& set = cluster.Secrets(true);
  const auto transaction = cluster.GetCatalogTransaction(context);
  DeclareModified(transaction, cluster);
  const duckdb::Identifier name{record.name};
  if (record.kind == ChangeKind::Drop) {
    set.Drop(transaction, name);
    return;
  }
  auto& bytes = record.secret;
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

void QualifyInLoggedSchema(BaseCatalog& catalog,
                           duckdb::CatalogTransaction transaction,
                           duckdb::CreateInfo& info) {
  if (!info.schema_oid) {
    return;
  }
  const auto entry = catalog.LookupOid(transaction, info.schema_oid);
  SDB_ENSURE(entry && entry->type == duckdb::CatalogType::SCHEMA_ENTRY,
             "the log creates in a missing schema ", info.schema_oid);
  const auto& schema = entry->Cast<duckdb::SchemaCatalogEntry>();
  auto path = schema.GetParentSchemaPath(transaction);
  path.push_back(schema.name);
  auto name = info.GetQualifiedName().Name();
  if (info.type == duckdb::CatalogType::SCHEMA_ENTRY) {
    path.push_back(info.Cast<duckdb::CreateSchemaInfo>().SchemaName());
  }
  info.SetQualifiedName(duckdb::QualifiedName::FromCatalogSchema(
    catalog.GetName(), std::move(path), std::move(name)));
}

struct ChangeContent {
  duckdb::unique_ptr<duckdb::CreateInfo> info;
  std::string alter;
  duckdb::LogicalDependencyList dependencies;
  duckdb::vector<duckdb::idx_t> created_constraints;
};

duckdb::vector<duckdb::idx_t> CreatedConstraints(const Change& change) {
  duckdb::vector<duckdb::idx_t> created;
  auto& version = *change.version;
  if (!version.HasChild() ||
      version.Child().type != duckdb::CatalogType::TABLE_ENTRY) {
    return created;
  }
  const auto& before =
    version.Child().Cast<duckdb::TableCatalogEntry>().GetConstraints();
  for (const auto& constraint :
       version.Cast<duckdb::TableCatalogEntry>().GetConstraints()) {
    if (absl::c_none_of(before, [&](const auto& old) {
          return old->oid == constraint->oid;
        })) {
      created.push_back(constraint->oid);
    }
  }
  return created;
}

duckdb::vector<duckdb::idx_t> References(const Change& change) {
  const auto& subject = change.Subject();
  duckdb::vector<duckdb::idx_t> references;
  const auto add = [&](duckdb::idx_t oid) {
    if (oid && oid != subject.oid) {
      references.push_back(oid);
    }
  };
  add(subject.ParentSchemaOid());
  if (change.set->Members()) {
    ForEachSubject(subject, add);
  } else {
    add(subject.permissions.owner);
    for (const auto& item : subject.permissions.acl) {
      add(item.grantee);
      add(item.grantor);
    }
  }
  for (const auto& defaults : subject.permissions.defaults) {
    add(defaults.role);
    for (const auto& item : defaults.acl) {
      add(item.grantee);
      add(item.grantor);
    }
  }
  if (subject.type == duckdb::CatalogType::TABLE_ENTRY) {
    for (const auto& column :
         subject.Cast<duckdb::TableCatalogEntry>().GetColumns().Logical()) {
      for (const auto& item : column.Acl()) {
        add(item.grantee);
        add(item.grantor);
      }
    }
  }
  if (subject.type == duckdb::CatalogType::ROLE_ENTRY) {
    for (const auto& membership : subject.Cast<RoleCatalogEntry>().MemberOf()) {
      add(membership.role);
      add(membership.grantor);
    }
  }
  absl::c_sort(references);
  references.erase(std::ranges::unique(references).begin(), references.end());
  return references;
}

constexpr std::string_view kComment = "comment";
constexpr std::string_view kDefinition = "definition";
constexpr std::string_view kConfig = "config";
constexpr std::string_view kFiring = "firing";
constexpr std::string_view kDefault = "default";
constexpr std::string_view kNotNull = "notnull";

std::string ColumnKey(std::string_view attribute,
                      const duckdb::CatalogEntry& subject,
                      const duckdb::Identifier& column) {
  if (subject.type == duckdb::CatalogType::TABLE_ENTRY) {
    const auto& columns =
      subject.Cast<duckdb::TableCatalogEntry>().GetColumns();
    if (columns.ColumnExists(column)) {
      return absl::StrCat(attribute, ":",
                          columns.GetColumn(column).CatalogOid());
    }
  }
  return absl::StrCat(attribute, ":name:", column.GetIdentifierName());
}

std::string OptionKey(std::string_view option) {
  return absl::StrCat("options:", absl::AsciiStrToLower(option));
}

void RoleKeys(const duckdb::AlterRoleInfo& alter,
              duckdb::vector<std::string>& keys) {
  if (!alter.new_name.GetIdentifierName().empty() ||
      !alter.grant_role.empty() || alter.grant_role_id != 0) {
    return;
  }
  const auto options = static_cast<uint32_t>(alter.set_options) |
                       static_cast<uint32_t>(alter.clear_options);
  for (uint32_t bit = 1; bit != 0 && bit <= options; bit <<= 1) {
    if ((options & bit) != 0) {
      keys.push_back(absl::StrCat("role:", bit));
    }
  }
  if (alter.set_password) {
    keys.emplace_back("password");
  }
  if (alter.set_conn_limit) {
    keys.emplace_back("connlimit");
  }
  if (alter.set_valid_until) {
    keys.emplace_back("validuntil");
  }
  if (alter.reset_all_config) {
    keys.emplace_back(kConfig);
  }
  for (const std::string_view setting : alter.set_config) {
    keys.push_back(absl::StrCat(
      kConfig, ":",
      absl::AsciiStrToLower(setting.substr(0, setting.find('=')))));
  }
  for (const auto& setting : alter.reset_config) {
    keys.push_back(absl::StrCat(kConfig, ":", absl::AsciiStrToLower(setting)));
  }
}

void TableKeys(const duckdb::AlterTableInfo& alter,
               const duckdb::CatalogEntry& subject,
               duckdb::vector<std::string>& keys) {
  const auto column = [&](std::string_view attribute,
                          const duckdb::vector<duckdb::Identifier>& path) {
    if (path.size() == 1) {
      keys.push_back(ColumnKey(attribute, subject, path.front()));
    }
  };
  switch (alter.alter_table_type) {
    case duckdb::AlterTableType::SET_TABLE_OPTIONS:
      for (const auto& [name, value] :
           alter.Cast<duckdb::SetTableOptionsInfo>().table_options) {
        keys.push_back(OptionKey(name));
      }
      break;
    case duckdb::AlterTableType::RESET_TABLE_OPTIONS:
      for (const auto& name :
           alter.Cast<duckdb::ResetTableOptionsInfo>().table_options) {
        keys.push_back(OptionKey(name.GetIdentifierName()));
      }
      break;
    case duckdb::AlterTableType::SET_TRIGGER_FIRING: {
      const auto& trigger = alter.Cast<duckdb::SetTriggerFiringInfo>()
                              .trigger_name.GetIdentifierName();
      keys.push_back(trigger.empty() ? std::string{kFiring}
                                     : absl::StrCat(kFiring, ":", trigger));
      break;
    }
    case duckdb::AlterTableType::SET_DEFAULT:
      column(kDefault, alter.Cast<duckdb::SetDefaultInfo>().column_path);
      break;
    case duckdb::AlterTableType::SET_NOT_NULL:
      column(kNotNull, alter.Cast<duckdb::SetNotNullInfo>().column_path);
      break;
    case duckdb::AlterTableType::DROP_NOT_NULL:
      column(kNotNull, alter.Cast<duckdb::DropNotNullInfo>().column_path);
      break;
    default:
      break;
  }
}

duckdb::vector<std::string> Supersedes(const duckdb::AlterInfo& info,
                                       const duckdb::CatalogEntry& subject) {
  duckdb::vector<std::string> keys;
  switch (info.type) {
    case duckdb::AlterType::REPLACE_DEFINITION:
      keys.emplace_back(kDefinition);
      break;
    case duckdb::AlterType::SET_COMMENT:
      keys.emplace_back(kComment);
      break;
    case duckdb::AlterType::SET_COLUMN_COMMENT:
      keys.push_back(
        ColumnKey(kComment, subject,
                  info.Cast<duckdb::SetColumnCommentInfo>().column_name));
      break;
    case duckdb::AlterType::CHANGE_OWNERSHIP:
      keys.emplace_back("owned_by");
      break;
    case duckdb::AlterType::ALTER_SEQUENCE:
      if (info.Cast<duckdb::AlterSequenceInfo>().alter_sequence_type ==
          duckdb::AlterSequenceType::RESTART_SEQUENCE) {
        keys.emplace_back("restart");
      }
      break;
    case duckdb::AlterType::ALTER_JOB:
      switch (info.Cast<duckdb::AlterJobInfo>().alter_job_type) {
        case duckdb::AlterJobType::SUSPEND:
        case duckdb::AlterJobType::RESUME:
          keys.emplace_back("state");
          break;
        case duckdb::AlterJobType::SET_SCHEDULE:
          keys.emplace_back("schedule");
          break;
        case duckdb::AlterJobType::RENAME:
          break;
      }
      break;
    case duckdb::AlterType::ALTER_ROLE:
      RoleKeys(info.Cast<duckdb::AlterRoleInfo>(), keys);
      break;
    case duckdb::AlterType::ALTER_INDEX: {
      const auto& alter = info.Cast<duckdb::AlterIndexInfo>();
      if (alter.alter_index_type == duckdb::AlterIndexType::SET_INDEX_OPTIONS) {
        for (const auto& [name, value] :
             alter.Cast<duckdb::SetIndexOptionsInfo>().options) {
          keys.push_back(OptionKey(name));
        }
      } else if (alter.alter_index_type ==
                 duckdb::AlterIndexType::RESET_INDEX_OPTIONS) {
        for (const auto& name :
             alter.Cast<duckdb::ResetIndexOptionsInfo>().options) {
          keys.push_back(OptionKey(name.GetIdentifierName()));
        }
      }
      break;
    }
    case duckdb::AlterType::ALTER_TABLE:
      TableKeys(info.Cast<duckdb::AlterTableInfo>(), subject, keys);
      break;
    default:
      break;
  }
  absl::c_sort(keys);
  keys.erase(std::ranges::unique(keys).begin(), keys.end());
  return keys;
}

bool Covers(std::string_view all, std::string_view key) {
  return key.size() > all.size() && key.starts_with(all) &&
         key[all.size()] == ':';
}

bool Interacts(std::string_view key, std::string_view other) {
  const auto one_way = [](std::string_view a, std::string_view b) {
    return ((a == kConfig || a == kFiring) && Covers(a, b)) ||
           (a == kDefinition && (Covers(kComment, b) || Covers(kDefault, b) ||
                                 Covers(kNotNull, b)));
  };
  return one_way(key, other) || one_way(other, key);
}

bool Commutes(std::span<const std::string_view> earlier,
              const duckdb::vector<std::string>& later) {
  if (later.empty()) {
    return absl::c_all_of(earlier,
                          [](std::string_view key) { return key == kComment; });
  }
  return absl::c_none_of(earlier, [&](std::string_view key) {
    return absl::c_any_of(
      later, [&](std::string_view other) { return Interacts(key, other); });
  });
}

std::string ChangeBody(const Change& change) {
  if (change.kind == ChangeKind::Drop) {
    return {};
  }
  auto& subject = change.Subject();
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  if (change.kind == ChangeKind::Create) {
    serializer.WritePropertyWithDefault(100, "info",
                                        subject.GetSerializedInfo());
  } else {
    serializer.WritePropertyWithDefault(101, "alter", change.alter);
    if (subject.type == duckdb::CatalogType::TABLE_ENTRY) {
      serializer.WriteProperty(
        102, "dependencies",
        subject.Cast<duckdb::TableCatalogEntry>().dependencies);
      serializer.WritePropertyWithDefault(104, "created_constraints",
                                          CreatedConstraints(change));
    }
  }
  serializer.End();
  return {reinterpret_cast<const char*>(stream.GetData()),
          stream.GetPosition()};
}

ChangeContent ReadChangeBody(duckdb::ClientContext& context,
                             std::string& body) {
  ChangeContent content;
  if (body.empty()) {
    return content;
  }
  duckdb::MemoryStream stream{reinterpret_cast<duckdb::data_ptr_t>(body.data()),
                              body.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Set<duckdb::ClientContext&>(context);
  deserializer.Begin();
  content.info =
    deserializer
      .ReadPropertyWithDefault<duckdb::unique_ptr<duckdb::CreateInfo>>(100,
                                                                       "info");
  content.alter =
    deserializer.ReadPropertyWithDefault<std::string>(101, "alter");
  content.dependencies =
    deserializer.ReadPropertyWithExplicitDefault<duckdb::LogicalDependencyList>(
      102, "dependencies", duckdb::LogicalDependencyList{});
  content.created_constraints =
    deserializer.ReadPropertyWithDefault<duckdb::vector<duckdb::idx_t>>(
      104, "created_constraints");
  deserializer.End();
  return content;
}

void ReplayChange(duckdb::ClientContext& context, ReplayTarget target,
                  ChangeRecord& change) {
  auto* owner = target(change.catalog);
  if (!owner) {
    return;
  }
  auto content = ReadChangeBody(context, change.body);
  auto& catalog = *owner;
  const auto oid = change.oid;
  const auto transaction = catalog.GetCatalogTransaction(context);
  switch (change.kind) {
    case ChangeKind::Create:
      if (change.owner) {
        LoggedTable(catalog, transaction, change.owner)
          .CreateTrigger(transaction,
                         content.info->Cast<duckdb::CreateTriggerInfo>());
        return;
      }
      QualifyInLoggedSchema(catalog, transaction, *content.info);
      catalog.ReplayCreate(context, std::move(content.info));
      return;
    case ChangeKind::Alter: {
      auto info = DeserializeAlter(context, content.alter);
      info->bind_mode = duckdb::AlterBindMode::SKIP_BINDING;
      const auto& created = content.created_constraints;
      std::vector<duckdb::idx_t> before;
      if (!created.empty()) {
        for (const auto& constraint :
             LoggedTable(catalog, transaction, oid).GetConstraints()) {
          before.push_back(constraint->oid);
        }
      }
      catalog.Alter(context, *info);
      if (!content.dependencies.Set().empty()) {
        auto& table = LoggedTable(catalog, transaction, oid);
        catalog.Oids().RemoveDependents(table, table.oid);
        table.dependencies = std::move(content.dependencies);
        catalog.Oids().AddDependents(table, table.oid);
      }
      if (!created.empty()) {
        std::vector<duckdb::Constraint*> made;
        for (const auto& constraint :
             LoggedTable(catalog, transaction, oid).GetConstraints()) {
          if (!absl::c_linear_search(before, constraint->oid)) {
            made.push_back(constraint.get());
          }
        }
        SDB_ENSURE(made.size() == created.size(), "the log gives table ", oid,
                   " ", created.size(), " new constraints, replay made ",
                   made.size());
        auto& oids = catalog.GetDatabase().GetDatabaseManager();
        for (size_t i = 0; i < made.size(); ++i) {
          made[i]->oid = created[i];
          oids.ClaimOid(created[i]);
        }
        catalog.Oids().Alias(created, oid);
      }
      return;
    }
    case ChangeKind::Drop: {
      auto entry = catalog.LookupOid(transaction, oid);
      SDB_ENSURE(entry, "the log drops a missing entry ", oid);
      if (change.owner) {
        LoggedTable(catalog, transaction, change.owner)
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

void ReplaySection(duckdb::ClientContext& context, ReplayTarget target,
                   duckdb::Deserializer& object) {
  const auto id = object.ReadProperty<duckdb::idx_t>(100, "catalog");
  const auto oid = object.ReadProperty<duckdb::idx_t>(101, "table");
  search::TableReplay* replay = nullptr;
  auto* catalog = target(id);
  if (catalog) {
    if (auto entry =
          catalog->LookupOid(catalog->GetCatalogTransaction(context), oid)) {
      std::shared_ptr<search::Store> store;
      if (IsSearchTable(*entry)) {
        store = entry->Cast<SearchTableEntry>().Storage();
      } else if (entry->type == duckdb::CatalogType::INDEX_ENTRY) {
        store = entry->Cast<InvertedIndexEntry>().Storage();
      }
      SDB_ENSURE(store, "the log writes a missing table ", oid);
      replay = store->Replay();
    }
  }
  object.ReadObject(102, "delta", [&](duckdb::Deserializer& delta) {
    search::ReplayTableDelta(context, replay, catalog, delta);
  });
}

void WriteSection(duckdb::Serializer::List& list, duckdb::idx_t catalog,
                  duckdb::idx_t table, const search::TableDelta& delta) {
  list.WriteObject([&](duckdb::Serializer& object) {
    object.WriteProperty(100, "catalog", catalog);
    object.WriteProperty(101, "table", table);
    object.WriteObject(102, "delta", [&](duckdb::Serializer& change) {
      search::WriteTableDelta(change, delta);
    });
  });
}

void ReplayLsn(duckdb::ClientContext& context, ReplayTarget target,
               const LsnRecord& record) {
  auto* owner = target(record.catalog);
  if (!owner) {
    return;
  }
  auto& catalog = *owner;
  const auto transaction = catalog.GetCatalogTransaction(context);
  auto entry = catalog.LookupOid(transaction, record.oid);
  if (!entry || entry->type != record.type) {
    entry = catalog.Set(record.type)
              .Get(transaction, duckdb::Identifier{record.name});
  }
  if (!entry) {
    return;
  }
  ApplyLsnUpdate(
    {
      .entry = &entry->Cast<ReplicationLsnEntry>(),
      .relation = record.relation,
      .remote_lsn = record.remote_lsn,
      .assign = record.assign,
    },
    0);
}

void ReplaySequence(duckdb::ClientContext& context, ReplayTarget target,
                    const SequenceRecordValue& value) {
  auto* catalog = target(value.catalog);
  if (!catalog) {
    return;
  }
  auto entry =
    catalog->LookupOid(catalog->GetCatalogTransaction(context), value.oid);
  if (entry && entry->type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    entry->Cast<duckdb::SequenceCatalogEntry>().ReplayValue(
      value.usage_count, value.counter, value.last_value);
  }
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
  CatalogRecord record;
  record.sequences.push_back({
    .catalog = catalog.RecordId(),
    .oid = value.entry->oid,
    .usage_count = value.usage_count,
    .counter = value.counter,
    .last_value = value.last_value,
  });
  return Join(WriteCatalogRecord(record), {});
}

std::vector<uint8_t> SearchRecord(duckdb::idx_t catalog, duckdb::idx_t table,
                                  const search::TableDelta& delta) {
  duckdb::MemoryStream stream;
  duckdb::BinarySerializer serializer{stream};
  serializer.Begin();
  serializer.WriteList(100, "search", 1,
                       [&](duckdb::Serializer::List& list, duckdb::idx_t) {
                         WriteSection(list, catalog, table, delta);
                       });
  serializer.End();
  return Join({}, Written(stream));
}

std::vector<uint8_t> ShutdownRecord() {
  return Join(WriteCatalogRecord(CatalogRecord{.shutdown = true}), {});
}

std::vector<uint8_t> StartupRecord() { return Join({}, {}); }

std::span<uint8_t> CatalogPart(std::span<uint8_t> payload) {
  return payload.subspan(sizeof(uint32_t), CatalogSize(payload));
}

std::vector<std::vector<uint8_t>> CompactHistory(
  std::span<const std::span<uint8_t>> parts) {
  std::vector<CatalogRecord> records;
  records.reserve(parts.size());
  for (const auto part : parts) {
    records.push_back(ReadCatalogRecord(part));
  }
  std::vector<Life> lives;
  std::vector<size_t> change_life;
  std::vector<const ChangeRecord*> changes;
  std::vector<bool> folded;
  struct Pending {
    size_t change = 0;
    std::vector<std::string_view> keys;
  };
  irs::containers::FlatHashMap<Key, std::vector<Pending>> superseded;
  irs::containers::FlatHashMap<duckdb::idx_t, std::vector<size_t>> referencing;
  irs::containers::FlatHashMap<Key, size_t> live;
  irs::containers::FlatHashMap<duckdb::idx_t, std::vector<size_t>> contained;
  irs::containers::FlatHashMap<Key, std::vector<size_t>> owned;
  absl::btree_map<StateKey, Slot<LsnRecord>> lsns;
  absl::btree_map<StateKey, Slot<SequenceRecordValue>> sequences;
  absl::btree_map<std::string, Slot<SecretRecord>> secrets;
  const auto end = [&](size_t life, size_t at) {
    if (lives[life].end != kNoLife) {
      return;
    }
    lives[life].end = at;
    if (const auto it = live.find(lives[life].key);
        it != live.end() && it->second == life) {
      live.erase(it);
    }
  };
  const auto end_all = [&](auto& index, const auto& key, size_t at) {
    if (const auto it = index.find(key); it != index.end()) {
      for (const auto life : it->second) {
        end(life, at);
      }
      index.erase(it);
    }
  };
  const auto life_of = [&](duckdb::idx_t catalog, duckdb::idx_t oid) {
    const auto it = live.find(Key{catalog, oid});
    return it == live.end() ? kNoLife : it->second;
  };
  const auto state_key = [&](size_t life, duckdb::idx_t catalog,
                             duckdb::idx_t oid, duckdb::idx_t relation) {
    return life == kNoLife
             ? StateKey{.catalog = catalog, .oid = oid, .relation = relation}
             : StateKey{.life = life, .relation = relation};
  };

  for (size_t r = 0; r < records.size(); ++r) {
    auto& record = records[r];
    for (const auto& change : record.changes) {
      const auto at = change_life.size();
      const Key key{change.catalog, change.oid};
      changes.push_back(&change);
      folded.push_back(false);
      auto& pending = superseded[key];
      std::erase_if(pending, [&](Pending& earlier) {
        std::erase_if(earlier.keys, [&](std::string_view kept) {
          return absl::c_binary_search(change.supersedes, kept);
        });
        if (earlier.keys.empty()) {
          folded[earlier.change] = true;
          return true;
        }
        return change.kind != ChangeKind::Alter ||
               !Commutes(earlier.keys, change.supersedes);
      });
      if (!change.supersedes.empty()) {
        pending.push_back(
          {.change = at,
           .keys = {change.supersedes.begin(), change.supersedes.end()}});
      }
      for (const auto oid : change.references) {
        referencing[oid].push_back(at);
      }
      switch (change.kind) {
        case ChangeKind::Create: {
          const auto life = lives.size();
          lives.push_back({.key = key, .create = at});
          live.insert_or_assign(key, life);
          contained[change.catalog].push_back(life);
          if (change.owner) {
            owned[Key{change.catalog, change.owner}].push_back(life);
          }
          change_life.push_back(life);
          break;
        }
        case ChangeKind::Drop: {
          const auto life = life_of(change.catalog, change.oid);
          change_life.push_back(life);
          if (life == kNoLife) {
            break;
          }
          end(life, at);
          end_all(owned, key, at);
          if (change.catalog == ClusterCatalog::kRecordId) {
            end_all(contained, change.oid, at);
          }
          break;
        }
        default:
          change_life.push_back(life_of(change.catalog, change.oid));
          break;
      }
    }
    for (auto& update : record.lsns) {
      const auto life = life_of(update.catalog, update.oid);
      const auto key =
        state_key(life, update.catalog, update.oid, update.relation);
      auto [it, inserted] = lsns.try_emplace(
        key, Slot<LsnRecord>{.record = r, .life = life, .value = update});
      if (inserted) {
        continue;
      }
      auto& slot = it->second;
      slot.record = r;
      if (update.relation != 0 || update.assign) {
        slot.value = std::move(update);
      } else {
        const auto remote_lsn =
          std::max(slot.value.remote_lsn, update.remote_lsn);
        const auto assign = slot.value.assign;
        slot.value = std::move(update);
        slot.value.remote_lsn = remote_lsn;
        slot.value.assign = assign;
      }
    }
    for (auto& value : record.sequences) {
      const auto life = life_of(value.catalog, value.oid);
      sequences.insert_or_assign(
        state_key(life, value.catalog, value.oid, 0),
        Slot<SequenceRecordValue>{
          .record = r, .life = life, .value = std::move(value)});
    }
    for (auto& secret : record.secrets) {
      auto name = secret.name;
      secrets.insert_or_assign(
        std::move(name),
        Slot<SecretRecord>{.record = r, .value = std::move(secret)});
    }
  }

  for (auto& life : lives) {
    life.eliminated = life.end != kNoLife;
  }
  const auto gone = [&](size_t life) {
    return life != kNoLife && lives[life].eliminated;
  };
  const auto references = [&](size_t at, const Life& life) {
    return at > life.create && at < life.end && !folded[at] &&
           !gone(change_life[at]) &&
           (life.key.first == ClusterCatalog::kRecordId ||
            changes[at]->catalog == life.key.first);
  };
  for (bool changed = true; changed;) {
    changed = false;
    for (auto& life : lives) {
      if (!life.eliminated) {
        continue;
      }
      if (const auto it = referencing.find(life.key.second);
          it != referencing.end() && absl::c_any_of(it->second, [&](size_t at) {
            return references(at, life);
          })) {
        life.eliminated = false;
        changed = true;
      }
    }
  }

  std::vector<CatalogRecord> kept(records.size());
  size_t at = 0;
  for (size_t r = 0; r < records.size(); ++r) {
    for (auto& change : records[r].changes) {
      if (!gone(change_life[at]) && !folded[at]) {
        kept[r].changes.push_back(std::move(change));
      }
      ++at;
    }
  }
  for (auto& [key, slot] : lsns) {
    if (!gone(slot.life)) {
      kept[slot.record].lsns.push_back(std::move(slot.value));
    }
  }
  for (auto& [key, slot] : sequences) {
    if (!gone(slot.life)) {
      kept[slot.record].sequences.push_back(std::move(slot.value));
    }
  }
  for (auto& [name, slot] : secrets) {
    if (slot.value.kind != ChangeKind::Drop) {
      kept[slot.record].secrets.push_back(std::move(slot.value));
    }
  }
  std::vector<std::vector<uint8_t>> compacted;
  for (const auto& record : kept) {
    if (!record.Empty()) {
      compacted.push_back(Join(WriteCatalogRecord(record), {}));
    }
  }
  return compacted;
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
                                 search::Writes& writes) {
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
  auto sections = writes.TakeLog(context);
  std::erase_if(sections, [](const search::LogSection& section) {
    return search::Empty(section.delta);
  });
  CatalogRecord record;
  for (const auto* change : changes) {
    auto& subject = change->Subject();
    auto& written = record.changes.emplace_back(ChangeRecord{
      .catalog = Owner(subject).RecordId(),
      .kind = change->kind,
      .oid = subject.oid,
      .owner = change->set->Owner(),
      .body = ChangeBody(*change),
    });
    if (change->kind == ChangeKind::Drop) {
      continue;
    }
    written.references = References(*change);
    if (change->kind == ChangeKind::Alter) {
      auto alter = change->alter;
      written.supersedes =
        Supersedes(*DeserializeAlter(context, alter), subject);
    }
  }
  for (const auto& update : transaction.LsnUpdates()) {
    record.lsns.push_back({
      .catalog = Owner(*update.entry).RecordId(),
      .oid = update.entry->oid,
      .type = update.entry->type,
      .name = std::string{update.entry->name.GetIdentifierName()},
      .relation = update.relation,
      .remote_lsn = update.remote_lsn,
      .assign = update.assign,
    });
  }
  for (auto* sequence : sequences) {
    const auto data = sequence->GetData();
    record.sequences.push_back({
      .catalog = Owner(*sequence).RecordId(),
      .oid = sequence->oid,
      .usage_count = data.usage_count,
      .counter = data.counter,
      .last_value = data.last_value,
    });
  }
  for (const auto* change : secrets) {
    const auto& subject = change->Subject();
    record.secrets.push_back({
      .kind = change->kind,
      .name = std::string{subject.name.GetIdentifierName()},
      .secret = change->kind == ChangeKind::Create ? SerializeSecret(subject)
                                                   : std::string{},
    });
  }
  std::vector<uint8_t> catalog;
  if (!record.Empty()) {
    catalog = WriteCatalogRecord(record);
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
          list, section.owner->ParentCatalog().Cast<BaseCatalog>().RecordId(),
          section.owner->oid, section.delta);
      });
    serializer.End();
  }
  if (catalog.empty() && search.GetPosition() == 0) {
    return {};
  }
  return Join(catalog, Written(search));
}

BaseCatalog* InstanceCatalog(duckdb::ClientContext& context, duckdb::idx_t id) {
  auto& cluster = ClusterOf(context);
  if (cluster.RecordId() == id) {
    return &cluster;
  }
  for (const auto& db :
       SereneDBDatabases(duckdb::DatabaseManager::Get(context))) {
    if (auto& catalog = db->GetCatalog().Cast<SereneDBCatalog>();
        catalog.Durable() && catalog.RecordId() == id) {
      return &catalog;
    }
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_DATA_CORRUPTED),
                  ERR_MSG("the log names a missing catalog ", id));
}

bool ReplayCatalogPart(duckdb::ClientContext& context, ReplayTarget target,
                       std::span<uint8_t> catalog) {
  auto record = ReadCatalogRecord(catalog);
  for (auto& change : record.changes) {
    ReplayChange(context, target, change);
  }
  for (const auto& update : record.lsns) {
    ReplayLsn(context, target, update);
  }
  for (const auto& value : record.sequences) {
    ReplaySequence(context, target, value);
  }
  for (auto& secret : record.secrets) {
    ReplaySecret(context, target, secret);
  }
  return record.shutdown;
}

void ReadDatabases(duckdb::ClientContext& context, std::span<uint8_t> catalog,
                   LoggedDatabases& databases) {
  if (catalog.empty()) {
    return;
  }
  for (auto& change : ReadCatalogRecord(catalog).changes) {
    if (change.catalog != ClusterCatalog::kRecordId) {
      continue;
    }
    if (change.kind == ChangeKind::Drop) {
      databases.erase(change.oid);
      continue;
    }
    if (change.kind != ChangeKind::Create) {
      continue;
    }
    const auto content = ReadChangeBody(context, change.body);
    if (content.info->type == duckdb::CatalogType::DATABASE_ENTRY) {
      databases.insert_or_assign(change.oid,
                                 content.info->GetQualifiedName().Name());
    }
  }
}

bool ReplayRecord(duckdb::ClientContext& context, ReplayTarget target,
                  std::span<uint8_t> payload) {
  const auto size = CatalogSize(payload);
  const bool shutdown =
    size != 0 &&
    ReplayCatalogPart(context, target, payload.subspan(sizeof(uint32_t), size));
  const auto search = payload.subspan(sizeof(uint32_t) + size);
  if (search.empty()) {
    return shutdown;
  }
  duckdb::MemoryStream stream{search.data(), search.size()};
  duckdb::BinaryDeserializer deserializer{stream};
  deserializer.Set<duckdb::ClientContext&>(context);
  deserializer.Begin();
  deserializer.ReadList(100, "search",
                        [&](duckdb::Deserializer::List& list, duckdb::idx_t) {
                          list.ReadObject([&](duckdb::Deserializer& object) {
                            ReplaySection(context, target, object);
                          });
                        });
  deserializer.End();
  return shutdown;
}

}  // namespace sdb::catalog
