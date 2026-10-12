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

#include <absl/algorithm/container.h>
#include <absl/container/inlined_vector.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry/duck_schema_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <ranges>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/oids.h"

namespace sdb::pg {
namespace {

constexpr auto kMemberTypes = std::views::filter([](duckdb::CatalogType type) {
  return type != duckdb::CatalogType::SCHEMA_ENTRY;
});

std::string_view ScanPrefix(const SystemCondition<std::string>& condition) {
  if (!condition.lower || !condition.upper) {
    return {};
  }
  const std::string_view lower = condition.lower->value;
  const std::string_view upper = condition.upper->value;
  const auto common =
    static_cast<size_t>(absl::c_mismatch(lower, upper).first - lower.begin());
  if (!condition.upper->inclusive && common + 1 == lower.size() &&
      upper.size() == lower.size() &&
      static_cast<unsigned char>(upper[common]) ==
        static_cast<unsigned char>(lower[common]) + 1) {
    return lower;
  }
  return lower.substr(0, common);
}

}  // namespace

const SystemIndex* SystemScan::Choose(std::span<const SystemIndex> indexes,
                                      size_t name_probes) const {
  const SystemIndex* chosen = nullptr;
  auto fewest = std::numeric_limits<size_t>::max();
  for (const auto& index : indexes) {
    const auto* filter = FilterOf(index.column);
    if (!filter || (index.lookup != SystemLookup::Object &&
                    index.lookup != SystemLookup::Dependents)) {
      continue;
    }
    const auto probes = filter->numbers.keys ? filter->numbers.keys->size()
                        : filter->texts.keys
                          ? filter->texts.keys->size() * name_probes
                          : fewest;
    if (probes < fewest) {
      chosen = &index;
      fewest = probes;
    }
  }
  return chosen;
}

std::string_view SystemScan::NamePrefix(
  std::span<const SystemIndex> indexes) const {
  for (const auto& index : indexes) {
    if (index.lookup != SystemLookup::Object ||
        _table.Sql().columns[index.column].physical !=
          duckdb::PhysicalType::VARCHAR) {
      continue;
    }
    if (const auto* filter = FilterOf(index.column)) {
      return ScanPrefix(filter->texts);
    }
  }
  return {};
}

SystemEntries SystemScan::Resolve(const CatalogSource& source) {
  using enum duckdb::CatalogType;
  SystemEntries result;
  auto& wanted = result.wanted;
  for (const auto type : source.types) {
    wanted.set(std::to_underlying(type));
  }
  std::optional<uint32_t> nsp_column;
  bool numbers = false;
  bool names = false;
  for (const auto& index : source.indexes) {
    if (index.lookup == SystemLookup::Namespace) {
      nsp_column = index.column;
    }
    const auto* filter = FilterOf(index.column);
    if (!filter) {
      continue;
    }
    switch (index.lookup) {
      case SystemLookup::Kind: {
        const bool text = _table.Sql().columns[index.column].physical ==
                          duckdb::PhysicalType::VARCHAR;
        for (const auto type : source.types) {
          if (absl::c_none_of(index.kinds, [&](const SystemKindTypes& kind) {
                return absl::c_linear_search(kind.types, type) &&
                       (text
                          ? Passes(index.column, static_cast<char>(kind.kind))
                          : Passes(index.column, kind.kind));
              })) {
            wanted.reset(std::to_underlying(type));
          }
        }
        break;
      }
      case SystemLookup::Namespace:
        break;
      case SystemLookup::Object:
      case SystemLookup::Dependents:
        numbers = numbers || filter->numbers.keys.has_value();
        names = names || filter->texts.keys.has_value();
        break;
    }
  }
  if (_rejecting != 0) {
    for (const auto type : source.types) {
      bool declared = false;
      bool possible = false;
      for (const auto& shape : _table.TypeShapes()) {
        if (shape.type == type) {
          declared = true;
          possible = possible || (_rejecting & ~shape.provided) == 0;
        }
      }
      if (declared && !possible) {
        wanted.reset(std::to_underlying(type));
      }
    }
  }
  if (wanted.none()) {
    return result;
  }
  std::vector<duckdb::SchemaCatalogEntry*> schemas;
  if (names || !numbers) {
    schemas = Schemas(source.schemas);
    std::erase_if(schemas, [&](const duckdb::SchemaCatalogEntry* schema) {
      return !AllowsSchema(nsp_column, *schema);
    });
  }
  auto member_types = source.types | kMemberTypes |
                      std::views::filter([&](duckdb::CatalogType type) {
                        return wanted.test(std::to_underlying(type));
                      });
  const auto* index =
    Choose(source.indexes,
           std::max<size_t>(
             names ? schemas.size() *
                       static_cast<size_t>(std::ranges::distance(member_types))
                   : 0,
             1));
  if (!index) {
    absl::InlinedVector<duckdb::CatalogType, 8> sets;
    if (!schemas.empty()) {
      auto& first = schemas.front()->Cast<duckdb::DuckSchemaEntry>();
      absl::InlinedVector<const duckdb::CatalogSet*, 8> walked;
      for (const auto type : member_types) {
        if (const auto* set = &first.GetCatalogSet(type);
            !absl::c_linear_search(walked, set)) {
          walked.emplace_back(set);
          sets.emplace_back(type);
        }
      }
    }
    const bool schema_rows = wanted.test(std::to_underlying(SCHEMA_ENTRY));
    for (auto* schema : schemas) {
      if (schema_rows) {
        result.walks.emplace_back(SystemWalk{schema, SCHEMA_ENTRY});
      }
      for (const auto type : sets) {
        result.walks.emplace_back(SystemWalk{schema, type});
      }
    }
    if (const auto text = NamePrefix(source.indexes);
        !text.empty() && !(text.starts_with('_') &&
                           wanted.test(std::to_underlying(TYPE_ENTRY)))) {
      result.prefix.emplace(SystemPrefix{
        .lower = duckdb::Identifier{text},
        .upper =
          duckdb::Identifier{absl::StrCat(text, std::string_view{"\xff", 1})}});
    }
    return result;
  }
  auto& entries = result.listed.found;
  const auto& filter = *FilterOf(index->column);
  if (const auto& keys = filter.texts.keys) {
    for (const auto& name : *keys) {
      AppendNamed(name, source.types, source.schemas, schemas, wanted, entries);
    }
  }
  if (const auto& oids = filter.numbers.keys) {
    for (const auto oid : *oids) {
      AppendOwned(static_cast<duckdb::idx_t>(oid), source.schemas, entries);
    }
  }
  if (index->lookup == SystemLookup::Dependents) {
    auto& dependencies = Dependencies();
    for (size_t i = 0, owners = entries.size(); i < owners; ++i) {
      dependencies.ScanEdges(
        Transaction(), *entries[i], false,
        [&](duckdb::CatalogEntry& dependent,
            const duckdb::DependencyDependentFlags&) {
          if (!dependent.internal &&
              wanted.test(std::to_underlying(dependent.type))) {
            entries.emplace_back(&dependent);
          }
        });
    }
  }
  if (entries.size() > 1) {
    irs::containers::FlatHashSet<const duckdb::CatalogEntry*> seen;
    std::erase_if(entries, [&](const duckdb::CatalogEntry* entry) {
      return !seen.insert(entry).second;
    });
  }
  result.listed.entries = entries;
  return result;
}

SystemSet SystemScan::SetOf(
  catalog::SnapshotCatalog& catalog, duckdb::CatalogSet& set,
  const duckdb::CatalogTransaction& transaction) const {
  auto snapshot = catalog.Snapshot(_context, set);
  if (!snapshot) {
    auto scanned = std::make_shared<catalog::CatalogSnapshot>();
    set.Scan(transaction, [&](duckdb::CatalogEntry& entry) {
      scanned->entries.emplace_back(&entry);
    });
    snapshot = std::move(scanned);
  }
  std::span<duckdb::CatalogEntry* const> entries{snapshot->entries};
  return {.snapshot = std::move(snapshot), .found = {}, .entries = entries};
}

SystemSet SystemScan::MemberSet(duckdb::SchemaCatalogEntry& schema,
                                duckdb::CatalogType type,
                                const SystemPrefix* prefix) const {
  auto result = SetOf(
    _database.Cast<catalog::SnapshotCatalog>(),
    schema.Cast<duckdb::DuckSchemaEntry>().GetCatalogSet(type), Transaction());
  if (prefix) {
    const duckdb::IdentifierCompare compare{
      schema.ParentCatalog().IsCaseSensitive()};
    const auto name =
      [](const duckdb::CatalogEntry* entry) -> const duckdb::Identifier& {
      return entry->name;
    };
    const auto first =
      std::ranges::lower_bound(result.entries, prefix->lower, compare, name);
    result.entries = {first,
                      std::ranges::upper_bound(first, result.entries.end(),
                                               prefix->upper, compare, name)};
  }
  return result;
}

std::span<
  const std::pair<duckdb::CatalogEntry*, duckdb::DependencyDependentFlags>>
SystemScan::WalkedSubjects(duckdb::CatalogEntry& entry) const {
  const auto collect = [&](duckdb::CatalogEntry& member) {
    std::vector<
      std::pair<duckdb::CatalogEntry*, duckdb::DependencyDependentFlags>>
      edges;
    Dependencies().ScanEdges(
      Transaction(), member, true,
      [&](duckdb::CatalogEntry& subject,
          const duckdb::DependencyDependentFlags& flags) {
        edges.emplace_back(&subject, flags);
      });
    return edges;
  };
  const auto& subjects = _walked->subjects.Get([&](auto& subjects) {
    for (auto* member : _walked->entries) {
      if (auto edges = collect(*member); !edges.empty()) {
        subjects.emplace(member, std::move(edges));
      }
    }
  });
  const auto it = subjects.find(&entry);
  const std::span<
    const std::pair<duckdb::CatalogEntry*, duckdb::DependencyDependentFlags>>
    edges = it == subjects.end()
              ? std::span<const std::pair<duckdb::CatalogEntry*,
                                          duckdb::DependencyDependentFlags>>{}
              : std::span{it->second};
  SDB_ASSERT(absl::c_equal(edges, collect(entry)));
  return edges;
}

bool SystemScan::Indexed(duckdb::CatalogEntry& relation) const {
  if (!_walked) {
    bool indexed = false;
    Dependencies().ScanEdges(
      Transaction(), relation, false,
      [&](duckdb::CatalogEntry& dependent,
          const duckdb::DependencyDependentFlags&) {
        indexed = indexed || dependent.type == duckdb::CatalogType::INDEX_ENTRY;
      });
    return indexed;
  }
  const auto& indexed = _walked->indexed.Get([&](auto& indexed) {
    const auto indexes = MemberSet(relation.ParentSchema(Transaction()),
                                   duckdb::CatalogType::INDEX_ENTRY, nullptr);
    for (auto* entry : indexes.entries) {
      indexed.insert(entry->Cast<duckdb::IndexCatalogEntry>().table_oid);
    }
  });
  return !indexed.empty() && indexed.contains(relation.oid);
}

bool SystemScan::Triggered(const duckdb::TableCatalogEntry& table) const {
  const auto any = [&](const duckdb::TableCatalogEntry& member) {
    bool triggered = false;
    member.ScanTriggers(Transaction(),
                        [&](duckdb::CatalogEntry&) { triggered = true; });
    return triggered;
  };
  if (!_walked) {
    return any(table);
  }
  const auto& triggered = _walked->triggered.Get([&](auto& triggered) {
    for (auto* entry : _walked->entries) {
      if (entry->type == duckdb::CatalogType::TABLE_ENTRY &&
          any(entry->Cast<duckdb::TableCatalogEntry>())) {
        triggered.insert(entry->oid);
      }
    }
  });
  return !triggered.empty() && triggered.contains(table.oid);
}

SystemEntries SystemScan::Resolve(const CatalogSetSource& source) const {
  catalog::SnapshotCatalog& catalog =
    source.catalog == SystemCatalog::Cluster
      ? catalog::ClusterOf(_context)
      : _database.Cast<catalog::SnapshotCatalog>();
  auto& set = catalog.GetCatalogSet(source.type);
  const auto transaction = catalog.GetCatalogTransaction(_context);
  SystemEntries result;
  auto& listed = result.listed;
  auto& entries = listed.found;
  const auto* index = Choose(source.indexes, 1);
  if (!index) {
    listed = SetOf(catalog, set, transaction);
    return result;
  }
  const auto add = [&](duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
    if (entry && entry->type == source.type) {
      entries.emplace_back(entry.get());
    }
  };
  const auto& filter = *FilterOf(index->column);
  if (const auto& names = filter.texts.keys) {
    for (const auto& name : *names) {
      add(set.GetEntry(transaction, duckdb::Identifier{name}));
    }
  }
  if (const auto& oids = filter.numbers.keys) {
    for (const auto oid : *oids) {
      add(catalog.GetOidIndex().GetVisible(static_cast<duckdb::idx_t>(oid),
                                           transaction.view));
    }
  }
  listed.entries = entries;
  return result;
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry> SystemScan::FindSchema(
  std::string_view name, SystemSchemas system) const {
  auto schema = _database.GetSchema(Transaction(), duckdb::Identifier{name},
                                    duckdb::OnEntryNotFound::RETURN_NULL);
  if (!schema ||
      (schema->internal && (system == SystemSchemas::Skip ||
                            !FindSystemObject(kSystemNamespaces, name)))) {
    return nullptr;
  }
  return schema;
}

std::vector<duckdb::SchemaCatalogEntry*> SystemScan::Schemas(
  SystemSchemas system) const {
  std::vector<duckdb::SchemaCatalogEntry*> schemas;
  if (system == SystemSchemas::Visit) {
    for (const auto& schema : kSystemNamespaces) {
      if (auto entry = FindSchema(schema.name, system)) {
        schemas.emplace_back(entry.get());
      }
    }
  }
  for (auto& schema : _database.GetSchemas(_context) |
                        std::views::filter([](const auto& schema) {
                          return !schema.get().internal;
                        })) {
    schemas.emplace_back(&schema.get());
  }
  return schemas;
}

void SystemScan::AppendOwned(
  duckdb::idx_t oid, SystemSchemas system,
  std::vector<duckdb::CatalogEntry*>& entries) const {
  if (system == SystemSchemas::Visit) {
    if (const auto* schema = FindSystemObject(kSystemNamespaces, oid)) {
      if (auto entry = FindSchema(schema->name, system)) {
        entries.emplace_back(entry.get());
      }
      return;
    }
    std::pair<std::string_view, std::string_view> relation;
    if (const auto* table = FindSystemTable(oid)) {
      relation = {table->Sql().schema, table->Sql().name};
    } else if (const auto* view = FindSystemView(oid)) {
      relation = {view->schema, view->name};
    }
    if (!relation.second.empty()) {
      if (auto schema = FindSchema(relation.first, system)) {
        if (auto entry = FindMember(Transaction(), *schema,
                                    duckdb::CatalogType::TABLE_ENTRY,
                                    duckdb::Identifier{relation.second})) {
          entries.emplace_back(entry.get());
        }
      }
      return;
    }
  }
  if (oid & kRowTypeOidBit) {
    oid = RowTypeRelation(oid);
  }
  auto owner = FindOwner(oid);
  if (!owner) {
    owner = FindOwner(oid + 1);
    if (owner && owner->type != duckdb::CatalogType::TYPE_ENTRY) {
      return;
    }
  }
  if (owner && !owner->internal) {
    entries.emplace_back(owner.get());
  }
}

void SystemScan::AppendNamed(
  std::string_view name, std::span<const duckdb::CatalogType> types,
  SystemSchemas system, std::span<duckdb::SchemaCatalogEntry* const> schemas,
  const std::bitset<256>& wanted,
  std::vector<duckdb::CatalogEntry*>& entries) const {
  using enum duckdb::CatalogType;
  const duckdb::Identifier identifier{name};
  for (auto* schema : schemas) {
    const auto found = entries.size();
    for (const auto type : types | kMemberTypes) {
      if (!wanted.test(std::to_underlying(type))) {
        continue;
      }
      if (auto entry = FindMember(Transaction(), *schema, type, identifier);
          entry && entry->type == type &&
          (!entry->internal || schema->internal)) {
        entries.emplace_back(entry.get());
      }
    }
    if (entries.size() == found &&
        wanted.test(std::to_underlying(INDEX_ENTRY))) {
      if (const auto key = FindKeyIndex(_context, *schema, name);
          key.key_index) {
        entries.emplace_back(key.table);
      }
    }
  }
  if (wanted.test(std::to_underlying(SCHEMA_ENTRY))) {
    if (auto schema = FindSchema(name, system)) {
      entries.emplace_back(schema.get());
    }
  }
  if (wanted.test(std::to_underlying(TYPE_ENTRY)) && name.starts_with('_')) {
    AppendNamed(name.substr(1), types, system, schemas, wanted, entries);
  }
}

}  // namespace sdb::pg
