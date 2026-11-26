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

#include "catalog/domain.h"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_set.h>

#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/extension_type_info.hpp>
#include <duckdb/common/type_visitor.hpp>
#include <duckdb/parser/parsed_data/alter_domain_info.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <optional>
#include <vector>

#include "catalog/dependency.h"
#include "catalog/entry/search_table.h"
#include "catalog/schema.h"

namespace sdb::catalog {
namespace {

const duckdb::Constraint* FindDomainConstraint(const duckdb::DomainInfo& domain,
                                               const std::string& name) {
  const auto it =
    absl::c_find_if(domain.constraints, [&](const auto& constraint) {
      return constraint->constraint_name == name;
    });
  return it == domain.constraints.end() ? nullptr : it->get();
}

const duckdb::Constraint* ConstraintToValidate(
  const duckdb::TypeCatalogEntry& old_domain,
  const duckdb::TypeCatalogEntry& new_domain,
  const duckdb::AlterDomainInfo& alter) {
  const auto& before = *old_domain.domain_info;
  const auto& after = *new_domain.domain_info;
  switch (alter.alter_domain_type) {
    case duckdb::AlterDomainType::ADD_CONSTRAINT:
      return alter.constraint->validated
               ? FindDomainConstraint(after, alter.constraint->constraint_name)
               : nullptr;
    case duckdb::AlterDomainType::SET_NOT_NULL: {
      if (before.IsNotNull()) {
        return nullptr;
      }
      const auto it =
        absl::c_find_if(after.constraints, [](const auto& constraint) {
          return constraint->type == duckdb::ConstraintType::NOT_NULL;
        });
      return it == after.constraints.end() ? nullptr : it->get();
    }
    case duckdb::AlterDomainType::VALIDATE_CONSTRAINT: {
      const auto* previous =
        FindDomainConstraint(before, alter.constraint_name);
      return previous && !previous->validated
               ? FindDomainConstraint(after, alter.constraint_name)
               : nullptr;
    }
    default:
      return nullptr;
  }
}

std::optional<duckdb::idx_t> CatalogOid(const duckdb::LogicalType& type) {
  const auto info = type.GetExtensionInfo();
  if (!info) {
    return std::nullopt;
  }
  const auto it =
    info->properties.find(duckdb::ExtensionTypeInfo::CATALOG_OID_PROPERTY);
  if (it == info->properties.end()) {
    return std::nullopt;
  }
  return it->second.GetValue<duckdb::idx_t>();
}

std::optional<duckdb::idx_t> DomainOid(const duckdb::LogicalType& type) {
  if (!duckdb::ExtensionTypeInfo::IsDomain(type.GetExtensionInfo())) {
    return std::nullopt;
  }
  return CatalogOid(type);
}

[[noreturn]] void ThrowNestedUse(const duckdb::TypeCatalogEntry& domain,
                                 const duckdb::TableCatalogEntry& table,
                                 const duckdb::ColumnDefinition& column) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("cannot alter type \"", domain.name.GetIdentifierName(),
            "\" because column \"", table.name.GetIdentifierName(), ".",
            column.Name().GetIdentifierName(), "\" uses it"));
}

struct DomainDependentTable {
  duckdb::TableCatalogEntry* table;
  std::vector<duckdb::LogicalIndex> columns;
};

std::vector<DomainDependentTable> DomainDependentColumns(
  duckdb::CatalogTransaction transaction, duckdb::TypeCatalogEntry& domain) {
  absl::flat_hash_set<duckdb::idx_t> domains{domain.oid};
  std::vector<duckdb::CatalogEntry*> pending{&domain};
  std::vector<duckdb::CatalogEntry*> tables;
  std::vector<duckdb::CatalogEntry*> wrappers;
  while (!pending.empty()) {
    auto& subject = *pending.back();
    pending.pop_back();
    ScanDependents(
      transaction, subject,
      [&](duckdb::CatalogEntry& dependent,
          const duckdb::DependencyDependentFlags&) {
        if (dependent.type == duckdb::CatalogType::TABLE_ENTRY) {
          tables.push_back(&dependent);
        } else if (dependent.type == duckdb::CatalogType::TYPE_ENTRY) {
          if (!dependent.Cast<duckdb::TypeCatalogEntry>().domain_info) {
            wrappers.push_back(&dependent);
          } else if (domains.insert(dependent.oid).second) {
            pending.push_back(&dependent);
          }
        }
      });
  }

  absl::flat_hash_set<duckdb::idx_t> wrapped;
  for (size_t i = 0; i < wrappers.size(); ++i) {
    auto& wrapper = *wrappers[i];
    if (!wrapped.insert(wrapper.oid).second) {
      continue;
    }
    ScanDependents(
      transaction, wrapper,
      [&](duckdb::CatalogEntry& dependent,
          const duckdb::DependencyDependentFlags&) {
        if (dependent.type == duckdb::CatalogType::TYPE_ENTRY) {
          wrappers.push_back(&dependent);
          return;
        }
        if (dependent.type != duckdb::CatalogType::TABLE_ENTRY) {
          return;
        }
        auto& table = dependent.Cast<duckdb::TableCatalogEntry>();
        for (const auto& column : table.GetColumns().Logical()) {
          if (duckdb::TypeVisitor::Contains(
                column.Type(), [&](const duckdb::LogicalType& type) {
                  const auto oid = CatalogOid(type);
                  return oid && *oid == wrapper.oid;
                })) {
            ThrowNestedUse(domain, table, column);
          }
        }
      });
  }

  std::vector<DomainDependentTable> result;
  absl::flat_hash_set<duckdb::idx_t> seen;
  for (auto* entry : tables) {
    auto& table = entry->Cast<duckdb::TableCatalogEntry>();
    if (!seen.insert(table.oid).second) {
      continue;
    }
    DomainDependentTable table_columns{.table = &table};
    for (const auto& column : table.GetColumns().Logical()) {
      const auto& type = column.Type();
      if (const auto oid = DomainOid(type); oid && domains.contains(*oid)) {
        table_columns.columns.push_back(column.Logical());
      } else if (duckdb::TypeVisitor::Contains(
                   type, [&](const duckdb::LogicalType& element) {
                     const auto oid = DomainOid(element);
                     return oid && domains.contains(*oid);
                   })) {
        ThrowNestedUse(domain, table, column);
      }
    }
    if (!table_columns.columns.empty()) {
      result.push_back(std::move(table_columns));
    }
  }
  return result;
}

}  // namespace

void ValidateDomainConstraint(duckdb::CatalogTransaction transaction,
                              const duckdb::TypeCatalogEntry& old_domain,
                              duckdb::TypeCatalogEntry& new_domain,
                              const duckdb::AlterDomainInfo& alter) {
  const auto* constraint = ConstraintToValidate(old_domain, new_domain, alter);
  if (!constraint) {
    return;
  }
  for (const auto& [table, columns] :
       DomainDependentColumns(transaction, new_domain)) {
    auto successor = table->Cast<SearchTableEntry>().VerifyDomainConstraint(
      transaction.GetContext(), columns, *constraint);
    successor->oid = table->oid;
    successor->permissions = table->permissions;
    table->ParentSchema(transaction)
      .Cast<Schema>()
      .Set(duckdb::CatalogType::TABLE_ENTRY)
      .Alter(transaction, std::move(successor), {});
  }
}

}  // namespace sdb::catalog
