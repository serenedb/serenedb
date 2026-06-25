//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/clickhouse_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <mutex>
#include <vector>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "clickhouse_connection.hpp"

namespace duckdb {
class ClickHouseCatalog;
class ClickHouseSchemaEntry;

class ClickHouseCatalog : public Catalog {
public:
	explicit ClickHouseCatalog(AttachedDatabase &db_p, const string &connection_string, AccessMode access_mode);
	~ClickHouseCatalog();

	ClickHouseConnectionParams params;
	AccessMode access_mode;

public:
	void Initialize(bool load_builtin) override;
	string GetCatalogType() override {
		return "clickhouse";
	}
	string GetDefaultSchema() const override {
		return default_schema.empty() ? "default" : default_schema;
	}

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;

	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, CatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;

	bool InMemory() override;
	string GetDBPath() override;

	ClickHouseConnection OpenConnection();
	const ClickHouseConnectionParams &GetConnectionParams() const {
		return params;
	}

	CatalogLookupBehavior CatalogTypeLookupRule(CatalogType type) const override {
		switch (type) {
		case CatalogType::TABLE_ENTRY:
		case CatalogType::VIEW_ENTRY:
			return CatalogLookupBehavior::STANDARD;
		default:
			return CatalogLookupBehavior::NEVER_LOOKUP;
		}
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	string default_schema;
	std::mutex schema_lock;
	case_insensitive_map_t<unique_ptr<ClickHouseSchemaEntry>> schemas;
	// Dropped schema entries are parked here (not destroyed) for the catalog's
	// lifetime, so a concurrently-bound statement holding a raw pointer into a
	// schema (and its table/retired_tables entries) never dangles.
	vector<unique_ptr<ClickHouseSchemaEntry>> retired_schemas;
};

} // namespace duckdb
