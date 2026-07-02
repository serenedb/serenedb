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

	//! Lease a connection: reuse an idle pooled one (same params) if available and the
	//! connection cache is enabled, otherwise open a fresh connection.
	ClickHouseConnection OpenConnection();
	//! Return a connection to the idle pool for reuse. Pass ONLY a connection left in a
	//! clean state -- a fully-drained scan or a committed statement. A connection that
	//! errored or was abandoned mid-stream must be dropped (let it destruct), never
	//! returned here, or it would poison the next lease.
	void ReturnConnection(ClickHouseConnection connection);
	const ClickHouseConnectionParams &GetConnectionParams() const {
		return params;
	}

	//! Drop all cached schema/table metadata so the next lookup re-reads it from
	//! the server (picks up remote schema drift). Retires entries rather than
	//! freeing them, keeping any concurrently-bound raw pointers valid.
	void ClearCache();

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

	// Idle connections kept for reuse across transactions/scans (all share this
	// catalog's params). Guarded by connection_pool_lock; capped at
	// MAX_POOLED_CONNECTIONS (excess returns are dropped).
	static constexpr idx_t MAX_POOLED_CONNECTIONS = 8;
	std::mutex connection_pool_lock;
	vector<ClickHouseConnection> idle_connections;
};

} // namespace duckdb
