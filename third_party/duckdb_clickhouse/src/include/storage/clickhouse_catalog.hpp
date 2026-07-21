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
#include <unordered_map>
#include <vector>

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "clickhouse_connection.hpp"
#include "storage/clickhouse_connection_pool.hpp"

namespace duckdb {
class ClickHouseCatalog;
class ClickHouseSchemaEntry;

class ClickHouseCatalog : public Catalog {
public:
	explicit ClickHouseCatalog(AttachedDatabase &db_p, const string &connection_string, AccessMode access_mode,
	                           ClientContext &context);
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

	//! The shared dbconnector connection pool (the PostgresConnectionPool analog).
	//! Lease with GetConnectionPool().GetConnection(); the RAII PooledConnection
	//! returns to the pool on destruction. A connection that errored or was
	//! abandoned mid-stream must be Invalidate()d by its holder before release,
	//! or it would poison the next lease (Ping() at lease time is the backstop).
	ClickHouseConnectionPool &GetConnectionPool() {
		return *connection_pool;
	}
	shared_ptr<ClickHouseConnectionPool> GetConnectionPoolPtr() {
		return connection_pool;
	}
	const ClickHouseConnectionParams &GetConnectionParams() const {
		return params;
	}

	//! Drop all cached schema/table metadata so the next lookup re-reads it from
	//! the server (picks up remote schema drift). Retires entries rather than
	//! freeing them, keeping any concurrently-bound raw pointers valid.
	void ClearCache();

	//! Cached clickhouse_query DESCRIBE results, keyed by the describe SQL (+
	//! settings that affect the mapping). A per-batch caller with a constant
	//! schema_query skips one server round trip per bind; cleared with the rest
	//! of the metadata cache.
	struct DescribeCacheEntry {
		vector<std::string> names;
		vector<LogicalType> types;
		vector<bool> stringified;
		vector<std::string> clickhouse_types;
	};
	bool TryGetDescribe(const string &key, DescribeCacheEntry &out) {
		std::lock_guard<std::mutex> guard(describe_cache_lock);
		auto it = describe_cache.find(key);
		if (it == describe_cache.end()) {
			return false;
		}
		out = it->second;
		return true;
	}
	void StoreDescribe(const string &key, DescribeCacheEntry entry) {
		std::lock_guard<std::mutex> guard(describe_cache_lock);
		describe_cache.emplace(key, std::move(entry));
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

	shared_ptr<ClickHouseConnectionPool> connection_pool;

	std::mutex describe_cache_lock;
	std::unordered_map<string, DescribeCacheEntry> describe_cache;
};

} // namespace duckdb
