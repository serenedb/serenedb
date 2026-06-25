#include "duckdb.hpp"

#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"

#include <clickhouse/client.h>
#include <clickhouse/block.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/exceptions.h>

#include "clickhouse_connection.hpp"
#include "clickhouse_types.hpp"
#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_schema_entry.hpp"
#include "storage/clickhouse_table_entry.hpp"

namespace duckdb {

ClickHouseCatalog::ClickHouseCatalog(AttachedDatabase &db_p, const string &connection_string, AccessMode access_mode_p)
    : Catalog(db_p), params(ClickHouseConnectionParams::FromConnectionString(connection_string)),
      access_mode(access_mode_p), default_schema(params.database) {
}

ClickHouseCatalog::~ClickHouseCatalog() = default;

void ClickHouseCatalog::Initialize(bool load_builtin) {
	(void)load_builtin;
}

ClickHouseConnection ClickHouseCatalog::OpenConnection() {
	return ClickHouseConnection::Open(params);
}

optional_ptr<CatalogEntry> ClickHouseCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto connection = OpenConnection();
	try {
		connection.GetClient().Execute("CREATE DATABASE IF NOT EXISTS " + ClickHouseQuoteIdentifier(info.schema));
	} catch (const clickhouse::Error &e) {
		throw IOException("ClickHouse error creating database \"%s\": %s", info.schema, e.what());
	}
	std::lock_guard<std::mutex> l(schema_lock);
	auto entry = schemas.find(info.schema);
	if (entry == schemas.end()) {
		auto schema_entry = make_uniq<ClickHouseSchemaEntry>(*this, info, info.schema);
		entry = schemas.emplace(info.schema, std::move(schema_entry)).first;
	}
	return entry->second.get();
}

void ClickHouseCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	auto connection = OpenConnection();
	try {
		connection.GetClient().Execute("DROP DATABASE IF EXISTS " + ClickHouseQuoteIdentifier(info.name));
	} catch (const clickhouse::Error &e) {
		throw IOException("ClickHouse error dropping database \"%s\": %s", info.name, e.what());
	}
	std::lock_guard<std::mutex> l(schema_lock);
	auto it = schemas.find(info.name);
	if (it != schemas.end()) {
		// Retire (do not destroy) the entry: a concurrently-bound statement may
		// still hold a raw pointer into it or its (retired) table entries.
		retired_schemas.push_back(std::move(it->second));
		schemas.erase(it);
	}
}

PhysicalOperator &ClickHouseCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                   LogicalMergeInto &op, PhysicalOperator &plan) {
	throw NotImplementedException("ClickHouse databases are read-only: MERGE INTO not supported");
}

unique_ptr<LogicalOperator> ClickHouseCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                               CatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("ClickHouse databases are read-only: CREATE INDEX not supported");
}

void ClickHouseCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto connection = OpenConnection();
	auto &client = connection.GetClient();
	vector<string> schema_names;
	client.BeginSelect("SELECT name FROM system.databases");
	while (auto block = client.NextBlock()) {
		if (block->GetColumnCount() == 0) {
			continue;
		}
		auto name_col = (*block)[0]->As<clickhouse::ColumnString>();
		if (!name_col) {
			continue;
		}
		for (idx_t row = 0; row < block->GetRowCount(); row++) {
			schema_names.emplace_back(name_col->At(row));
		}
	}
	for (auto &schema_name : schema_names) {
		std::lock_guard<std::mutex> l(schema_lock);
		auto entry = schemas.find(schema_name);
		if (entry == schemas.end()) {
			CreateSchemaInfo info;
			info.schema = schema_name;
			auto schema_entry = make_uniq<ClickHouseSchemaEntry>(*this, info, schema_name);
			entry = schemas.emplace(schema_name, std::move(schema_entry)).first;
		}
		callback(*entry->second);
	}
}

optional_ptr<SchemaCatalogEntry> ClickHouseCatalog::LookupSchema(CatalogTransaction transaction,
                                                                 const EntryLookupInfo &schema_lookup,
                                                                 OnEntryNotFound if_not_found) {
	auto schema_name = schema_lookup.GetEntryName();
	auto connection = OpenConnection();
	auto &client = connection.GetClient();
	bool found = false;
	auto sql = "SELECT name FROM system.databases WHERE name = '" + ClickHouseEscapeSingleQuotes(schema_name) + "'";
	client.BeginSelect(sql);
	while (auto block = client.NextBlock()) {
		if (block->GetRowCount() > 0) {
			found = true;
		}
	}
	if (!found) {
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			return nullptr;
		}
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	std::lock_guard<std::mutex> l(schema_lock);
	auto entry = schemas.find(schema_name);
	if (entry == schemas.end()) {
		CreateSchemaInfo info;
		info.schema = schema_name;
		auto schema_entry = make_uniq<ClickHouseSchemaEntry>(*this, info, schema_name);
		entry = schemas.emplace(schema_name, std::move(schema_entry)).first;
	}
	return entry->second.get();
}

DatabaseSize ClickHouseCatalog::GetDatabaseSize(ClientContext &context) {
	auto connection = OpenConnection();
	auto &client = connection.GetClient();
	auto sql = "SELECT sum(bytes_on_disk) FROM system.parts WHERE database = '" + ClickHouseEscapeSingleQuotes(default_schema) +
	           "' AND active";
	DatabaseSize size;
	client.BeginSelect(sql);
	while (auto block = client.NextBlock()) {
		if (block->GetColumnCount() == 0 || block->GetRowCount() == 0) {
			continue;
		}
		auto bytes_col = (*block)[0]->As<clickhouse::ColumnUInt64>();
		if (bytes_col) {
			size.bytes = bytes_col->At(0);
		}
	}
	return size;
}

bool ClickHouseCatalog::InMemory() {
	return false;
}

string ClickHouseCatalog::GetDBPath() {
	return params.host + ":" + std::to_string(params.port) + "/" + params.database;
}

} // namespace duckdb
