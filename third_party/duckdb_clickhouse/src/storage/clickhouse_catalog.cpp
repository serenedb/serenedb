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

// The access-mode parameter is deliberately unused: access control is delegated to
// ClickHouse RBAC via the mapped remote user, not re-implemented client-side.
ClickHouseCatalog::ClickHouseCatalog(AttachedDatabase &db_p, const string &connection_string, AccessMode access_mode_p)
    : Catalog(db_p), params(ClickHouseConnectionParams::FromConnectionString(connection_string)),
      default_schema(params.database) {
}

ClickHouseCatalog::~ClickHouseCatalog() = default;

void ClickHouseCatalog::Initialize(bool load_builtin) {
	(void)load_builtin;
}

ClickHouseConnection ClickHouseCatalog::OpenConnection() {
	if (ClickHouseConnection::ConnectionCacheEnabled()) {
		std::lock_guard<std::mutex> l(connection_pool_lock);
		while (!idle_connections.empty()) {
			auto conn = std::move(idle_connections.back());
			idle_connections.pop_back();
			if (conn.IsOpen()) {
				return conn;
			}
			// A closed/moved-from entry: discard and try the next.
		}
	}
	return ClickHouseConnection::Open(params);
}

void ClickHouseCatalog::ReturnConnection(ClickHouseConnection connection) {
	// Drop (let it close) if it is not reusable or caching is disabled.
	if (!connection.IsOpen() || !ClickHouseConnection::ConnectionCacheEnabled()) {
		return;
	}
	std::lock_guard<std::mutex> l(connection_pool_lock);
	if (idle_connections.size() < MAX_POOLED_CONNECTIONS) {
		idle_connections.push_back(std::move(connection));
	}
	// Pool full: drop the surplus connection.
}

optional_ptr<CatalogEntry> ClickHouseCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		// Replacing a database silently drops every table in it and ClickHouse has no
		// atomic OR REPLACE DATABASE -- require an explicit DROP instead.
		throw NotImplementedException("CREATE OR REPLACE SCHEMA is not supported for ClickHouse; DROP it explicitly");
	}
	auto connection = OpenConnection();
	// Conflict handling delegated to ClickHouse: IF NOT EXISTS only when the statement
	// asked for it; a plain CREATE on an existing database is CH's own loud error.
	string sql = "CREATE DATABASE ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		sql += "IF NOT EXISTS ";
	}
	sql += ClickHouseQuoteIdentifier(info.schema);
	try {
		ClickHouseConnection::LogQuery(sql);
		connection.GetClient().Execute(sql);
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("creating database", sql, e);
	}
	ReturnConnection(std::move(connection));
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
	auto sql = "DROP DATABASE IF EXISTS " + ClickHouseQuoteIdentifier(info.name);
	try {
		ClickHouseConnection::LogQuery(sql);
		connection.GetClient().Execute(sql);
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("dropping database", sql, e);
	}
	ReturnConnection(std::move(connection));
	std::lock_guard<std::mutex> l(schema_lock);
	auto it = schemas.find(info.name);
	if (it != schemas.end()) {
		// Retire (do not destroy) the entry: a concurrently-bound statement may
		// still hold a raw pointer into it or its (retired) table entries.
		retired_schemas.push_back(std::move(it->second));
		schemas.erase(it);
	}
}

void ClickHouseCatalog::ClearCache() {
	std::lock_guard<std::mutex> l(schema_lock);
	// Retire every cached schema (and its cached table metadata) rather than
	// freeing it, so bound statements keep working; the next lookup rebuilds a
	// fresh entry from the server.
	for (auto &entry : schemas) {
		retired_schemas.push_back(std::move(entry.second));
	}
	schemas.clear();
}

unique_ptr<LogicalOperator> ClickHouseCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt,
                                                               CatalogEntry &table, unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("ClickHouse databases are read-only: CREATE INDEX not supported");
}

void ClickHouseCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto connection = OpenConnection();
	auto &client = connection.GetClient();
	vector<string> schema_names;
	const string sql = "SELECT name FROM system.databases";
	ClickHouseConnection::LogQuery(sql);
	try {
		client.BeginSelect(sql);
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
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("listing databases", sql, e);
	}
	ReturnConnection(std::move(connection));
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
	auto sql = "SELECT name FROM system.databases WHERE name = " + ClickHouseStringLiteral(schema_name);
	ClickHouseConnection::LogQuery(sql);
	try {
		client.BeginSelect(sql);
		while (auto block = client.NextBlock()) {
			if (block->GetRowCount() > 0) {
				found = true;
			}
		}
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("probing database", sql, e);
	}
	ReturnConnection(std::move(connection));
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
	auto sql = "SELECT sum(bytes_on_disk) FROM system.parts WHERE database = " +
	           ClickHouseStringLiteral(default_schema) + " AND active";
	DatabaseSize size;
	ClickHouseConnection::LogQuery(sql);
	try {
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
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("reading database size", sql, e);
	}
	ReturnConnection(std::move(connection));
	return size;
}

bool ClickHouseCatalog::InMemory() {
	return false;
}

string ClickHouseCatalog::GetDBPath() {
	return params.host + ":" + std::to_string(params.port) + "/" + params.database;
}

} // namespace duckdb
