//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/clickhouse_schema_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
class ClickHouseCatalog;
class ClickHouseTableEntry;

class ClickHouseSchemaEntry : public SchemaCatalogEntry {
public:
	ClickHouseSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, string database);

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

private:
	ClickHouseCatalog &GetClickHouseCatalog();
	//! `context` (may be null) supplies session settings that shape the surfaced
	//! column types (ch_binary_as_blob); null falls back to the defaults.
	ClickHouseTableEntry &GetOrCreateTableEntry(optional_ptr<ClientContext> context, const string &table_name);
	ClickHouseTableEntry &LoadTableEntry(optional_ptr<ClientContext> context, const string &table_name);
	//! Drop `table_name` from the live cache WITHOUT freeing the entry: an in-flight
	//! bound statement (scan/INSERT) may still hold a raw pointer into it. Caller must
	//! hold tables_lock. The entry is parked in `retired_tables` for the catalog's life.
	void RetireTableLocked(const string &table_name);

private:
	string database;
	std::mutex tables_lock;
	case_insensitive_map_t<unique_ptr<ClickHouseTableEntry>> tables;
	//! Entries removed from `tables` by DROP/ALTER/CREATE-OR-REPLACE, kept alive so
	//! concurrently-bound statements that captured a pointer don't dangle.
	vector<unique_ptr<ClickHouseTableEntry>> retired_tables;
};

} // namespace duckdb
