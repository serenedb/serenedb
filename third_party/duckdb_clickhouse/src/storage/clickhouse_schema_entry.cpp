#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"

#include "clickhouse_types.hpp"

#include <clickhouse/client.h>
#include <clickhouse/block.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/exceptions.h>

#include "clickhouse_connection.hpp"
#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_schema_entry.hpp"
#include "storage/clickhouse_table_entry.hpp"

namespace duckdb {

ClickHouseSchemaEntry::ClickHouseSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, string database_p)
    : SchemaCatalogEntry(catalog, info), database(std::move(database_p)) {
}

ClickHouseCatalog &ClickHouseSchemaEntry::GetClickHouseCatalog() {
	return catalog.Cast<ClickHouseCatalog>();
}

static void RunClickHouseDDL(const ClickHouseConnectionParams &params, const string &sql) {
	auto conn = ClickHouseConnection::Open(params);
	try {
		conn.GetClient().Execute(sql);
	} catch (const clickhouse::Error &e) {
		throw IOException("ClickHouse DDL failed: %s\nSQL: %s", e.what(), sql);
	}
}

static string GetCreateTableSQL(const string &database, CreateTableInfo &info) {
	auto &columns = info.columns;

	vector<idx_t> not_null_columns;
	vector<string> order_by;
	for (auto &constraint : info.constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			not_null_columns.push_back(constraint->Cast<NotNullConstraint>().index.index);
		} else if (constraint->type == ConstraintType::UNIQUE) {
			auto &unique = constraint->Cast<UniqueConstraint>();
			if (!unique.IsPrimaryKey()) {
				continue;
			}
			if (unique.HasIndex()) {
				order_by.push_back(columns.GetColumn(unique.GetIndex()).GetName());
			} else {
				for (auto &col_name : unique.GetColumnNames()) {
					order_by.push_back(col_name);
				}
			}
		}
	}
	auto is_not_null = [&](idx_t logical_index, const string &col_name) {
		for (auto idx : not_null_columns) {
			if (idx == logical_index) {
				return true;
			}
		}
		for (auto &key : order_by) {
			if (key == col_name) {
				return true;
			}
		}
		return false;
	};

	string sql = "CREATE TABLE " + ClickHouseQuoteIdentifier(database) + "." + ClickHouseQuoteIdentifier(info.table) + " (";
	for (idx_t i = 0; i < columns.LogicalColumnCount(); i++) {
		auto &col = columns.GetColumn(LogicalIndex(i));
		if (i > 0) {
			sql += ", ";
		}
		bool nullable = !is_not_null(i, col.GetName());
		sql += ClickHouseQuoteIdentifier(col.GetName()) + " " + LogicalTypeToClickHouseType(col.GetType(), nullable);
	}
	sql += ") ENGINE = MergeTree ORDER BY ";
	if (order_by.empty()) {
		sql += "tuple()";
	} else {
		sql += "(";
		for (idx_t i = 0; i < order_by.size(); i++) {
			if (i > 0) {
				sql += ", ";
			}
			sql += ClickHouseQuoteIdentifier(order_by[i]);
		}
		sql += ")";
	}
	return sql;
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                              BoundCreateTableInfo &info) {
	auto &base = info.Base();
	auto &params = GetClickHouseCatalog().GetConnectionParams();

	if (base.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DropInfo drop;
		drop.type = CatalogType::TABLE_ENTRY;
		drop.name = base.table;
		drop.schema = name;
		drop.if_not_found = OnEntryNotFound::RETURN_NULL;
		drop.cascade = false;
		DropEntry(transaction.GetContext(), drop);
	} else if (base.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		auto existing = LookupEntry(transaction, EntryLookupInfo(CatalogType::TABLE_ENTRY, base.table));
		if (existing) {
			return existing;
		}
	}

	RunClickHouseDDL(params, GetCreateTableSQL(database, base));

	{
		std::lock_guard<std::mutex> l(tables_lock);
		RetireTableLocked(base.table);
	}
	return &GetOrCreateTableEntry(base.table);
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                                 CreateFunctionInfo &info) {
	throw NotImplementedException("ClickHouse databases are read-only: cannot create function");
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                              TableCatalogEntry &table) {
	throw NotImplementedException("ClickHouse databases are read-only: cannot create index");
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("ClickHouse databases are read-only: cannot create view");
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                                 CreateSequenceInfo &info) {
	throw NotImplementedException("ClickHouse databases are read-only: cannot create sequence");
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                      CreateTableFunctionInfo &info) {
	throw NotImplementedException("ClickHouse databases are read-only: cannot create table function");
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                     CreateCopyFunctionInfo &info) {
	throw NotImplementedException("ClickHouse databases are read-only: cannot create copy function");
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                       CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("ClickHouse databases are read-only: cannot create pragma function");
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                                  CreateCollationInfo &info) {
	throw NotImplementedException("ClickHouse databases are read-only: cannot create collation");
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("ClickHouse databases are read-only: cannot create type");
}

void ClickHouseSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw NotImplementedException("ClickHouse: only ALTER TABLE is supported");
	}
	auto &alter = info.Cast<AlterTableInfo>();
	auto &params = GetClickHouseCatalog().GetConnectionParams();
	string qualified = ClickHouseQuoteIdentifier(database) + "." + ClickHouseQuoteIdentifier(alter.name);

	string sql;
	switch (alter.alter_table_type) {
	case AlterTableType::RENAME_TABLE: {
		auto &rename = alter.Cast<RenameTableInfo>();
		sql = "RENAME TABLE " + qualified + " TO " + ClickHouseQuoteIdentifier(database) + "." +
		      ClickHouseQuoteIdentifier(rename.new_table_name);
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &rename = alter.Cast<RenameColumnInfo>();
		sql = "ALTER TABLE " + qualified + " RENAME COLUMN " + ClickHouseQuoteIdentifier(rename.old_name) + " TO " +
		      ClickHouseQuoteIdentifier(rename.new_name);
		break;
	}
	case AlterTableType::ADD_COLUMN: {
		auto &add = alter.Cast<AddColumnInfo>();
		sql = "ALTER TABLE " + qualified + " ADD COLUMN " + ClickHouseQuoteIdentifier(add.new_column.GetName()) + " " +
		      LogicalTypeToClickHouseType(add.new_column.GetType(), true);
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove = alter.Cast<RemoveColumnInfo>();
		sql = "ALTER TABLE " + qualified + " DROP COLUMN " + ClickHouseQuoteIdentifier(remove.removed_column);
		break;
	}
	default:
		throw NotImplementedException("ClickHouse: unsupported ALTER TABLE operation");
	}

	RunClickHouseDDL(params, sql);
	std::lock_guard<std::mutex> l(tables_lock);
	RetireTableLocked(alter.name);
}

ClickHouseTableEntry &ClickHouseSchemaEntry::LoadTableEntry(const string &table_name) {
	auto &clickhouse_catalog = GetClickHouseCatalog();
	auto &params = clickhouse_catalog.GetConnectionParams();

	CreateTableInfo create_info;
	create_info.schema = name;
	create_info.table = table_name;

	// PK columns from the engine's own metadata. ClickHouse's primary key is the
	// MergeTree sorting prefix -- not a uniqueness constraint -- but it is the
	// stable lookup key serenedb keys an inverted index on (PkSpec::ExternalDBKey).
	vector<string> pk_columns;
	vector<string> clickhouse_types;
	{
		string sql = "SELECT name, type, is_in_primary_key FROM system.columns WHERE database = '" +
		             ClickHouseEscapeSingleQuotes(database) + "' AND table = '" +
		             ClickHouseEscapeSingleQuotes(table_name) + "' ORDER BY position";
		auto conn = ClickHouseConnection::Open(params);
		auto &client = conn.GetClient();
		client.Select(sql, [&](const clickhouse::Block &block) {
			if (block.GetColumnCount() == 0) {
				return;
			}
			auto names = block[0]->As<clickhouse::ColumnString>();
			auto types = block[1]->As<clickhouse::ColumnString>();
			auto in_pk = block[2]->As<clickhouse::ColumnUInt8>();
			for (size_t row = 0; row < block.GetRowCount(); row++) {
				string column_name(names->At(row));
				string type_str(types->At(row));
				create_info.columns.AddColumn(ColumnDefinition(column_name, ClickHouseTypeStringToLogicalType(type_str)));
				clickhouse_types.push_back(type_str);
				if (in_pk && in_pk->At(row) != 0) {
					pk_columns.push_back(column_name);
				}
			}
		});
	}
	// system.columns returned no rows: the table does not exist (or was dropped
	// concurrently), or is otherwise unreadable. Do not construct + cache a
	// zero-column entry (which would then mis-bind); LookupEntry maps this
	// CatalogException to its not-found result.
	if (create_info.columns.LogicalColumnCount() == 0) {
		throw CatalogException("ClickHouse table \"%s\".\"%s\" has no columns (it may have been dropped concurrently)",
		                       database, table_name);
	}
	if (!pk_columns.empty()) {
		create_info.constraints.push_back(make_uniq<UniqueConstraint>(std::move(pk_columns), /*is_primary_key=*/true));
	}

	auto entry = make_uniq<ClickHouseTableEntry>(catalog, *this, create_info);
	entry->params = params;
	entry->database = database;
	entry->table = table_name;
	entry->clickhouse_types = std::move(clickhouse_types);
	auto &result = *entry;
	tables[table_name] = std::move(entry);
	return result;
}

ClickHouseTableEntry &ClickHouseSchemaEntry::GetOrCreateTableEntry(const string &table_name) {
	std::lock_guard<std::mutex> l(tables_lock);
	auto entry = tables.find(table_name);
	if (entry != tables.end()) {
		return *entry->second;
	}
	return LoadTableEntry(table_name);
}

void ClickHouseSchemaEntry::RetireTableLocked(const string &table_name) {
	auto entry = tables.find(table_name);
	if (entry == tables.end()) {
		return;
	}
	// Move (don't free) the entry so any concurrently-bound statement holding a
	// raw pointer into it stays valid; then drop the name from the live cache.
	retired_tables.push_back(std::move(entry->second));
	tables.erase(entry);
}

void ClickHouseSchemaEntry::Scan(ClientContext &context, CatalogType type,
                                 const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto &clickhouse_catalog = GetClickHouseCatalog();
	auto &params = clickhouse_catalog.GetConnectionParams();

	vector<string> table_names;
	{
		string sql = "SELECT name FROM system.tables WHERE database = '" + ClickHouseEscapeSingleQuotes(database) + "'";
		auto conn = ClickHouseConnection::Open(params);
		auto &client = conn.GetClient();
		client.Select(sql, [&](const clickhouse::Block &block) {
			if (block.GetColumnCount() == 0) {
				return;
			}
			auto names = block[0]->As<clickhouse::ColumnString>();
			for (size_t row = 0; row < block.GetRowCount(); row++) {
				table_names.emplace_back(names->At(row));
			}
		});
	}

	for (auto &table_name : table_names) {
		// A table can vanish (a concurrent DROP on ClickHouse) between the
		// system.tables listing above and the per-table column fetch in
		// GetOrCreateTableEntry; skip it rather than aborting the entire scan.
		try {
			auto &entry = GetOrCreateTableEntry(table_name);
			callback(entry);
		} catch (const std::exception &) {
			continue;
		}
	}
}

void ClickHouseSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	throw NotImplementedException("Scan without context not supported");
}

void ClickHouseSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type != CatalogType::TABLE_ENTRY && info.type != CatalogType::VIEW_ENTRY) {
		throw NotImplementedException("ClickHouse: cannot drop entry of this type");
	}
	auto &params = GetClickHouseCatalog().GetConnectionParams();
	const char *kind = info.type == CatalogType::VIEW_ENTRY ? "VIEW" : "TABLE";
	string sql = string("DROP ") + kind + " IF EXISTS " + ClickHouseQuoteIdentifier(database) + "." + ClickHouseQuoteIdentifier(info.name);
	RunClickHouseDDL(params, sql);
	std::lock_guard<std::mutex> l(tables_lock);
	RetireTableLocked(info.name);
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                              const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetCatalogType() != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	auto &table_name = lookup_info.GetEntryName();

	{
		std::lock_guard<std::mutex> l(tables_lock);
		auto entry = tables.find(table_name);
		if (entry != tables.end()) {
			return entry->second.get();
		}
	}

	// Cache miss: load directly. LoadTableEntry runs a single remote query
	// (system.columns) and throws a CatalogException when the table has no
	// columns -- it does not exist, or was dropped concurrently -- which is the
	// not-found result here. A connection/protocol error (clickhouse::Error) or an
	// unsupported column type (NotImplementedException) is not a CatalogException
	// and propagates. This subsumes what a separate system.tables existence probe
	// did, avoiding a second round-trip per cache miss.
	try {
		return &GetOrCreateTableEntry(table_name);
	} catch (const CatalogException &) {
		return nullptr;
	}
}

} // namespace duckdb
