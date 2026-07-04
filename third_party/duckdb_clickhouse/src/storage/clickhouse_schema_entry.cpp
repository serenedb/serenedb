#include "duckdb.hpp"

#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

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

static void RunClickHouseDDL(ClickHouseCatalog &ch_catalog, const string &sql) {
	auto conn = ch_catalog.OpenConnection();
	try {
		ClickHouseConnection::LogQuery(sql);
		conn.GetClient().Execute(sql);
	} catch (const clickhouse::Error &e) {
		ClickHouseConnection::ThrowError("running DDL", sql, e);
	}
	ch_catalog.ReturnConnection(std::move(conn));
}

// Render a *constant* column DEFAULT into ClickHouse DDL. Non-constant defaults
// (e.g. now(), nextval) are dropped rather than mistranslated -- matching the
// postgres connector, which only forwards defaults it can render safely.
static string RenderConstantDefault(const ColumnDefinition &col) {
	if (!col.HasDefaultValue()) {
		return "";
	}
	auto &expr = col.DefaultValue();
	if (expr.GetExpressionClass() != ExpressionClass::CONSTANT) {
		return "";
	}
	auto &value = expr.Cast<ConstantExpression>().GetValue();
	if (value.IsNull()) {
		return "";
	}
	try {
		return ClickHouseValueLiteral(value);
	} catch (const NotImplementedException &) {
		// A constant with no exact ClickHouse literal form (nested types): drop the
		// default rather than mistranslate it.
		return "";
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

	// Conflict handling is delegated to ClickHouse: OR REPLACE / IF NOT EXISTS are
	// atomic server-side, where a client-side probe/drop sequence is racy and costs
	// extra round-trips.
	string sql = "CREATE ";
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		sql += "OR REPLACE ";
	}
	sql += "TABLE ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		sql += "IF NOT EXISTS ";
	}
	sql += ClickHouseQuoteIdentifier(database) + "." + ClickHouseQuoteIdentifier(info.table) + " (";
	for (idx_t i = 0; i < columns.LogicalColumnCount(); i++) {
		auto &col = columns.GetColumn(LogicalIndex(i));
		if (i > 0) {
			sql += ", ";
		}
		bool nullable = !is_not_null(i, col.GetName());
		sql += ClickHouseQuoteIdentifier(col.GetName()) + " " + LogicalTypeToClickHouseType(col.GetType(), nullable);
		auto default_literal = RenderConstantDefault(col);
		if (!default_literal.empty()) {
			sql += " DEFAULT " + default_literal;
		}
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

	// OR REPLACE / IF NOT EXISTS are rendered into the DDL (see GetCreateTableSQL):
	// ClickHouse resolves the conflict atomically, and a plain CREATE on an existing
	// table is its loud "already exists" error -- no client-side probe or drop needed.
	RunClickHouseDDL(GetClickHouseCatalog(), GetCreateTableSQL(database, base));

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

// The view's parsed SELECT references ClickHouse tables via this catalog's ATTACH
// alias, e.g. `ch`.`db`.`t` -- a three-part name that ClickHouse (which knows only
// `db`.`t`) cannot parse. Strip the catalog qualifier from every table reference,
// including those nested in joins and sub-queries, before rendering the DDL.
static void StripCatalogFromTableRef(TableRef &ref);
static void StripCatalogFromQueryNode(QueryNode &node);

static void StripCatalogFromExpression(unique_ptr<ParsedExpression> &expr) {
	if (expr->GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expr->Cast<SubqueryExpression>();
		if (subquery.subquery && subquery.subquery->node) {
			StripCatalogFromQueryNode(*subquery.subquery->node);
		}
	} else if (expr->GetExpressionClass() == ExpressionClass::FUNCTION) {
		// DuckDB renders COUNT(*) as count_star(); ClickHouse spells it count().
		auto &func = expr->Cast<FunctionExpression>();
		if (func.function_name == "count_star" && func.children.empty()) {
			func.function_name = "count";
		}
	}
	ParsedExpressionIterator::EnumerateChildren(*expr, StripCatalogFromExpression);
}

static void StripCatalogFromTableRef(TableRef &ref) {
	// Leaf action only. EnumerateQueryNodeChildren / EnumerateTableRefChildren already
	// invoke this callback on every nested table ref (joins, sub-queries) -- and
	// EnumerateTableRefChildren calls ref_callback on the ref itself at the end, so
	// re-entering Enumerate here would recurse on `ref` forever (stack overflow).
	if (ref.type == TableReferenceType::BASE_TABLE) {
		ref.Cast<BaseTableRef>().catalog_name = "";
	}
}

static void StripCatalogFromQueryNode(QueryNode &node) {
	ParsedExpressionIterator::EnumerateQueryNodeChildren(node, StripCatalogFromExpression, StripCatalogFromTableRef);
}

static string GetCreateViewSQL(const string &database, CreateViewInfo &info) {
	// ClickHouse CREATE VIEW takes no column-alias list after the name (unlike
	// postgres); the SELECT's own output names define the view's columns. Render a
	// catalog-stripped copy of the query so its table references are valid CH SQL.
	auto query = info.query->Copy();
	auto &select = query->Cast<SelectStatement>();
	if (select.node) {
		StripCatalogFromQueryNode(*select.node);
	}
	// Conflict handling delegated to ClickHouse (atomic OR REPLACE / IF NOT EXISTS),
	// as in GetCreateTableSQL.
	string sql = "CREATE ";
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		sql += "OR REPLACE ";
	}
	sql += "VIEW ";
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
		sql += "IF NOT EXISTS ";
	}
	return sql + ClickHouseQuoteIdentifier(database) + "." + ClickHouseQuoteIdentifier(info.view_name) + " AS " +
	       select.ToString();
}

optional_ptr<CatalogEntry> ClickHouseSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	if (!info.query) {
		throw BinderException("Cannot create a ClickHouse view from an empty SQL statement");
	}
	// Conflict handling delegated to ClickHouse, as in CreateTable.
	RunClickHouseDDL(GetClickHouseCatalog(), GetCreateViewSQL(database, info));
	{
		std::lock_guard<std::mutex> l(tables_lock);
		RetireTableLocked(info.view_name);
	}
	return &GetOrCreateTableEntry(info.view_name);
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

	RunClickHouseDDL(GetClickHouseCatalog(), sql);
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
	vector<bool> stringified_columns;
	{
		string sql = "SELECT name, type, is_in_primary_key FROM system.columns WHERE database = " +
		             ClickHouseStringLiteral(database) + " AND table = " + ClickHouseStringLiteral(table_name) +
		             " ORDER BY position";
		auto conn = clickhouse_catalog.OpenConnection();
		auto &client = conn.GetClient();
		ClickHouseConnection::LogQuery(sql);
		try {
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
					LogicalType logical_type;
					bool needs_to_string = false;
					try {
						logical_type = ClickHouseTypeStringToLogicalType(type_str);
					} catch (const NotImplementedException &) {
						// A column type with no DuckDB mapping must not make the WHOLE
						// table unreadable: degrade this column to VARCHAR; the scan
						// projects toString(col) so the wire carries a plain String.
						logical_type = LogicalType::VARCHAR;
						needs_to_string = true;
					}
					create_info.columns.AddColumn(ColumnDefinition(column_name, std::move(logical_type)));
					clickhouse_types.push_back(type_str);
					stringified_columns.push_back(needs_to_string);
					if (in_pk && in_pk->At(row) != 0) {
						pk_columns.push_back(column_name);
					}
				}
			});
		} catch (const clickhouse::Error &e) {
			ClickHouseConnection::ThrowError("reading table columns", sql, e);
		}
		clickhouse_catalog.ReturnConnection(std::move(conn));
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
	entry->stringified = std::move(stringified_columns);
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

	vector<string> table_names;
	{
		string sql = "SELECT name FROM system.tables WHERE database = " + ClickHouseStringLiteral(database);
		auto conn = clickhouse_catalog.OpenConnection();
		auto &client = conn.GetClient();
		ClickHouseConnection::LogQuery(sql);
		try {
			client.Select(sql, [&](const clickhouse::Block &block) {
				if (block.GetColumnCount() == 0) {
					return;
				}
				auto names = block[0]->As<clickhouse::ColumnString>();
				for (size_t row = 0; row < block.GetRowCount(); row++) {
					table_names.emplace_back(names->At(row));
				}
			});
		} catch (const clickhouse::Error &e) {
			ClickHouseConnection::ThrowError("listing tables", sql, e);
		}
		clickhouse_catalog.ReturnConnection(std::move(conn));
	}

	for (auto &table_name : table_names) {
		// Skip a table that vanished (concurrent DROP between the system.tables listing
		// and the per-table column fetch) or whose columns cannot be mapped yet -- but
		// let connection/protocol errors propagate rather than silently emptying the
		// listing.
		try {
			auto &entry = GetOrCreateTableEntry(table_name);
			callback(entry);
		} catch (const CatalogException &) {
			continue;
		} catch (const NotImplementedException &) {
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
	const char *kind = info.type == CatalogType::VIEW_ENTRY ? "VIEW" : "TABLE";
	string sql = string("DROP ") + kind + " IF EXISTS " + ClickHouseQuoteIdentifier(database) + "." + ClickHouseQuoteIdentifier(info.name);
	RunClickHouseDDL(GetClickHouseCatalog(), sql);
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
