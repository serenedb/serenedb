#include "duckdb.hpp"

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_table_entry.hpp"
#include "clickhouse_scanner.hpp"
#include "clickhouse_types.hpp"

#include <clickhouse/block.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/exceptions.h>

namespace duckdb {

ClickHouseTableEntry::ClickHouseTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
}

static bool IsRowIdInteger(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
		return true;
	default:
		// UBIGINT / (U)HUGEINT may overflow int64, so they are not usable as the rowid.
		return false;
	}
}

bool ClickHouseTableEntry::TryGetRowIdColumn(string &result) const {
	for (auto &constraint : GetConstraints()) {
		if (constraint->type != ConstraintType::UNIQUE) {
			continue;
		}
		auto &unique = constraint->Cast<UniqueConstraint>();
		if (!unique.IsPrimaryKey()) {
			continue;
		}
		string column_name;
		if (unique.HasIndex()) {
			column_name = GetColumns().GetColumn(unique.GetIndex()).GetName();
		} else {
			auto &names = unique.GetColumnNames();
			if (names.size() != 1) {
				return false;
			}
			column_name = names[0];
		}
		auto &col = GetColumns().GetColumn(column_name);
		if (IsRowIdInteger(col.GetType())) {
			result = column_name;
			return true;
		}
		return false;
	}
	return false;
}

unique_ptr<BaseStatistics> ClickHouseTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

TableFunction ClickHouseTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto bd = make_uniq<ClickHouseBindData>();
	bd->params = params;
	bd->database = database;
	bd->table = table;
	bd->from_query = false;
	bd->names = GetColumns().GetColumnNames();
	bd->types = GetColumns().GetColumnTypes();
	bd->stringified = stringified;
	string rowid_column;
	if (TryGetRowIdColumn(rowid_column)) {
		bd->rowid_column = std::move(rowid_column);
	}
	bd->table_entry = this;

	// Lazily capture an approximate row count for the optimizer's cardinality estimate.
	// ifNull(...) keeps the count as a non-null UInt64; the second column flags whether
	// the server actually knows it (NULL for View / Distributed engines).
	std::lock_guard<std::mutex> row_count_guard(row_count_lock);
	if (!row_count_fetched) {
		row_count_fetched = true;
		try {
			auto &ch_catalog = catalog.Cast<ClickHouseCatalog>();
			auto conn = ch_catalog.OpenConnection();
			string sql = "SELECT ifNull(total_rows, 0) AS n, total_rows IS NOT NULL AS known "
			             "FROM system.tables WHERE database = " + ClickHouseStringLiteral(database) +
			             " AND name = " + ClickHouseStringLiteral(table);
			ClickHouseConnection::LogQuery(sql);
			conn.GetClient().Select(sql, [&](const clickhouse::Block &block) {
				if (block.GetColumnCount() < 2 || block.GetRowCount() == 0) {
					return;
				}
				auto n = block[0]->As<clickhouse::ColumnUInt64>();
				auto known = block[1]->As<clickhouse::ColumnUInt8>();
				if (n && known && known->At(0) != 0) {
					cached_row_count = static_cast<idx_t>(n->At(0));
					cached_row_count_known = true;
				}
			});
			ch_catalog.ReturnConnection(std::move(conn));
		} catch (...) {
			// A stats failure must never break the scan -- fall back to no estimate.
		}
	}
	bd->has_cardinality = cached_row_count_known;
	bd->approx_row_count = cached_row_count;

	bind_data = std::move(bd);
	return ClickHouseScanFunctionFilterPushdown();
}

TableStorageInfo ClickHouseTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	result.cardinality = 0;
	return result;
}

void VerifyRowIdCoverage(ClickHouseConnection &connection, const string &database, const string &table_name,
                         const string &pk_column, const string &id_list, idx_t distinct_keys, const char *op) {
	if (id_list.empty()) {
		return;
	}
	// How many rows does `pk IN (id_list)` match on the server? More than one row per
	// distinct key means the key is not unique: the mutation would also touch sibling
	// rows the statement may not have matched (undetectable from here -- see header).
	string sql = "SELECT count() FROM " + ClickHouseQuoteIdentifier(database) + "." + ClickHouseQuoteIdentifier(table_name) +
	             " WHERE " + ClickHouseQuoteIdentifier(pk_column) + " IN (" + id_list + ")";
	ClickHouseConnection::LogQuery(sql);
	uint64_t table_count = 0;
	try {
		connection.GetClient().Select(sql, [&](const clickhouse::Block &block) {
			if (block.GetColumnCount() == 0 || block.GetRowCount() == 0) {
				return;
			}
			auto n = block[0]->As<clickhouse::ColumnUInt64>();
			if (n) {
				table_count = n->At(0);
			}
		});
	} catch (const clickhouse::Error &e) {
		throw IOException("ClickHouse error verifying row identity for %s: %s", op, e.what());
	}
	if (table_count > distinct_keys) {
		throw InvalidInputException(
		    "Refusing to %s ClickHouse table \"%s\": the key \"%s\" used as the row identifier is not "
		    "unique (a MergeTree ORDER BY key is a sorting prefix, not a uniqueness constraint), so this "
		    "statement could %s row(s) sharing a key value that it did not match. Use a table whose key "
		    "is unique.",
		    op, table_name, pk_column, op);
	}
}

} // namespace duckdb
