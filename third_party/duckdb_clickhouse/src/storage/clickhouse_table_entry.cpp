#include "duckdb.hpp"

#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"

#include "storage/clickhouse_catalog.hpp"
#include "storage/clickhouse_table_entry.hpp"
#include "clickhouse_scanner.hpp"

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
	string rowid_column;
	if (TryGetRowIdColumn(rowid_column)) {
		bd->rowid_column = std::move(rowid_column);
	}
	bd->table_entry = this;
	bind_data = std::move(bd);
	return ClickHouseScanFunctionFilterPushdown();
}

TableStorageInfo ClickHouseTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	result.cardinality = 0;
	return result;
}

} // namespace duckdb
