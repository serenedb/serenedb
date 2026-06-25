#include "duckdb.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <clickhouse/client.h>
#include <clickhouse/block.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/exceptions.h>

#include <optional>

#include "clickhouse_connection.hpp"
#include "clickhouse_types.hpp"
#include "clickhouse_filter_pushdown.hpp"
#include "clickhouse_scanner.hpp"

namespace duckdb {

unique_ptr<FunctionData> ClickHouseBindData::Copy() const {
	auto result = make_uniq<ClickHouseBindData>();
	result->params = params;
	result->database = database;
	result->table = table;
	result->sql = sql;
	result->from_query = from_query;
	result->names = names;
	result->rowid_column = rowid_column;
	result->table_entry = table_entry;
	return std::move(result);
}

bool ClickHouseBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<ClickHouseBindData>();
	return database == other.database && table == other.table && sql == other.sql &&
	       from_query == other.from_query && names == other.names;
}

struct ClickHouseGlobalState : public GlobalTableFunctionState {
	ClickHouseGlobalState() = default;

	ClickHouseConnection connection;
	std::optional<clickhouse::Block> current_block;
	idx_t block_offset = 0;
	bool done = false;

	// One Select cursor (NextBlock) per scan; ClickHouse parallelises server-side.
	idx_t MaxThreads() const override {
		return 1;
	}
};

void ClickHouseDiscoverColumns(ClickHouseConnection &connection, const std::string &describe_sql,
                               vector<LogicalType> &return_types, vector<std::string> &names) {
	auto &client = connection.GetClient();
	client.BeginSelect(describe_sql);
	while (auto block = client.NextBlock()) {
		idx_t name_idx = block->GetColumnCount();
		idx_t type_idx = block->GetColumnCount();
		for (idx_t c = 0; c < block->GetColumnCount(); c++) {
			if (block->GetColumnName(c) == "name") {
				name_idx = c;
			} else if (block->GetColumnName(c) == "type") {
				type_idx = c;
			}
		}
		if (name_idx == block->GetColumnCount() || type_idx == block->GetColumnCount()) {
			continue;
		}
		auto name_col = (*block)[name_idx]->As<clickhouse::ColumnString>();
		auto type_col = (*block)[type_idx]->As<clickhouse::ColumnString>();
		if (!name_col || !type_col) {
			continue;
		}
		for (idx_t row = 0; row < block->GetRowCount(); row++) {
			std::string col_name(name_col->At(row));
			std::string col_type(type_col->At(row));
			names.push_back(col_name);
			return_types.push_back(ClickHouseTypeStringToLogicalType(col_type));
		}
	}
}

static unique_ptr<FunctionData> ClickHouseBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<std::string> &names) {
	auto bind_data = make_uniq<ClickHouseBindData>();

	auto connection_string = input.inputs[0].GetValue<string>();
	bind_data->params = ClickHouseConnectionParams::FromConnectionString(connection_string);
	bind_data->database = input.inputs[1].GetValue<string>();
	bind_data->table = input.inputs[2].GetValue<string>();

	auto describe_sql =
	    StringUtil::Format("DESCRIBE TABLE `%s`.`%s`", bind_data->database, bind_data->table);

	try {
		auto connection = ClickHouseConnection::Open(bind_data->params);
		ClickHouseDiscoverColumns(connection, describe_sql, return_types, names);
	} catch (const clickhouse::ServerException &e) {
		throw IOException("ClickHouse error describing table \"%s\".\"%s\": %s", bind_data->database,
		                  bind_data->table, e.what());
	} catch (const clickhouse::Error &e) {
		throw IOException("ClickHouse error describing table \"%s\".\"%s\": %s", bind_data->database,
		                  bind_data->table, e.what());
	}

	bind_data->names = names;
	return std::move(bind_data);
}

static std::string BuildScanSQL(const ClickHouseBindData &bind_data, const vector<column_t> &column_ids,
                                optional_ptr<TableFilterSet> filters, bool filter_pushdown) {
	std::string col_names;
	for (auto &column_id : column_ids) {
		if (!col_names.empty()) {
			col_names += ", ";
		}
		if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
			if (bind_data.rowid_column.empty()) {
				col_names += "NULL";
			} else {
				col_names += "toInt64(" + ClickHouseQuoteIdentifier(bind_data.rowid_column) + ")";
			}
		} else {
			col_names += ClickHouseQuoteIdentifier(bind_data.names[column_id]);
		}
	}
	if (col_names.empty()) {
		col_names = "NULL";
	}

	std::string query;
	if (bind_data.from_query) {
		query = StringUtil::Format("SELECT %s FROM (%s)", col_names, bind_data.sql);
	} else {
		query = StringUtil::Format("SELECT %s FROM %s.%s", col_names, ClickHouseQuoteIdentifier(bind_data.database),
		                           ClickHouseQuoteIdentifier(bind_data.table));
	}

	if (filter_pushdown && filters) {
		auto filter_string = ClickHouseFilterPushdown::TransformFilters(column_ids, filters, bind_data.names);
		if (!filter_string.empty()) {
			query += " WHERE " + filter_string;
		}
	}
	return query;
}

static unique_ptr<GlobalTableFunctionState> ClickHouseInitGlobalStateInternal(ClientContext &context,
                                                                              TableFunctionInitInput &input,
                                                                              bool filter_pushdown) {
	auto &bind_data = input.bind_data->Cast<ClickHouseBindData>();
	auto result = make_uniq<ClickHouseGlobalState>();

	auto sql = BuildScanSQL(bind_data, input.column_ids, input.filters, filter_pushdown);

	try {
		result->connection = ClickHouseConnection::Open(bind_data.params);
		result->connection.GetClient().BeginSelect(sql);
	} catch (const clickhouse::ServerException &e) {
		throw IOException("ClickHouse error starting scan: %s", e.what());
	} catch (const clickhouse::Error &e) {
		throw IOException("ClickHouse error starting scan: %s", e.what());
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ClickHouseInitGlobalState(ClientContext &context,
                                                                      TableFunctionInitInput &input) {
	return ClickHouseInitGlobalStateInternal(context, input, false);
}

static unique_ptr<GlobalTableFunctionState> ClickHouseInitGlobalStateFilterPushdown(ClientContext &context,
                                                                                    TableFunctionInitInput &input) {
	return ClickHouseInitGlobalStateInternal(context, input, true);
}

static void ClickHouseScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &gstate = data.global_state->Cast<ClickHouseGlobalState>();
	try {
		while (true) {
			if (gstate.done) {
				output.SetChildCardinality(0);
				return;
			}
			if (!gstate.current_block || gstate.block_offset >= gstate.current_block->GetRowCount()) {
				auto block = gstate.connection.GetClient().NextBlock();
				if (!block) {
					gstate.done = true;
					output.SetChildCardinality(0);
					return;
				}
				gstate.current_block = std::move(block);
				gstate.block_offset = 0;
				continue;
			}
			auto &block = *gstate.current_block;
			idx_t remaining = block.GetRowCount() - gstate.block_offset;
			idx_t count = MinValue<idx_t>(remaining, STANDARD_VECTOR_SIZE);
			for (idx_t c = 0; c < output.ColumnCount() && c < block.GetColumnCount(); c++) {
				ClickHouseColumnToVector(*block[c], output.data[c], gstate.block_offset, count);
			}
			output.SetChildCardinality(count);
			gstate.block_offset += count;
			return;
		}
	} catch (const clickhouse::ServerException &e) {
		throw IOException("ClickHouse error during scan: %s", e.what());
	} catch (const clickhouse::Error &e) {
		throw IOException("ClickHouse error during scan: %s", e.what());
	}
}

static BindInfo ClickHouseGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<ClickHouseBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.table_entry;
	return info;
}

static void ClickHouseScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data_p,
                                    const TableFunction &function) {
	throw NotImplementedException("ClickHouseScanSerialize");
}

static unique_ptr<FunctionData> ClickHouseScanDeserialize(Deserializer &deserializer, TableFunction &function) {
	throw NotImplementedException("ClickHouseScanDeserialize");
}

ClickHouseScanFunction::ClickHouseScanFunction()
    : TableFunction("clickhouse_scan", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    ClickHouseScan, ClickHouseBind, ClickHouseInitGlobalState) {
	serialize = ClickHouseScanSerialize;
	deserialize = ClickHouseScanDeserialize;
	get_bind_info = ClickHouseGetBindInfo;
	projection_pushdown = true;
}

ClickHouseScanFunctionFilterPushdown::ClickHouseScanFunctionFilterPushdown()
    : TableFunction("clickhouse_scan_pushdown", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    ClickHouseScan, ClickHouseBind, ClickHouseInitGlobalStateFilterPushdown) {
	serialize = ClickHouseScanSerialize;
	deserialize = ClickHouseScanDeserialize;
	get_bind_info = ClickHouseGetBindInfo;
	projection_pushdown = true;
	filter_pushdown = true;
}

} // namespace duckdb
