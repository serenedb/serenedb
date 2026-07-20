#include "storage/clickhouse_connection_pool.hpp"

#include <memory>

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database_manager.hpp"

#include "storage/clickhouse_catalog.hpp"

// The postgres_configure_pool mirror: clickhouse_configure_pool([catalog_name],
// [pool options...]) reconfigures a live catalog's connection pool and reports
// every ClickHouse pool's state. No health_check_query column/option -- the
// ClickHouse health check is the protocol-level Ping(), not a SQL query.

namespace duckdb {

namespace {

enum class ExecState { UNINITIALIZED, EXHAUSTED };

struct ConfigurePoolBindData : public TableFunctionData {
	std::pair<std::string, bool> catalog_name;
	std::pair<dbconnector::pool::AcquireMode, bool> acquire_mode;
	std::pair<uint64_t, bool> max_connections;
	std::pair<uint64_t, bool> wait_timeout_millis;
	std::pair<bool, bool> enable_thread_local_cache;
	std::pair<uint64_t, bool> max_lifetime_millis;
	std::pair<uint64_t, bool> idle_timeout_millis;
	std::pair<bool, bool> enable_reaper_thread;

	static Value Lookup(const named_parameter_map_t &map, const std::string &key) {
		auto it = map.find(Identifier(key));
		if (it == map.end()) {
			return Value();
		}
		return it->second;
	}

	static std::pair<std::string, bool> LookupString(const named_parameter_map_t &map, const std::string &key) {
		Value val = Lookup(map, key);
		if (val.IsNull()) {
			return std::make_pair("", true);
		}
		std::string str = StringValue::Get(val);
		return std::make_pair(std::move(str), false);
	}

	static std::pair<uint64_t, bool> LookupUBigInt(const named_parameter_map_t &map, const std::string &key) {
		Value val = Lookup(map, key);
		if (val.IsNull()) {
			return std::make_pair(0, true);
		}
		return std::make_pair(UBigIntValue::Get(val), false);
	}

	static std::pair<bool, bool> LookupBool(const named_parameter_map_t &map, const std::string &key) {
		Value val = Lookup(map, key);
		if (val.IsNull()) {
			return std::make_pair(false, true);
		}
		return std::make_pair(BooleanValue::Get(val), false);
	}

	static std::pair<dbconnector::pool::AcquireMode, bool> LookupAcquireMode(const named_parameter_map_t &map,
	                                                                         const std::string &key) {
		std::pair<std::string, bool> st_pair = LookupString(map, key);
		if (st_pair.second) {
			return std::make_pair(dbconnector::pool::AcquireMode::FORCE, true);
		}
		try {
			dbconnector::pool::AcquireMode mode = dbconnector::pool::AcquireModeHelpers::FromString(st_pair.first);
			return std::make_pair(mode, false);
		} catch (const std::exception &e) {
			throw BinderException(e.what());
		}
	}

	ConfigurePoolBindData(const named_parameter_map_t &map)
	    : catalog_name(LookupString(map, "catalog_name")), acquire_mode(LookupAcquireMode(map, "acquire_mode")),
	      max_connections(LookupUBigInt(map, "max_connections")),
	      wait_timeout_millis(LookupUBigInt(map, "wait_timeout_millis")),
	      enable_thread_local_cache(LookupBool(map, "enable_thread_local_cache")),
	      max_lifetime_millis(LookupUBigInt(map, "max_lifetime_millis")),
	      idle_timeout_millis(LookupUBigInt(map, "idle_timeout_millis")),
	      enable_reaper_thread(LookupBool(map, "enable_reaper_thread")) {
		if (catalog_name.second &&
		    !(acquire_mode.second && max_connections.second && wait_timeout_millis.second &&
		      enable_thread_local_cache.second && max_lifetime_millis.second && idle_timeout_millis.second &&
		      enable_reaper_thread.second)) {
			throw BinderException("'catalog_name' argument must be specified to change any option value on the "
			                      "connection pool of this catalog");
		}
	}
};

struct GlobalState : public GlobalTableFunctionState {};

struct LocalState : public LocalTableFunctionState {
	ExecState exec_state = ExecState::UNINITIALIZED;
};

} // namespace

static void AddColumn(vector<LogicalType> &return_types, vector<string> &names, const std::string &col_name,
                      LogicalType col_type) {
	names.emplace_back(col_name);
	return_types.emplace_back(col_type);
}

static unique_ptr<FunctionData> ConfigurePoolBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	AddColumn(return_types, names, "catalog_name", LogicalType::VARCHAR);
	AddColumn(return_types, names, "acquire_mode", LogicalType::VARCHAR);
	AddColumn(return_types, names, "available_connections", LogicalType::UBIGINT);
	AddColumn(return_types, names, "max_connections", LogicalType::UBIGINT);
	AddColumn(return_types, names, "wait_timeout_millis", LogicalType::UBIGINT);
	AddColumn(return_types, names, "cache_hits", LogicalType::UBIGINT);
	AddColumn(return_types, names, "cache_misses", LogicalType::UBIGINT);
	AddColumn(return_types, names, "try_failures", LogicalType::UBIGINT);
	AddColumn(return_types, names, "thread_local_cache_enabled", LogicalType::BOOLEAN);
	AddColumn(return_types, names, "thread_local_cache_hits", LogicalType::UBIGINT);
	AddColumn(return_types, names, "thread_local_cache_misses", LogicalType::UBIGINT);
	AddColumn(return_types, names, "max_lifetime_millis", LogicalType::UBIGINT);
	AddColumn(return_types, names, "idle_timeout_millis", LogicalType::UBIGINT);
	AddColumn(return_types, names, "reaper_thread_running", LogicalType::BOOLEAN);
	AddColumn(return_types, names, "reaper_thread_period_millis", LogicalType::UBIGINT);

	return make_uniq<ConfigurePoolBindData>(input.named_parameters);
}

static unique_ptr<GlobalTableFunctionState> ConfigurePoolInitGlobalState(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GlobalState>();
}

static unique_ptr<LocalTableFunctionState> ConfigurePoolInitLocalState(ExecutionContext &, TableFunctionInitInput &,
                                                                       GlobalTableFunctionState *) {
	return make_uniq<LocalState>();
}

static void ConfigurePoolFunction(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bdata = input.bind_data->Cast<ConfigurePoolBindData>();
	auto &lstate = input.local_state->Cast<LocalState>();

	if (lstate.exec_state == ExecState::EXHAUSTED) {
		output.SetChildCardinality(0);
		return;
	}

	// collect pools
	std::vector<std::string> cat_names;
	std::vector<shared_ptr<ClickHouseConnectionPool>> pools;
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &db = *db_ref;
		auto &catalog = db.GetCatalog();
		if (catalog.GetCatalogType() != "clickhouse") {
			continue;
		}
		if (!bdata.catalog_name.second && catalog.GetName() != Identifier(bdata.catalog_name.first)) {
			continue;
		}
		cat_names.push_back(catalog.GetName().GetIdentifierName());
		shared_ptr<ClickHouseConnectionPool> pool = catalog.Cast<ClickHouseCatalog>().GetConnectionPoolPtr();
		pools.emplace_back(std::move(pool));
	}

	if (!bdata.catalog_name.second && pools.size() == 0) {
		throw InvalidInputException("Catalog not found, name: '%s'", bdata.catalog_name.first);
	}

	// configure the single pool if specified
	if (!bdata.catalog_name.second && pools.size() > 0) {
		auto &pool = pools.at(0);
		if (!bdata.acquire_mode.second) {
			pool->SetAcquireMode(bdata.acquire_mode.first);
		}
		if (!bdata.max_connections.second) {
			pool->SetMaxConnections(bdata.max_connections.first);
		}
		if (!bdata.wait_timeout_millis.second) {
			pool->SetWaitTimeoutMillis(bdata.wait_timeout_millis.first);
		}
		if (!bdata.enable_thread_local_cache.second) {
			pool->SetThreadLocalCacheEnabled(bdata.enable_thread_local_cache.first);
		}
		if (!bdata.max_lifetime_millis.second) {
			pool->SetMaxLifetimeMillis(bdata.max_lifetime_millis.first);
		}
		if (!bdata.idle_timeout_millis.second) {
			pool->SetIdleTimeoutMillis(bdata.idle_timeout_millis.first);
		}
		if (!bdata.enable_reaper_thread.second) {
			if (bdata.enable_reaper_thread.first) {
				pool->EnsureReaperRunning();
			} else {
				pool->ShutdownReaper();
			}
		}
	}

	// set results
	idx_t row_idx = 0;
	for (auto &pool : pools) {
		idx_t col_idx = 0;
		output.data[col_idx++].SetValue(row_idx, Value(cat_names.at(row_idx)));
		output.data[col_idx++].SetValue(row_idx,
		                                Value(dbconnector::pool::AcquireModeHelpers::ToString(pool->GetAcquireMode())));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetAvailableConnections()));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetMaxConnections()));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetWaitTimeoutMillis()));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetCacheHits()));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetCacheMisses()));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetTryFailures()));
		output.data[col_idx++].SetValue(row_idx, Value::BOOLEAN(pool->IsThreadLocalCacheEnabled()));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetThreadLocalCacheHits()));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetThreadLocalCacheMisses()));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetMaxLifetimeMillis()));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetIdleTimeoutMillis()));
		output.data[col_idx++].SetValue(row_idx, Value::BOOLEAN(pool->IsReaperRunning()));
		output.data[col_idx++].SetValue(row_idx, Value::UBIGINT(pool->GetReaperPeriodMillis()));
		row_idx++;
	}

	output.SetChildCardinality(row_idx);
	lstate.exec_state = ExecState::EXHAUSTED;
}

ClickHouseConfigurePoolFunction::ClickHouseConfigurePoolFunction()
    : TableFunction("clickhouse_configure_pool", std::vector<LogicalType>(), ConfigurePoolFunction, ConfigurePoolBind,
                    ConfigurePoolInitGlobalState, ConfigurePoolInitLocalState) {
	named_parameters["catalog_name"] = LogicalType::VARCHAR;
	named_parameters["acquire_mode"] = LogicalType::VARCHAR;
	named_parameters["max_connections"] = LogicalType::UBIGINT;
	named_parameters["wait_timeout_millis"] = LogicalType::UBIGINT;
	named_parameters["enable_thread_local_cache"] = LogicalType::BOOLEAN;
	named_parameters["max_lifetime_millis"] = LogicalType::UBIGINT;
	named_parameters["idle_timeout_millis"] = LogicalType::UBIGINT;
	named_parameters["enable_reaper_thread"] = LogicalType::BOOLEAN;
}

} // namespace duckdb
