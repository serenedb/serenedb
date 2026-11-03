#pragma once

#ifndef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_BUILD_LOADABLE_EXTENSION
#endif
#include "duckdb.hpp"

class ClickhouseScannerExtension : public duckdb::Extension {
public:
	std::string Name() override {
		return "clickhouse_scanner";
	}
	void Load(duckdb::ExtensionLoader &loader) override;
	std::string Version() const override;

	static void OnConnectionOpened(duckdb::ClientContext &context);
};

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(clickhouse_scanner, loader);
}
