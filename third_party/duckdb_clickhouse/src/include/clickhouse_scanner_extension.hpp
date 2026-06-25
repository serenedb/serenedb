#ifndef DUCKDB_BUILD_LOADABLE_EXTENSION
#define DUCKDB_BUILD_LOADABLE_EXTENSION
#endif
#include "duckdb.hpp"

using namespace duckdb;

class ClickhouseScannerExtension : public Extension {
public:
	std::string Name() override {
		return "clickhouse_scanner";
	}
	void Load(ExtensionLoader &loader) override;
};

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(clickhouse_scanner, loader);
}
