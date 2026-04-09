////////////////////////////////////////////////////////////////////////////////
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
/// Licensed under the Apache License, Version 2.0
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <duckdb/main/database.hpp>

namespace sdb::connector {

void RegisterPgSystemFunctions(duckdb::DatabaseInstance& db);

}  // namespace sdb::connector
