////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "app/app_server.h"
#include "app/global_context.h"
#include "app/logger_feature.h"
#include "app/options/program_options.h"
#include "basics/common.h"
#include "basics/crash_handler.h"
#include "basics/directories.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/operating-system.h"
#include "catalog/catalog.h"
#include "general_server/general_server_feature.h"
#include "general_server/scheduler_feature.h"
#include "general_server/ssl_server_feature.h"
#include "general_server/state.h"
#include "pg/pg_feature.h"
#include "rest_server/database_path_feature.h"
#include "rest_server/endpoint_feature.h"
#include "rest_server/flush_feature.h"
#include "rest_server/lockfile_feature.h"
#include "rest_server/server_feature.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"
#include "rocksdb_engine_catalog/rocksdb_recovery_manager.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/search_engine.h"
