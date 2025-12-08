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
#include "app/app_version.h"
#include "app/bump_file_descriptors.h"
#include "app/communication_phase.h"
#include "app/config.h"
#include "app/global_context.h"
#include "app/greetings.h"
#include "app/language.h"
#include "app/logger_feature.h"
#include "app/options/program_options.h"
#include "app/options_check.h"
#include "app/shell_colors.h"
#include "app/shutdown.h"
#include "app/temp_path.h"
#include "basics/common.h"
#include "basics/crash_handler.h"
#include "basics/directories.h"
#include "basics/file_utils.h"
#include "basics/operating-system.h"
#include "catalog/catalog.h"
#include "general_server/authentication_feature.h"
#include "general_server/general_server_feature.h"
#include "general_server/scheduler_feature.h"
#include "general_server/server_options_feature.h"
#include "general_server/ssl_server_feature.h"
#include "general_server/state.h"
#include "metrics/metrics_feature.h"
#include "network/network_feature.h"
#include "pg/pg_feature.h"
#include "rest_server/bootstrap_feature.h"
#include "rest_server/check_version_feature.h"
#include "rest_server/cpu_usage_feature.h"
#include "rest_server/daemon_feature.h"
#include "rest_server/database_feature.h"
#include "rest_server/database_path_feature.h"
#include "rest_server/dump_limits_feature.h"
#include "rest_server/endpoint_feature.h"
#include "rest_server/environment_feature.h"
#include "rest_server/file_descriptors_feature.h"
#include "rest_server/flush_feature.h"
#include "rest_server/init_database_feature.h"
#include "rest_server/language_check_feature.h"
#include "rest_server/lockfile_feature.h"
#include "rest_server/log_buffer_feature.h"
#include "rest_server/max_map_count_feature.h"
#include "rest_server/privilege_feature.h"
#include "rest_server/restart_action.h"
#include "rest_server/server_feature.h"
#include "rest_server/server_id_feature.h"
#include "rest_server/soft_shutdown_feature.h"
#include "rest_server/supervisor_feature.h"
#include "rest_server/time_zone_feature.h"
#include "rest_server/upgrade_feature.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_option_feature.h"
#include "rocksdb_engine_catalog/rocksdb_recovery_manager.h"
#include "statistics/statistics_feature.h"
#include "storage_engine/engine_selector_feature.h"

#ifdef SDB_CLUSTER
#include "cluster/cluster_includes.h"
#endif
