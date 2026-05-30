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

#include "app/app_feature.h"
#include "basics/operating-system.h"
#include "basics/type_list.h"

namespace sdb {
namespace app {

template<typename Features>
class AppServerImpl;

}  // namespace app
namespace metrics {

class MetricsFeature;

}  // namespace metrics
namespace catalog {

class CatalogFeature;

}  // namespace catalog
namespace pg {

class PostgresFeature;

}  // namespace pg
namespace search {

class SearchEngine;

}  // namespace search

class LoggerFeature;
class SslServerFeature;
class MaxMapCountFeature;
class FileDescriptorsFeature;
class TempPath;
class DatabasePathFeature;
class PrivilegeFeature;
class SchedulerFeature;
class AuthenticationFeature;
class RocksDBOptionFeature;
class EngineFeature;
class FlushFeature;
class LockfileFeature;
class RocksDBRecoveryManager;
class ServerFeature;
class HttpEndpointProvider;
class GeneralServerFeature;
class ShutdownFeature;

// clang-format off
using SerenedFeaturesList = type::List<
  LoggerFeature,
  SslServerFeature,
  metrics::MetricsFeature,
#ifdef SERENEDB_HAVE_GETRLIMIT
  FileDescriptorsFeature,
#endif
  TempPath,
  DatabasePathFeature,
  PrivilegeFeature,
  SchedulerFeature,
  AuthenticationFeature,
  RocksDBOptionFeature,
  EngineFeature,
  FlushFeature,
  LockfileFeature,
  RocksDBRecoveryManager,
  catalog::CatalogFeature,
  search::SearchEngine,
  ServerFeature,
  HttpEndpointProvider,
  GeneralServerFeature,
  pg::PostgresFeature,
  ShutdownFeature>;
// clang-format on

struct SerenedFeatures : SerenedFeaturesList {};
using SerenedServer = app::AppServerImpl<SerenedFeatures>;
using SerenedFeature = app::AppFeatureImpl<SerenedServer>;

}  // namespace sdb
