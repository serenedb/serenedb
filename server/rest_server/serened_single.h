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

class CommunicationFeaturePhase;

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

class LoggerFeature;
class ServerOptionsFeature;
class AppVersion;
class SslServerFeature;
class ConfigFeature;
class GreetingsFeature;
class LogBufferFeature;
class BumpFileDescriptorsFeature;
class CpuUsageFeature;
class DaemonFeature;
class MaxMapCountFeature;
class OptionsCheckFeature;
class EnvironmentFeature;
class FileDescriptorsFeature;
class LanguageFeature;
class SupervisorFeature;
class TempPath;
class DatabasePathFeature;
class PrivilegeFeature;
class SchedulerFeature;
class AuthenticationFeature;
class LanguageCheckFeature;
class InitDatabaseFeature;
class RocksDBOptionFeature;
class RocksDBEngineCatalog;
class EngineSelectorFeature;
class DatabaseFeature;
class ServerIdFeature;
class CheckVersionFeature;
class FlushFeature;
class LockfileFeature;
class RocksDBRecoveryManager;
class UpgradeFeature;
class ServerFeature;
class NetworkFeature;
class DumpLimitsFeature;
class HttpEndpointProvider;
class GeneralServerFeature;
class StatisticsFeature;
class BootstrapFeature;
class ShutdownFeature;
class SoftShutdownFeature;
class TimeZoneFeature;

// clang-format off
using SerenedFeaturesList = type::List<
  LoggerFeature,
  ServerOptionsFeature,
  AppVersion,
  SslServerFeature,
  ConfigFeature,
  GreetingsFeature,
  metrics::MetricsFeature,
  LogBufferFeature,
#ifdef SERENEDB_HAVE_GETRLIMIT
  BumpFileDescriptorsFeature,
#endif
  CpuUsageFeature,
#ifdef SERENEDB_HAVE_FORK
  DaemonFeature,
#endif
  MaxMapCountFeature,
  OptionsCheckFeature,
  EnvironmentFeature,
#ifdef SERENEDB_HAVE_GETRLIMIT
  FileDescriptorsFeature,
#endif
  LanguageFeature,
#ifdef SERENEDB_HAVE_FORK
  SupervisorFeature,
#endif
  TempPath,
  DatabasePathFeature,
  PrivilegeFeature,
  SchedulerFeature,
  AuthenticationFeature,
  LanguageCheckFeature,
  InitDatabaseFeature,
  RocksDBOptionFeature,
  RocksDBEngineCatalog,
  EngineSelectorFeature,
  DatabaseFeature,
  ServerIdFeature,
  CheckVersionFeature,
  app::CommunicationFeaturePhase,
  FlushFeature,
  LockfileFeature,
  RocksDBRecoveryManager,
  catalog::CatalogFeature,
  UpgradeFeature,
  ServerFeature,
  NetworkFeature,
  DumpLimitsFeature,
  HttpEndpointProvider,
  GeneralServerFeature,
  StatisticsFeature,
  pg::PostgresFeature,
  BootstrapFeature,
  ShutdownFeature,
  SoftShutdownFeature,
  TimeZoneFeature>;
// clang-format on

struct SerenedFeatures : SerenedFeaturesList {};
using SerenedServer = app::AppServerImpl<SerenedFeatures>;
using SerenedFeature = app::AppFeatureImpl<SerenedServer>;

}  // namespace sdb
