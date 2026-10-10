////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "replication/settings.h"

#include <duckdb/main/config.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <string_view>

#include "query/config.h"
#include "query/config_variable_names.h"

namespace sdb::replication {
namespace {

duckdb::Value Global(std::string_view name) {
  return SettingRef{name}.Global(
    duckdb::DBConfig::GetConfig(irs::DuckDBEngine::Instance().instance()));
}

int64_t Millis(std::string_view name) {
  return ParseDurationMillis(name, Global(name).ToString(), 1);
}

}  // namespace

int64_t WalReceiverTimeoutMillis() {
  return Millis(kWalReceiverTimeoutSetting);
}

int64_t WalReceiverStatusIntervalMillis() {
  return Millis(kWalReceiverStatusIntervalSetting);
}

int64_t WalRetrieveRetryIntervalMillis() {
  return Millis(kWalRetrieveRetryIntervalSetting);
}

uint32_t MaxSyncWorkersPerSubscription() {
  return Global(kMaxSyncWorkersPerSubscriptionSetting).GetValue<uint32_t>();
}

}  // namespace sdb::replication
