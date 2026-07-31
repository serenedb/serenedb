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

#pragma once

namespace sdb {

// Routes the Azure SDK's libcurl connection-pool cleanup loop onto
// BackgroundScheduler instead of an SDK-owned thread (see the
// SetCurlCleanupScheduler hook in our azure-sdk-for-cpp fork). Call once at
// startup, after BackgroundScheduler::start() and the io pool are up.
void InstallAzureCleanupScheduler();

// Join any armed cleanup loops. Call during shutdown after search stops and
// before BackgroundScheduler::stop() (mirrors SearchEngine::stop()'s loop
// join).
void StopAzureCleanupScheduler();

}  // namespace sdb
