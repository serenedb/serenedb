////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

namespace sdb::signals {

// Block every signal except the failure-class set (SEGV/BUS/ILL/FPE/ABRT) on
// the calling thread, so shutdown signals are only delivered to the main
// thread.
void MaskAllSignalsServer();

void UnmaskAllSignals();

// Routes SIGTERM/SIGINT/SIGQUIT into lifecycle::BeginShutdown(). Must run
// after CrashHandler::installCrashHandler: absl's failure handler claims
// SIGTERM and would dump a crash stack on a plain `docker stop`.
void InstallShutdownHandlers();

}  // namespace sdb::signals
