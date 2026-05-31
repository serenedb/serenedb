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

namespace sdb::signal_handling {

// Install the shutdown signal handlers (SIGTERM/SIGINT/SIGQUIT -> eventfd
// wakeup via lifecycle::BeginShutdown) plus SIG_IGN for SIGPIPE. Must run
// AFTER absl::InstallFailureSignalHandler so SIGTERM lands on our handler
// (abseil treats SIGTERM as a fatal stack-dump signal otherwise) and AFTER
// the eventfd is created by lifecycle::InitShutdown.
void Install();

// Restore SIG_IGN on the signals we installed handlers for, so a late
// SIGTERM/SIGINT/SIGQUIT during teardown cannot dereference freed state.
void Shutdown();

}  // namespace sdb::signal_handling
