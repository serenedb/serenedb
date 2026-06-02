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

// Block every signal except the failure-class set (SEGV/BUS/ILL/FPE/ABRT)
// on the calling thread. Called by InitThread() so worker threads never
// receive SIGTERM/SIGINT/SIGQUIT/SIGHUP/SIGPIPE -- they always reach the
// main thread, where signal_handling::Install() converts them into a clean
// shutdown via the eventfd.
void MaskAllSignalsServer();

// Reverse of MaskAllSignalsServer for the thread that wants to receive
// signals (the main thread, just before installing the shutdown handlers).
void UnmaskAllSignals();

}  // namespace sdb::signals
