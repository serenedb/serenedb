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

#include <string_view>

namespace sdb::lifecycle {

// True once shutdown has begun. Background tasks poll this to bail
// out of long-running loops promptly. Also serves as the "Ctrl-C
// observed" flag for the AppServer wait loop -- the signal handler
// flips it via BeginShutdown().
bool IsStopping() noexcept;

// Marks the server as stopping. Idempotent.
void BeginShutdown() noexcept;

// The optional positional data-dir argument (the single positional
// after absl::ParseCommandLine strips argv[0]). Empty when no
// positional was supplied.
void SetDataDirArg(std::string_view arg);
std::string_view DataDirArg() noexcept;

}  // namespace sdb::lifecycle
