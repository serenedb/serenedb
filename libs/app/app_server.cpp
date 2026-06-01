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

#include "app_server.h"

#include <absl/flags/parse.h>

#include "basics/lifecycle.h"
#include "basics/logger/logger.h"

namespace sdb::app {

void AppServer::parseOptions(int argc, char* argv[]) {
  auto positionals = absl::ParseCommandLine(argc, argv);
  if (positionals.size() == 2) {
    lifecycle::SetDataDirArg(positionals[1]);
  } else if (positionals.size() > 2) {
    SDB_FATAL(GENERAL, "expected at most one positional data-dir arg");
  }
}

void AppServer::wait() {
  lifecycle::WaitForShutdown();
  SDB_INFO(GENERAL, "received shutdown signal, beginning shut down sequence");
}

}  // namespace sdb::app
