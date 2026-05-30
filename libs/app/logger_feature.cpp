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

#include "logger_feature.h"

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/logger/logger.h"

namespace sdb {

using namespace options;

LoggerFeature::LoggerFeature(app::AppServer& server)
  : AppFeature(server, name()) {
  setOptional(false);
  _levels.push_back("info");
}

LoggerFeature::~LoggerFeature() {
  if (log::IsActive()) {
    log::Shutdown();
  }
}

void LoggerFeature::collectOptions(std::shared_ptr<ProgramOptions> options) {
  options
    ->addOption("--log.level,-l",
                "Log levels per topic. `level` sets the default; "
                "`topic=level` sets one topic; `all=level` sets every topic.",
                new VectorParameter<StringParameter>(&_levels))
    .setLongDescription(R"(Accepts trace, debug, info, warning, error, fatal.
Topics: general, startup, http, ssl, storage, search, iresearch, crash.

Examples:
    --log.level=info
    --log.level=storage=debug
    --log.level=all=warning)");
}

void LoggerFeature::validateOptions(std::shared_ptr<ProgramOptions>) {
  log::SetLogLevels(_levels);
}

void LoggerFeature::prepare() {
  log::SetLogLevels(_levels);
  log::Initialize();
}

void LoggerFeature::unprepare() { log::Flush(); }

}  // namespace sdb
