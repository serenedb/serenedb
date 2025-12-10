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

#include "time_zone_feature.h"

#include <stdlib.h>

#include <stdexcept>

#include "app/app_server.h"
#include "app/global_context.h"
#include "app/options/option.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/application-exit.h"
#include "basics/directories.h"
#include "basics/error.h"
#include "basics/exitcodes.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/logger.h"
#include "basics/utf8_helper.h"
#include "date/tz.h"

using namespace sdb::basics;
using namespace sdb::options;

namespace sdb {

TimeZoneFeature::TimeZoneFeature(Server& server)
  : SerenedFeature{server, name()} {
  setOptional(false);
}

void TimeZoneFeature::prepareTimeZoneData(
  const std::string& binary_path, const std::string& binary_execution_path,
  const std::string& binary_name) {
  std::string tz_path;
  if (!SdbGETENV("TZ_DATA", tz_path)) {
    tz_path.clear();
    std::string test_exe =
      file_utils::BuildFilename(binary_execution_path, "tzdata");

    if (file_utils::IsDirectory(test_exe)) {
      file_utils::MakePathAbsolute(test_exe);
      file_utils::NormalizePath(test_exe);
      tz_path = test_exe;
    } else {
      std::string argv0 =
        file_utils::BuildFilename(binary_execution_path, binary_name);
      std::string path =
        SdbLocateInstallDirectory(argv0.c_str(), binary_path.c_str());
      path =
        file_utils::BuildFilename(path, ICU_DESTINATION_DIRECTORY, "tzdata");
      file_utils::MakePathAbsolute(path);
      file_utils::NormalizePath(path);
      tz_path = path;
    }
  }

  if (file_utils::IsDirectory(tz_path)) {
    date::set_install(tz_path);
  } else {
    SDB_FATAL_EXIT_CODE(
      "xxxxx", sdb::Logger::STARTUP, EXIT_TZDATA_INITIALIZATION_FAILED,
      "failed to locate timezone data ", tz_path,
      ". please set the TZ_DATA environment variable to the ",
      "tzdata directory in case you are running an unusual setup");
  }

  bool got_tz = true;
  try {
    const auto* zone = date::current_zone();
    got_tz = zone != nullptr;
  } catch (const std::runtime_error&) {
    got_tz = false;
  }

  if (!got_tz) {
    SDB_ERROR("xxxxx", sdb::Logger::STARTUP,
              "Could not get current timezone from ", tz_path,
              ". Functionality using timezones may misbehave!");
  }
}

void TimeZoneFeature::prepare() {
  auto context = GlobalContext::gContext;
  TimeZoneFeature::prepareTimeZoneData(
    server().getBinaryPath(), context->getBinaryPath(), context->binaryName());
}

void TimeZoneFeature::start() {
  try {
    date::reload_tzdb();
  } catch (const std::runtime_error& ex) {
    SDB_FATAL_EXIT_CODE("xxxxx", sdb::Logger::STARTUP,
                        EXIT_TZDATA_INITIALIZATION_FAILED, ex.what());
  }
}

}  // namespace sdb
