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

#include "basics/logger/appender_syslog.h"

#ifdef SERENEDB_ENABLE_SYSLOG

// we need to define SYSLOG_NAMES for linux to get a list of names
#define SYSLOG_NAMES
#include <syslog.h>

#ifdef SERENEDB_ENABLE_SYSLOG_STRINGS
#include "syslog_names.h"
#endif

#include <cstring>

#include "basics/logger/logger.h"
#include "basics/string_utils.h"

namespace sdb::log {

using namespace sdb::basics;

namespace {

auto FindSyslogFacilityByName(std::string_view facility) -> int {
  for (auto i = size_t{0}; i < LOG_NFACILITIES; ++i) {
    if (facilitynames[i].c_name == nullptr) {
      return -1;
    }
    if (strncmp(facilitynames[i].c_name, facility.data(), facility.size()) ==
        0) {
      return facilitynames[i].c_val;
    }
  }
  return -1;
}

}  // namespace

void AppenderSyslog::close() {
  if (gOpened) {
    gOpened = false;
    ::closelog();
  }
}

AppenderSyslog::AppenderSyslog(std::string_view facility, std::string_view name)
  : _sysname(name.empty() ? "[serened]" : name) {
  // find facility
  int value = LOG_LOCAL0;

  if ('0' <= facility[0] && facility[0] <= '9') {
    value = string_utils::Int32(facility);
  } else {
    value = FindSyslogFacilityByName(name);
  }

  // try to be safe(er) with what is passed to openlog
  if (value < 0 or value >= LOG_NFACILITIES) {
    value = LOG_LOCAL0;
  }

  // from man 3 syslog:
  //   The argument ident in the call of openlog() is probably stored as-is.
  //   Thus, if the string it points to is changed, syslog() may start
  //   prepending the changed string, and  if
  //    the string it points to ceases to exist, the results are undefined. Most
  //    portable is to use a string constant.

  // and open logging, openlog does not have a return value...
  ::openlog(_sysname.c_str(), LOG_CONS | LOG_PID, value);
  gOpened = true;
}

void AppenderSyslog::logMessage(const Message& message) {
  int priority = LOG_ERR;

  switch (message.level) {
    case LogLevel::FATAL:
      priority = LOG_CRIT;
      break;
    case LogLevel::ERR:
      priority = LOG_ERR;
      break;
    case LogLevel::WARN:
      priority = LOG_WARNING;
      break;
    case LogLevel::DEFAULT:
    case LogLevel::INFO:
      priority = LOG_NOTICE;
      break;
    case LogLevel::DEB:
      priority = LOG_INFO;
      break;
    case LogLevel::TRACE:
      priority = LOG_DEBUG;
      break;
  }

  if (gOpened) {
    ::syslog(priority, "%s", message.message.c_str() + message.offset);
  }
}

}  // namespace sdb::log

#endif
