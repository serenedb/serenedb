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

#include "system-functions.h"

#include <chrono>
#include <string>

#include "basics/operating-system.h"
#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

namespace sdb::utilities {

void GetLocalTime(time_t tt, struct tm* tb) {
#ifdef SERENEDB_HAVE_LOCALTIME_R
  localtime_r(&tt, tb);
#else
#ifdef SERENEDB_HAVE_LOCALTIME_S
  localtime_s(tb, &tt);
#else
  struct tm* tp = localtime(&tt);

  if (tp != nullptr) {
    memcpy(tb, tp, sizeof(struct tm));
  }
#endif
#endif
}

void GetGmtime(time_t tt, struct tm* tb) {
#ifdef SERENEDB_HAVE_GMTIME_R
  gmtime_r(&tt, tb);
#else
#ifdef SERENEDB_HAVE_GMTIME_S
  gmtime_s(tb, &tt);
#else
  struct tm* tp = gmtime(&tt);

  if (tp != nullptr) {
    memcpy(tb, tp, sizeof(struct tm));
  }
#endif
#endif
}

time_t SetGmtime(struct tm* tm) {
  // Linux, OSX and BSD variants:
  return timegm(tm);
}

double GetMicrotime() noexcept {
  return std::chrono::duration<double>(
           std::chrono::system_clock::now().time_since_epoch())
    .count();
}

std::string TimeString(char sep, char fin) {
  time_t tt = time(nullptr);
  struct tm tb;
  GetGmtime(tt, &tb);
  char buffer[32];
  size_t len = strftime(buffer, sizeof(buffer), "%Y-%m-%dT%H:%M:%SZ", &tb);
  buffer[10] = sep;
  buffer[19] = fin;

  if (fin == '\0') {
    --len;
  }

  return std::string(buffer, len);
}

std::string Hostname() {
  char buffer[1024];

  int res = gethostname(buffer, sizeof(buffer) - 1);

  if (res == 0) {
    return buffer;
  }

  return "localhost";
}

}  // namespace sdb::utilities
