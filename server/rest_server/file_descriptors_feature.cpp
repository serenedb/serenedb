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

#include "file_descriptors_feature.h"

#include <cstdio>

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/file_descriptors.h"
#include "basics/file_utils.h"
#include "basics/logger/logger.h"
#include "metrics/gauge_builder.h"
#include "metrics/metrics_feature.h"
#include "rest_server/environment_feature.h"

#ifdef SERENEDB_HAVE_SYS_RESOURCE_H
#include <sys/resource.h>
#endif

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>

using namespace sdb::app;
using namespace sdb::basics;
using namespace sdb::options;

#ifdef SERENEDB_HAVE_GETRLIMIT
DECLARE_GAUGE(
  serenedb_file_descriptors_current, uint64_t,
  "Number of currently open file descriptors for the serened process");
DECLARE_GAUGE(
  serenedb_file_descriptors_limit, uint64_t,
  "Limit for the number of open file descriptors for the serened process");

namespace sdb {

FileDescriptorsFeature::FileDescriptorsFeature(Server& server)
  : SerenedFeature{server, name()},
#ifdef __linux__
    _count_descriptors_interval(60 * 1000),
#else
    _count_descriptors_interval(0),
#endif
    _file_descriptors_current(server.getFeature<metrics::MetricsFeature>().add(
      serenedb_file_descriptors_current{})),
    _file_descriptors_limit(server.getFeature<metrics::MetricsFeature>().add(
      serenedb_file_descriptors_limit{})) {
  setOptional(false);

  static_assert(
    Server::isCreatedAfter<FileDescriptorsFeature, metrics::MetricsFeature>());
}

void FileDescriptorsFeature::collectOptions(
  std::shared_ptr<ProgramOptions> options) {
  options->addOption(
    "--server.count-descriptors-interval",
    "Controls the interval (in milliseconds) in which the number of open "
    "file descriptors for the process is determined "
    "(0 = disable counting).",
    new UInt64Parameter(&_count_descriptors_interval),
    sdb::options::MakeFlags(sdb::options::Flags::DefaultNoOs,
                            sdb::options::Flags::OsLinux));
}

void FileDescriptorsFeature::validateOptions(
  std::shared_ptr<ProgramOptions> /*options*/) {
  constexpr uint64_t kLowerBound = 10000;
  if (_count_descriptors_interval > 0 &&
      _count_descriptors_interval < kLowerBound) {
    SDB_WARN(
      "xxxxx", Logger::SYSCALL,
      "too low value for `--server.count-descriptors-interval`. Should be "
      "at least ",
      kLowerBound);
    _count_descriptors_interval = kLowerBound;
  }
}

void FileDescriptorsFeature::prepare() {
  FileDescriptors current;
  if (Result res = FileDescriptors::load(current); res.fail()) {
    SDB_THROW(std::move(res));
  }

  _file_descriptors_limit.store(current.soft, std::memory_order_relaxed);
}

void FileDescriptorsFeature::countOpenFiles() {
#ifdef __linux__
  try {
    size_t num_files = file_utils::CountFiles("/proc/self/fd");
    _file_descriptors_current.store(num_files, std::memory_order_relaxed);
  } catch (const std::exception& ex) {
    SDB_DEBUG(
      "xxxxx", Logger::SYSCALL,
      "unable to count number of open files for serened process: ", ex.what());
  } catch (...) {
    SDB_DEBUG("xxxxx", Logger::SYSCALL,
              "unable to count number of open files for serened process");
  }
#endif
}

void FileDescriptorsFeature::countOpenFilesIfNeeded() {
  if (_count_descriptors_interval == 0) {
    return;
  }

  auto now = std::chrono::steady_clock::now();

  std::unique_lock guard{_last_count_mutex, std::try_to_lock};

  if (guard.owns_lock() &&
      (_last_count_stamp.time_since_epoch().count() == 0 ||
       now - _last_count_stamp >
         std::chrono::milliseconds(_count_descriptors_interval))) {
    countOpenFiles();
    _last_count_stamp = now;
  }
}
}  // namespace sdb

#endif
