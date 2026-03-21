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

#include "global_context.h"

#include <absl/debugging/symbolize.h>

#include <yaclib/log.hpp>

#include "basics/application-exit.h"
#include "basics/debugging.h"
#include "basics/error.h"
#include "basics/file_utils.h"
#include "basics/files.h"
#include "basics/logger/appender.h"
#include "basics/logger/logger.h"
#include "basics/operating-system.h"
#include "basics/process-utils.h"
#include "basics/random/random_generator.h"
#include "basics/signals.h"
#include "basics/string_utils.h"
#include "rest/version.h"
#include "vpack/vpack_helper.h"
#define ZLIB_COMPAT
#include <functable.h>
#include <stdlib.h>
#include <string.h>

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

#ifdef SERENEDB_HAVE_SIGNAL_H
#include <signal.h>
#endif

using namespace sdb;
using namespace sdb::basics;

namespace {

static void ReopenLog(int) { log::Appender::reopen(); }

}  // namespace

GlobalContext* GlobalContext::gContext = nullptr;

GlobalContext::GlobalContext(int /*argc*/, char* argv[],
                             const char* install_directory)
  : _binary_name(SdbBinaryName(argv[0])),
    _binary_path(SdbLocateBinaryPath(argv[0])),
    _run_root(SdbGetInstallRoot(SdbLocateBinaryPath(argv[0]).c_str(),
                                install_directory)),
    _ret(EXIT_FAILURE) {
#ifndef __GLIBC__
  // Increase default stack size for libmusl:
  pthread_attr_t a;
  pthread_attr_init(&a);
  pthread_attr_setstacksize(&a, 8 * 1024 * 1024);  // 8MB as in glibc
  pthread_attr_setguardsize(&a, 4096);             // one page
  pthread_setattr_default_np(&a);
#endif

  // global initialization
  random::Reset();
  rest::Version::initialize();
  VPackHelper::initialize();
  FUNCTABLE_INIT;
  absl::InitializeSymbolizer(argv[0]);
  YACLIB_INIT_DEBUG([](std::string_view file, size_t line,
                       std::string_view func, std::string_view condition,
                       std::string_view message) noexcept {
    CrashHandler::assertionFailure(file.data(), static_cast<int>(line),
                                   func.data(), condition.data(),
                                   message.data());
  });
  gContext = this;
}

GlobalContext::~GlobalContext() {
  gContext = nullptr;

  signal(SIGHUP, SIG_IGN);

  random::Reset();
  ShutdownProcess();
}

int GlobalContext::exit(int ret) {
  _ret = ret;
  return _ret;
}

void GlobalContext::installHup() { signal(SIGHUP, ReopenLog); }

void GlobalContext::normalizePath(std::vector<std::string>& paths,
                                  const char* which_path, bool fatal) {
  for (auto& path : paths) {
    normalizePath(path, which_path, fatal);
  }
}

void GlobalContext::normalizePath(std::string& path, const char* which_path,
                                  bool fatal) {
  path = string_utils::RTrim(path, SERENEDB_DIR_SEPARATOR_STR);

  basics::file_utils::NormalizePath(path);
  if (!basics::file_utils::Exists(path)) {
    auto directory = basics::file_utils::BuildFilename(_run_root, path);
    if (!basics::file_utils::Exists(directory)) {
      if (!fatal) {
        return;
      }
      SDB_FATAL("xxxxx", sdb::Logger::FIXME, "failed to locate ", which_path,
                " directory, its neither available in '", path, "' nor in '",
                directory, "'");
    }
    basics::file_utils::NormalizePath(directory);
    path = directory;
  } else {
    if (!SdbPathIsAbsolute(path)) {
      basics::file_utils::MakePathAbsolute(path);
    }
  }
}
