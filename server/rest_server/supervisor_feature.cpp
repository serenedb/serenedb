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

#include "supervisor_feature.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <cstdint>

#include "app/app_server.h"
#include "app/logger_feature.h"
#include "app/options/option.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "basics/application-exit.h"
#include "basics/debugging.h"
#include "basics/logger/appender.h"
#include "basics/logger/logger.h"
#include "basics/operating-system.h"
#include "basics/process-utils.h"
#include "basics/signals.h"
#include "rest_server/daemon_feature.h"

#ifdef SERENEDB_HAVE_SYS_PRCTL_H
#include <sys/prctl.h>
#endif

#ifdef SERENEDB_HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#ifdef SERENEDB_HAVE_SIGNAL_H
#include <signal.h>
#endif

#ifdef SERENEDB_HAVE_UNISTD_H
#include <unistd.h>
#endif

using namespace sdb;
using namespace sdb::app;
using namespace sdb::basics;
using namespace sdb::options;

namespace {

static bool gDone = false;
static int gClientPid = false;

static const char* gRestartMessage = "will now start a new child process";
static const char* gNoRestartMessage =
  "will intentionally not start a new child process";
static const char* gFixErrorMessage =
  "please check what causes the child process to fail and fix the error "
  "first";

static void StopHandler(int) {
  SDB_INFO("xxxxx", Logger::STARTUP,
           "received SIGINT for supervisor); commanding client [", gClientPid,
           "] to shut down.");
  int rc = kill(gClientPid, SIGTERM);
  if (rc < 0) {
    SDB_ERROR("xxxxx", Logger::STARTUP, "commanding client [", gClientPid,
              "] to shut down failed: [", errno, "] ", strerror(errno));
  }
  gDone = true;
}

}  // namespace

static void HUPHandler(int) {
  SDB_INFO("xxxxx", Logger::STARTUP,
           "received SIGHUP for supervisor); commanding client [", gClientPid,
           "] to logrotate.");
  int rc = kill(gClientPid, SIGHUP);
  if (rc < 0) {
    SDB_ERROR("xxxxx", Logger::STARTUP, "commanding client [", gClientPid,
              "] to logrotate failed: [", errno, "] ", strerror(errno));
  }
}

SupervisorFeature::SupervisorFeature(Server& server)
  : SerenedFeature{server, name()}, _supervisor(false), _client_pid(0) {
  setOptional(true);
}

void SupervisorFeature::collectOptions(
  std::shared_ptr<ProgramOptions> options) {
  options
    ->addOption(
      "--supervisor",
      "Start the server in supervisor mode. Requires --pid-file to be set.",
      new BooleanParameter(&_supervisor),
      sdb::options::MakeDefaultFlags(sdb::options::Flags::Uncommon))
    .setLongDescription(R"(Runs an serened process as supervisor with another
serened process as child, which acts as the server. In the event that the server
unexpectedly terminates due to an internal error, the supervisor automatically
restarts the server. Enabling this option implies that the server runs as a
daemon.)");
}

void SupervisorFeature::validateOptions(
  std::shared_ptr<ProgramOptions> options) {
  if (_supervisor) {
    try {
      DaemonFeature& daemon = server().getFeature<DaemonFeature>();

      // force daemon mode
      daemon.setDaemon(true);

      // revalidate options
      daemon.validateOptions(options);
    } catch (...) {
      SDB_FATAL("xxxxx", sdb::Logger::FIXME,
                "daemon mode not available, cannot start supervisor");
    }
  }
}

void SupervisorFeature::daemonize() {
  static const time_t kMinTimeAliveInSec = 30;

  if (!_supervisor) {
    return;
  }

  time_t start_time = time(nullptr);
  time_t t;
  bool done = false;
  int result = EXIT_SUCCESS;

  // will be reseted in SchedulerFeature
  sdb::signals::UnmaskAllSignals();

  if (!server().hasFeature<LoggerFeature>()) {
    SDB_FATAL("xxxxx", Logger::STARTUP, "unknown feature 'Logger', giving up");
  }

  LoggerFeature& logger = server().getFeature<LoggerFeature>();
  logger.setSupervisor(true);
  logger.prepare();

  SDB_DEBUG("xxxxx", Logger::STARTUP, "starting supervisor loop");

  while (!done) {
    logger.setSupervisor(false);

    signal(SIGINT, SIG_DFL);
    signal(SIGTERM, SIG_DFL);

    SDB_DEBUG("xxxxx", Logger::STARTUP,
              "supervisor will now try to fork a new child process");

    // fork of the server
    _client_pid = fork();

    if (_client_pid < 0) {
      SDB_FATAL("xxxxx", Logger::STARTUP, "fork failed, giving up");
    }

    // parent (supervisor)
    if (0 < _client_pid) {
      signal(SIGINT, StopHandler);
      signal(SIGTERM, StopHandler);
      signal(SIGHUP, HUPHandler);

      SDB_INFO("xxxxx", Logger::STARTUP,
               "supervisor has forked a child process with pid ", _client_pid);

      SetProcessTitle("serenedb [supervisor]");

      SDB_DEBUG("xxxxx", Logger::STARTUP, "supervisor mode: within parent");

      gClientPid = _client_pid;
      gDone = false;

      int status;
      int res = waitpid(_client_pid, &status, 0);
      bool horrible = true;

      SDB_INFO("xxxxx", Logger::STARTUP, "waitpid woke up with return value ",
               res, " and status ", status,
               " and DONE = ", (gDone ? "true" : "false"));

      if (gDone) {
        // signal handler for SIGINT or SIGTERM was invoked
        done = true;
        horrible = false;
      } else {
        SDB_ASSERT(horrible);

        if (WIFEXITED(status)) {
          // give information about cause of death
          if (WEXITSTATUS(status) == 0) {
            SDB_INFO("xxxxx", Logger::STARTUP, "child process ", _client_pid,
                     " terminated normally. ", gNoRestartMessage);

            done = true;
            horrible = false;
          } else {
            t = time(nullptr) - start_time;

            if (t < kMinTimeAliveInSec) {
              SDB_ERROR(
                "xxxxx", Logger::STARTUP, "child process ", _client_pid,
                " terminated unexpectedly, exit status ", WEXITSTATUS(status),
                ". the child process only survived for ", t,
                " seconds. this is lower than the minimum threshold value "
                "of ",
                kMinTimeAliveInSec, " s. ", gNoRestartMessage, ". ",
                gFixErrorMessage);

              SDB_ASSERT(horrible);
              done = true;
            } else {
              SDB_ERROR("xxxxx", Logger::STARTUP, "child process ", _client_pid,
                        " terminated unexpectedly, exit status ",
                        WEXITSTATUS(status), ". ", gRestartMessage);

              done = false;
            }
          }
        } else if (WIFSIGNALED(status)) {
          const int s = WTERMSIG(status);
          switch (s) {
            case 2:   // SIGINT
            case 9:   // SIGKILL
            case 15:  // SIGTERM
              SDB_INFO("xxxxx", Logger::STARTUP, "child process ", _client_pid,
                       " terminated normally, exit status ", s, " (",
                       sdb::signals::Name(s), "). ", gNoRestartMessage);

              done = true;
              horrible = false;
              break;

            default:
              SDB_ASSERT(horrible);
              t = time(nullptr) - start_time;

              if (t < kMinTimeAliveInSec) {
                SDB_ERROR("xxxxx", Logger::STARTUP, "child process ",
                          _client_pid, " terminated unexpectedly, signal ", s,
                          " (", sdb::signals::Name(s),
                          "). the child process only survived for ", t,
                          " seconds. this is lower than the minimum threshold "
                          "value of ",
                          kMinTimeAliveInSec, " s. ", gNoRestartMessage, ". ",
                          gFixErrorMessage);

                done = true;

#ifdef WCOREDUMP
                if (WCOREDUMP(status)) {
                  SDB_WARN("xxxxx", Logger::STARTUP, "child process ",
                           _client_pid, " also produced a core dump");
                }
#endif
              } else {
                SDB_ERROR("xxxxx", Logger::STARTUP, "child process ",
                          _client_pid, " terminated unexpectedly, signal ", s,
                          " (", sdb::signals::Name(s), "). ", gRestartMessage);

                done = false;
              }

              break;
          }
        } else {
          SDB_ERROR("xxxxx", Logger::STARTUP, "child process ", _client_pid,
                    " terminated unexpectedly, unknown cause. ",
                    gRestartMessage);

          done = false;
        }
      }

      if (horrible) {
        result = EXIT_FAILURE;
      } else {
        result = EXIT_SUCCESS;
      }
    }

    // child - run the normal boot sequence
    else {
      log::Shutdown();

      log::Appender::allowStdLogging(false);
      DaemonFeature::remapStandardFileDescriptors();

      SDB_DEBUG("xxxxx", Logger::STARTUP, "supervisor mode: within child");
      SetProcessTitle("serenedb [server]");

#ifdef SERENEDB_HAVE_PRCTL
      // force child to stop if supervisor dies
      prctl(PR_SET_PDEATHSIG, SIGTERM, 0, 0, 0);
#endif

      try {
        DaemonFeature& daemon = server().getFeature<DaemonFeature>();

        // disable daemon mode
        daemon.setDaemon(false);
      } catch (...) {
      }

      return;
    }
  }

  SDB_DEBUG("xxxxx", Logger::STARTUP, "supervisor mode: finished (exit ",
            result, ")");

  log::Flush();
  log::Shutdown();

  exit(result);
}
