#include "app_server.h"

#include <absl/flags/parse.h>

#include "basics/lifecycle.h"

namespace sdb::app {

AppServer::AppServer(const char* binary_path) : _binary_path{binary_path} {
  SDB_ASSERT(gInstance == nullptr, "AppServer is a singleton");
  gInstance = this;
}

AppServer::~AppServer() { gInstance = nullptr; }

bool AppServer::isStopping() const noexcept {
  auto s = state();
  return s == State::InShutdown || s == State::InStop ||
         s == State::InUnprepare || s == State::Stopped || s == State::Aborted;
}

void AppServer::beginShutdown() noexcept {
  State old = state();
  do {
    if (old == State::InShutdown || old == State::InStop ||
        old == State::InUnprepare || old == State::Stopped ||
        old == State::Aborted) {
      return;
    }
  } while (!_state.compare_exchange_weak(old, State::InShutdown,
                                         std::memory_order_release,
                                         std::memory_order_acquire));

  lifecycle::BeginShutdown();

  absl::MutexLock guard{&_shutdown_condition.mutex};
  _abort_waiting = true;
  _shutdown_condition.cv.notify_one();
}

void AppServer::parseOptions(int argc, char* argv[]) {
  // All CLI knobs are ABSL_FLAGs declared in their owning .cpp files
  // and read via absl::GetFlag during validateOptions/prepare. Run
  // absl's parser; positional args land in lifecycle:: for features
  // (DatabasePathFeature) to pick up.
  auto positionals = absl::ParseCommandLine(argc, argv);
  lifecycle::SetPositionalArgs(positionals);
}

void AppServer::wait() {
  while (true) {
    if (lifecycle::gCtrlC.load()) {
      beginShutdown();
    }
    absl::MutexLock guard{&_shutdown_condition.mutex};
    if (_abort_waiting) {
      break;
    }
    _shutdown_condition.cv.WaitWithTimeout(&_shutdown_condition.mutex,
                                           absl::Milliseconds(100));
  }
}

}  // namespace sdb::app
