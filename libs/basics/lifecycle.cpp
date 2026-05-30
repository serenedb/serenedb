#include "basics/lifecycle.h"

namespace sdb::lifecycle {
namespace {

std::atomic_bool gIsStopping = false;
std::vector<std::string> gPositionalArgs;

}  // namespace

bool IsStopping() noexcept {
  return gIsStopping.load(std::memory_order_acquire);
}

void BeginShutdown() noexcept {
  gIsStopping.store(true, std::memory_order_release);
}

void SetPositionalArgs(std::span<char* const> positionals) {
  gPositionalArgs.clear();
  gPositionalArgs.reserve(positionals.size());
  for (auto* p : positionals) {
    gPositionalArgs.emplace_back(p);
  }
}

const std::vector<std::string>& PositionalArgs() noexcept {
  return gPositionalArgs;
}

}  // namespace sdb::lifecycle
