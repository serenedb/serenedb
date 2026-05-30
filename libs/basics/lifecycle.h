#pragma once

#include <atomic>
#include <span>
#include <string>
#include <vector>

namespace sdb::lifecycle {

// Set by the SIGTERM/SIGINT handler; the main wait loop polls this.
inline std::atomic_bool gCtrlC = false;

// True once shutdown has begun. Background tasks poll this to bail
// out of long-running loops promptly.
bool IsStopping() noexcept;

// Marks the server as stopping. Idempotent.
void BeginShutdown() noexcept;

// Positional argv (post absl::ParseCommandLine). [0] is argv[0].
void SetPositionalArgs(std::span<char* const> positionals);
const std::vector<std::string>& PositionalArgs() noexcept;

}  // namespace sdb::lifecycle
