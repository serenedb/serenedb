////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>

namespace sdb::network {

inline constexpr size_t kReadBlock = 16 * 1024;
inline constexpr size_t kBufferMaxGrowth = 1u << 20;

// Send-side write-behind: committed bytes auto-start an async socket write at
// this threshold, so encoding the next rows overlaps the write of earlier
// ones.
inline constexpr size_t kSendFlushSize = 64 * 1024;
// Backpressure high-water mark: a producer encoding rows pauses while more
// than this many committed bytes are not yet written to the socket. Bounds
// per-connection memory for results a slow client doesn't drain.
inline constexpr size_t kSendHighWater = 4u << 20;

// Largest single pg-wire message accepted -- a distinct concept from the
// buffer's chunk-growth ceiling above (they previously shared one constant).
// Bounds per-connection peak memory; bulk data goes through COPY, which streams
// per CopyData frame and is not subject to this. Default for PgServerContext;
// overridable via --network_pg_max_message_bytes.
inline constexpr uint32_t kDefaultMaxMessageBytes = 64u * 1024 * 1024;

// Per-read HTTP inactivity timeouts: bound how long one async socket read may
// stall so a slow/idle client cannot pin an io thread (slow-loris). Re-armed
// around each read; the keep-alive value applies while waiting for the first
// byte of the next request.
inline constexpr auto kHttpHeaderReadTimeout = std::chrono::seconds{10};
inline constexpr auto kHttpKeepAliveIdleTimeout = std::chrono::seconds{75};
inline constexpr auto kHttpBodyReadTimeout = std::chrono::seconds{30};

}  // namespace sdb::network
