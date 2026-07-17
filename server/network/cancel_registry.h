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

#include <absl/synchronization/mutex.h>

#include <cstdint>
#include <duckdb/common/shared_ptr.hpp>
#include <duckdb/main/client_context.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/random/random_generator.h"

namespace sdb::network {

// A stoppable session, as the registry sees it (Transport implements it).
class TransportBase {
 public:
  virtual void Stop() = 0;

 protected:
  ~TransportBase() = default;
};

// Cross-thread cancellation handle for one connection (pg and http alike). A
// CancelRequest arrives on a DIFFERENT connection (any io thread) and calls
// Cancel() while the target session's query runs on a DuckDB worker thread;
// ClientContext::Interrupt() is a lock-free atomic, so the only synchronization
// needed is to stop a foreign thread from touching the ClientContext once the
// owning session begins tearing down -- Detach() nulls it under the same mutex
// Cancel() takes. The token is shared (shared_ptr) by the registry and the
// session, so it outlives either.
struct CancelToken {
  void Cancel() {
    absl::MutexLock lock{&mu};
    if (ctx) {
      ctx->Interrupt();
    }
  }
  // pg_terminate_backend / server shutdown: interrupt the in-flight query and
  // stop the whole session. Stop runs outside `mu` (its OnStop re-enters
  // Cancel()) on a locked shared_ptr, so a session mid-teardown is kept alive
  // for the call and a torn-down one is skipped.
  void Terminate() {
    duckdb::shared_ptr<TransportBase> locked;
    {
      absl::MutexLock lock{&mu};
      if (ctx) {
        ctx->Interrupt();
      }
      locked = session.lock();
    }
    if (locked) {
      locked->Stop();
    }
  }
  void Detach() {
    absl::MutexLock lock{&mu};
    ctx = nullptr;
    session.reset();
  }

  absl::Mutex mu;
  duckdb::ClientContext* ctx = nullptr;
  duckdb::weak_ptr<TransportBase> session;
};

// Process-wide registry mapping a connection's cancel key to its CancelToken.
// One instance per server (owned by Server, handed to sessions via the
// listener contexts, like ssl/credentials). The 64-bit key is split into the
// pid/secret halves sent in BackendKeyData; a CancelRequest must present both
// halves to match, so a guessed pid alone cannot cancel a query.
class CancelRegistry {
 public:
  uint64_t Register(std::shared_ptr<CancelToken> token) {
    bool terminating;
    uint64_t key;
    {
      absl::MutexLock lock{&_mu};
      for (;;) {
        // The high 32 bits are the "pid" reported by pg_backend_pid() and sent
        // in BackendKeyData; PG pids are positive int32, so keep that half in
        // [1, INT32_MAX]. The low 32 bits stay the full-entropy secret.
        const uint64_t entropy = sdb::random::RandU64();
        uint32_t pid = static_cast<uint32_t>(entropy >> 32) & 0x7fffffffu;
        if (pid == 0) {
          pid = 1;
        }
        key = (static_cast<uint64_t>(pid) << 32) | static_cast<uint32_t>(entropy);
        if (_tokens.try_emplace(key, token).second) {
          break;
        }
      }
      terminating = _terminating;
    }
    if (terminating) {
      // Server shutdown already broadcast: this session raced past it.
      token->Terminate();
    }
    return key;
  }

  void Unregister(uint64_t key) {
    absl::MutexLock lock{&_mu};
    _tokens.erase(key);
  }

  void Cancel(uint64_t key) {
    std::shared_ptr<CancelToken> token;
    {
      absl::MutexLock lock{&_mu};
      if (const auto it = _tokens.find(key); it != _tokens.end()) {
        token = it->second;
      }
    }
    // Never hold the registry lock across Interrupt(): the token keeps the
    // target alive for the duration of the call.
    if (token) {
      token->Cancel();
    }
  }

  // Server shutdown: terminate every session. Latches, so a session
  // registering after the broadcast is terminated in Register().
  void TerminateAll() {
    std::vector<std::shared_ptr<CancelToken>> tokens;
    {
      absl::MutexLock lock{&_mu};
      _terminating = true;
      tokens.reserve(_tokens.size());
      for (const auto& [key, token] : _tokens) {
        tokens.push_back(token);
      }
    }
    for (const auto& token : tokens) {
      token->Terminate();
    }
  }

  enum class CancelResult {
    NotFound,
    Cancelled,
    Ambiguous,
  };

  // Cancel by the pid half of the key alone (the high 32 bits sent in
  // BackendKeyData / reported by pg_backend_pid). Unlike Cancel(), this needs
  // no secret -- it backs the SQL pg_cancel_backend()/pg_terminate_backend(),
  // which are authorised by being a (super)user session, not by the cancel key.
  // The pid is the high half of a RANDOM key, so it is not guaranteed unique
  // across live backends (unlike a real OS pid); if two share it the request is
  // ambiguous and we cancel NONE rather than risk hitting the wrong backend.
  CancelResult CancelByPid(uint32_t pid, bool terminate = false) {
    std::shared_ptr<CancelToken> token;
    int matches = 0;
    {
      absl::MutexLock lock{&_mu};
      for (const auto& [key, candidate] : _tokens) {
        if (static_cast<uint32_t>(key >> 32) == pid && ++matches == 1) {
          token = candidate;
        }
      }
    }
    if (matches == 0) {
      return CancelResult::NotFound;
    }
    if (matches > 1) {
      return CancelResult::Ambiguous;
    }
    if (terminate) {
      token->Terminate();
    } else {
      token->Cancel();
    }
    return CancelResult::Cancelled;
  }

 private:
  absl::Mutex _mu;
  containers::FlatHashMap<uint64_t, std::shared_ptr<CancelToken>> _tokens;
  bool _terminating = false;
};

}  // namespace sdb::network
