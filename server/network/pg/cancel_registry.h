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
#include <duckdb/main/client_context.hpp>
#include <memory>
#include <utility>

#include "basics/containers/flat_hash_map.h"
#include "basics/random/random_generator.h"

namespace sdb::network::pg {

// Cross-thread cancellation handle for one connection. A CancelRequest arrives
// on a DIFFERENT connection (any io thread) and calls Cancel() while the target
// session's query runs on a DuckDB worker thread; ClientContext::Interrupt() is
// a lock-free atomic, so the only synchronization needed is to stop a foreign
// thread from touching the ClientContext once the owning session begins tearing
// down -- Detach() nulls it under the same mutex Cancel() takes. The token is
// shared (shared_ptr) by the registry and the session, so it outlives either.
struct CancelToken {
  void Cancel() {
    absl::MutexLock lock{&mu};
    if (ctx) {
      ctx->Interrupt();
    }
  }
  void Detach() {
    absl::MutexLock lock{&mu};
    ctx = nullptr;
  }

  absl::Mutex mu;
  duckdb::ClientContext* ctx = nullptr;
};

// Process-wide registry mapping a connection's cancel key to its CancelToken.
// One instance per server (owned by Server, handed to sessions
// via PgServerContext, like ssl/credentials). The 64-bit key is split into the
// pid/secret halves sent in BackendKeyData; a CancelRequest must present both
// halves to match, so a guessed pid alone cannot cancel a query.
class CancelRegistry {
 public:
  uint64_t Register(std::shared_ptr<CancelToken> token) {
    absl::MutexLock lock{&_mu};
    for (;;) {
      const uint64_t key = sdb::random::RandU64();
      if (key != 0 && _tokens.try_emplace(key, std::move(token)).second) {
        return key;
      }
    }
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

 private:
  absl::Mutex _mu;
  containers::FlatHashMap<uint64_t, std::shared_ptr<CancelToken>> _tokens;
};

}  // namespace sdb::network::pg
