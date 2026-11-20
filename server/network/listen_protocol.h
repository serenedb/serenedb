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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <yaclib/algo/wait_group.hpp>

#include "network/acceptor.h"
#include "network/cancel_registry.h"
#include "network/credentials.h"
#include "network/io_context.h"
#include "network/listen_spec.h"
#include "network/tls_context.h"
#include "server/utils/asio_ns.h"
#include "server/utils/registry.h"

namespace sdb::network {

struct ListenContext {
  IoThreadPool& pool;
  CancelRegistry& cancel;
  std::atomic<uint32_t>& active;
  yaclib::WaitGroup<>& sessions;
  const CredentialProvider* credentials = nullptr;
  const TlsOptions& tls;
  uint32_t max_connections = 0;
};

struct ListenProtocol {
  static constexpr std::string_view kKind = "listen protocol";

  std::shared_ptr<const ListenOptions> (*parse)(
    const ListenSpec& spec, std::span<const ListenParam> params);
  std::unique_ptr<AcceptorBase> (*listen)(const ListenContext& context,
                                          const ListenSpec& spec);
};

struct ListenTls {
  bool required = false;
  bool client_cert = false;
};

AcceptorOptions MakeAcceptorOptions(const ListenSpec& spec);

std::unique_ptr<asio_ns::ssl::context> MakeTls(const ListenContext& context,
                                               const ListenSpec& spec,
                                               ListenTls tls);

void LogListening(const ListenSpec& spec, bool tls);

std::chrono::milliseconds AuthTimeout();

}  // namespace sdb::network

#define SDB_LISTEN_PROTOCOL \
  const ::sdb::Registry<::sdb::network::ListenProtocol>::Register
