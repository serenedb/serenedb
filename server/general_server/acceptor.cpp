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

#include "general_server/acceptor.h"

#include "basics/log.h"
#include "basics/operating-system.h"
#include "general_server/acceptor_tcp.h"

#ifdef SERENEDB_HAVE_DOMAIN_SOCKETS
#include "general_server/acceptor_unix_domain.h"
#endif

using namespace sdb;
using namespace sdb::rest;

Acceptor::Acceptor(rest::GeneralServer& server, rest::IoContext& context,
                   Endpoint* endpoint)
  : _server(server),
    _ctx(context),
    _endpoint(endpoint),
    _open(false),
    _accept_failures(0) {}

std::unique_ptr<Acceptor> Acceptor::factory(rest::GeneralServer& server,
                                            rest::IoContext& context,
                                            Endpoint* endpoint) {
#ifdef SERENEDB_HAVE_DOMAIN_SOCKETS
  if (endpoint->domainType() == Endpoint::DomainType::UNIX) {
    return std::make_unique<AcceptorUnixDomain>(server, context, endpoint);
  }
#endif
  if (endpoint->encryption() == Endpoint::EncryptionType::SSL) {
    return std::make_unique<AcceptorTcp<rest::SocketType::Ssl>>(server, context,
                                                                endpoint);
  } else {
    SDB_ASSERT(endpoint->encryption() == Endpoint::EncryptionType::None);
    return std::make_unique<AcceptorTcp<rest::SocketType::Tcp>>(server, context,
                                                                endpoint);
  }
}

void Acceptor::handleError(const asio_ns::error_code& ec) {
  // On shutdown, the _open flag will be reset first and then the
  // acceptor is cancelled and closed, in this case we do not want
  // to start another async_accept. In other cases, we do want to
  // continue after an error.
  if (!_open && ec == asio_ns::error::operation_aborted) {
    // this "error" is accepted, so it doesn't justify a warning
    SDB_DEBUG(HTTP, "accept failed: ", ec.message());
    return;
  }

  if (++_accept_failures <= kMaxAcceptErrors) {
    SDB_WARN(HTTP, "accept failed: ", ec.message());
    if (_accept_failures == kMaxAcceptErrors) {
      SDB_WARN(HTTP, "too many accept failures, stopping to report");
    }
  }
  asyncAccept();  // retry
  return;
}
