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

#include "endpoint_unix_domain.h"

#ifdef SERENEDB_HAVE_DOMAIN_SOCKETS

using namespace sdb;

EndpointUnixDomain::EndpointUnixDomain(int listen_backlog,
                                       const std::string& path)
  : Endpoint(DomainType::UNIX, TransportType::HTTP, EncryptionType::None,
             "http+unix://" + path, listen_backlog),
    _path(path) {}

#endif
