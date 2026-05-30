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

#include "endpoint_srv.h"

#include <algorithm>
#include <vector>

#define BIND_4_COMPAT 1  // LINUX
#define BIND_8_COMPAT 1  // MACOSX

#include <arpa/nameser.h>
#include <netinet/in.h>
#include <resolv.h>

#include "basics/logger/logger.h"
#include "basics/string_utils.h"

using namespace sdb;
using namespace sdb::basics;

#if PACKETSZ > 1024
#define MAXPACKET PACKETSZ
#else
#define MAXPACKET 1024
#endif

union QueryBuffer {
  ::HEADER header;
  unsigned char buffer[MAXPACKET];
};

struct SrvRecord {
  int priority;
  int weight;
  int port;
  std::string name;
};

static std::vector<SrvRecord> SrvRecords(const std::string& specification) {
  res_init();

  const char* dname = specification.c_str();
  int nclass = ns_c_in;
  int type = ns_t_srv;

  QueryBuffer answer;
  int anslen = sizeof(answer);

  int n = res_search(dname, nclass, type, answer.buffer, anslen);

  std::vector<SrvRecord> services;

  if (n != -1) {
    HEADER* hp = &answer.header;

    int qdcount = ntohs(hp->qdcount);
    int ancount = ntohs(hp->ancount);

    unsigned char* msg = answer.buffer;
    unsigned char* eom = msg + n;
    unsigned char* cp = msg + sizeof(HEADER);

    char hostbuf[256];

    while (0 < qdcount-- && cp < eom) {
      n = dn_expand(msg, eom, cp, hostbuf, 256);

      if (n < 0) {
        SDB_WARN(GENERAL, "DNS record for '", specification,
                 "' is corrupt");
        return {};
      }

      cp += n + QFIXEDSZ;
    }

    // loop through the answer buffer and extract SRV records
    while (0 < ancount-- && cp < eom) {
      n = dn_expand(msg, eom, cp, hostbuf, 256);

      if (n < 0) {
        SDB_WARN(GENERAL, "DNS record for '", specification,
                 "' is corrupt");
        return {};
      }

      cp += n;

      int type;
      GETSHORT(type, cp);

      int nclass;
      GETSHORT(nclass, cp);

      int ttl;
      GETLONG(ttl, cp);

      int dlen;
      GETSHORT(dlen, cp);

      int priority;
      GETSHORT(priority, cp);

      int weight;
      GETSHORT(weight, cp);

      int port;
      GETSHORT(port, cp);

      n = dn_expand(msg, eom, cp, hostbuf, 256);

      if (n < 0) {
        SDB_WARN(GENERAL, "DNS record for '", specification,
                 "' is corrupt");
        break;
      }

      cp += n;

      SDB_TRACE(GENERAL, "DNS record for '", specification,
                "': type ", type, ", typename ", nclass, ", ttl ", ttl,
                ", len ", dlen, ", prio ", priority, ", weight ", weight,
                ", port ", port, ", host '", hostbuf, "'");

      if (type != T_SRV) {
        continue;
      }

      SrvRecord srv;

      srv.weight = weight;
      srv.priority = priority;
      srv.port = port;
      srv.name = hostbuf;

      services.push_back(srv);
    }
  } else {
    SDB_WARN(GENERAL, "DNS record for '", specification,
             "' not found");
  }

  absl::c_sort(services, [](const auto& lhs, const auto& rhs) {
    if (lhs.priority != rhs.priority) {
      return lhs.priority < rhs.priority;
    }

    return lhs.weight > rhs.weight;
  });

  return services;
}

EndpointSrv::EndpointSrv(const std::string& specification)
  : Endpoint(DomainType::SRV, EndpointType::Client, TransportType::HTTP,
             EncryptionType::None, specification, 0) {}

EndpointSrv::~EndpointSrv() = default;

bool EndpointSrv::isConnected() const {
  if (_endpoint != nullptr) {
    return _endpoint->isConnected();
  }

  return false;
}

SocketWrapper EndpointSrv::connect(double connect_timeout,
                                   double request_timeout) {
  auto services = SrvRecords(_specification);

  SocketWrapper res;

  for (auto service : services) {
    std::string spec =
      "tcp://" + service.name + ":" + string_utils::Itoa(service.port);

    _endpoint = Endpoint::clientFactory(spec);

    res = _endpoint->connect(connect_timeout, request_timeout);

    if (_endpoint->isConnected()) {
      return res;
    }
  }

  Sdbinvalidatesocket(&res);
  return res;
}

void EndpointSrv::disconnect() {
  if (_endpoint != nullptr) {
    _endpoint->disconnect();
  }
}

int EndpointSrv::domain() const {
  if (_endpoint != nullptr) {
    return _endpoint->domain();
  }

  return -1;
}

int EndpointSrv::port() const {
  if (_endpoint != nullptr) {
    return _endpoint->port();
  }

  return -1;
}

std::string EndpointSrv::host() const {
  if (_endpoint != nullptr) {
    return _endpoint->host();
  }

  return "";
}

std::string EndpointSrv::hostAndPort() const {
  if (_endpoint != nullptr) {
    return _endpoint->hostAndPort();
  }

  return "";
}
