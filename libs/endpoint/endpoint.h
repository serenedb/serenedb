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

#pragma once

#include <memory>
#include <ostream>
#include <string>

#include "basics/common.h"
#include "basics/operating-system.h"

namespace sdb {

inline constexpr std::string_view kPgSql = "pgsql+";
inline constexpr std::string_view kHttp = "http+";

class Endpoint {
 public:
  enum class TransportType {
    HTTP,
    PGSQL,
  };
  enum class EncryptionType {
    None = 0,
    SSL,
  };
  enum class DomainType {
    Unknown = 0,
    UNIX,
    IPv4,
    IPv6,
  };

 protected:
  Endpoint(DomainType, TransportType, EncryptionType, std::string_view, int);

 public:
  virtual ~Endpoint() = default;

 public:
  static std::string uriForm(std::string_view);
  static std::string unifiedForm(std::string_view);
  static std::unique_ptr<Endpoint> serverFactory(std::string_view, int,
                                                 bool reuse_address);

 public:
  TransportType transport() const { return _transport; }
  EncryptionType encryption() const { return _encryption; }
  std::string specification() const { return _specification; }

 public:
  virtual DomainType domainType() const { return _domain_type; }

  virtual int domain() const = 0;
  virtual int port() const = 0;
  virtual std::string host() const = 0;

  int listenBacklog() const { return _listen_backlog; }

 protected:
  DomainType _domain_type;
  TransportType _transport;
  EncryptionType _encryption;
  std::string _specification;
  int _listen_backlog;
};

}  // namespace sdb
