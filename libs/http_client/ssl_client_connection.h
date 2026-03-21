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

#include <openssl/ossl_typ.h>
#include <stddef.h>

#include <cstdint>
#include <memory>

#include "http_client/general_client_connection.h"

namespace sdb {

class Endpoint;
namespace basics {

class StringBuffer;

}  // namespace basics
namespace httpclient {

class SslClientConnection final : public GeneralClientConnection {
 public:
  SslClientConnection(app::CommunicationFeaturePhase& comm,
                      std::unique_ptr<Endpoint> endpoint, double, double,
                      size_t, uint64_t);

  SslClientConnection(const SslClientConnection&) = delete;
  SslClientConnection& operator=(const SslClientConnection&) = delete;

  ~SslClientConnection() final;

  uint64_t sslProtocol() const { return _ssl_protocol; }

  void setVerifyCertificates(bool value) { _verify_certificates = value; }

  void setVerifyDepth(int value) { _verify_depth = value; }

 private:
  bool cleanUpSocketFlags();
  bool setSocketToNonBlocking();

  void init();

  bool connectSocket() final;

  void disconnectSocket() final;

  bool writeClientConnection(const void*, size_t, size_t*) final;

  bool readClientConnection(basics::StringBuffer&,
                            bool& connection_closed) final;

  bool readable() final;

  SSL* _ssl = nullptr;

  SSL_CTX* _ctx = nullptr;

  uint64_t _ssl_protocol;

  int _socket_flags = 0;

  int _verify_depth = 0;

  bool _verify_certificates = false;
};

}  // namespace httpclient
}  // namespace sdb
