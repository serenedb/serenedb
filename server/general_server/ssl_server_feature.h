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
#include <string>

#include "basics/asio_ns.h"

namespace sdb {
namespace options {

class ProgramOptions;
}

class SslServerFeature {
 public:
  typedef std::shared_ptr<std::vector<asio_ns::ssl::context>> SslContextList;

  inline static SslServerFeature* gInstance = nullptr;
  static SslServerFeature& instance() noexcept { return *gInstance; }

  SslServerFeature();
  ~SslServerFeature();

  void start();
  void stop();

  void verifySslOptions();

  SslContextList createSslContexts();

 protected:
  struct SNIEntry {
    std::string server_name;      // empty for default
    std::string keyfile_name;     // name of key file
    std::string keyfile_content;  // content of key file
    SNIEntry(std::string name, std::string keyfile_name)
      : server_name(std::move(name)), keyfile_name(std::move(keyfile_name)) {}
  };

  std::string _cafile;
  std::string _keyfile;  // name of default keyfile
  // For SNI, we have two maps, one mapping to the filename for a certain
  // server, another, to keep the actual keyfile in memory.
  std::vector<SNIEntry>
    _sni_entries;  // the first entry is the default server keyfile
  std::string _cipher_list;
  uint64_t _ssl_protocol;
  uint64_t _ssl_options;
  std::string _ecdh_curve;
  bool _session_cache;
  bool _prefer_http11_in_alpn;

 private:
  asio_ns::ssl::context createSslContextInternal(std::string keyfile_name,
                                                 std::string& content);

  std::string stringifySslOptions(uint64_t opts) const;

  std::string _rctx;
};

}  // namespace sdb
