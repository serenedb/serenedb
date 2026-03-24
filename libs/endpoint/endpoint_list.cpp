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

#include "endpoint_list.h"

#include <utility>

#include "basics/assert.h"
#include "basics/logger/logger.h"

namespace sdb {

bool EndpointList::add(const std::string& specification, int back_log_size,
                       bool reuse_address) {
  std::string key = Endpoint::unifiedForm(specification);

  if (key.empty()) {
    return false;
  }

  auto it = _endpoints.find(key);

  if (it != _endpoints.end()) {
    return true;
  }

  std::unique_ptr<Endpoint> ep(
    Endpoint::serverFactory(key, back_log_size, reuse_address));

  if (ep == nullptr) {
    return false;
  }

  _endpoints.emplace(std::move(key), std::move(ep));
  return true;
}

std::vector<std::string> EndpointList::all(
  Endpoint::TransportType transport) const {
  std::string_view prefix;

  switch (transport) {
    case Endpoint::TransportType::HTTP:
      prefix = kHttp;
      break;

    case Endpoint::TransportType::PGSQL:
      prefix = kPgSql;
      break;
  }

  std::vector<std::string> result;
  for (auto& it : _endpoints) {
    if (it.first.starts_with(prefix)) {
      result.emplace_back(it.first);
    }
  }

  return result;
}

void EndpointList::apply(
  const std::function<void(const std::string&, Endpoint&)>& cb) {
  for (auto& it : _endpoints) {
    SDB_ASSERT(it.second != nullptr);
    cb(it.first, *it.second);
  }
}

bool EndpointList::hasSsl() const {
  for (auto& it : _endpoints) {
    if (it.second->encryption() == Endpoint::EncryptionType::SSL) {
      return true;
    }
  }

  return false;
}

void EndpointList::dump() const {
  for (const auto& it : _endpoints) {
    SDB_ASSERT(it.second != nullptr);
    SDB_INFO("xxxxx", sdb::Logger::FIXME, "using endpoint '", it.first,
             "' for ", encryptionName(it.second->encryption()), " requests");
  }
}

std::string_view EndpointList::encryptionName(
  Endpoint::EncryptionType encryption) {
  switch (encryption) {
    case Endpoint::EncryptionType::SSL:
      return "ssl-encrypted";
    case Endpoint::EncryptionType::None:
    default:
      return "non-encrypted";
  }
}

}  // namespace sdb
