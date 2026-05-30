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

#include "endpoint/endpoint_list.h"
#include "rest_server/serened.h"

namespace sdb {

class EndpointFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Endpoint"; }

  inline static EndpointFeature* gInstance = nullptr;
  static EndpointFeature& instance() noexcept { return *gInstance; }

  explicit EndpointFeature(SerenedServer& server);
  ~EndpointFeature();

  void validateOptions() final;

  std::vector<std::string> httpEndpoints();
  EndpointList& endpointList() { return _endpoint_list; }
  const EndpointList& endpointList() const { return _endpoint_list; }

 private:
  void buildEndpointLists();

  std::vector<std::string> _endpoints;
  bool _reuse_address = true;
  uint64_t _backlog_size = 64;

  EndpointList _endpoint_list;
};

}  // namespace sdb
