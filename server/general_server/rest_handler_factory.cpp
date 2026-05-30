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

#include "rest_handler_factory.h"

#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "rest/general_request.h"
#include "rest/general_response.h"

using namespace sdb;
using namespace sdb::basics;
using namespace sdb::rest;

RestHandlerFactory::RestHandlerFactory() : _sealed(false) {}

std::shared_ptr<RestHandler> RestHandlerFactory::createHandler(
  SerenedServer& server, std::unique_ptr<GeneralRequest> req,
  std::unique_ptr<GeneralResponse> res) const {
  SDB_ASSERT(_sealed);

  const std::string_view path = req->requestPath();

  auto it = _constructors.find(path);

  if (it != _constructors.end()) {
    return it->second.first(server, req.release(), res.release(),
                            it->second.second);
  }

  const std::string* prefix = nullptr;

  // find longest match
  const size_t path_length = path.size();
  // prefixes are sorted by length descending
  for (const auto& p : _prefixes) {
    const size_t p_size = p.size();
    if (p_size >= path_length) {
      // prefix too long
      continue;
    }

    if (path[p_size] != '/') {
      // requested path does not have a '/' at the prefix length's position
      // so it cannot match
      continue;
    }
    if (path.starts_with(p)) {
      prefix = &p;
      break;  // first match is longest match
    }
  }

  size_t l;
  if (prefix == nullptr) {
    it = _constructors.find("/");
    l = 1;
  } else {
    SDB_ASSERT(!prefix->empty());
    it = _constructors.find(*prefix);
    l = prefix->size() + 1;
  }

  // we must have found a handler - at least the catch-all handler must be
  // present
  SDB_ASSERT(it != _constructors.end());

  size_t n = path.find('/', l);

  while (n != std::string::npos) {
    req->addSuffix(path.substr(l, n - l));
    l = n + 1;
    n = path.find('/', l);
  }

  if (l < path.size()) {
    req->addSuffix(path.substr(l));
  }

  req->setPrefix(it->first);

  return it->second.first(server, req.release(), res.release(),
                          it->second.second);
}

void RestHandlerFactory::addHandler(std::string_view path, create_fptr func,
                                    void* data) {
  SDB_ASSERT(!_sealed);

  if (!_constructors.try_emplace(path, func, data).second) {
    // there should only be one handler for each path
    SDB_THROW(ERROR_INTERNAL,
              "attempt to register duplicate path handler for: ", path);
  }
}

void RestHandlerFactory::addPrefixHandler(std::string_view path,
                                          create_fptr func, void* data) {
  SDB_ASSERT(!_sealed);

  addHandler(path, func, data);

  _prefixes.emplace_back(path);
}

void RestHandlerFactory::seal() {
  SDB_ASSERT(!_sealed);

  // sort prefixes by their lengths
  absl::c_sort(_prefixes, [](const auto& a, const auto& b) {
    return a.size() > b.size();
  });

  _sealed = true;
}
