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

#include "general_request.h"

#include <vpack/vpack_helper.h>

#include "basics/debugging.h"
#include "basics/logger/logger.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "rest/request_context.h"

using namespace sdb;
using namespace sdb::basics;

namespace {

rest::RequestType TranslateMethodHelper(std::string_view method) noexcept {
  if (method == "DELETE") {
    return RequestType::DeleteReq;
  } else if (method == "GET") {
    return RequestType::Get;
  } else if (method == "HEAD") {
    return RequestType::Head;
  } else if (method == "OPTIONS") {
    return RequestType::Options;
  } else if (method == "PATCH") {
    return RequestType::Patch;
  } else if (method == "POST") {
    return RequestType::Post;
  } else if (method == "PUT") {
    return RequestType::Put;
  }
  return RequestType::Illegal;
}

}  // namespace

GeneralRequest::GeneralRequest(const ConnectionInfo& connection_info,
                               uint64_t mid)
  : _connection_info(connection_info),
    _message_id(mid),
    _request_context(),
    _token_expiry(0.0),
    _memory_usage(0),
    _authentication_method(rest::AuthenticationMethod::None),
    _type(RequestType::Illegal),
    _content_type(ContentType::Unset),
    _content_type_response(ContentType::Unset),
    _accept_encoding(EncodingType::Unset),
    _authenticated(false) {}

std::string_view GeneralRequest::translateMethod(RequestType method) {
  switch (method) {
    case RequestType::DeleteReq:
      return "DELETE";
    case RequestType::Get:
      return "GET";
    case RequestType::Head:
      return "HEAD";
    case RequestType::Options:
      return "OPTIONS";
    case RequestType::Patch:
      return "PATCH";
    case RequestType::Post:
      return "POST";
    case RequestType::Put:
      return "PUT";
    default:
      SDB_WARN("xxxxx", Logger::FIXME,
               "illegal http request method encountered in switch");
      return "UNKNOWN";
  }
}

rest::RequestType GeneralRequest::translateMethod(std::string_view method) {
  auto ret = TranslateMethodHelper(method);
  if (ret == RequestType::Illegal) {
    return TranslateMethodHelper(absl::AsciiStrToUpper(method));
  }
  return ret;
}

void GeneralRequest::setRequestContext(
  std::shared_ptr<RequestContext> request_context) {
  SDB_ASSERT(request_context != nullptr);

  _request_context = std::move(request_context);
}

void GeneralRequest::setPayload(vpack::BufferUInt8 buffer) {
  auto old = _payload.size();
  _payload = std::move(buffer);
  _memory_usage += _payload.size();
  SDB_ASSERT(_memory_usage >= old);
  _memory_usage -= old;
}

void GeneralRequest::setFullUrl(std::string_view full_url) {
  setStringValue(_full_url, full_url);
  if (_full_url.empty()) {
    _full_url.push_back('/');
    _memory_usage += 1;
  }
}

void GeneralRequest::setRequestPath(std::string_view path) {
  setStringValue(_request_path, path);
}

void GeneralRequest::setDatabaseName(std::string_view database_name) {
  setStringValue(_database_name, database_name);
}

void GeneralRequest::setUser(std::string_view user) {
  setStringValue(_user, user);
}

void GeneralRequest::setPrefix(std::string_view prefix) {
  setStringValue(_prefix, prefix);
}

void GeneralRequest::addSuffix(std::string_view part) {
  // part will not be URL-decoded here!
  _suffixes.emplace_back(part);
  _memory_usage += _suffixes.back().size();
}

std::vector<std::string> GeneralRequest::decodedSuffixes() const {
  std::vector<std::string> result;
  result.reserve(_suffixes.size());

  for (const auto& it : _suffixes) {
    result.emplace_back(string_utils::UrlDecodePath(it));
  }
  return result;
}

const std::string& GeneralRequest::header(std::string_view key,
                                          bool& found) const {
  auto it = _headers.find(key);
  if (it == _headers.end()) {
    found = false;
    return StaticStrings::kEmpty;
  }

  found = true;
  return it->second;
}

const std::string& GeneralRequest::header(std::string_view key) const {
  bool unused = true;
  return header(key, unused);
}

void GeneralRequest::removeHeader(std::string_view key) {
  auto it = _headers.find(key);
  if (it != _headers.end()) {
    auto old = (*it).first.size() + (*it).second.size();
    _headers.erase(it);
    SDB_ASSERT(_memory_usage >= old);
    _memory_usage -= old;
  }
}

void GeneralRequest::addHeader(std::string key, std::string value) {
  auto memory_usage = key.size() + value.size();
  auto it = _headers.try_emplace(std::move(key), std::move(value));
  if (it.second) {
    _memory_usage += memory_usage;
  }
}

const std::string& GeneralRequest::value(std::string_view key,
                                         bool& found) const {
  if (!_values.empty()) {
    auto it = _values.find(key);

    if (it != _values.end()) {
      found = true;
      return it->second;
    }
  }

  found = false;
  return StaticStrings::kEmpty;
}

const std::string& GeneralRequest::value(std::string_view key) const {
  bool unused = true;
  return value(key, unused);
}

std::map<std::string, std::string> GeneralRequest::parameters() const {
  std::map<std::string, std::string> parameters{};
  for (const auto& param_pair : values()) {
    parameters.try_emplace(param_pair.first, param_pair.second);
  }
  return parameters;
}

void GeneralRequest::setStringValue(std::string& target,
                                    std::string_view value) {
  auto old = target.size();
  target = value;
  _memory_usage += target.size();
  SDB_ASSERT(_memory_usage >= old);
  _memory_usage -= old;
}

// needs to be here because of a gcc bug with templates and namespaces
// https://stackoverflow.com/a/25594741/1473569
namespace sdb {

template<>
auto GeneralRequest::ParsedValue(std::string_view key)
  -> std::optional<std::string> {
  bool found = false;
  const auto& val = this->value(key, found);
  if (found) {
    return val;
  } else {
    return std::nullopt;
  }
}

template<>
auto GeneralRequest::ParsedValue(std::string_view key) -> std::optional<bool> {
  bool found = false;
  const auto& val = this->value(key, found);
  if (found) {
    return string_utils::Boolean(val);
  } else {
    return std::nullopt;
  }
}

template<>
auto GeneralRequest::ParsedValue(std::string_view key)
  -> std::optional<uint64_t> {
  bool found = false;
  const auto& val = this->value(key, found);
  if (found) {
    return string_utils::Uint64(val);
  } else {
    return std::nullopt;
  }
}

template<>
auto GeneralRequest::ParsedValue(std::string_view key)
  -> std::optional<double> {
  bool found = false;
  const auto& val = this->value(key, found);
  if (found) {
    return string_utils::DoubleDecimal(val);
  } else {
    return std::nullopt;
  }
}

template<typename T>
auto GeneralRequest::ParsedValue(std::string_view key, T value_not_found) -> T {
  if (auto res = ParsedValue<decltype(value_not_found)>(key); res.has_value()) {
    return *res;
  } else {
    return value_not_found;
  }
}
template auto GeneralRequest::ParsedValue<bool>(std::string_view, bool) -> bool;
template auto GeneralRequest::ParsedValue<uint64_t>(std::string_view, uint64_t)
  -> uint64_t;
template auto GeneralRequest::ParsedValue<double>(std::string_view, double)
  -> double;

/// get VPack options for validation. effectively turns off
/// validation if strictValidation is false. This optimization can be used for
/// internal requests
const vpack::Options* GeneralRequest::validationOptions(
  bool strict_validation) {
  if (strict_validation) {
    return &basics::VPackHelper::gStrictRequestValidationOptions;
  }
  return &basics::VPackHelper::gLooseRequestValidationOptions;
}

}  // namespace sdb
