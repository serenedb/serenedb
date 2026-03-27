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

#include <vpack/builder.h>
#include <vpack/options.h>
#include <vpack/slice.h>

#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "basics/common.h"
#include "basics/containers/flat_hash_map.h"
#include "endpoint/connection_info.h"
#include "endpoint/endpoint.h"
#include "rest/common_defines.h"

namespace sdb {

class RequestContext;

using rest::ContentType;
using rest::EncodingType;
using rest::RequestType;

class GeneralRequest {
  GeneralRequest(const GeneralRequest&) = delete;
  GeneralRequest& operator=(const GeneralRequest&) = delete;

 public:
  GeneralRequest(GeneralRequest&&) = default;

  explicit GeneralRequest(const ConnectionInfo& connection_info, uint64_t mid);

  virtual ~GeneralRequest() = default;

  // translate an RequestType enum value into an "HTTP method string"
  static std::string_view translateMethod(RequestType);

  // translate "HTTP method string" into RequestType enum value
  static RequestType translateMethod(std::string_view method);

  size_t memoryUsage() const noexcept { return _memory_usage; }
  void increaseMemoryUsage(size_t value) noexcept { _memory_usage += value; }

  const ConnectionInfo& connectionInfo() const { return _connection_info; }

  /// Database used for this request, _system by default
  const std::string& databaseName() const noexcept { return _database_name; }
  void setDatabaseName(std::string_view database_name);

  /// User exists on this server or on external auth system
  ///  and password was checked. Must not imply any access rights
  ///  to any specific resource.
  bool authenticated() const { return _authenticated; }
  void setAuthenticated(bool a) { _authenticated = a; }

  double tokenExpiry() const { return _token_expiry; }
  void setTokenExpiry(double value) { _token_expiry = value; }

  // User sending this request
  const std::string& user() const { return _user; }
  void setUser(std::string_view user);

  /// the request context depends on the application
  std::shared_ptr<RequestContext> requestContext() const {
    return _request_context;
  }

  /// set request context
  void setRequestContext(std::shared_ptr<RequestContext>);

  RequestType requestType() const { return _type; }

  void setRequestType(RequestType type) { _type = type; }

  const std::string& fullUrl() const { return _full_url; }
  void setFullUrl(std::string_view full_url);

  const std::string& requestUrl() const { return _full_url; }

  // consists of the URL without the host and without any parameters.
  const std::string& requestPath() const { return _request_path; }
  void setRequestPath(std::string_view path);

  // The request path consists of the URL without the host and without any
  // parameters.  The request path is split into two parts: the prefix, namely
  // the part of the request path that was match by a handler and the suffix
  // with all the remaining arguments.
  std::string prefix() const { return _prefix; }
  void setPrefix(std::string_view prefix);

  // Returns the request path suffixes in non-URL-decoded form
  const std::vector<std::string>& suffixes() const { return _suffixes; }

  void addSuffix(std::string_view part);

#ifdef SDB_GTEST
  void clearSuffixes() {
    size_t memory_usage = 0;
    for (const auto& it : _suffixes) {
      memory_usage += it.size();
    }
    _suffixes.clear();
    _memory_usage -= memory_usage;
  }
#endif

  // Returns the request path suffixes in URL-decoded form. Note: this will
  // re-compute the suffix list on every call!
  std::vector<std::string> decodedSuffixes() const;

  uint64_t messageId() const { return _message_id; }

  // get value from headers map. The key must be lowercase.
  const std::string& header(std::string_view key) const;
  const std::string& header(std::string_view key, bool& found) const;
  const containers::FlatHashMap<std::string, std::string>& headers() const {
    return _headers;
  }

  void removeHeader(std::string_view key);
  void addHeader(std::string key, std::string value);

  // the value functions give access to to query string parameters
  const std::string& value(std::string_view key) const;
  const std::string& value(std::string_view key, bool& found) const;
  const auto& values() const { return _values; }

  // returns the query parameters as fuerte needs them (as a map)
  std::map<std::string, std::string> parameters() const;

  const auto& arrayValues() const { return _array_values; }

  /// returns parsed value, returns valueNotFound if parameter was not
  /// found
  template<typename T>
  T ParsedValue(std::string_view key, T value_not_found);
  /// returns parsed value, returns std::nullopt if parameter was not
  /// found
  template<typename T>
  auto ParsedValue(std::string_view key) -> std::optional<T>;

  /// the content length
  virtual size_t contentLength() const noexcept = 0;
  /// unprocessed request payload
  virtual std::string_view rawPayload() const = 0;
  /// parsed request payload
  virtual vpack::Slice payload(bool strict_validation = true) = 0;
  /// overwrite payload
  virtual void setPayload(vpack::BufferUInt8 buffer);

  virtual void setDefaultContentType() noexcept = 0;
  /// @brieg should reflect the Content-Type header
  ContentType contentType() const noexcept { return _content_type; }
  /// should generally reflect the Accept header
  ContentType contentTypeResponse() const noexcept {
    return _content_type_response;
  }

  const std::string& contentTypeResponsePlain() const {
    return _content_type_response_plain;
  }
  /// should generally reflect the Accept-Encoding header
  EncodingType acceptEncoding() const { return _accept_encoding; }

  rest::AuthenticationMethod authenticationMethod() const {
    return _authentication_method;
  }

  void setAuthenticationMethod(rest::AuthenticationMethod method) {
    _authentication_method = method;
  }

 protected:
  template<typename Key, typename Value>
  void setValue(Key&& key, Value&& value) {
    const auto new_size = key.size() + value.size();
    auto [it, inserted] =
      _values.try_emplace(std::forward<Key>(key), std::forward<Value>(value));
    if (!inserted) {
      const auto old_size = it->first.size() + it->second.size();
      it->second = std::forward<Value>(value);
      _memory_usage -= old_size;
    }
    _memory_usage += new_size;
  }

  template<typename Key, typename Value>
  void setArrayValue(Key&& key, Value&& value) {
    auto new_size = value.size();
    auto [it, inserted] = _array_values.try_emplace(key);
    if (inserted) {
      new_size += key.size();
    }
    it->second.emplace_back(std::forward<Value>(value));
    _memory_usage += new_size;
  }

  void setStringValue(std::string& target, std::string_view value);

  /// get VPack options for validation. effectively turns off
  /// validation if strictValidation is false. This optimization can be used for
  /// internal requests
  const vpack::Options* validationOptions(bool strict_validation);

  ConnectionInfo _connection_info;  /// connection info

  /// request payload buffer, exact access semantics are defined in subclass
  vpack::BufferUInt8 _payload;

  std::string _database_name;
  std::string _user;

  std::string _full_url;
  std::string _request_path;
  std::string _prefix;  // part of path matched by rest route
  std::string _content_type_response_plain;
  std::vector<std::string> _suffixes;  // path suffixes

  containers::FlatHashMap<std::string, std::string> _headers;
  containers::FlatHashMap<std::string, std::string> _values;
  containers::FlatHashMap<std::string, std::vector<std::string>> _array_values;

  /// if payload was not VPack this will store parsed result
  std::shared_ptr<vpack::Builder> _vpack_builder;

  const uint64_t _message_id;

  // request context (might contain database)
  std::shared_ptr<RequestContext> _request_context;

  double _token_expiry;

  size_t _memory_usage;

  rest::AuthenticationMethod _authentication_method;

  // information about the payload
  RequestType _type;          // GET, POST, ..
  ContentType _content_type;  // Unset, VPACK, JSON
  ContentType _content_type_response;
  EncodingType _accept_encoding;
  bool _authenticated;
};

}  // namespace sdb
