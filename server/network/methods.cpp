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

#include "methods.h"

#include <basics/buffer.h>
#include <fuerte/connection.h>
#include <fuerte/requests.h>
#include <fuerte/types.h>
#include <vpack/slice.h>

#include "app/app_server.h"
#include "basics/common.h"
#include "basics/exceptions.h"
#include "basics/hybrid_logical_clock.h"
#include "basics/logger/logger.h"
#include "basics/utf8_helper.h"
#include "catalog/identifiers/shard_id.h"
#include "database/ticks.h"
#include "general_server/scheduler.h"
#include "general_server/scheduler_feature.h"
#include "general_server/state.h"
#include "network/connection_pool.h"
#include "network/network_feature.h"
#include "network/utils.h"
#include "rest_server/serened.h"
#include "utils/coro_helper.h"

#ifdef SDB_CLUSTER
#include "agency/agency_feature.h"
#include "cluster/cluster_feature.h"
#include "cluster/cluster_info.h"
#endif

namespace sdb {
namespace network {
using namespace sdb::fuerte;

using PromiseRes = yaclib::Promise<network::Response>;

Response::Response() noexcept : error(fuerte::Error::ConnectionCanceled) {}

Response::Response(DestinationId&& destination, fuerte::Error error,
                   std::unique_ptr<sdb::fuerte::Request>&& request,
                   std::unique_ptr<sdb::fuerte::Response>&& response) noexcept
  : destination(std::move(destination)),
    error(error),
    _request(std::move(request)),
    _response(std::move(response)) {
  SDB_ASSERT(_request != nullptr || error == fuerte::Error::ConnectionCanceled);
}

sdb::fuerte::Request& Response::request() const {
  SDB_ASSERT(hasRequest());
  if (_request == nullptr) {
    SDB_THROW(ERROR_INTERNAL, "no valid request object");
  }
  return *_request;
}

sdb::fuerte::Response& Response::response() const {
  SDB_ASSERT(hasResponse());
  if (_response == nullptr) {
    SDB_THROW(ERROR_INTERNAL, "no valid response object");
  }
  return *_response;
}

#ifdef SDB_GTEST
/// inject a different response - only use this from tests!
void Response::setResponse(std::unique_ptr<sdb::fuerte::Response> response) {
  _response = std::move(response);
}
#endif

/// steal the response from here. this may return a unique_ptr
/// containing a nullptr. it is the caller's responsibility to check that.
std::unique_ptr<sdb::fuerte::Response> Response::stealResponse() noexcept {
  return std::unique_ptr<sdb::fuerte::Response>(_response.release());
}

// returns a slice of the payload if there was no error
vpack::Slice Response::slice() const noexcept {
  if (error == fuerte::Error::NoError && _response) {
    try {
      return _response->slice();
    } catch (const std::exception& ex) {
      // BTS-163: catch exceptions in slice() method so that
      // NetworkFeature can be used in a safer way.
      SDB_WARN("xxxxx", Logger::COMMUNICATION,
               "caught exception in Response::slice(): ", ex.what());
    }
    // fallthrough intentional
  }
  return vpack::Slice();  // none slice
}

size_t Response::payloadSize() const noexcept {
  if (_response != nullptr) {
    return _response->payloadSize();
  }
  return 0;
}

fuerte::StatusCode Response::statusCode() const noexcept {
  if (error == fuerte::Error::NoError && _response) {
    return _response->statusCode();
  }
  return fuerte::kStatusUndefined;
}

Result Response::combinedResult() const {
  if (fail()) {
    // fuerte connection failed
    return Result{FuerteToSereneErrorCode(*this),
                  FuerteToSereneErrorMessage(*this)};
  }
  if (!StatusIsSuccess(_response->statusCode())) {
    // HTTP status error. Try to extract a precise error from the body, and
    // fall back to the HTTP status.
    return ResultFromBody(_response->slice(),
                          FuerteStatusToSereneErrorCode(*_response));
  }
  return Result{};
}

/// shardId or empty
ResultOr<ShardID> Response::destinationShard() const {
  if (this->destination.size() > 6 && this->destination.starts_with("shard:")) {
    return ShardID::shardIdFromString(this->destination.substr(6));
  }
  return std::unexpected<Result>{std::in_place, ERROR_BAD_PARAMETER,
                                 "destination not a shard"};
}

std::string Response::serverId() const {
  if (this->destination.size() > 7 &&
      this->destination.starts_with("server:")) {
    return this->destination.substr(7);
  }
  return {};
}

auto PrepareRequest(ConnectionPool* pool, RestVerb type, std::string path,
                    vpack::BufferUInt8 payload, const RequestOptions& options,
                    Headers headers) {
  SDB_ASSERT(path.find("/_db/") == std::string::npos);
  SDB_ASSERT(path.find('?') == std::string::npos);
  SDB_ASSERT(options.database == NormalizeUtf8ToNFC(options.database));

  auto req =
    fuerte::CreateRequest(type, path, options.parameters, std::move(payload));

  req->header.database = options.database;
  req->header.setMeta(std::move(headers));

  if (!options.content_type.empty()) {
    req->header.contentType(options.content_type);
  }
  if (!options.accept_type.empty()) {
    req->header.acceptType(options.accept_type);
  }

  if (options.send_hlc_header) {
    // add x-serene-hlc header to outgoing request, so that the peer
    // can update its own HLC value to at least the value of our own HLC
    Tick time_stamp = NewTickHybridLogicalClock();
    req->header.addMeta(
      StaticStrings::kHlcHeader,
      basics::HybridLogicalClock::encodeTimeStamp(time_stamp));
  }

#ifdef SDB_CLUSTER
  consensus::Agent* agent = nullptr;
  if (pool) {
    auto& server = SerenedServer::Instance();
    if (server.hasFeature<AgencyFeature>() &&
        server.isEnabled<AgencyFeature>()) {
      agent = server.getFeature<AgencyFeature>().agent();
    }
  }
  // note: agent can be a nullptr here
  network::AddSourceHeader(agent, *req);
#endif

  return req;
}

/// Function to produce a response object from thin air:
static std::unique_ptr<fuerte::Response> BuildResponse(
  fuerte::StatusCode status_code, const Result& res) {
  vpack::BufferUInt8 buffer;
  vpack::Builder builder(buffer);
  {
    vpack::ObjectBuilder guard(&builder);
    auto error_num = res.errorNumber();
    builder.add(StaticStrings::kError, error_num != ERROR_OK);
    builder.add(StaticStrings::kErrorNum, error_num.value());
    if (error_num != ERROR_OK) {
      builder.add(StaticStrings::kErrorMessage, res.errorMessage());
    }
    builder.add(StaticStrings::kCode, static_cast<int>(status_code));
  }
  fuerte::ResponseHeader response_header;
  response_header.response_code = status_code;
  response_header.contentType(ContentType::VPack);
  auto resp = std::make_unique<fuerte::Response>(std::move(response_header));
  resp->setPayload(std::move(buffer), 0);
  return resp;
}

namespace {

struct Pack {
  DestinationId dest;
  yaclib::Promise<network::Response> promise;
  std::unique_ptr<fuerte::Response> tmp_res;
  std::unique_ptr<fuerte::Request> tmp_req;
  fuerte::Error tmp_err;
  RequestLane continuation_lane;
  bool skip_scheduler;
  bool handle_content_encoding;
  Pack(DestinationId&& dest, yaclib::Promise<network::Response>&& p,
       RequestLane lane, bool skip, bool handle)
    : dest(std::move(dest)),
      promise(std::move(p)),
      continuation_lane(lane),
      skip_scheduler(skip),
      handle_content_encoding(handle) {}
};

void ActuallySendRequest(std::shared_ptr<Pack>&& p, ConnectionPool* pool,
                         const RequestOptions& options, std::string endpoint,
                         std::unique_ptr<Request>&& req) {
  NetworkFeature& nf = SerenedServer::Instance().getFeature<NetworkFeature>();
  nf.sendRequest(
    *pool, options, endpoint, std::move(req),
    [pack(std::move(p)), pool, options, endpoint](
      fuerte::Error err, std::unique_ptr<fuerte::Request> req,
      std::unique_ptr<fuerte::Response> res, bool is_from_pool) mutable {
      SDB_ASSERT(req != nullptr || err == fuerte::Error::ConnectionCanceled);

      if (is_from_pool && (err == fuerte::Error::ConnectionClosed ||
                           err == fuerte::Error::WriteError)) {
        // retry under certain conditions
        ActuallySendRequest(std::move(pack), pool, std::move(options),
                            std::move(endpoint), std::move(req));
        return;
      }

      // We access the global SCHEDULER pointer here via an atomic
      // reference. This is to silence TSAN, which often detects a data
      // race on this pointer, which is actually totally harmless.
      // What happens is that this access here occurs earlier in time
      // than the write in SchedulerFeature::unprepare, which
      // invalidates the pointer. TSAN does not see an official "happens
      // before" relation between the two threads and complains.
      // However, this is totally fine (and to some extent expected),
      // since potentially network responses might come in later than
      // the time when the scheduler has been shut down. Even in that
      // case, all is fine, since we check for nullptr below.
      // std::atomic_ref<Scheduler*>
      // schedulerRef{SchedulerFeature::SCHEDULER}; auto* sch =
      // schedulerRef.load(std::memory_order_relaxed);
      auto* sch = SchedulerFeature::gScheduler;
      if (pack->skip_scheduler || sch == nullptr) {
        std::move(pack->promise)
          .Set(network::Response{std::move(pack->dest), err, std::move(req),
                                 std::move(res)});
        return;
      }

      pack->tmp_err = err;
      pack->tmp_res = std::move(res);
      pack->tmp_req = std::move(req);

      SDB_ASSERT(pack->tmp_req != nullptr);

      sch->queue(pack->continuation_lane, [pack]() mutable {
        std::move(pack->promise)
          .Set(Response{std::move(pack->dest), pack->tmp_err,
                        std::move(pack->tmp_req), std::move(pack->tmp_res)});
      });
    });
}

}  // namespace

/// send a request to a given destination
FutureRes SendRequest(ConnectionPool* pool, DestinationId dest, RestVerb type,
                      std::string path, vpack::BufferUInt8 payload,
                      const RequestOptions& options, Headers headers) {
  SDB_DEBUG("xxxxx", Logger::COMMUNICATION, "request to '", dest, "' '",
            fuerte::ToString(type), " ", path, "'");

  // FIXME build future.reset(..)
  try {
    auto req = PrepareRequest(pool, type, std::move(path), std::move(payload),
                              options, std::move(headers));
    req->timeout(
      std::chrono::duration_cast<std::chrono::milliseconds>(options.timeout));

    if (!pool || !pool->config().cluster_info) {
      SDB_ERROR("xxxxx", Logger::COMMUNICATION, "connection pool unavailable");
      co_return Response{std::move(dest), Error::ConnectionCanceled,
                         std::move(req), nullptr};
    }

    // resolve destination.
    // it is at least temporarily possible for the destination to be an empty
    // string. this can happen if a server is in shutdown and has unregistered
    // itself from the cluster. if this happens, the dest value can be empty,
    // and trying to resolve it via resolveDestination() will produce error
    // message 77a84. we are trying to suppress this error message here, and
    // simply return "backend unavailable" (as we did before) and do not log the
    // error.
    sdb::network::EndpointSpec spec;
    ErrorCode res = ERROR_CLUSTER_BACKEND_UNAVAILABLE;

    yaclib::Future<ErrorCode> future_res;
    if (dest.starts_with("tcp://") || dest.starts_with("ssl://")) {
      spec.endpoint = dest;
      res = ERROR_OK;
    } else if (dest.starts_with("http+tcp://") ||
               dest.starts_with("http+ssl://")) {
      spec.endpoint = dest.substr(5);
      res = ERROR_OK;
    } else {
#ifdef SDB_CLUSTER
      if (options.override_destination.empty()) {
        if (!dest.empty()) {
          res = co_await ResolveDestination(*pool->config().cluster_info, dest,
                                            spec);
        }
      } else {
        res = co_await ResolveDestination(
          *pool->config().cluster_info,
          "server:" + options.override_destination, spec);
      }
#else
      SDB_VERIFY(false, ERROR_INTERNAL);
#endif
    }

    if (res != ERROR_OK) {
      // We fake a successful request with statusCode 503 and a backend not
      // available error here:
      auto resp = BuildResponse(fuerte::kStatusServiceUnavailable, Result{res});
      co_return Response{std::move(dest), Error::NoError, std::move(req),
                         std::move(resp)};
    }
    SDB_ASSERT(!spec.endpoint.empty());

    // fits in SSO of std::function
    static_assert(sizeof(std::shared_ptr<Pack>) <= 2 * sizeof(void*), "");

    auto [f, promise] = yaclib::MakeContract<network::Response>();
    auto p = std::make_shared<Pack>(
      std::move(dest), std::move(promise), options.continuation_lane,
      options.skip_scheduler, options.handle_content_encoding);
    ActuallySendRequest(std::move(p), pool, options, spec.endpoint,
                        std::move(req));
    co_return co_await std::move(f);
  } catch (const std::exception& e) {
    SDB_DEBUG("xxxxx", Logger::COMMUNICATION,
              "failed to send request: ", e.what());
  } catch (...) {
    SDB_DEBUG("xxxxx", Logger::COMMUNICATION, "failed to send request.");
  }
  co_return Response{std::string(), Error::ConnectionCanceled, nullptr,
                     nullptr};
}

FutureRes SendRequest(ConnectionPool& pool, DestinationId destination,
                      fuerte::RestVerb type, std::string path,
                      std::span<const uint8_t> payload,
                      const RequestOptions& options, Headers headers) {
  // TODO(gnusi): use gRPC
  vpack::BufferUInt8 buffer;
  buffer.append(payload.data(), payload.size());

  return SendRequest(&pool, std::move(destination), type, std::move(path),
                     std::move(buffer), options, std::move(headers));
}

/// Stateful handler class with enough information to keep retrying
/// a request until an overall timeout is hit (or the request succeeds)
class RequestsState final : public std::enable_shared_from_this<RequestsState>,
                            public RetryableRequest {
 public:
  RequestsState(yaclib::Promise<network::Response>&& promise,
                ConnectionPool* pool, DestinationId&& destination,
                RestVerb type, std::string&& path, vpack::BufferUInt8&& payload,
                Headers&& headers, const RequestOptions& options)
    : _destination(std::move(destination)),
      _options(options),
      _pool(pool),
      _promise(std::move(promise)),
      _start_time(std::chrono::steady_clock::now()),
      _end_time(_start_time +
                std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                  options.timeout)) {
    _tmp_req = PrepareRequest(pool, type, std::move(path), std::move(payload),
                              _options, std::move(headers));

    SDB_ASSERT(_pool != nullptr);
    SDB_ASSERT(_pool->config().cluster_info != nullptr);
  }

  ~RequestsState() final {
    if (_promise.Valid()) {
      std::move(_promise).Set(std::string(), Error::ConnectionCanceled, nullptr,
                              nullptr);
    }
  }

  // schedule requests that are due
  void StartRequest() {
    SDB_ASSERT(_tmp_req != nullptr);
    // the following assertions hold true because we are already checking
    // these values in the constructor. furthermore, all users or RequestsState
    // make sure that _pool and _pool->config().clusterInfo are always
    // non-nullptrs.
    SDB_ASSERT(_pool != nullptr);
    SDB_ASSERT(_pool->config().cluster_info != nullptr);

    auto now = std::chrono::steady_clock::now();
    if (now > _end_time) {
      _tmp_err = Error::RequestTimeout;
      _tmp_res = nullptr;
      ResolvePromise();
      return;  // we are done
    }
    if (SerenedServer::Instance().isStopping()) {
      _tmp_err = Error::NoError;
      _tmp_res = BuildResponse(fuerte::kStatusServiceUnavailable,
                               Result{ERROR_SHUTTING_DOWN});
      ResolvePromise();
      return;  // we are done
    }

    yaclib::Future<ErrorCode> future_res;
    if (_destination.starts_with("tcp://") ||
        _destination.starts_with("ssl://")) {
      _spec.endpoint = _destination;
      future_res = yaclib::MakeFuture<ErrorCode>(ERROR_OK);
    } else if (_destination.starts_with("http+tcp://") ||
               _destination.starts_with("http+ssl://")) {
      _spec.endpoint = _destination.substr(5);
      future_res = yaclib::MakeFuture<ErrorCode>(ERROR_OK);
    } else {
#ifdef SDB_CLUSTER
      future_res = _options.override_destination.empty()
                     ? ResolveDestination(*_pool->config().cluster_info,
                                          _destination, _spec)
                     : ResolveDestination(
                         *_pool->config().cluster_info,
                         "server:" + _options.override_destination, _spec);
#else
      SDB_VERIFY(false, ERROR_INTERNAL);
#endif
    }

    std::move(future_res)
      .DetachInline([this, self = shared_from_this(),
                     now](yaclib::Result<ErrorCode> try_res) {
        auto res = basics::SafeCall([&] {
                     return std::move(try_res).Ok();
                   }).errorNumber();

        if (res != ERROR_OK) {  // ClusterInfo did not work
          // We fake a successful request with statusCode 503 and a backend not
          // available error here:
          _tmp_err = Error::NoError;
          _tmp_res =
            BuildResponse(fuerte::kStatusServiceUnavailable, Result{res});
          ResolvePromise();
          return;
        }

        // simon: shorten actual request timeouts to allow time for retry
        //        otherwise resilience_failover tests likely fail
        auto t = _end_time - now;
        if (t >= std::chrono::duration<double>(100)) {
          t -= std::chrono::seconds(30);
        }
        SDB_ASSERT(t.count() > 0);
        _tmp_req->timeout(
          std::chrono::duration_cast<std::chrono::milliseconds>(t));

        NetworkFeature& nf =
          SerenedServer::Instance().getFeature<NetworkFeature>();
        nf.sendRequest(
          *_pool, _options, _spec.endpoint, std::move(_tmp_req),
          [self = shared_from_this()](
            fuerte::Error err, std::unique_ptr<fuerte::Request> req,
            std::unique_ptr<fuerte::Response> res, bool is_from_pool) {
            self->_tmp_err = err;
            self->_tmp_req = std::move(req);
            self->_tmp_res = std::move(res);
            self->HandleResponse(is_from_pool);
          });
      });
  }

 private:
  void HandleResponse(bool is_from_pool) {
    if (is_from_pool && (_tmp_err == fuerte::Error::ConnectionClosed ||
                         _tmp_err == fuerte::Error::WriteError)) {
      // If this connection comes from the pool and we immediately
      // get a connection closed, then we do want to retry. Therefore,
      // we fake the error code here and pretend that it was connection
      // refused. This will lead further down in the switch to a retry,
      // as opposed to a "ConnectionClosed", which must not be retried.
      _tmp_err = fuerte::Error::CouldNotConnect;
    }
    switch (_tmp_err) {
      case fuerte::Error::NoError: {
        SDB_ASSERT(_tmp_res != nullptr);
        if (CheckResponseContent()) {
          break;
        }
        [[fallthrough]];  // retry case
      }

      case fuerte::Error::ConnectionCanceled:
        // One would think that one must not retry a cancelled connection.
        // However, in case a dbserver fails and a failover happens,
        // then we artificially break all connections to it. In that case
        // we need a retry to continue the operation with the new leader.
        // This is not without problems: It is now possible that a request
        // is retried which has actually already happened. This can lead
        // to wrong replies to the customer, but there is nothing we seem
        // to be able to do against this without larger changes.
      case fuerte::Error::CouldNotConnect: {
        // Note that this case includes the refusal of a leader to accept
        // the operation, in which case we have to retry and wait for
        // a potential failover to happen.

        const auto now = std::chrono::steady_clock::now();
        auto try_again_after = now - _start_time;
        if (try_again_after < std::chrono::milliseconds(200)) {
          try_again_after = std::chrono::milliseconds(200);
        } else if (try_again_after > std::chrono::seconds(3)) {
          try_again_after = std::chrono::seconds(3);
        }

        // Now check if the request was directed to an explicit server and see
        // if that server is failed, if so, we should no longer retry,
        // regardless of the timeout:
        bool found = false;

#ifdef SDB_CLUSTER
        if (_destination.size() > 7 && _destination.starts_with("server:")) {
          auto failed_servers =
            _pool->config().cluster_info->getFailedServers();
          auto server_id = std::string_view{_destination}.substr(7);
          if (failed_servers.contains(server_id)) {
            found = true;
            SDB_DEBUG("xxxxx", Logger::COMMUNICATION, "Found destination ",
                      _destination,
                      " to be in failed servers list, will no longer retry, "
                      "aborting operation");
          }
        }
#endif

        if (found || (now + try_again_after) >= _end_time) {  // cancel out
          ResolvePromise();
        } else {
          RetryLater(try_again_after);
        }
        break;
      }

      case fuerte::Error::ConnectionClosed:
      case fuerte::Error::RequestTimeout:
      // In these cases we have to report an error, since we cannot know
      // if the request actually went out and was received and executed
      // on the other side.
      default:  // a "proper error" which has to be returned to the client
        ResolvePromise();
        break;
    }
  }

  bool CheckResponseContent() {
    SDB_ASSERT(_tmp_res != nullptr);
    switch (_tmp_res->statusCode()) {
      case fuerte::kStatusOk:
      case fuerte::kStatusCreated:
      case fuerte::kStatusAccepted:
      case fuerte::kStatusNoContent:
        _tmp_err = Error::NoError;
        ResolvePromise();
        return true;  // done

      case fuerte::kStatusMisdirectedRequest:
        // This is an expected leader refusing to execute an operation
        // (which could consider itself a follower in the meantime).
        // We need to retry to eventually wait for a failover and for us
        // recognizing the new leader.
      case fuerte::kStatusServiceUnavailable:
        return false;  // goto retry

      case fuerte::kStatusNotFound:
        if (_options.retry_not_found &&
            ERROR_SERVER_DATA_SOURCE_NOT_FOUND ==
              network::ErrorCodeFromBody(_tmp_res->slice())) {
          return false;  // goto retry
        }
        [[fallthrough]];
      case fuerte::kStatusNotAcceptable:
        // This is, for example, a follower refusing to do the bidding
        // of a leader. Or, it could be a leader refusing a to do a
        // replication. In both cases, we must not retry because we must
        // drop the follower.
      default:  // a "proper error" which has to be returned to the client
        _tmp_err = Error::NoError;
        ResolvePromise();
        return true;  // done
    }
  }

  /// schedule calling the response promise
  void ResolvePromise() {
    SDB_ASSERT(_tmp_req != nullptr);
    SDB_ASSERT(_tmp_res != nullptr || _tmp_err != Error::NoError);
    SDB_DEBUG_IF("xxxxx", Logger::COMMUNICATION,
                 _tmp_err != fuerte::Error::NoError, "error on request to '",
                 _destination, "' '", fuerte::ToString(_tmp_req->type()), " ",
                 _tmp_req->header.path, "' '", fuerte::ToString(_tmp_err), "'");

    Scheduler* sch = SchedulerFeature::gScheduler;
    if (_options.skip_scheduler || sch == nullptr) {
      std::move(_promise).Set(std::move(_destination), _tmp_err,
                              std::move(_tmp_req), std::move(_tmp_res));
      return;
    }

    sch->queue(_options.continuation_lane,
               [self = shared_from_this()]() mutable {
                 std::move(self->_promise)
                   .Set(std::move(self->_destination), self->_tmp_err,
                        std::move(self->_tmp_req), std::move(self->_tmp_res));
               });
  }

  void RetryLater(std::chrono::steady_clock::duration try_again_after) {
    SDB_ASSERT(_tmp_req != nullptr);
    SDB_DEBUG("xxxxx", Logger::COMMUNICATION, "retry request to '",
              _destination, "' '", fuerte::ToString(_tmp_req->type()), " ",
              _tmp_req->header.path, "'");

    auto* sch = SchedulerFeature::gScheduler;
    if (sch == nullptr) [[unlikely]] {
      std::move(_promise).Set(Response{std::move(_destination),
                                       fuerte::Error::ConnectionCanceled,
                                       nullptr, nullptr});
      return;
    }

    SDB_ASSERT(_pool != nullptr);
    SDB_ASSERT(_pool->config().cluster_info != nullptr);

    NetworkFeature& nf = SerenedServer::Instance().getFeature<NetworkFeature>();
    nf.retryRequest(shared_from_this(), _options.continuation_lane,
                    try_again_after);
  }

 public:
  bool IsDone() const override { return !_promise.Valid(); }
  void retry() override { StartRequest(); }
  void cancel() override {
    std::move(_promise).Set(std::move(_destination), Error::ConnectionCanceled,
                            nullptr, nullptr);
  }

 private:
  DestinationId _destination;
  const RequestOptions _options;
  ConnectionPool* _pool;

  std::unique_ptr<fuerte::Request> _tmp_req;
  std::unique_ptr<fuerte::Response> _tmp_res;  /// temporary response

  yaclib::Promise<network::Response> _promise;  /// promise called

  const std::chrono::steady_clock::time_point _start_time;
  const std::chrono::steady_clock::time_point _end_time;
  network::EndpointSpec _spec;
  fuerte::Error _tmp_err;
};

/// send a request to a given destination, retry until timeout is
/// exceeded
FutureRes SendRequestRetry(ConnectionPool* pool, DestinationId destination,
                           sdb::fuerte::RestVerb type, std::string path,
                           vpack::BufferUInt8 payload,
                           const RequestOptions& options, Headers headers) {
  try {
    if (!pool || !pool->config().cluster_info) {
      SDB_ERROR("xxxxx", Logger::COMMUNICATION, "connection pool unavailable");
      return yaclib::MakeFuture<Response>(
        std::move(destination), Error::ConnectionCanceled, nullptr, nullptr);
    }

    SDB_DEBUG("xxxxx", Logger::COMMUNICATION, "request to '", destination,
              "' '", fuerte::ToString(type), " ", path, "'");

    auto [f, p] = yaclib::MakeContract<Response>();
    auto rs = std::make_shared<RequestsState>(
      std::move(p), pool, std::string{destination}, type, std::move(path),
      std::move(payload), std::move(headers), options);
    rs->StartRequest();  // will auto reference itself
    return std::move(f);

  } catch (const std::exception& e) {
    SDB_DEBUG("xxxxx", Logger::COMMUNICATION,
              "failed to send request: ", e.what());
  } catch (...) {
    SDB_DEBUG("xxxxx", Logger::COMMUNICATION, "failed to send request.");
  }

  return yaclib::MakeFuture<Response>(
    std::move(destination), Error::ConnectionCanceled, nullptr, nullptr);
}

}  // namespace network
}  // namespace sdb
