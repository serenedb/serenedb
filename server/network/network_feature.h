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

#include <fuerte/requests.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "general_server/scheduler.h"
#include "metrics/fwd.h"
#include "network/connection_pool.h"
#include "rest_server/serened.h"

namespace sdb {

class Thread;

namespace network {

struct RequestOptions;

struct RetryableRequest {
  virtual ~RetryableRequest() = default;
  virtual bool IsDone() const = 0;
  virtual void retry() = 0;
  virtual void cancel() = 0;
};

}  // namespace network

class NetworkFeature final : public SerenedFeature {
 public:
  using RequestCallback = std::function<void(
    fuerte::Error err, std::unique_ptr<fuerte::Request> req,
    std::unique_ptr<fuerte::Response> res, bool is_from_pool)>;

  static constexpr std::string_view name() noexcept { return "Network"; }

  NetworkFeature(Server& server, metrics::MetricsFeature& metrics,
                 network::ConnectionPool::Config);
  ~NetworkFeature() final;

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void validateOptions(std::shared_ptr<options::ProgramOptions>) final;
  void prepare() final;
  void start() final;
  void beginShutdown() final;
  void stop() final;
  void unprepare() final;

  bool prepared() const noexcept;

  /// global connection pool
  network::ConnectionPool* pool() const noexcept;

#ifdef SDB_GTEST
  void setPoolTesting(network::ConnectionPool* pool);
#endif

  /// increase the counter for forwarded requests
  void trackForwardedRequest() noexcept;

  size_t requestsInFlight() const noexcept;

  bool isCongested() const noexcept;  // in-flight above low-water mark
  bool isSaturated() const noexcept;  // in-flight above high-water mark
  void sendRequest(network::ConnectionPool& pool,
                   const network::RequestOptions& options,
                   const std::string& endpoint,
                   std::unique_ptr<fuerte::Request>&& req,
                   RequestCallback&& cb);

  void retryRequest(std::shared_ptr<network::RetryableRequest>, RequestLane,
                    std::chrono::steady_clock::duration);

  static uint64_t defaultIOThreads();

 protected:
  void prepareRequest(const network::ConnectionPool& pool,
                      std::unique_ptr<fuerte::Request>& req);
  void finishRequest(const network::ConnectionPool& pool, fuerte::Error err,
                     const std::unique_ptr<fuerte::Request>& req,
                     std::unique_ptr<fuerte::Response>& res);

 private:
  void cancelGarbageCollection() noexcept;
  void injectAcceptEncodingHeader(fuerte::Request& req) const;
  bool compressRequestBody(const network::RequestOptions& opts,
                           fuerte::Request& req) const;

  // configuration
  uint64_t _max_open_connections;
  uint64_t _idle_ttl_milli;
  uint32_t _num_io_threads;
  bool _verify_hosts;

  std::atomic<bool> _prepared;

  /// where rhythm is life, and life is rhythm :)
  std::function<void(bool)> _gcfunc;

  std::unique_ptr<network::ConnectionPool> _pool;
  std::atomic<network::ConnectionPool*> _pool_ptr;

  // protects _work_item and _retry_requests
  absl::Mutex _work_item_mutex;
  Scheduler::WorkHandle _work_item;

  std::unique_ptr<Thread> _retry_thread;

  /// number of cluster-internal forwarded requests
  /// (from one coordinator to another, in case load-balancing
  /// is used)
  metrics::Counter& _forwarded_requests;

  uint64_t _max_in_flight;
  metrics::Gauge<uint64_t>& _requests_in_flight;

  metrics::Counter& _request_timeouts;
  metrics::Histogram<metrics::FixScale<double>>& _request_durations;

  metrics::Counter& _unfinished_sends;
  metrics::Histogram<metrics::FixScale<double>>& _dequeue_durations;
  metrics::Histogram<metrics::FixScale<double>>& _send_durations;
  metrics::Histogram<metrics::FixScale<double>>& _response_durations;

  uint64_t _compress_request_threshold;

  enum class CompressionType {
    None,
    Deflate,
    GZip,
    Lz4,
    Auto,
  };
  CompressionType _compression_type;
  std::string _compression_type_label;
};

}  // namespace sdb
