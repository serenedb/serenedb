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

#include "network_feature.h"

#include <absl/cleanup/cleanup.h>
#include <fuerte/connection.h>

#include <bit>
#include <chrono>
#include <queue>
#include <vector>

#include "app/app_server.h"
#include "app/options/parameters.h"
#include "app/options/program_options.h"
#include "app/options/section.h"
#include "basics/application-exit.h"
#include "basics/debugging.h"
#include "basics/encoding_utils.h"
#include "basics/logger/logger.h"
#include "basics/number_of_cores.h"
#include "basics/thread.h"
#include "general_server/general_server_feature.h"
#include "general_server/scheduler_feature.h"
#include "metrics/counter_builder.h"
#include "metrics/fix_scale.h"
#include "metrics/gauge_builder.h"
#include "metrics/histogram_builder.h"
#include "metrics/metrics_feature.h"
#include "network/methods.h"

using namespace sdb;
using namespace sdb::basics;
using namespace sdb::options;

namespace {

// executes network request retry operations in a separate thread,
// so that they do not have be executed via the scheduler.
// the reason to execute them from a dedicated thread is that a
// dedicated thread will always have capacity to execute, whereas
// pushing retry operations to the scheduler needs to use the correct
// priority lanes and also could be blocked by scheduler threads
// not pulling any more new tasks due to overload/overwhelm.
class RetryThread : public ServerThread<SerenedServer> {
  static constexpr auto kDefaultSleepTime = std::chrono::seconds(10);

 public:
  explicit RetryThread(SerenedServer& server)
    : ServerThread<SerenedServer>(server, "NetworkRetry"),
      _next_retry_time(std::chrono::steady_clock::now() + kDefaultSleepTime) {}

  ~RetryThread() override {
    shutdown();
    CancelAll();
  }

  void beginShutdown() override {
    Thread::beginShutdown();
    absl::MutexLock guard{&_mutex};
    _cv.notify_one();
  }

  void CancelAll() noexcept {
    absl::MutexLock guard{&_mutex};

    // pop everything from the queue until it is empty.
    while (!_retry_requests.empty()) {
      auto& top = _retry_requests.top();
      auto req = std::move(top.req);
      SDB_ASSERT(req);
      _retry_requests.pop();
      try {
        // canceling a request can throw in case a concurrent
        // thread has already resolved or canceled the request.
        // in this case, simply ignore the exception.
        req->cancel();
      } catch (...) {
        // does not matter
      }
    }
  }

  void Push(std::shared_ptr<network::RetryableRequest> req,
            std::chrono::steady_clock::time_point retry_time) {
    SDB_ASSERT(req);

    absl::Cleanup cancel_guard = [req]() noexcept {
      try {
        // canceling a request can throw in case a concurrent
        // thread has already resolved or canceled the request.
        // in this case, simply ignore the exception.
        req->cancel();
      } catch (...) {
        // does not matter
      }
    };

    SDB_IF_FAILURE("NetworkFeature::retryRequestFail") {
      SDB_THROW(ERROR_DEBUG);
    }

    absl::ReleasableMutexLock guard{&_mutex};

    if (isStopping()) {
      return;
    }

    bool must_notify = (retry_time < _next_retry_time);
    if (must_notify) {
      _next_retry_time = retry_time;
    }

    _retry_requests.push(RetryItem{retry_time, std::move(req)});
    guard.Release();

    std::move(cancel_guard).Cancel();

    if (!must_notify) {
      // retry time is already in the past?
      must_notify = retry_time <= std::chrono::steady_clock::now();
    }

    // notify retry thread about the new item
    if (must_notify) {
      _cv.notify_one();
    }
  }

 protected:
  void run() override {
    while (!isStopping()) {
      try {
        std::unique_lock guard{_mutex};

        // by default, sleep an arbitrary amount of 10 seconds.
        // note that this value may be reduced if we have an
        // element in the queue that is due earlier.
        _next_retry_time = std::chrono::steady_clock::now() + kDefaultSleepTime;

        while (!_retry_requests.empty()) {
          auto& top = _retry_requests.top();
          auto now = std::chrono::steady_clock::now();
          if (top.retry_time > now) {
            // next retry operation is in the future...
            _next_retry_time = top.retry_time;
            break;
          }

          auto req = std::move(top.req);
          SDB_ASSERT(req);
          _retry_requests.pop();

          if (!_retry_requests.empty()) {
            _next_retry_time = _retry_requests.top().retry_time;
          } else {
            _next_retry_time = now + kDefaultSleepTime;
          }

          if (isStopping()) {
            try {
              req->cancel();
            } catch (...) {
              // ignore error during cancelation
            }
            break;
          }

          guard.unlock();

          try {
            // the actual retry action is carried out here.
            // note: there is a small opportunity of a race here, if
            // a concurrent request sets resolves the request's
            // promise. this will lead to an exception here, which
            // we can ignore.
            if (!req->IsDone()) {
              req->retry();
            }
          } catch (const std::exception& ex) {
            SDB_WARN("xxxxx", Logger::COMMUNICATION,
                     "network retry thread caught exception while "
                     "retrying/canceling request: ",
                     ex.what());
          }

          guard.lock();
        }

        // nothing (more) to do
        SDB_ASSERT(guard.owns_lock());

        if (!isStopping()) {
          _cv.WaitWithTimeout(
            &_mutex, absl::FromChrono(_next_retry_time -
                                      std::chrono::steady_clock::now()));
        }
      } catch (const std::exception& ex) {
        SDB_WARN("xxxxx", Logger::COMMUNICATION,
                 "network retry thread caught exception: ", ex.what());
      }
    }

    // cancel all outstanding requests
    CancelAll();
  }

 private:
  struct RetryItem {
    std::chrono::steady_clock::time_point retry_time;
    std::shared_ptr<network::RetryableRequest> req;
  };

  // comparator for _retry_requests priority queue:
  // the item with the lowest retryTime timestamp sits at the top and
  // will be pulled from the queue first
  struct RetryItemComparator {
    bool operator()(const RetryItem& lhs, const RetryItem& rhs) const noexcept {
      if (lhs.retry_time > rhs.retry_time) {
        // priority_queue: elements with higher times need to return true for
        // the comparator.
        return true;
      }
      if (lhs.retry_time == rhs.retry_time) {
        // equal retry time. use pointer values to define order.
        return lhs.req.get() < rhs.req.get();
      }
      return false;
    }
  };

  absl::Mutex _mutex;
  absl::CondVar _cv;
  std::priority_queue<RetryItem, std::vector<RetryItem>, RetryItemComparator>
    _retry_requests;
  std::chrono::steady_clock::time_point _next_retry_time{};
};

void QueueGarbageCollection(absl::Mutex& mutex,
                            sdb::Scheduler::WorkHandle& work_item,
                            std::function<void(bool)>& gcfunc,
                            std::chrono::seconds offset) {
  std::lock_guard guard(mutex);
  work_item = sdb::SchedulerFeature::gScheduler->queueDelayed(
    "networkfeature-gc", sdb::RequestLane::InternalLow, offset, gcfunc);
}

constexpr double kCongestionRatio = 0.5;
constexpr uint64_t kMaxAllowedInFlight = 65536;
constexpr uint64_t kMinAllowedInFlight = 64;

}  // namespace
namespace sdb {

struct NetworkFeatureScale {
  static metrics::FixScale<double> scale() {
    return {0.0, 100.0, {1.0, 5.0, 15.0, 50.0}};
  }
};

struct NetworkFeatureSendScaleSmall {
  static metrics::FixScale<double> scale() {
    return {0.0, 10.0, {0.000001, 0.00001, 0.0001, 0.001, 0.01, 0.1, 1.0}};
  }
};

struct NetworkFeatureSendScaleLarge {
  static metrics::FixScale<double> scale() {
    return {0.0, 10000.0, {0.01, 0.1, 1.0, 10.0, 100.0, 1000.0}};
  }
};

DECLARE_COUNTER(serenedb_network_forwarded_requests_total,
                "Number of requests forwarded to another coordinator");
DECLARE_COUNTER(serenedb_network_request_timeouts_total,
                "Number of internal requests that have timed out");
DECLARE_HISTOGRAM(
  serenedb_network_request_duration_as_percentage_of_timeout,
  NetworkFeatureScale,
  "Internal request round-trip time as a percentage of timeout [%]");
DECLARE_COUNTER(serenedb_network_unfinished_sends_total,
                "Number of times the sending of a request remained unfinished");
DECLARE_HISTOGRAM(
  serenedb_network_dequeue_duration, NetworkFeatureSendScaleSmall,
  "Time to dequeue a queued network request in fuerte in seconds");
DECLARE_HISTOGRAM(serenedb_network_send_duration, NetworkFeatureSendScaleLarge,
                  "Time to send out internal requests in seconds");
DECLARE_HISTOGRAM(
  serenedb_network_response_duration, NetworkFeatureSendScaleLarge,
  "Time to wait for network response after it was sent out in seconds");
DECLARE_GAUGE(serenedb_network_requests_in_flight, uint64_t,
              "Number of outgoing internal requests in flight");

NetworkFeature::NetworkFeature(Server& server, metrics::MetricsFeature& metrics,
                               network::ConnectionPool::Config config)
  : SerenedFeature{server, name()},
    _max_open_connections(config.max_open_connections),
    _idle_ttl_milli(config.idle_connection_milli),
    _num_io_threads(defaultIOThreads()),
    _verify_hosts(config.verify_hosts),
    _prepared(false),
    _forwarded_requests(
      metrics.add(serenedb_network_forwarded_requests_total{})),
    _max_in_flight(::kMaxAllowedInFlight),
    _requests_in_flight(metrics.add(serenedb_network_requests_in_flight{})),
    _request_timeouts(metrics.add(serenedb_network_request_timeouts_total{})),
    _request_durations(metrics.add(
      serenedb_network_request_duration_as_percentage_of_timeout{})),
    _unfinished_sends(metrics.add(serenedb_network_unfinished_sends_total{})),
    _dequeue_durations(metrics.add(serenedb_network_dequeue_duration{})),
    _send_durations(metrics.add(serenedb_network_send_duration{})),
    _response_durations(metrics.add(serenedb_network_response_duration{})),
    _compress_request_threshold(200),
    // note: we cannot use any compression method by default here for the
    // 3.12 release because that could cause upgrades from 3.11 to 3.12
    // to break. for example, if we enable compression here and during the
    // upgrade the 3.12 servers could pick it up and send compressed
    // requests to 3.11 servers which cannot handle them.
    // we should set the compression type "auto" for future releases though
    // to save some traffic.
    _compression_type(CompressionType::None),
    _compression_type_label("none") {
  setOptional(true);
}

NetworkFeature::~NetworkFeature() {
  if (_pool) {
    _pool->stop();
  }
}

void NetworkFeature::collectOptions(
  std::shared_ptr<options::ProgramOptions> options) {
  options->addSection("network", "cluster-internal networking");

  options->addOption(
    "--network.io-threads",
    "The number of network I/O threads for cluster-internal "
    "communication.",
    new UInt32Parameter(&_num_io_threads, /*base*/ 1, /*minValue*/ 1));
  options->addOption(
    "--network.max-open-connections",
    "The maximum number of open TCP connections for "
    "cluster-internal communication per endpoint",
    new UInt64Parameter(&_max_open_connections, /*base*/ 1, /*minValue*/ 8));
  options->addOption("--network.idle-connection-ttl",
                     "The default time-to-live of idle connections for "
                     "cluster-internal communication (in milliseconds).",
                     new UInt64Parameter(&_idle_ttl_milli));
  options->addOption(
    "--network.verify-hosts",
    "Verify peer certificates when using TLS in cluster-internal "
    "communication.",
    new BooleanParameter(&_verify_hosts));

  options->addOption("--network.max-requests-in-flight",
                     "The number of internal requests that can be in "
                     "flight at a given point in time.",
                     new options::UInt64Parameter(&_max_in_flight),
                     options::MakeDefaultFlags(options::Flags::Uncommon));

  options
    ->addOption(
      "--network.compress-request-threshold",
      "The HTTP request body size from which on cluster-internal "
      "requests are transparently compressed.",
      new UInt64Parameter(&_compress_request_threshold),
      sdb::options::MakeFlags(sdb::options::Flags::DefaultNoComponents,
                              sdb::options::Flags::OnDBServer,
                              sdb::options::Flags::OnCoordinator))

    .setLongDescription(
      R"(Automatically compress outgoing HTTP requests in cluster-internal
traffic with the deflate, gzip or lz4 compression format.
Compression will only happen if the size of the uncompressed request body exceeds
the threshold value controlled by this startup option,
and if the request body size after compression is less than the original
request body size.
Using the value 0 disables the automatic compression.")");

  options
    ->addOption(
      "--network.compression-method",
      "The compression method used for cluster-internal requests.",
      new DiscreteValuesParameter<StringParameter>(
        &_compression_type_label,
        {
          StaticStrings::kEncodingGzip,
          StaticStrings::kEncodingDeflate,
          StaticStrings::kEncodingLz4,
          "auto",
          "none",
        }),
      sdb::options::MakeFlags(sdb::options::Flags::DefaultNoComponents,
                              sdb::options::Flags::OnDBServer,
                              sdb::options::Flags::OnCoordinator))

    .setLongDescription(
      R"(Setting this option to 'none' will disable compression for
cluster-internal requests.
To enable compression for cluster-internal requests, set this option to either
'deflate', 'gzip', 'lz4' or 'auto'.
The 'deflate' and 'gzip' compression methods are general purpose,
but have significant CPU overhead for performing the compression work.
The 'lz4' compression method compresses slightly worse, but has a lot lower
CPU overhead for performing the compression.
The 'auto' compression method will use 'deflate' by default, and 'lz4' for
requests which have a size that is at least 3 times the configured threshold
size.
The compression method only matters if `--network.compress-request-threshold`
is set to value greater than zero. If the threshold is set to value of 0,
then no compression will be performed.)");
}

void NetworkFeature::validateOptions(
  std::shared_ptr<options::ProgramOptions> opts) {
  if (!opts->processingResult().touched("--network.idle-connection-ttl")) {
    auto& gs = server().getFeature<GeneralServerFeature>();
    _idle_ttl_milli = uint64_t(gs.keepAliveTimeout() * 1000 / 2);
  }
  if (_idle_ttl_milli < 10000) {
    _idle_ttl_milli = 10000;
  }

  uint64_t clamped =
    std::clamp(_max_in_flight, ::kMinAllowedInFlight, ::kMaxAllowedInFlight);
  if (clamped != _max_in_flight) {
    SDB_WARN("xxxxx", Logger::CONFIG,
             "Must set --network.max-requests-in-flight between ",
             ::kMinAllowedInFlight, " and ", ::kMaxAllowedInFlight,
             ", clamping value");
    _max_in_flight = clamped;
  }

  if (_compression_type_label == StaticStrings::kEncodingGzip) {
    _compression_type = CompressionType::GZip;
  } else if (_compression_type_label == StaticStrings::kEncodingDeflate) {
    _compression_type = CompressionType::Deflate;
  } else if (_compression_type_label == StaticStrings::kEncodingLz4) {
    _compression_type = CompressionType::Lz4;
  } else if (_compression_type_label == "auto") {
    _compression_type = CompressionType::Auto;
  } else if (_compression_type_label == "none") {
    _compression_type = CompressionType::None;
  } else {
    SDB_FATAL("xxxxx", Logger::CONFIG,
              "invalid value for `--network.compression-method` ('",
              _compression_type_label, "')");
  }
}

void NetworkFeature::prepare() {
  ClusterInfo* ci = nullptr;

  network::ConnectionPool::Config config(
    server().getFeature<metrics::MetricsFeature>());
  config.num_io_threads = static_cast<unsigned>(_num_io_threads);
  config.max_open_connections = _max_open_connections;
  config.idle_connection_milli = _idle_ttl_milli;
  config.verify_hosts = _verify_hosts;
  config.cluster_info = ci;
  config.name = "ClusterComm";

  _pool = std::make_unique<network::ConnectionPool>(config);
  _pool_ptr.store(_pool.get(), std::memory_order_relaxed);

  _gcfunc = [=, this](bool canceled) {
    if (canceled) {
      return;
    }

    _pool->pruneConnections();

    if (!server().isStopping() && !canceled) {
      std::chrono::seconds off(12);
      ::QueueGarbageCollection(_work_item_mutex, _work_item, _gcfunc, off);
    }
  };

  _prepared = true;
}

void NetworkFeature::start() {
  _retry_thread = std::make_unique<RetryThread>(server());
  if (!_retry_thread->start()) {
    SDB_FATAL("xxxxx", Logger::COMMUNICATION,
              "unable to start network request retry thread");
  }

  Scheduler* scheduler = SchedulerFeature::gScheduler;
  if (scheduler != nullptr) {  // is nullptr in unit tests
    auto off = std::chrono::seconds(1);
    ::QueueGarbageCollection(_work_item_mutex, _work_item, _gcfunc, off);
  }
}

void NetworkFeature::beginShutdown() {
  cancelGarbageCollection();
  if (_retry_thread) {
    _retry_thread->beginShutdown();
  }
  _pool_ptr.store(nullptr, std::memory_order_relaxed);
  if (_pool) {  // first cancel all connections
    _pool->shutdownConnections();
  }
}

void NetworkFeature::stop() {
  cancelGarbageCollection();
  if (_pool) {
    _pool->shutdownConnections();
    _pool->drainConnections();
    _pool->stop();
  }
  _retry_thread.reset();
}

void NetworkFeature::unprepare() { cancelGarbageCollection(); }

void NetworkFeature::cancelGarbageCollection() noexcept try {
  std::lock_guard guard(_work_item_mutex);
  _work_item.reset();
} catch (const std::exception& ex) {
  SDB_WARN("xxxxx", Logger::COMMUNICATION,
           "caught exception while canceling retry requests: ", ex.what());
}

network::ConnectionPool* NetworkFeature::pool() const noexcept {
  return _pool_ptr.load(std::memory_order_relaxed);
}

#ifdef SDB_GTEST
void NetworkFeature::setPoolTesting(network::ConnectionPool* pool) {
  _pool_ptr.store(pool, std::memory_order_release);
}
#endif

bool NetworkFeature::prepared() const noexcept { return _prepared; }

void NetworkFeature::trackForwardedRequest() noexcept { ++_forwarded_requests; }

size_t NetworkFeature::requestsInFlight() const noexcept {
  return _requests_in_flight.load();
}

bool NetworkFeature::isCongested() const noexcept {
  return _requests_in_flight.load() >= (_max_in_flight * ::kCongestionRatio);
}

bool NetworkFeature::isSaturated() const noexcept {
  return _requests_in_flight.load() >= _max_in_flight;
}

uint64_t NetworkFeature::defaultIOThreads() {
  return std::max(uint64_t(1), uint64_t(number_of_cores::GetValue() / 4));
}

void NetworkFeature::sendRequest(network::ConnectionPool& pool,
                                 const network::RequestOptions& options,
                                 const std::string& endpoint,
                                 std::unique_ptr<fuerte::Request>&& req,
                                 RequestCallback&& cb) {
  SDB_ASSERT(req != nullptr);

  injectAcceptEncodingHeader(*req);

  bool did_compress = compressRequestBody(options, *req);

  prepareRequest(pool, req);

  bool is_from_pool = false;
  auto now = std::chrono::steady_clock::now();
  auto conn = pool.leaseConnection(endpoint, is_from_pool);
  auto dur = std::chrono::steady_clock::now() - now;
  if (dur > std::chrono::seconds(1)) {
    SDB_WARN(
      "xxxxx", Logger::COMMUNICATION, "have leased connection to '", endpoint,
      "' came from pool: ", is_from_pool, " leasing took ",
      std::chrono::duration_cast<std::chrono::duration<double>>(dur).count(),
      " seconds, url: ", ToString(req->header.rest_verb), " ", req->header.path,
      ", request ptr: ", std::bit_cast<size_t>(req.get()));
  } else {
    SDB_TRACE("xxxxx", Logger::COMMUNICATION, "have leased connection to '",
              endpoint, "' came from pool: ", is_from_pool,
              ", url: ", ToString(req->header.rest_verb), " ", req->header.path,
              ", request ptr: ", std::bit_cast<size_t>(req.get()));
  }
  conn->sendRequest(
    std::move(req),
    [this, &pool, is_from_pool,
     handle_content_encoding = options.handle_content_encoding || did_compress,
     cb = std::move(cb), endpoint = std::move(endpoint)](
      fuerte::Error err, std::unique_ptr<fuerte::Request> req,
      std::unique_ptr<fuerte::Response> res) {
      SDB_ASSERT(req != nullptr);
      if (req->timeQueued().time_since_epoch().count() != 0 &&
          req->timeAsyncWrite().time_since_epoch().count() != 0) {
        // In the 0 cases fuerte did not even accept or start to send
        // the request, so there is nothing to report.
        auto dur = std::chrono::duration_cast<std::chrono::duration<double>>(
          req->timeAsyncWrite() - req->timeQueued());
        _dequeue_durations.count(dur.count());
        if (req->timeSent().time_since_epoch().count() == 0) {
          // The request sending was never finished. This could be a timeout
          // during the sending phase.
          SDB_DEBUG(
            "xxxxx", Logger::COMMUNICATION, "Time to dequeue request to ",
            endpoint, ": ", dur.count(),
            " seconds, however, the sending has not yet finished so far",
            ", endpoint: ", endpoint,
            ", request ptr: ", std::bit_cast<size_t>(req.get()),
            ", response ptr: ", std::bit_cast<size_t>(req.get()),
            ", error: ", uint16_t(err));
          _unfinished_sends.count();
        } else {
          // The request was fully sent off, we have received the callback
          // from asio.
          dur = std::chrono::duration_cast<std::chrono::duration<double>>(
            req->timeSent() - req->timeAsyncWrite());
          _send_durations.count(dur.count());
          // If you suspect network delays in your infrastructure, you
          // can use the following log message to track them down and
          // to associate them with particular requests:
          SDB_DEBUG_IF("xxxxx", Logger::COMMUNICATION,
                       dur > std::chrono::seconds(3),
                       "Time to send request to ", endpoint, ": ", dur.count(),
                       " seconds, endpoint: ", endpoint,
                       ", request ptr: ", std::bit_cast<size_t>(req.get()),
                       ", response ptr: ", std::bit_cast<size_t>(res.get()),
                       ", error: ", uint16_t(err));

          dur = std::chrono::duration_cast<std::chrono::duration<double>>(
            std::chrono::steady_clock::now() - req->timeSent());
          // If you suspect network delays in your infrastructure, you
          // can use the following log message to track them down and
          // to associate them with particular requests:
          SDB_DEBUG_IF(
            "xxxxx", Logger::COMMUNICATION, dur > std::chrono::seconds(61),
            "Time since request was sent out to ", endpoint, " until now was ",
            dur.count(), " seconds, endpoint: ", endpoint,
            ", request ptr: ", std::bit_cast<size_t>(req.get()),
            ", response ptr: ", std::bit_cast<size_t>(res.get()),
            ", error: ", uint16_t(err));
          _response_durations.count(dur.count());
        }
      }
      finishRequest(pool, err, req, res);
      if (res != nullptr && handle_content_encoding) {
        // transparently handle decompression
        const auto& encoding =
          res->header.metaByKey(StaticStrings::kContentEncoding);
        if (encoding == StaticStrings::kEncodingGzip) {
          vpack::BufferUInt8 uncompressed;
          auto r = encoding::GZipUncompress(
            reinterpret_cast<const uint8_t*>(res->payload().data()),
            res->payload().size(), uncompressed);
          if (r != ERROR_OK) {
            SDB_THROW(r);
          }
          // replace response body and remove "content-encoding"
          // handler to prevent duplicate uncompression
          res->setPayload(std::move(uncompressed), 0);
          res->header.contentEncoding(fuerte::ContentEncoding::Identity);
          res->header.removeMeta(StaticStrings::kContentEncoding);
        } else if (encoding == StaticStrings::kEncodingDeflate) {
          vpack::BufferUInt8 uncompressed;
          auto r = encoding::ZLibInflate(
            reinterpret_cast<const uint8_t*>(res->payload().data()),
            res->payload().size(), uncompressed);
          if (r != ERROR_OK) {
            SDB_THROW(r);
          }
          // replace response body and remove "content-encoding"
          // handler to prevent duplicate uncompression
          res->setPayload(std::move(uncompressed), 0);
          res->header.contentEncoding(fuerte::ContentEncoding::Identity);
          res->header.removeMeta(StaticStrings::kContentEncoding);
        } else if (encoding == StaticStrings::kEncodingSereneLz4) {
          vpack::BufferUInt8 uncompressed;
          auto r = encoding::Lz4Uncompress(
            reinterpret_cast<const uint8_t*>(res->payload().data()),
            res->payload().size(), uncompressed);
          if (r != ERROR_OK) {
            SDB_THROW(r);
          }
          // replace response body and remove "content-encoding"
          // handler to prevent duplicate uncompression
          res->setPayload(std::move(uncompressed), 0);
          res->header.contentEncoding(fuerte::ContentEncoding::Identity);
          res->header.removeMeta(StaticStrings::kContentEncoding);
        }
      }
      cb(err, std::move(req), std::move(res), is_from_pool);
    });
}

void NetworkFeature::prepareRequest(const network::ConnectionPool& pool,
                                    std::unique_ptr<fuerte::Request>& req) {
  _requests_in_flight += 1;
  if (req) {
    req->timestamp(std::chrono::steady_clock::now());
  }
}

void NetworkFeature::finishRequest(const network::ConnectionPool& pool,
                                   fuerte::Error err,
                                   const std::unique_ptr<fuerte::Request>& req,
                                   std::unique_ptr<fuerte::Response>& res) {
  _requests_in_flight -= 1;
  if (err == fuerte::Error::RequestTimeout) {
    _request_timeouts.count();
  } else if (req && res) {
    res->timestamp(std::chrono::steady_clock::now());
    std::chrono::milliseconds duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(res->timestamp() -
                                                            req->timestamp());
    std::chrono::milliseconds timeout = req->timeout();
    SDB_ASSERT(timeout.count() > 0);
    if (timeout.count() > 0) {
      // only go in here if we are sure to not divide by zero
      double percentage =
        std::clamp(100.0 * static_cast<double>(duration.count()) /
                     static_cast<double>(timeout.count()),
                   0.0, 100.0);
      _request_durations.count(percentage);
    } else {
      // the timeout value was 0, for whatever reason. this is unexpected,
      // but we must not make the program crash here.
      // so instead log a warning and interpret this as a request that took
      // 100% of the timeout duration.
      _request_durations.count(100.0);
      SDB_WARN("xxxxx", Logger::FIXME,
               "encountered invalid 0s timeout for internal request to path ",
               req->header.path);
    }
  }
}

void NetworkFeature::retryRequest(
  std::shared_ptr<network::RetryableRequest> req, RequestLane /*lane*/,
  std::chrono::steady_clock::duration duration) {
  if (!req) {
    return;
  }

  if (server().isStopping()) {
    try {
      req->cancel();
    } catch (...) {
      // does not matter if we are stopping anyway
    }
  } else {
    SDB_ASSERT(_retry_thread);
    static_cast<RetryThread&>(*_retry_thread)
      .Push(std::move(req), std::chrono::steady_clock::now() + duration);
  }
}

void NetworkFeature::injectAcceptEncodingHeader(fuerte::Request& req) const {
  // inject "Accept-Encoding" header into the request if it was not
  // already set.
  if (req.header.meta().contains(StaticStrings::kAcceptEncoding)) {
    // header already set in original request
    return;
  }

  if (_compression_type == CompressionType::None) {
    return;
  }

  if (_compression_type == CompressionType::Deflate) {
    // if cluster-internal compression type is set to "deflate", add
    // "accept-encoding: deflate" header
    req.header.addMeta(StaticStrings::kAcceptEncoding,
                       StaticStrings::kEncodingDeflate);
  } else if (_compression_type == CompressionType::GZip) {
    // if cluster-internal compression type is set to "gzip", add
    // "accept-encoding: gzip, deflate" header. we leave "deflate" in
    // as a general fallback
    req.header.addMeta(StaticStrings::kAcceptEncoding,
                       absl::StrCat(StaticStrings::kEncodingGzip, ",",
                                    StaticStrings::kEncodingDeflate));
  } else if (_compression_type == CompressionType::Lz4 ||
             _compression_type == CompressionType::Auto) {
    // if cluster-internal compression type is set to "lz4", add
    // "accept-encoding: lz4, deflate" header. we leave "deflate" in
    // as a general fallback
    req.header.addMeta(StaticStrings::kAcceptEncoding,
                       absl::StrCat(StaticStrings::kEncodingSereneLz4, ",",
                                    StaticStrings::kEncodingDeflate));
  } else {
    SDB_ASSERT(false);
  }
}

bool NetworkFeature::compressRequestBody(const network::RequestOptions& opts,
                                         fuerte::Request& req) const {
  if (!opts.allow_compression) {
    // compression explicitly disallowed
    return false;
  }

  uint64_t threshold = _compress_request_threshold;
  if (threshold == 0) {
    // opted out of compression by configuration
    return false;
  }

  if (_compression_type == CompressionType::None) {
    return false;
  }

  if (req.header.meta().contains(StaticStrings::kContentEncoding)) {
    // Content-Encoding already set. better not overwrite it
    return false;
  }

  auto& pfm = req.payloadForModification();
  size_t body_size = pfm.size();
  if (body_size < threshold) {
    // request body size too small for compression
    return false;
  }

  auto compression_type = _compression_type;

  if (compression_type == CompressionType::Auto) {
    // "auto" compression means that we will pick deflate for all
    // requests that exceed the threshold size, and lz4 for all
    // requests with substantially larger ones
    compression_type = CompressionType::Deflate;
    if (body_size >= threshold * 3) {
      compression_type = CompressionType::Lz4;
    }
  }

  SDB_ASSERT(compression_type != CompressionType::None);
  SDB_ASSERT(compression_type != CompressionType::Auto);

  vpack::BufferUInt8 compressed;
  if (compression_type == CompressionType::Deflate) {
    if (encoding::ZLibDeflate(pfm.data(), pfm.size(), compressed) != ERROR_OK) {
      return false;
    }

    if (compressed.size() >= body_size) {
      // compression did not provide any benefit. better leave it
      return false;
    }

    pfm = std::move(compressed);
    req.header.addMeta(StaticStrings::kContentEncoding,
                       StaticStrings::kEncodingDeflate);
  } else if (compression_type == CompressionType::GZip) {
    if (encoding::GZipCompress(pfm.data(), pfm.size(), compressed) !=
        ERROR_OK) {
      return false;
    }

    if (compressed.size() >= body_size) {
      // compression did not provide any benefit. better leave it
      return false;
    }

    pfm = std::move(compressed);
    req.header.addMeta(StaticStrings::kContentEncoding,
                       StaticStrings::kEncodingGzip);
  } else if (compression_type == CompressionType::Lz4) {
    if (encoding::Lz4Compress(pfm.data(), pfm.size(), compressed) != ERROR_OK) {
      return false;
    }

    if (compressed.size() >= body_size) {
      // compression did not provide any benefit. better leave it
      return false;
    }

    pfm = std::move(compressed);
    req.header.addMeta(StaticStrings::kContentEncoding,
                       StaticStrings::kEncodingSereneLz4);
  } else {
    // unknown compression type
    return false;
  }

  return true;
}

}  // namespace sdb
