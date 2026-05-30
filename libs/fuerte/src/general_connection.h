////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
///
/// @author Simon Grätzer
////////////////////////////////////////////////////////////////////////////////

#pragma once

/// Here is an overview over the class structure for connections in fuerte.
/// There is a lot of inheritance and templating going on which can be a
/// bit confusing at first sight.
///
/// The base class for all connections is `Connection` in `connection.h`
/// which has `std::enable_shared_from_this<Connection>` to sort out
/// memory allocation and object lifetimes. In this way, a callback
/// closure can own a shared_ptr to the connection object it is working
/// on. This base class basically only defines the interface for all
/// other derived classes. There is not a lot implemented in there.
///
/// The class template GeneralConnection<ST, RT> implements code which
/// is the same for all three cases HTTP/1 and HTTP/2. Here, ST
/// stands for the socket type (which can be Tcp (unencrypted), Ssl (TLS
/// encryption with openssl) and Unix (unix domain socket, no encryption).
/// Note that all 9 combination between protocol version and socket type
/// are possible. RT is a type which represents a request which is run
/// on the connection. For HTTP/1, this will be a `RequestItem`, for
/// HTTP/2 it will be a `Stream`.
/// All two RTs are separate classes but behave similarly and have
/// similar methods, such that the generic code can use them. They also
/// have differences, since requests have to be handled differently
/// in the three protocols.
///
/// For the HTTP/2 case there is an intermediate class template
/// `MultiConnection<ST, RT>`, which inherits from `GeneralConnection<ST, RT>`
/// and is needed, because - contrary to HTTP/1 - there can be more than
/// one request in flight on a connection due to multiplexing. Furthermore,
/// for HTTP/2, a single request or response can be distributed across
/// multiple "frames".
///
/// Therefore, the `MultiConnection<ST, RT>` implementation has a member
/// `_streams` which holds the current list of requests (instances of RT)
/// which are currently in flight.
///
/// Finally, there are the three class templates
///   `H1Connection` inheriting from `GeneralConnection<ST, RequestItem>`
///   and `H2Connection` inheriting from `MultiConnection<ST, Stream>`.

#include <fuerte/connection.h>
#include <fuerte/types.h>

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <map>

#include "asio_sockets.h"

namespace sdb::fuerte {

using Clock = std::chrono::steady_clock;

// GeneralConnection implements shared code between all protocol implementations
template<SocketType ST, typename RT>
class GeneralConnection : public fuerte::Connection {
 public:
  explicit GeneralConnection(EventLoopService& loop,
                             const detail::ConnectionConfiguration& config)
    : Connection(config),
      _io_context(loop.nextIOContext()),
      _proto(loop, *_io_context) {
#ifdef SDB_GTEST
    _fail_connect_attempts = config.fail_connect_attempts;
#endif
  }

  ~GeneralConnection() override {
    _state.store(Connection::State::Closed);
    terminateActivity(fuerte::Error::ConnectionCanceled);
  }

  /// @brief connection state
  Connection::State state() const final {
    return _state.load(std::memory_order_acquire);
  }

  /// The following public methods can be called from any thread:

  // Start an asynchronous request.
  void sendRequest(std::unique_ptr<Request> req, RequestCallback cb) override {
    // construct RequestItem
    req->setTimeQueued();
    auto item = this->createRequest(std::move(req), std::move(cb));
    // set the point-in-time when this request expires
    if (item->request->timeout().count() > 0) {
      item->expires = Clock::now() + item->request->timeout();
    } else {
      item->expires = Clock::time_point::max();
    }

    // Don't send once in Closed state:
    if (this->_state.load(std::memory_order_relaxed) ==
        Connection::State::Closed) {
      item->invokeOnError(Error::ConnectionClosed);
      return;
    }

    // Prepare a new request
    this->_num_queued.fetch_add(1, std::memory_order_relaxed);
    if (!this->_queue.push(item.get())) {
      SDB_ERROR(GENERAL, "connection queue capacity exceeded");
      uint32_t q = this->_num_queued.fetch_sub(1, std::memory_order_relaxed);
      SDB_ASSERT(q > 0);
      item->invokeOnError(Error::QueueCapacityExceeded);
      return;
    }
    item.release();  // queue owns this now

    SDB_DEBUG(GENERAL,
              "queued item: this=", reinterpret_cast<uintptr_t>(this));

    // Note that we have first posted on the queue with
    // std::memory_order_seq_cst and now we check _active
    // std::memory_order_seq_cst. This prevents a sleeping barber with the
    // check-set-check combination in `asyncWriteNextRequest`. If we are the
    // ones to exchange the value to `true`, then we post on the `_io_context`
    // to activate the connection. Note that the connection can be in the
    // `Disconnected` or `Connected` or `Failed` state, but not in the
    // `Connecting` state in this case.
    if (!this->_active.exchange(true)) {
      asio_ns::post(*_io_context, [self = Connection::shared_from_this()] {
        static_cast<GeneralConnection<ST, RT>&>(*self).activate();
      });
    }
  }

  /// @brief cancel the connection, unusable afterwards
  void cancel() override {
    SDB_DEBUG(GENERAL,
              "cancel: this=", reinterpret_cast<uintptr_t>(this));
    asio_ns::post(*_io_context, [weak = weak_from_this()] {
      if (auto self = weak.lock()) {
        static_cast<GeneralConnection<ST, RT>&>(*self).shutdownConnection(
          Error::ConnectionCanceled);
      }
    });
  }

  /// All protected or private methods below here must only be called on the
  /// IO thread.
 protected:
  void startConnection() {
    // start connecting only if state is disconnected
    SDB_ASSERT(this->_active.load());
    Connection::State exp = Connection::State::Created;
    if (_state.compare_exchange_strong(exp, Connection::State::Connecting)) {
      SDB_DEBUG(GENERAL,
                "startConnection: this=", reinterpret_cast<uintptr_t>(this));
      SDB_ASSERT(_config.max_connect_retries > 0);
      tryConnect(_config.max_connect_retries);
    } else {
      SDB_DEBUG(GENERAL,
                "startConnection: this=", reinterpret_cast<uintptr_t>(this),
                " found unexpected state ", static_cast<int>(exp),
                " not equal to 'Created'");
      SDB_ASSERT(false);
    }
  }

  // shutdown connection, cancel async operations
  void shutdownConnection(fuerte::Error err, const std::string& msg = "") {
    SDB_DEBUG(GENERAL, "shutdownConnection: err = '",
              ToString(err), "' ");
    if (!msg.empty()) {
      SDB_DEBUG(GENERAL, ", msg = '", msg, "' ");
    }
    SDB_DEBUG(GENERAL,
              "this=", reinterpret_cast<uintptr_t>(this));

    auto exp = _state.load();
    if (exp == Connection::State::Closed ||
        !_state.compare_exchange_strong(exp, Connection::State::Closed)) {
      SDB_DEBUG(GENERAL, "connection is already shutdown this=",
                reinterpret_cast<uintptr_t>(this));
      return;
    }

    abortRequests(err, /*now*/ Clock::time_point::max());

    _proto.shutdown([=, self = shared_from_this()](asio_ns::error_code ec) {
      auto& me = static_cast<GeneralConnection<ST, RT>&>(*self);
      me.terminateActivity(err);
      me.onFailure(err, msg);
    });  // Close socket
  }

  // Call on IO-Thread: read from socket
  void AsyncReadSome() {
    SDB_TRACE(GENERAL,
              "AsyncReadSome: this=", reinterpret_cast<uintptr_t>(this));

    // TODO perform a non-blocking read on linux

    // Start reading data from the network.

    // reserve 32kB in output buffer
    auto mutable_buff = _receive_buffer.prepare(kReadBlockSize);

    _reading = true;
    setIOTimeout();  // must be after setting _reading
    _proto.socket.async_read_some(
      mutable_buff, [self = shared_from_this()](const auto& ec, size_t nread) {
        SDB_TRACE(GENERAL, "received ", nread, " bytes");

        // received data is "committed" from output sequence to input sequence
        auto& me = static_cast<GeneralConnection<ST, RT>&>(*self);
        me._reading = false;
        me._receive_buffer.commit(nread);
        me.asyncReadCallback(ec);
      });
  }

  /// abort all requests lingering in the queue
  void drainQueue(const fuerte::Error ec) {
    SDB_DEBUG(GENERAL,
              "drain queue this=", reinterpret_cast<uintptr_t>(this));
    RT* item = nullptr;
    while (_queue.pop(item)) {
      SDB_ASSERT(item);
      std::unique_ptr<RT> guard(item);
      uint32_t q = this->_num_queued.fetch_sub(1, std::memory_order_relaxed);
      SDB_ASSERT(q > 0);
      guard->invokeOnError(ec);
    }
  }

  void activate() {
    Connection::State state = this->_state.load();
    SDB_ASSERT(state != Connection::State::Connecting);
    if (state == Connection::State::Connected) {
      SDB_DEBUG(GENERAL, "activate: connected");
      this->DoWrite();
    } else if (state == Connection::State::Created) {
      SDB_DEBUG(GENERAL, "activate: not connected");
      this->startConnection();
    } else if (state == Connection::State::Closed) {
      SDB_ERROR(GENERAL,
                "activate: queued request on failed connection");
      this->drainQueue(fuerte::Error::ConnectionClosed);
      this->_active.store(false);  // No more activity from our side
    }
    // If the state is `Connecting`, we do not need to do anything, this can
    // happen if this `activate` was posted when the connection was still
    // `Disconnected`, but in the meantime the connect has started.
  }

  /// The following is called when the connection is permanently failed. It is
  /// used to shut down any activity in the derived classes in a way that avoids
  /// sleeping barbers
  void terminateActivity(fuerte::Error err) noexcept {
    // Usually, we are `active == true` when we get here, except for the
    // following case: If we are inactive but the connection is still open and
    // then the idle timeout goes off, then we shutdownConnection and in the TLS
    // case we call this method here. In this case it is OK to simply proceed,
    // therefore no assertion on `_active`.
    SDB_ASSERT(this->_state.load() == Connection::State::Closed);
    SDB_DEBUG(GENERAL, "terminateActivity: active=true, this=",
              reinterpret_cast<uintptr_t>(this));
    while (true) {
      this->drainQueue(err);
      this->_active.store(false);
      // Now need to check again:
      if (this->_queue.empty()) {
        return;
      }
      this->_active.store(true);
    }
  }

  void cancelTimer() noexcept {
    try {
      this->_proto.timer.cancel();
    } catch (const std::exception& ex) {
      SDB_ERROR(GENERAL,
                "caught exception during timer cancelation: ", ex.what());
    }
  }

 protected:
  virtual void finishConnect() = 0;

  /// perform writes
  virtual void DoWrite() = 0;

  // called by the async_read handler (called from IO thread)
  virtual void asyncReadCallback(const asio_ns::error_code&) = 0;

  /// abort ongoing / unfinished requests expiring before given timpoint
  virtual void abortRequests(fuerte::Error, Clock::time_point now) = 0;

  virtual void setIOTimeout() = 0;

  virtual std::unique_ptr<RT> createRequest(std::unique_ptr<Request>&& req,
                                            RequestCallback&& cb) = 0;

 private:
  // Try to connect with a given number of retries
  void tryConnect(unsigned retries) {
    SDB_ASSERT(retries > 0);

    if (_state.load() != Connection::State::Connecting) {
      return;
    }

    SDB_DEBUG(GENERAL, "tryConnect (", retries,
              ") this=", reinterpret_cast<uintptr_t>(this));
    auto self = Connection::shared_from_this();

    _proto.connect_timer_role = ConnectTimerRole::Connect;
    _proto.timer.expires_after(_config.connect_timeout);

    _proto.connect(_config, [self, retries](asio_ns::error_code ec) mutable {
      auto& me = static_cast<GeneralConnection<ST, RT>&>(*self);
      me.cancelTimer();
      // Note that is is possible that the alarm has already gone off. In this
      // case its closure could have already executed, or it may be queued right
      // after ourselves! In the first case the socket has already been closed,
      // so we need to check that. For the latter case we now set the state to
      // `Connected`, so the closure will simply do nothing.

      if (!ec && !me._proto.isOpen()) {
        // timer went off earlier and has already closed the socket.
        ec = asio_ns::error::operation_aborted;
      }
      if (!ec) {
        SDB_DEBUG(GENERAL, "tryConnect (", retries,
                  ") established connection this=",
                  reinterpret_cast<uintptr_t>(self.get()));
        me.finishConnect();
        return;
      }

#ifdef SDB_GTEST
      if (me._fail_connect_attempts > 0) {
        --me._fail_connect_attempts;
      }
#endif

      SDB_DEBUG(GENERAL, "tryConnect (", retries,
                "), connecting failed: ", ec.message());
      if (retries > 1 && ec != asio_ns::error::operation_aborted) {
        SDB_DEBUG(GENERAL, "tryConnect (", retries,
                  "), scheduling retry operation. this=",
                  reinterpret_cast<uintptr_t>(self.get()));
        me._proto.connect_timer_role = ConnectTimerRole::Reconnect;
        me._proto.timer.expires_after(me._config.connect_retry_pause);
        me._proto.timer.async_wait(
          [self = std::move(self), retries](asio_ns::error_code ec) mutable {
            auto& me = static_cast<GeneralConnection<ST, RT>&>(*self);
            if (ec) {
              SDB_DEBUG(GENERAL,
                        "tryConnect, retry timer canceled. this=",
                        reinterpret_cast<uintptr_t>(self.get()));
              me.shutdownConnection(Error::CouldNotConnect,
                                    "connecting failed: retry timer canceled");
              return;
            }
            // rearm socket so that we can use it again
            SDB_DEBUG(GENERAL,
                      "tryConnect, rearming connection this=",
                      reinterpret_cast<uintptr_t>(self.get()));
            me._proto.rearm();
            me.tryConnect(retries - 1);
          });
      } else {
        std::string msg("connecting failed: ");
        msg.append((ec != asio_ns::error::operation_aborted) ? ec.message()
                                                             : "timeout");
        SDB_DEBUG(GENERAL,
                  "tryConnect, calling shutdownConnection: ", msg,
                  " this=", reinterpret_cast<uintptr_t>(self.get()));
        me.shutdownConnection(Error::CouldNotConnect, msg);
      }
    });

    // only if we are still in the connect phase, we want to schedule a timer
    // for the connect timeout. if the connect already failed and scheduled a
    // timer for the reconnect timeout, we do not want to mess with the timer
    // here.
    if (_proto.connect_timer_role == ConnectTimerRole::Connect) {
      _proto.timer.async_wait([self = std::move(self)](asio_ns::error_code ec) {
        if (!ec && self->state() == Connection::State::Connecting) {
          // note: if the timer fires successfully, ec is empty here.
          // the connect handler below gets 'operation_aborted' error
          auto& me = static_cast<GeneralConnection<ST, RT>&>(*self);
          // cancel the socket operations only if we are still in the connect
          // phase.
          // otherwise, we are in the reconnect phase already, and we
          // do not want to cancel the socket.
          if (me._proto.connect_timer_role == ConnectTimerRole::Connect) {
            SDB_DEBUG(GENERAL,
                      "tryConnect, connect timeout this=",
                      reinterpret_cast<uintptr_t>(self.get()));
            me._proto.cancel();
          }
        }
      });
    }
  }

 protected:
  /// @brief io context to use
  std::shared_ptr<asio_ns::io_context> _io_context;
  /// @brief underlying socket
  Socket<ST> _proto;

  /// elements to send out
  boost::lockfree::queue<RT*, boost::lockfree::capacity<32>> _queue;

  /// default max chunksize is 30kb in serenedb
  static constexpr size_t kReadBlockSize = 1024 * 32;
  ::asio_ns::streambuf _receive_buffer;

  std::atomic<unsigned> _num_queued{0};  /// queued items

  /// @brief is the connection established, do not synchronize on that
  std::atomic<Connection::State> _state{Connection::State::Created};

  std::atomic<bool> _active{false};

#ifdef SDB_GTEST
  // if this member is > 0, then this many connection attempts will fail
  // in this connection
  unsigned _fail_connect_attempts = 0;
#endif

  bool _reading = false;  // set to true while an async_read is ongoing
  bool _writing = false;  // set between starting an asyncWrite operation and
                          // executing the completion handler};
  bool _timeout_on_read_write = false;
};

// common superclass HTTP2
template<SocketType ST, typename RT>
struct MultiConnection : public GeneralConnection<ST, RT> {
  explicit MultiConnection(EventLoopService& loop,
                           const detail::ConnectionConfiguration& config)
    : GeneralConnection<ST, RT>(loop, config) {}

  ~MultiConnection() override = default;

 public:
  // Return the number of unfinished requests.
  size_t requestsLeft() const override {
    uint32_t qd = this->_num_queued.load(std::memory_order_relaxed);
    qd += _stream_count.load(std::memory_order_relaxed);
    return qd;
  }

 protected:
  void setIOTimeout() override {
    setTimeout(/*setIOTimeout*/ this->_reading || this->_writing);
  }

  std::unique_ptr<RT> createRequest(std::unique_ptr<Request>&& req,
                                    RequestCallback&& cb) override {
    return std::make_unique<RT>(std::move(req), std::move(cb));
  }

  fuerte::Error translateError(const asio_ns::error_code& e,
                               fuerte::Error c) const {
    if (e == asio_ns::error::misc_errors::eof ||
        e == asio_ns::error::connection_reset) {
      return fuerte::Error::ConnectionClosed;
    } else if (e == asio_ns::error::operation_aborted ||
               e == asio_ns::error::connection_aborted) {
      // keepalive timeout may have expired
      return this->_timeout_on_read_write ? fuerte::Error::ConnectionClosed
                                          : fuerte::Error::ConnectionCanceled;
    }

    return c;
  }

 private:
  void setTimeout(bool set_io_begin) {
    const bool was_idle = _streams.empty();
    if (was_idle && !this->_config.use_idle_timeout) {
      this->cancelTimer();
      return;
    }

    const auto now = Clock::now();
    if (set_io_begin) {
      _last_io = Clock::now();
    }

    auto tp = Clock::time_point::max();
    if (was_idle) {  // use default connection timeout
      SDB_TRACE(GENERAL, "setting idle keep alive timer, this=",
                reinterpret_cast<uintptr_t>(this));
      tp = now + this->_config.idle_timeout;
    } else {
      for (const auto& pair : _streams) {
        tp = std::min(tp, pair.second->expires);
      }
    }

    const bool was_reading = this->_reading;
    const bool was_writing = this->_writing;
    if (was_reading || was_writing) {
      SDB_ASSERT(_last_io + kIOTimeout > now);
      tp = std::min(tp, _last_io + kIOTimeout);
    }

    // expires_after cancels pending ops
    this->_proto.timer.expires_at(tp);
    this->_proto.timer.async_wait(
      [=, this, self = Connection::weak_from_this()](const auto& ec) {
        std::shared_ptr<Connection> s;
        if (ec || !(s = self.lock())) {  // was canceled / deallocated
          return;
        }

        auto& me = static_cast<MultiConnection<ST, RT>&>(*s);
        auto now = Clock::now();
        bool is_idle = me._streams.empty();
        me.abortRequests(Error::RequestTimeout, now);

        if (was_reading && me._reading) {
          // reading may take longer than kIOTimeout
          // users may adjust request timeout accordingly
          if (now - _last_io > kIOTimeout && me._streams.empty()) {
            SDB_DEBUG(GENERAL, "IO read timeout",
                      " this=", reinterpret_cast<uintptr_t>(&me));
            me._timeout_on_read_write = true;
            me._proto.cancel();
            // We simply cancel all ongoing asynchronous operations, the
            // completion handlers will do the rest.
          } else {
            // eventually all request expire, then cancel connection
            setTimeout(/*setIOBegin*/ false);
          }
        } else if (was_writing && me._writing && now - _last_io > kIOTimeout) {
          SDB_DEBUG(GENERAL, "IO write timeout",
                    " this=", reinterpret_cast<uintptr_t>(&me));
          me._timeout_on_read_write = true;
          me._proto.cancel();
          // We simply cancel all ongoing asynchronous operations, the
          // completion handlers will do the rest.

        } else if (was_idle && is_idle) {
          if (!me._active && me._state == Connection::State::Connected) {
            SDB_DEBUG(GENERAL, "HTTP-Request idle timeout",
                      " this=", reinterpret_cast<uintptr_t>(&me));
            me.shutdownConnection(Error::CloseRequested);
          }
        }
        // In all other cases we do nothing, since we have been posted to the
        // iocontext but the thing we should be timing out has already
        // completed.
      });
  }

 protected:
  static constexpr std::chrono::seconds kIOTimeout{300};

  std::map<uint64_t, std::unique_ptr<RT>> _streams;
  std::atomic<unsigned> _stream_count{0};

 private:
  Clock::time_point _last_io;
};

}  // namespace sdb::fuerte
