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

#include "basics/common.h"
#include "statistics/figures.h"
#include "statistics/statistics_feature.h"

namespace sdb {

class ConnectionStatistics {
 public:
  static uint64_t memoryUsage() noexcept;
  static void initialize();

  class Item {
   public:
    constexpr Item() noexcept : _stat(nullptr) {}
    explicit Item(ConnectionStatistics* stat) noexcept : _stat(stat) {}

    Item(const Item&) = delete;
    Item& operator=(const Item&) = delete;

    Item(Item&& r) noexcept : _stat(r._stat) { r._stat = nullptr; }
    Item& operator=(Item&& r) noexcept {
      if (&r != this) {
        reset();
        _stat = r._stat;
        r._stat = nullptr;
      }
      return *this;
    }

    ~Item() { reset(); }

    void reset() noexcept {
      if (_stat != nullptr) {
        _stat->release();
        _stat = nullptr;
      }
    }

    void SET_START() {
      if (_stat != nullptr) {
        _stat->_conn_start = StatisticsFeature::time();
      }
    }

    void SET_END() {
      if (_stat != nullptr) {
        _stat->_conn_end = StatisticsFeature::time();
      }
    }

    void SET_HTTP();

   private:
    ConnectionStatistics* _stat;
  };

  ConnectionStatistics() noexcept { reset(); }
  static Item acquire() noexcept;

  struct Snapshot {
    statistics::Counter http_connections;
    statistics::Counter total_requests;
    statistics::Counter total_requests_superuser;
    statistics::Counter total_requests_user;
    statistics::MethodRequestCounters method_requests;
    statistics::Counter async_requests;
    statistics::Distribution connection_time;
  };

  static void getSnapshot(Snapshot& snapshot);

 private:
  void release() noexcept;

  void reset() noexcept {
    _conn_start = 0.0;
    _conn_end = 0.0;
    _http = false;
    _error = false;
  }

 private:
  double _conn_start;
  double _conn_end;

  bool _http;
  bool _error;
};

}  // namespace sdb
