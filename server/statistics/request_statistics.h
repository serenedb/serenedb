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

#include <cstddef>
#include <cstdint>

#include "basics/common.h"
#include "rest/common_defines.h"
#include "statistics/descriptions.h"
#include "statistics/figures.h"
#include "statistics/statistics_feature.h"

namespace sdb {

class RequestStatistics {
 public:
  static uint64_t memoryUsage() noexcept;
  static void initialize();
  static size_t processAll();

  class ItemView {
   public:
    ItemView() = default;
    explicit ItemView(RequestStatistics* stat) noexcept : _stat(stat) {}

    operator bool() const noexcept { return _stat != nullptr; }

    void SET_ASYNC() const noexcept {
      if (_stat != nullptr) {
        _stat->_async = true;
      }
    }

    void SET_REQUEST_TYPE(rest::RequestType t) const noexcept {
      if (_stat != nullptr) {
        _stat->_request_type = t;
      }
    }

    void SET_READ_START(double start) const noexcept {
      if (_stat != nullptr) {
        if (_stat->_read_start == 0.0) {
          _stat->_read_start = start;
        }
      }
    }

    void SET_READ_END() const {
      if (_stat != nullptr) {
        _stat->_read_end = StatisticsFeature::time();
      }
    }

    void SET_WRITE_START() const {
      if (_stat != nullptr) {
        _stat->_write_start = StatisticsFeature::time();
      }
    }

    void SET_WRITE_END() const {
      if (_stat != nullptr) {
        _stat->_write_end = StatisticsFeature::time();
      }
    }

    void SET_QUEUE_START() const {
      if (_stat != nullptr) {
        _stat->_queue_start = StatisticsFeature::time();
      }
    }

    void SET_QUEUE_END() const {
      if (_stat != nullptr) {
        _stat->_queue_end = StatisticsFeature::time();
      }
    }

    void ADD_RECEIVED_BYTES(size_t bytes) const {
      if (_stat != nullptr) {
        _stat->_received_bytes += bytes;
      }
    }

    void ADD_SENT_BYTES(size_t bytes) const {
      if (_stat != nullptr) {
        _stat->_sent_bytes += bytes;
      }
    }

    void SET_REQUEST_START() const {
      if (_stat != nullptr) {
        _stat->_request_start = StatisticsFeature::time();
      }
    }

    void SET_REQUEST_END() const {
      if (_stat != nullptr) {
        _stat->_request_end = StatisticsFeature::time();
      }
    }

    void SET_REQUEST_START_END() const {
      if (_stat != nullptr) {
        _stat->_request_start = StatisticsFeature::time();
        _stat->_request_end = StatisticsFeature::time();
      }
    }

    double ELAPSED_SINCE_READ_START() const {
      if (_stat != nullptr) {
        return StatisticsFeature::time() - _stat->_read_start;
      } else {
        return 0.0;
      }
    }

    double ELAPSED_WHILE_QUEUED() const noexcept {
      if (_stat != nullptr) {
        return _stat->_queue_end - _stat->_queue_start;
      } else {
        return 0.0;
      }
    }

    void SET_SUPERUSER() const noexcept {
      if (_stat != nullptr) {
        _stat->_superuser = true;
      }
    }

   protected:
    RequestStatistics* _stat = nullptr;
  };

  class Item : public ItemView {
   public:
    using ItemView::ItemView;

    Item(const Item&) = delete;
    Item& operator=(const Item&) = delete;

    Item(Item&& r) noexcept : ItemView{r._stat} { r._stat = nullptr; }
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
      if (_stat) {
        _stat->release();
        _stat = nullptr;
      }
    }
  };

  RequestStatistics() noexcept { reset(); }
  static Item acquire() noexcept;

  struct Snapshot {
    statistics::Distribution total_time;
    statistics::Distribution request_time;
    statistics::Distribution queue_time;
    statistics::Distribution io_time;
    statistics::Distribution bytes_sent;
    statistics::Distribution bytes_received;
  };

  static void getSnapshot(Snapshot& snapshot,
                          stats::RequestStatisticsSource source);

 private:
  static void process(RequestStatistics*);

  void release() noexcept;

  void reset() noexcept {
    _read_start = 0.0;
    _read_end = 0.0;
    _queue_start = 0.0;
    _queue_end = 0.0;
    _request_start = 0.0;
    _request_end = 0.0;
    _write_start = 0.0;
    _write_end = 0.0;
    _received_bytes = 0.0;
    _sent_bytes = 0.0;
    _request_type = rest::RequestType::Illegal;
    _async = false;
    _superuser = false;
  }

  double _read_start;   // CommTask::processRead - read first byte of message
  double _read_end;     // CommTask::processRead - message complete
  double _queue_start;  // job added to JobQueue
  double _queue_end;    // job removed from JobQueue

  double _request_start;  // GeneralServerJob::work
  double _request_end;
  double _write_start;
  double _write_end;

  double _received_bytes;
  double _sent_bytes;

  rest::RequestType _request_type;

  bool _async;
  bool _superuser;
};

}  // namespace sdb
