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

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <cstdint>

#include "basics/condition_variable.h"
#include "basics/thread.h"

namespace sdb {

class LogGroup;
namespace app {

class AppServer;
}

namespace log {

struct Message;

class LogThread final : public Thread {
  struct MessageEnvelope {
    LogGroup* group;
    Message* msg;
  };

 public:
  explicit LogThread(app::AppServer& server, const std::string& name,
                     uint32_t max_queued_log_messages);
  ~LogThread() final;

  bool isSystem() const override { return true; }
  bool isSilent() const override { return true; }
  void run() override;

  bool log(LogGroup&, std::unique_ptr<log::Message>&);
  // flush all pending log messages
  void flush() noexcept;

  // whether or not the log thread has messages queued
  bool hasMessages() const noexcept;
  // wake up the log thread from the outside
  void wakeup() noexcept;

  // handle all queued messages - normally this should not be called
  // by anyone, except from the crash handler
  bool processPendingMessages();

 private:
  struct {
    absl::CondVar cv;
    absl::Mutex mutex;
  } _condition;
  boost::lockfree::queue<MessageEnvelope> _messages;
  std::atomic<size_t> _pending_messages{0};
  uint32_t _max_queued_log_messages{10000};
};

}  // namespace log
}  // namespace sdb
