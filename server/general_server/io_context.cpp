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

#include "io_context.h"

#include "basics/logger/logger.h"
#include "basics/thread_id.h"

using namespace sdb;
using namespace sdb::rest;

IoContext::IoContext()
  : io_context(1),  // only a single thread per context
    _work(io_context.get_executor()),
    _clients(0),
    _io_thread([this] {
      InitThread("Io");
      try {
        io_context.run();
      } catch (const std::exception& ex) {
        SDB_WARN(GENERAL, "caught exception in IO thread: ", ex.what());
      }
    }) {}

IoContext::~IoContext() { stop(); }

void IoContext::stop() {
  _work.reset();
  io_context.stop();
  // The jthread joins automatically in its destructor; no busy-wait here.
}
