////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <yaclib/exe/executor.hpp>
#include <yaclib/exe/job.hpp>

#include "basics/asio_ns.h"

namespace sdb::network {

class IoExecutor : public yaclib::IExecutor {
 public:
  explicit IoExecutor(asio_ns::io_context& io) : _io{io} {}

  Type Tag() const noexcept override { return Type::Custom; }

  bool Alive() const noexcept override { return true; }

  void Submit(yaclib::Job& job) noexcept override {
    asio_ns::post(_io, [&job] { job.Call(); });
  }

 private:
  asio_ns::io_context& _io;
};

}  // namespace sdb::network
