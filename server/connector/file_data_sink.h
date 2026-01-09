////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <velox/connectors/Connector.h>
#include <velox/dwio/common/Options.h>
#include <velox/dwio/text/writer/TextWriter.h>

#include "basics/fwd.h"

namespace sdb::connector {

class FileDataSink final : public velox::connector::DataSink {
 public:
  explicit FileDataSink(std::shared_ptr<velox::dwio::common::Writer> writer);

  ~FileDataSink() override;

  void appendData(velox::RowVectorPtr input) override;

  bool finish() override;

  std::vector<std::string> close() override;

  void abort() override;

  velox::connector::DataSink::Stats stats() const override { return _stats; }

 private:
  std::shared_ptr<velox::dwio::common::Writer> _writer;
  velox::connector::DataSink::Stats _stats;
  bool _closed = false;
};

}  // namespace sdb::connector
