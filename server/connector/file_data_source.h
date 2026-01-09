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
#include <velox/dwio/text/reader/TextReader.h>

#include "basics/fwd.h"

namespace sdb::connector {

class FileDataSource final : public velox::connector::DataSource {
 public:
  explicit FileDataSource(std::shared_ptr<velox::dwio::common::Reader> reader);

  ~FileDataSource() override;

  void addSplit(
    std::shared_ptr<velox::connector::ConnectorSplit> split) override {}

  std::optional<velox::RowVectorPtr> next(
    uint64_t size, velox::ContinueFuture& future) override;

  void addDynamicFilter(
    velox::column_index_t output_channel,
    const std::shared_ptr<velox::common::Filter>& filter) override {}

  uint64_t getCompletedBytes() override { return _completed_bytes; }

  uint64_t getCompletedRows() override { return _completed_rows; }

 private:
  std::shared_ptr<velox::dwio::common::Reader> _reader;
  std::unique_ptr<velox::dwio::common::RowReader> _row_reader;
  // We store RowReaderOptions to keep ScanSpec alive
  velox::dwio::common::RowReaderOptions _row_reader_options;

  uint64_t _completed_rows = 0;
  uint64_t _completed_bytes = 0;
};

}  // namespace sdb::connector
