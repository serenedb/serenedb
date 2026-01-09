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

#include "file_data_source.h"

#include <velox/connectors/hive/HiveConnectorUtil.h>

namespace sdb::connector {

FileDataSource::FileDataSource(
  std::shared_ptr<velox::dwio::common::Reader> reader)
  : _reader(std::move(reader)) {
  auto row_type = _reader->rowType();
  auto spec = std::make_shared<velox::common::ScanSpec>("root");
  for (size_t i = 0; i < row_type->size(); ++i) {
    spec->addField(row_type->nameOf(i), i);
  }

  _row_reader_options.setScanSpec(std::move(spec));
  _row_reader = _reader->createRowReader(_row_reader_options);
}

FileDataSource::~FileDataSource() = default;

std::optional<velox::RowVectorPtr> FileDataSource::next(
  uint64_t size, velox::ContinueFuture& future) {
  velox::VectorPtr batch;
  uint64_t rows_read = _row_reader->next(size, batch);
  if (rows_read == 0) {
    return nullptr;
  }
  _completed_rows += rows_read;
  return std::dynamic_pointer_cast<velox::RowVector>(batch);
}

}  // namespace sdb::connector
