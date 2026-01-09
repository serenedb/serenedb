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

#include "file_data_sink.h"

#include <velox/common/file/File.h>
#include <velox/dwio/common/FileSink.h>
#include <velox/dwio/common/Writer.h>
#include <velox/dwio/common/WriterFactory.h>
#include <velox/dwio/text/writer/TextWriter.h>

namespace sdb::connector {

FileDataSink::FileDataSink(std::shared_ptr<velox::dwio::common::Writer> writer)
  : _writer(std::move(writer)) {}

FileDataSink::~FileDataSink() = default;

void FileDataSink::appendData(velox::RowVectorPtr input) {
  _writer->write(input);
  _stats.numWrittenBytes += input->estimateFlatSize();
}

bool FileDataSink::finish() {
  if (!_writer) {
    return true;
  }
  _writer->flush();
  return true;
}

std::vector<std::string> FileDataSink::close() {
  if (_closed) {
    return {};
  }
  if (_writer) {
    _writer->close();
    _writer.reset();
  }
  _closed = true;
  return {};
}

void FileDataSink::abort() {
  if (_writer) {
    _writer->abort();
    _writer.reset();
  }
  _closed = true;
}

}  // namespace sdb::connector
