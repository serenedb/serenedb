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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <axiom/connectors/ConnectorMetadata.h>
#include <velox/common/file/File.h>
#include <velox/connectors/Connector.h>
#include <velox/core/ExpressionEvaluator.h>
#include <velox/core/Expressions.h>
#include <velox/dwio/common/MetadataFilter.h>
#include <velox/dwio/common/Options.h>
#include <velox/dwio/common/Reader.h>
#include <velox/dwio/common/Writer.h>
#include <velox/dwio/text/reader/TextReader.h>
#include <velox/dwio/text/writer/TextWriter.h>
#include <velox/exec/OperatorUtils.h>
#include <velox/expression/Expr.h>
#include <velox/type/Filter.h>

#include <limits>

#include "basics/assert.h"
#include "basics/fwd.h"
#include "basics/message_buffer.h"

namespace sdb {
class StorageOptions;
}

namespace sdb::connector {

using ReportCallback = std::function<void(uint64_t)>;

struct FileConnectorSplit final : public velox::connector::ConnectorSplit {
  const uint64_t start;
  const uint64_t length;

  FileConnectorSplit(const std::string& connector_id, uint64_t start = 0,
                     uint64_t length = std::numeric_limits<uint64_t>::max())
    : ConnectorSplit(connector_id), start(start), length(length) {}
};

struct DwioWriterOptions {
  std::shared_ptr<velox::dwio::common::WriterOptions> writer;
};

struct DwioReaderOptions {
  std::shared_ptr<velox::dwio::common::ReaderOptions> reader;
  std::shared_ptr<velox::dwio::common::RowReaderOptions> row_reader;
};

struct WriterOptions {
  DwioWriterOptions dwio;
  std::shared_ptr<sdb::StorageOptions> storage_options;

  const auto& Writer() const { return dwio.writer; }
  auto& Writer() { return dwio.writer; }
};

struct ReaderOptions {
  DwioReaderOptions dwio;
  // if set then progress messages are written here
  ReportCallback report_callback;
  std::shared_ptr<sdb::StorageOptions> storage_options;

  const auto& Reader() const { return dwio.reader; }
  auto& Reader() { return dwio.reader; }
  const auto& RowReader() const { return dwio.row_reader; }
  auto& RowReader() { return dwio.row_reader; }
};

class FileSplitSource final : public axiom::connector::SplitSource {
 public:
  FileSplitSource(std::shared_ptr<ReaderOptions> options,
                  std::string connector_id,
                  axiom::connector::SplitOptions split_options);

  std::vector<SplitSource::SplitAndGroup> getSplits(
    uint64_t target_bytes) final;

 private:
  std::vector<SplitSource::SplitAndGroup> GetParquetSplits() const;
  std::vector<SplitSource::SplitAndGroup> WholeFile() const;

  std::shared_ptr<ReaderOptions> _options;
  std::string _connector_id;
  axiom::connector::SplitOptions _split_options;
  bool _done = false;
};

class FileTable : public axiom::connector::Table {
 public:
  explicit FileTable(velox::RowTypePtr type, std::string_view file_path);

  const std::vector<const axiom::connector::TableLayout*>& layouts()
    const final {
    return _layouts;
  }

  uint64_t numRows() const final { return 0; }

  std::vector<velox::connector::ColumnHandlePtr> rowIdHandles(
    axiom::connector::WriteKind kind) const final {
    return {};
  }

 protected:
  std::vector<const axiom::connector::TableLayout*> _layouts;
  std::vector<std::unique_ptr<axiom::connector::TableLayout>> _layout_handles;
};

class ReadFileTable final : public FileTable {
 public:
  ReadFileTable(velox::RowTypePtr type, std::string_view file_path,
                std::shared_ptr<ReaderOptions> options)
    : FileTable{std::move(type), file_path}, _options{std::move(options)} {}

  const std::shared_ptr<ReaderOptions>& GetOptions() const { return _options; }

 private:
  std::shared_ptr<ReaderOptions> _options;
};

class WriteFileTable final : public FileTable {
 public:
  WriteFileTable(velox::RowTypePtr type, std::string_view file_path,
                 std::shared_ptr<WriterOptions> options)
    : FileTable{std::move(type), file_path}, _options{std::move(options)} {}

  const std::shared_ptr<WriterOptions>& GetOptions() const { return _options; }

 private:
  std::shared_ptr<WriterOptions> _options;
};

class FileTableHandle final : public velox::connector::ConnectorTableHandle {
 public:
  FileTableHandle(std::shared_ptr<ReaderOptions> options,
                  velox::common::SubfieldFilters subfield_filters,
                  velox::core::TypedExprPtr remaining_filter)
    : velox::connector::ConnectorTableHandle{"serenedb"},
      _options{std::move(options)},
      _subfield_filters{std::move(subfield_filters)},
      _remaining_filter{std::move(remaining_filter)},
      _name{absl::StrCat(
        "File(",
        velox::dwio::common::toString(_options->Reader()->fileFormat()), ")")} {
  }

  const std::string& name() const final { return _name; }

  std::string toString() const override {
    std::string result = name();
    if (!_subfield_filters.empty()) {
      absl::StrAppend(&result, ", [",
                      absl::StrJoin(_subfield_filters, ", ",
                                    [](std::string* out, const auto& e) {
                                      absl::StrAppend(
                                        out, "(", e.first.toString(), ", ",
                                        e.second->toString(), ")");
                                    }),
                      "]");
    }
    return result;
  }

  bool supportsIndexLookup() const final { return false; }

  const std::shared_ptr<ReaderOptions>& GetOptions() const { return _options; }

  const velox::common::SubfieldFilters& GetSubfieldFilters() const {
    return _subfield_filters;
  }

  const velox::core::TypedExprPtr& GetRemainingFilter() const {
    return _remaining_filter;
  }

 private:
  std::shared_ptr<ReaderOptions> _options;
  velox::common::SubfieldFilters _subfield_filters;
  velox::core::TypedExprPtr _remaining_filter;
  std::string _name;
};

class FileInsertTableHandle final
  : public velox::connector::ConnectorInsertTableHandle {
 public:
  explicit FileInsertTableHandle(std::shared_ptr<WriterOptions> options)
    : _options{std::move(options)} {}

  bool supportsMultiThreading() const final { return false; }

  std::string toString() const final { return "filewrite()"; }

  const std::shared_ptr<WriterOptions>& GetOptions() const { return _options; }

 private:
  std::shared_ptr<WriterOptions> _options;
};

class FileConnectorWriteHandle final
  : public axiom::connector::ConnectorWriteHandle {
 public:
  explicit FileConnectorWriteHandle(std::shared_ptr<WriterOptions> options)
    : ConnectorWriteHandle{
        std::make_shared<FileInsertTableHandle>(std::move(options)),
        velox::ROW("rows", velox::BIGINT())} {}
};

class FileDataSink final : public velox::connector::DataSink {
 public:
  FileDataSink(std::shared_ptr<WriterOptions> options,
               velox::memory::MemoryPool& leaf_pool,
               velox::memory::MemoryPool& aggregate_pool);

  void appendData(velox::RowVectorPtr input) final;

  bool finish() final;

  std::vector<std::string> close() final;

  void abort() final;

  velox::connector::DataSink::Stats stats() const final { return _stats; }

 private:
  std::shared_ptr<velox::dwio::common::Writer> _writer;
  velox::connector::DataSink::Stats _stats;
  bool _closed = false;
};

class FileDataSource final : public velox::connector::DataSource {
 public:
  FileDataSource(std::shared_ptr<ReaderOptions> options,
                 const velox::common::SubfieldFilters& subfield_filters,
                 velox::RowTypePtr output_type,
                 const velox::connector::ColumnHandleMap& column_handles,
                 velox::memory::MemoryPool& memory_pool,
                 const velox::core::TypedExprPtr& remaining_filter,
                 velox::core::ExpressionEvaluator* evaluator);

  void addSplit(std::shared_ptr<velox::connector::ConnectorSplit> split) final;

  std::optional<velox::RowVectorPtr> next(uint64_t size,
                                          velox::ContinueFuture& future) final;

  void addDynamicFilter(
    velox::column_index_t output_channel,
    const std::shared_ptr<velox::common::Filter>& filter) final {}

  uint64_t getCompletedBytes() final { return 0; }

  uint64_t getCompletedRows() final { return _completed_rows; }

 private:
  velox::memory::MemoryPool* _pool;
  velox::RowTypePtr _output_type;
  std::shared_ptr<velox::ReadFile> _source;
  std::unique_ptr<velox::dwio::common::Reader> _reader;
  std::shared_ptr<velox::dwio::common::ReaderOptions> _reader_options;
  std::unique_ptr<velox::dwio::common::RowReader> _row_reader;
  // We store RowReaderOptions to keep ScanSpec alive
  std::shared_ptr<velox::dwio::common::RowReaderOptions> _row_reader_options;

  uint64_t _completed_rows = 0;
  std::chrono::high_resolution_clock::time_point _last_report_time;
  ReportCallback _report_callback;
};

}  // namespace sdb::connector
