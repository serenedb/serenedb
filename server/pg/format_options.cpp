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

#include "pg/format_options.h"

#include <velox/dwio/common/Options.h>
#include <velox/dwio/common/WriterFactory.h>
#include <velox/dwio/text/reader/TextReader.h>
#include <velox/dwio/text/writer/TextWriter.h>

#include "connector/file_table.hpp"
#include "query/utils.h"

namespace sdb::pg {

namespace {

std::shared_ptr<connector::WriterOptions> CreateDefaultWriterOptions(
  velox::dwio::common::FileFormat format, velox::RowTypePtr schema) {
  const auto& writer_factory = velox::dwio::common::getWriterFactory(format);
  std::shared_ptr<velox::dwio::common::WriterOptions> dwio_options{
    writer_factory->createWriterOptions().release()};
  dwio_options->schema = std::move(schema);
  dwio_options->fileFormat = format;
  return std::make_shared<connector::WriterOptions>(std::move(dwio_options));
}

std::shared_ptr<connector::ReaderOptions> CreateDefaultReaderOptions(
  velox::dwio::common::FileFormat format, velox::RowTypePtr schema) {
  auto dwio_options =
    std::make_shared<velox::dwio::common::ReaderOptions>(nullptr);
  dwio_options->setFileFormat(format);
  dwio_options->setFileSchema(std::move(schema));
  auto row_reader_options =
    std::make_shared<velox::dwio::common::RowReaderOptions>();
  return std::make_shared<connector::ReaderOptions>(
    std::move(dwio_options), std::move(row_reader_options));
}

}  // namespace

std::shared_ptr<connector::WriterOptions>
TextFormatOptions::createWriterOptions(velox::RowTypePtr schema) const {
  velox::dwio::common::SerDeOptions serde_options{_delim, '\2', '\3', _escape,
                                                  false};
  serde_options.nullString = _null_string;

  auto text_options = std::make_shared<velox::text::WriterOptions>();
  if (_header) {
    text_options->header = query::ToAliases(schema->names());
  }
  text_options->serDeOptions = std::move(serde_options);
  text_options->schema = std::move(schema);
  text_options->fileFormat = velox::dwio::common::FileFormat::TEXT;
  return std::make_shared<connector::WriterOptions>(std::move(text_options));
}

std::shared_ptr<connector::ReaderOptions>
TextFormatOptions::createReaderOptions(velox::RowTypePtr schema) const {
  velox::dwio::common::SerDeOptions serde_options{_delim, '\2', '\3', _escape,
                                                  false};
  serde_options.nullString = _null_string;

  auto text_options = std::make_shared<velox::text::ReaderOptions>(nullptr);
  text_options->setSerDeOptions(std::move(serde_options));
  text_options->setFileSchema(std::move(schema));
  text_options->setFileFormat(velox::dwio::common::FileFormat::TEXT);

  auto row_reader_options =
    std::make_shared<velox::dwio::common::RowReaderOptions>();
  row_reader_options->setSkipRows(_header);
  return std::make_shared<connector::ReaderOptions>(
    std::move(text_options), std::move(row_reader_options));
}

void TextFormatOptions::toVPack(vpack::Builder& b) const {
  b.add("format", std::to_underlying(_format));
  b.add("delim", _delim);
  b.add("escape", static_cast<unsigned>(_escape));
  b.add("null_string", std::string_view{_null_string});
  b.add("header", _header);
}

std::shared_ptr<connector::WriterOptions>
ParquetFormatOptions::createWriterOptions(velox::RowTypePtr schema) const {
  return CreateDefaultWriterOptions(velox::dwio::common::FileFormat::PARQUET,
                                    std::move(schema));
}

std::shared_ptr<connector::ReaderOptions>
ParquetFormatOptions::createReaderOptions(velox::RowTypePtr schema) const {
  return CreateDefaultReaderOptions(velox::dwio::common::FileFormat::PARQUET,
                                    std::move(schema));
}

void ParquetFormatOptions::toVPack(vpack::Builder& b) const {
  b.add("format", static_cast<unsigned>(std::to_underlying(_format)));
}

std::shared_ptr<connector::WriterOptions>
DwrfFormatOptions::createWriterOptions(velox::RowTypePtr schema) const {
  return CreateDefaultWriterOptions(velox::dwio::common::FileFormat::DWRF,
                                    std::move(schema));
}

std::shared_ptr<connector::ReaderOptions>
DwrfFormatOptions::createReaderOptions(velox::RowTypePtr schema) const {
  return CreateDefaultReaderOptions(velox::dwio::common::FileFormat::DWRF,
                                    std::move(schema));
}

void DwrfFormatOptions::toVPack(vpack::Builder& b) const {
  b.add("format", static_cast<unsigned>(std::to_underlying(_format)));
}

std::shared_ptr<connector::WriterOptions> OrcFormatOptions::createWriterOptions(
  velox::RowTypePtr schema) const {
  return CreateDefaultWriterOptions(velox::dwio::common::FileFormat::ORC,
                                    std::move(schema));
}

std::shared_ptr<connector::ReaderOptions> OrcFormatOptions::createReaderOptions(
  velox::RowTypePtr schema) const {
  return CreateDefaultReaderOptions(velox::dwio::common::FileFormat::ORC,
                                    std::move(schema));
}

void OrcFormatOptions::toVPack(vpack::Builder& b) const {
  b.add("format", static_cast<unsigned>(std::to_underlying(_format)));
}

std::shared_ptr<FormatOptions> FormatOptions::fromVPack(vpack::Slice slice) {
  if (!slice.isObject()) {
    return nullptr;
  }
  auto format_slice = slice.get("format");
  if (!format_slice.isNumber()) {
    return nullptr;
  }
  switch (static_cast<FileFormat>(format_slice.getNumber<unsigned>())) {
    case FileFormat::Text: {
      uint8_t delim = '\t';
      uint8_t escape = '\\';
      std::string null_string = "\\N";
      bool header = false;
      if (auto s = slice.get("delim"); s.isNumber()) {
        delim = s.getNumber<unsigned>();
      }
      if (auto s = slice.get("escape"); s.isNumber()) {
        escape = s.getNumber<unsigned>();
      }
      if (auto s = slice.get("null_string"); s.isString()) {
        null_string = std::string{s.stringView()};
      }
      if (auto s = slice.get("header"); s.isBool()) {
        header = s.getBool();
      }
      return std::make_shared<TextFormatOptions>(
        delim, escape, std::move(null_string), header);
    }
    case FileFormat::Parquet:
      return std::make_shared<ParquetFormatOptions>();
    case FileFormat::Dwrf:
      return std::make_shared<DwrfFormatOptions>();
    case FileFormat::Orc:
      return std::make_shared<OrcFormatOptions>();
    case FileFormat::None:
      break;
  }
  return nullptr;
}

}  // namespace sdb::pg
