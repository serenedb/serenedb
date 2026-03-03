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

#include <velox/type/Type.h>

#include <memory>
#include <string>
#include <utility>

#include "basics/fwd.h"
#include "catalog/types.h"
#include "connector/file_table.hpp"
#include "vpack/builder.h"
#include "vpack/slice.h"

namespace sdb::pg {

class FormatOptions {
 public:
  virtual ~FormatOptions() = default;
  virtual connector::DwioWriterOptions createWriterOptions(
    velox::RowTypePtr schema) const = 0;
  virtual connector::DwioReaderOptions createReaderOptions(
    velox::RowTypePtr schema) const = 0;
  virtual void toVPack(vpack::Builder&) const = 0;

  FileFormat format() const noexcept { return _format; }

  static std::shared_ptr<FormatOptions> fromVPack(vpack::Slice slice);

 protected:
  FormatOptions(FileFormat format) : _format{format} {}
  FileFormat _format;
};

class TextFormatOptions : public FormatOptions {
 public:
  TextFormatOptions(uint8_t delim, uint8_t escape, std::string null_string,
                    uint8_t header)
    : FormatOptions{FileFormat::Text},
      _delim{delim},
      _escape{escape},
      _null_string{std::move(null_string)},
      _header{header} {}

  connector::DwioWriterOptions createWriterOptions(
    velox::RowTypePtr schema) const final;
  connector::DwioReaderOptions createReaderOptions(
    velox::RowTypePtr schema) const final;
  void toVPack(vpack::Builder& b) const final;

 private:
  uint8_t _delim;
  uint8_t _escape;
  std::string _null_string;
  uint8_t _header;
};

class ParquetFormatOptions : public FormatOptions {
 public:
  ParquetFormatOptions() : FormatOptions{FileFormat::Parquet} {}

  connector::DwioWriterOptions createWriterOptions(
    velox::RowTypePtr schema) const final;
  connector::DwioReaderOptions createReaderOptions(
    velox::RowTypePtr schema) const final;
  void toVPack(vpack::Builder& b) const final;
};

class DwrfFormatOptions : public FormatOptions {
 public:
  DwrfFormatOptions() : FormatOptions{FileFormat::Dwrf} {}

  connector::DwioWriterOptions createWriterOptions(
    velox::RowTypePtr schema) const final;
  connector::DwioReaderOptions createReaderOptions(
    velox::RowTypePtr schema) const final;
  void toVPack(vpack::Builder& b) const final;
};

class OrcFormatOptions : public FormatOptions {
 public:
  OrcFormatOptions() : FormatOptions{FileFormat::Orc} {}

  connector::DwioWriterOptions createWriterOptions(
    velox::RowTypePtr schema) const final;
  connector::DwioReaderOptions createReaderOptions(
    velox::RowTypePtr schema) const final;
  void toVPack(vpack::Builder& b) const final;
};

template<typename Context>
void VPackWrite(Context ctx,
                const std::shared_ptr<FormatOptions>& format_options) {
  auto& b = ctx.vpack();
  if (!format_options) {
    b.add(vpack::Slice::nullSlice());
    return;
  }
  b.openObject();
  format_options->toVPack(b);
  b.close();
}

template<typename Context>
void VPackRead(Context ctx, std::shared_ptr<FormatOptions>& format_options) {
  format_options = FormatOptions::fromVPack(ctx.vpack());
}

}  // namespace sdb::pg
