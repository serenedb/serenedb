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

#include "connector/duckdb_pg_text_copy.h"

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/formats/copy_from.hpp>
#include <duckdb/function/copy_function.hpp>
#include <duckdb/function/table/read_csv.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/parser/parsed_data/create_copy_function_info.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "connector/copy_to_file.h"
#include "formats/text_encoding.h"
#include "pg/connection_context.h"
#include "server/utils/message_buffer.h"

namespace sdb::connector {
namespace {

sdb::pg::SerializationContext TextCopyContext(
  duckdb::ClientContext& context, const duckdb::CopyFormatOptions& options,
  size_t columns) {
  sdb::pg::SerializationContext ctx;
  sdb::pg::FillContext(ConnectionContext::Get(context), ctx);
  SetCopyFormat(ctx, options, columns);
  return ctx;
}

class PgTextCopyEncoder final : public CopyChunkEncoder {
 public:
  PgTextCopyEncoder(duckdb::ClientContext& context,
                    std::span<const duckdb::LogicalType> types,
                    const duckdb::CopyFormatOptions& options,
                    std::span<const sdb::pg::VarFormat> formats)
    : _ctx{TextCopyContext(context, options, types.size())},
      _serializer{types, formats, _ctx},
      _shape{.delimited = true} {
    (options.suffix.empty() ? _shape.after : _shape.before) = _ctx.copy_newline;
  }

  void Encode(const duckdb::DataChunk& chunk, message::Buffer& out) final {
    _serializer.Write(out, _ctx, chunk, 0, chunk.size(), _shape);
  }

 private:
  sdb::pg::SerializationContext _ctx;
  sdb::pg::ChunkSerializer _serializer;
  sdb::pg::RowShape _shape;
};

class PgTextCopyBindData final : public CopyToFileBindData {
 public:
  PgTextCopyBindData(duckdb::vector<duckdb::LogicalType> types,
                     duckdb::CopyFormatOptions options,
                     duckdb::vector<duckdb::Identifier> names,
                     std::vector<sdb::pg::VarFormat> formats)
    : _sql_types{std::move(types)},
      _options{std::move(options)},
      _names{std::move(names)},
      _formats{std::move(formats)} {}

  duckdb::unique_ptr<duckdb::FunctionData> Copy() const final {
    return duckdb::make_uniq<PgTextCopyBindData>(_sql_types, _options, _names,
                                                 _formats);
  }
  bool Equals(const duckdb::FunctionData& other) const final {
    const auto& o = other.Cast<PgTextCopyBindData>();
    return _sql_types == o._sql_types && _names == o._names &&
           _formats == o._formats && _options.format == o._options.format &&
           _options.delimiter == o._options.delimiter &&
           _options.quote == o._options.quote &&
           _options.escape == o._options.escape &&
           _options.null_str == o._options.null_str &&
           _options.header == o._options.header &&
           _options.compression == o._options.compression;
  }

  std::unique_ptr<CopyChunkEncoder> MakeEncoder(
    duckdb::ClientContext& context) const final {
    return std::make_unique<PgTextCopyEncoder>(context, _sql_types, _options,
                                               _formats);
  }

  void WriteHeader(duckdb::ClientContext& context,
                   message::Buffer& out) const final {
    message::Writer writer{out};
    writer.Write(_options.prefix);
    if (_options.header) {
      sdb::pg::SerializationContext ctx;
      sdb::pg::FillContext(ConnectionContext::Get(context), ctx);
      SetCopyFormat(ctx, _options, _sql_types.size());
      ctx.writer = &writer;
      SerializeCopyHeader(ctx, _options, _names);
    }
    writer.Commit(false);
  }

  void WriteTrailer(message::Buffer& out) const final {
    message::Writer writer{out};
    writer.Write(_options.suffix);
    writer.Commit(false);
  }

  duckdb::FileCompressionType Compression() const final {
    return _options.compression;
  }

  std::string_view RowSeparator() const final {
    return _options.suffix.empty() ? std::string_view{} : _options.new_line;
  }

  formats::TextEncoding Encoding() const final {
    return formats::ParseTextEncoding(_options.encoding);
  }

 private:
  duckdb::vector<duckdb::LogicalType> _sql_types;
  duckdb::CopyFormatOptions _options;
  duckdb::vector<duckdb::Identifier> _names;
  std::vector<sdb::pg::VarFormat> _formats;
};

template<duckdb::CopyTextFormat Format>
duckdb::unique_ptr<duckdb::FunctionData> BindCopyTo(
  duckdb::ClientContext&, duckdb::CopyFunctionBindInput& input,
  const duckdb::vector<duckdb::Identifier>& names,
  const duckdb::vector<duckdb::LogicalType>& sql_types) {
  auto options =
    duckdb::ParseCopyFormatOptions(Format, false, input.info.options);
  duckdb::CheckCopyFormatOptions(options, false);
  const auto& compression = options.compression;
  if ((compression == duckdb::FileCompressionType::GZIP ||
       compression == duckdb::FileCompressionType::ZSTD) &&
      !duckdb::IsFileCompressed(input.file_extension, compression)) {
    input.file_extension += duckdb::CompressionExtensionFromType(compression);
  }
  auto formats = CopyFieldFormats(options, CopyNames(names));
  return duckdb::make_uniq<PgTextCopyBindData>(
    sql_types, std::move(options), input.query_names, std::move(formats));
}

template<duckdb::CopyTextFormat Format>
duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> SelectCopyTo(
  duckdb::CopyToSelectInput& input) {
  const auto options =
    duckdb::ParseCopyFormatOptions(Format, false, input.options);
  return CopyToSelect(input, options.date_format, options.timestamp_format,
                      options.rendering);
}

template<duckdb::CopyTextFormat Format>
duckdb::unique_ptr<duckdb::FunctionData> BindCopyFrom(
  duckdb::ClientContext& context, duckdb::CopyFromFunctionBindInput& input,
  duckdb::vector<duckdb::Identifier>& expected_names,
  duckdb::vector<duckdb::LogicalType>& expected_types) {
  return duckdb::PostgresCopyFromBind(context, input, expected_names,
                                      expected_types, Format);
}

template<duckdb::CopyTextFormat Format>
duckdb::CopyFunction MakeCopyFunction(const char* name) {
  duckdb::CopyFunction function{name};
  function.extension = name;
  function.copy_to_bind = BindCopyTo<Format>;
  function.copy_to_select = SelectCopyTo<Format>;
  SetCopyToFileCallbacks(function);
  function.copy_from_bind = BindCopyFrom<Format>;
  function.copy_from_function = duckdb::ReadCSVTableFunction::GetFunction();
  return function;
}

}  // namespace

void SetCopyFormat(sdb::pg::SerializationContext& context,
                   const duckdb::CopyFormatOptions& options, size_t columns) {
  if (options.format == duckdb::CopyTextFormat::CSV) {
    sdb::pg::SetCopyCsv(context, options.delimiter, options.quote,
                        options.escape, options.null_str, options.new_line,
                        columns == 1);
  } else {
    sdb::pg::SetCopyText(context, options.delimiter, options.null_str);
  }
}

std::vector<sdb::pg::VarFormat> CopyFieldFormats(
  const duckdb::CopyFormatOptions& options,
  std::span<const std::string> names) {
  if (options.format == duckdb::CopyTextFormat::TEXT) {
    return std::vector<sdb::pg::VarFormat>(names.size(),
                                           sdb::pg::VarFormat::CopyText);
  }
  const auto quoted = duckdb::CopyColumnMask(
    options.force_quote, {names.begin(), names.end()}, "FORCE_QUOTE");
  std::vector<sdb::pg::VarFormat> formats(names.size(),
                                          sdb::pg::VarFormat::CopyCsv);
  for (size_t column = 0; column < quoted.size(); ++column) {
    if (quoted[column]) {
      formats[column] = sdb::pg::VarFormat::CopyCsvQuoted;
    }
  }
  return formats;
}

void SerializeCopyHeader(sdb::pg::SerializationContext& context,
                         const duckdb::CopyFormatOptions& options,
                         std::span<const duckdb::Identifier> names) {
  if (options.format == duckdb::CopyTextFormat::CSV) {
    sdb::pg::SerializeCopyCsvHeader(context, names);
  } else {
    sdb::pg::SerializeCopyTextHeader(context, names);
  }
}

std::vector<std::string> CopyNames(std::span<const duckdb::Identifier> names) {
  std::vector<std::string> result;
  result.reserve(names.size());
  for (const auto& name : names) {
    result.push_back(name.GetIdentifierName());
  }
  return result;
}

void RegisterPgTextCopyFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader(db, "serenedb");
  loader.RegisterFunction(
    MakeCopyFunction<duckdb::CopyTextFormat::TEXT>("text"));

  auto& catalog = duckdb::Catalog::GetSystemCatalog(db);
  auto transaction = duckdb::CatalogTransaction::GetSystemTransaction(db);
  duckdb::CreateCopyFunctionInfo info{
    MakeCopyFunction<duckdb::CopyTextFormat::CSV>("csv")};
  info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  info.SetQualifiedName(duckdb::QualifiedName(info.GetQualifiedName().Catalog(),
                                              *catalog.GetDefaultSchema(),
                                              info.GetQualifiedName().Name()));
  catalog.CreateCopyFunction(transaction, info);
}

}  // namespace sdb::connector
