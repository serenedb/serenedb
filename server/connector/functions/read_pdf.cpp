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

#include "connector/functions/read_pdf.h"

#include <absl/strings/ascii.h>
#include <podofo/main/PdfError.h>
#include <podofo/main/PdfMemDocument.h>
#include <podofo/main/PdfPage.h>
#include <podofo/main/PdfPageCollection.h>

#include <algorithm>
#include <cstdint>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/multi_file/multi_file_reader.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace sdb::connector {
namespace {

struct PdfBindData final : duckdb::TableFunctionData {
  duckdb::vector<duckdb::OpenFileInfo> files;
  std::string password;
  uint32_t first_page = 1;
  uint32_t last_page = std::numeric_limits<uint32_t>::max();
  bool blocks = false;
};

struct PdfState final : duckdb::GlobalTableFunctionState {
  size_t file = 0;
  std::string bytes;
  std::unique_ptr<PoDoFo::PdfMemDocument> document;
  std::vector<PoDoFo::PdfTextEntry> entries;
  size_t entry = 0;
  uint32_t page = 0;
  uint32_t end_page = 0;
  double width = 0;
  double height = 0;
  bool page_loaded = false;
  bool found_text = false;
};

duckdb::unique_ptr<duckdb::FunctionData> PdfBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<duckdb::Identifier>& names) {
  auto data = duckdb::make_uniq<PdfBindData>();
  data->blocks = input.table_function.GetName() == "read_pdf_blocks";
  for (const auto& [key, value] : input.named_parameters) {
    if (value.IsNull()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("read_pdf: options must not be NULL"));
    }
    if (key == "password") {
      data->password = duckdb::StringValue::Get(value);
    } else {
      const auto page = value.GetValue<int64_t>();
      if (page < 1 || page > std::numeric_limits<uint32_t>::max()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("read_pdf: page numbers must be between 1 and 4294967295"));
      }
      if (key == "first_page") {
        data->first_page = page;
      } else {
        data->last_page = page;
      }
    }
  }
  if (data->first_page > data->last_page) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("read_pdf: first_page must not exceed last_page"));
  }
  auto reader = duckdb::MultiFileReader::Create(input.table_function);
  data->files = reader->CreateFileList(context, input.inputs[0])->GetAllFiles();
  names = {"filename", "page", "text", "width", "height", "has_text_layer"};
  types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
           duckdb::LogicalType::VARCHAR, duckdb::LogicalType::DOUBLE,
           duckdb::LogicalType::DOUBLE,  duckdb::LogicalType::BOOLEAN};
  if (data->blocks) {
    names = {"filename", "page", "block", "text", "x", "y", "width", "height"};
    types = {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
             duckdb::LogicalType::BIGINT,  duckdb::LogicalType::VARCHAR,
             duckdb::LogicalType::DOUBLE,  duckdb::LogicalType::DOUBLE,
             duckdb::LogicalType::DOUBLE,  duckdb::LogicalType::DOUBLE};
  }
  return data;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> PdfInit(
  duckdb::ClientContext&, duckdb::TableFunctionInitInput&) {
  return duckdb::make_uniq<PdfState>();
}

void OpenPdf(duckdb::ClientContext& context, const PdfBindData& bind,
             PdfState& state) {
  auto& fs = duckdb::FileSystem::GetFileSystem(context);
  const auto& path = bind.files[state.file];
  auto handle = fs.OpenFile(path, duckdb::FileFlags::FILE_FLAGS_READ);
  const auto size = fs.GetFileSize(*handle);
  if (size <= 0 || !handle->CanSeek()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("read_pdf: expected a nonempty seekable file: ", path.path));
  }
  state.bytes.resize(size);
  int64_t offset = 0;
  while (offset < size) {
    context.InterruptCheck();
    const auto count = fs.Read(*handle, state.bytes.data() + offset,
                               std::min<int64_t>(size - offset, 1024 * 1024));
    if (count <= 0) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                      ERR_MSG("read_pdf: unexpected end of file: ", path.path));
    }
    offset += count;
  }
  state.document = std::make_unique<PoDoFo::PdfMemDocument>();
  state.document->LoadFromBuffer(
    PoDoFo::bufferview{state.bytes.data(), state.bytes.size()}, bind.password);
  const auto pages = state.document->GetPages().GetCount();
  if (pages == 0) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("read_pdf: PDF has no pages: ", path.path));
  }
  if (bind.first_page > pages) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("read_pdf: first_page exceeds the PDF page count: ", path.path));
  }
  state.page = bind.first_page - 1;
  state.end_page = std::min(bind.last_page, pages);
  state.found_text = false;
  state.page_loaded = false;
}

void LoadPdfPage(duckdb::ClientContext& context, PdfState& state) {
  context.InterruptCheck();
  state.entries.clear();
  state.entry = 0;
  auto& page = state.document->GetPages().GetPageAt(state.page);
  PoDoFo::PdfTextExtractParams params;
  params.Flags = PoDoFo::PdfTextExtractFlags::ComputeBoundingBox;
  params.AbortCheck = [&](const PoDoFo::AbortCheckInfo&) {
    context.InterruptCheck();
    return false;
  };
  page.ExtractTextTo(state.entries, params);
  std::erase_if(state.entries, [](const auto& entry) {
    return absl::StripAsciiWhitespace(entry.Text).empty();
  });
  const auto rect = page.GetRect();
  state.width = rect.Width;
  state.height = rect.Height;
  state.found_text |= !state.entries.empty();
  state.page_loaded = true;
}

void SetPdfString(duckdb::Vector& vector, size_t row, std::string_view text) {
  duckdb::FlatVector::GetDataMutable<duckdb::string_t>(vector)[row] =
    duckdb::StringVector::AddString(vector, text);
}

void PdfScan(duckdb::ClientContext& context, duckdb::TableFunctionInput& input,
             duckdb::DataChunk& output) {
  const auto& bind = input.bind_data->Cast<PdfBindData>();
  auto& state = input.global_state->Cast<PdfState>();
  size_t count = 0;
  try {
    while (count < STANDARD_VECTOR_SIZE && state.file < bind.files.size()) {
      context.InterruptCheck();
      if (!state.document) {
        OpenPdf(context, bind, state);
      }
      if (state.page == state.end_page) {
        if (!state.found_text) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                          ERR_MSG("read_pdf: selected pages have no "
                                  "extractable text; OCR is not supported: ",
                                  bind.files[state.file].path));
        }
        state.entries.clear();
        state.document.reset();
        state.bytes.clear();
        ++state.file;
        continue;
      }
      if (!state.page_loaded) {
        LoadPdfPage(context, state);
      }
      if (bind.blocks && state.entry == state.entries.size()) {
        ++state.page;
        state.page_loaded = false;
        continue;
      }
      SetPdfString(output.data[0], count, bind.files[state.file].path);
      duckdb::FlatVector::GetDataMutable<int64_t>(output.data[1])[count] =
        state.page + 1;
      if (bind.blocks) {
        const auto& entry = state.entries[state.entry];
        duckdb::FlatVector::GetDataMutable<int64_t>(output.data[2])[count] =
          state.entry + 1;
        SetPdfString(output.data[3], count, entry.Text);
        duckdb::FlatVector::GetDataMutable<double>(output.data[4])[count] =
          entry.X;
        duckdb::FlatVector::GetDataMutable<double>(output.data[5])[count] =
          entry.Y;
        if (entry.BoundingBox) {
          duckdb::FlatVector::GetDataMutable<double>(output.data[6])[count] =
            entry.BoundingBox->Width;
          duckdb::FlatVector::GetDataMutable<double>(output.data[7])[count] =
            entry.BoundingBox->Height;
        } else {
          duckdb::FlatVector::SetNull(output.data[6], count, true);
          duckdb::FlatVector::SetNull(output.data[7], count, true);
        }
        ++state.entry;
      } else {
        std::string text;
        for (const auto& entry : state.entries) {
          if (!text.empty()) {
            text.push_back('\n');
          }
          text.append(entry.Text);
        }
        SetPdfString(output.data[2], count, text);
        duckdb::FlatVector::GetDataMutable<double>(output.data[3])[count] =
          state.width;
        duckdb::FlatVector::GetDataMutable<double>(output.data[4])[count] =
          state.height;
        duckdb::FlatVector::GetDataMutable<bool>(output.data[5])[count] =
          !state.entries.empty();
        ++state.page;
        state.page_loaded = false;
      }
      ++count;
    }
  } catch (const PoDoFo::PdfError& error) {
    if (error.GetCode() == PoDoFo::PdfErrorCode::InvalidPassword) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("read_pdf: PDF is encrypted; supply the correct password"));
    }
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
      ERR_MSG("read_pdf: could not extract PDF text: ", error.GetName()));
  }
  output.SetChildCardinality(count);
}

}  // namespace

void RegisterPdfFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  for (const auto name : {"read_pdf", "read_pdf_blocks"}) {
    duckdb::FunctionSignature signature;
    signature.AddPositionalOnly("path", duckdb::LogicalType::VARCHAR)
      .WithTypedKwargs("options", [](duckdb::TypedKwargs& options) {
        options.Add("password", duckdb::LogicalType::VARCHAR)
          .Add("first_page", duckdb::LogicalType::BIGINT)
          .Add("last_page", duckdb::LogicalType::BIGINT);
      });
    loader.RegisterFunction(duckdb::TableFunction{name, std::move(signature),
                                                  PdfScan, PdfBind, PdfInit});
  }
}

}  // namespace sdb::connector
