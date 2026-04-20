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

#include <vpack/builder.h>
#include <vpack/slice.h>

#include <memory>
#include <string>
#include <utility>

#include "catalog/storage_options.h"
#include "catalog/types.h"

namespace sdb {

// Bag of name -> value WITH-options for an external table. Forwarded
// verbatim to DuckDB's file reader (read_parquet / read_csv_auto /
// read_json_auto) at scan time. DuckDB decides which names it accepts.
//
// The catalog stores everything as strings; DuckDB's reader does its own
// type coercion when it consumes the named_parameter_map_t.
class FormatOptions {
 public:
  FormatOptions() = default;
  explicit FormatOptions(std::vector<std::pair<std::string, std::string>> pairs)
    : _pairs{std::move(pairs)} {}

  const std::vector<std::pair<std::string, std::string>>& Options()
    const noexcept {
    return _pairs;
  }

  void toVPack(vpack::Builder& b) const;
  static std::shared_ptr<FormatOptions> fromVPack(vpack::Slice slice);

 private:
  std::vector<std::pair<std::string, std::string>> _pairs;
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

}  // namespace sdb
