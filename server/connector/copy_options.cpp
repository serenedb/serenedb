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

#include "connector/copy_options.h"

#include <absl/algorithm/container.h>

#include <duckdb/formats/copy_options.hpp>
#include <duckdb/planner/binder.hpp>

namespace sdb::connector {

duckdb::unique_ptr<duckdb::CopyInfo> BindCopyOptions(
  duckdb::ClientContext& context, const duckdb::CopyInfo& info) {
  auto bound = info.Copy();
  context.RunFunctionInTransaction(
    [&] { duckdb::Binder::CreateBinder(context)->BindCopyOptions(*bound); });
  return bound;
}

bool HasEncodingOption(const duckdb::CopyInfo& info) {
  const auto encoding = [](const auto& option) {
    return option.first == "encoding";
  };
  return absl::c_any_of(info.parsed_options, encoding) ||
         absl::c_any_of(info.options, encoding);
}

bool HasForeignCopyOption(const duckdb::CopyInfo& info) {
  const auto foreign = [](const auto& option) {
    return !duckdb::IsCopyFormatOption(option.first);
  };
  return absl::c_any_of(info.parsed_options, foreign) ||
         absl::c_any_of(info.options, foreign);
}

}  // namespace sdb::connector
