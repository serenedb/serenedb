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

#pragma once

#include <memory>
#include <optional>
#include <span>
#include <vector>
#include <yaclib/coro/task.hpp>

#include "iresearch/formats/ann_build_env.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

struct BasicTermReader;
class ColReader;
class ColumnReader;
class IdxWriter;
struct MergeSource;

class AnnWriter {
 public:
  virtual ~AnnWriter() = default;

  void SetIdxWriter(IdxWriter& idx) noexcept { _idx = &idx; }

  virtual AnnKind Kind() const noexcept = 0;

  virtual field_id ColumnId() const noexcept = 0;

  virtual bool Empty() const noexcept = 0;

  virtual void SetMergeSources(std::span<const MergeSource>) noexcept {}

  virtual yaclib::Task<> Compute(const ColumnReader& col, ReadContext& ctx,
                                 const AnnBuildEnv* env) = 0;

  virtual void Flush() = 0;

  virtual const BasicTermReader* ClusterReader(ReadContext&, const ColReader&) {
    return nullptr;
  }

 protected:
  IdxWriter* _idx = nullptr;
};

inline std::vector<const BasicTermReader*> PrepareIvfClusterReaders(
  std::span<const std::unique_ptr<AnnWriter>> writers, ColReader* col_reader,
  std::optional<ReadContext>& ctx) {
  std::vector<const BasicTermReader*> out;
  if (col_reader == nullptr || writers.empty()) {
    return out;
  }
  ctx.emplace(*col_reader);
  out.reserve(writers.size());
  for (const auto& w : writers) {
    if (!w) {
      continue;
    }
    if (const auto* r = w->ClusterReader(ctx.value(), *col_reader)) {
      out.push_back(r);
    }
  }
  return out;
}

}  // namespace irs
