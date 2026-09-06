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

#include <span>
#include <yaclib/async/future.hpp>

#include "iresearch/formats/ann_build_env.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ColumnReader;
class IdxWriter;
class ReadContext;
struct MergeSource;

class AnnWriter {
 public:
  virtual ~AnnWriter() = default;

  void SetIdxWriter(IdxWriter& idx) noexcept { _idx = &idx; }

  virtual AnnKind Kind() const noexcept = 0;

  virtual field_id ColumnId() const noexcept = 0;

  virtual bool Empty() const noexcept = 0;

  // Segments this build merges, in output order; empty for a flush. Lets a
  // backend seed itself from what the sources already computed instead of
  // rebuilding from scratch. The span outlives Compute.
  virtual void SetMergeSources(std::span<const MergeSource>) noexcept {}

  virtual auto Compute(const ColumnReader& col, ReadContext& ctx,
                       const AnnBuildEnv* env) -> yaclib::Future<> = 0;

  virtual void Flush() = 0;

 protected:
  IdxWriter* _idx = nullptr;
};

}  // namespace irs
