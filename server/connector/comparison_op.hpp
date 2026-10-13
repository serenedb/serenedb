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

#include <iresearch/utils/system_compiler.hpp>

namespace sdb::connector {

enum class ComparisonOp {
  None,
  Lt,
  Le,
  Gt,
  Ge,
};

constexpr ComparisonOp InvertComparisonOp(ComparisonOp op) noexcept {
  switch (op) {
    case ComparisonOp::Le:
      return ComparisonOp::Gt;
    case ComparisonOp::Ge:
      return ComparisonOp::Lt;
    case ComparisonOp::Gt:
      return ComparisonOp::Le;
    case ComparisonOp::Lt:
      return ComparisonOp::Ge;
    case ComparisonOp::None:
      return ComparisonOp::None;
  }
  SDB_UNREACHABLE();
}

constexpr ComparisonOp SwapComparisonOp(ComparisonOp op) noexcept {
  switch (op) {
    case ComparisonOp::Lt:
      return ComparisonOp::Gt;
    case ComparisonOp::Le:
      return ComparisonOp::Ge;
    case ComparisonOp::Gt:
      return ComparisonOp::Lt;
    case ComparisonOp::Ge:
      return ComparisonOp::Le;
    case ComparisonOp::None:
      return ComparisonOp::None;
  }
  SDB_UNREACHABLE();
}

}  // namespace sdb::connector
