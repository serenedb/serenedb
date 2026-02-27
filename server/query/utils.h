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

#include <axiom/logical_plan/ExprPrinter.h>
#include <axiom/logical_plan/LogicalPlanNode.h>

#include <span>
#include <string>
#include <vector>

#include "basics/fwd.h"

namespace sdb::query {

inline constexpr std::string_view kColumnSeparator = ":";
inline constexpr std::string_view kReservedSymbol = "$";

// in text clean colnames: column_name<separator>unique_id -> column_name
std::string CleanColumnNames(std::string text);

// column_name<separator>unique_id -> column_name
std::string_view ToAlias(std::string_view name);
std::vector<std::string> ToAliases(std::span<const std::string> names);

bool Equals(const axiom::logical_plan::Expr& lhs,
            const axiom::logical_plan::Expr& rhs);

bool Equals(const axiom::logical_plan::Expr* lhs,
            const axiom::logical_plan::Expr* rhs);

}  // namespace sdb::query
