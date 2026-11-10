////////////////////////////////////////////////////////////////////////////////
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
/// http://www.apache.org/licenses/LICENSE-2.0
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "podofo_layout.hpp"
#include <podofo/main/PdfPage.h>

#include <functional>
#include <vector>

namespace duckdb::pdf {

struct Rule {
	double x1;
	double y1;
	double x2;
	double y2;
};

struct Table {
	double left;
	double top;
	std::vector<std::vector<std::string>> rows;
};

std::vector<Rule> GetPageRules(const PoDoFo::PdfPage &page, const std::function<void()> &interrupt);
std::vector<Table> GetRuledTables(std::span<const Rule> rules, std::span<const TextWord> words,
                                  const std::function<void()> &interrupt);

} // namespace duckdb::pdf
