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

#include "podofo_layout.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <numeric>
#include <vector>

namespace duckdb::pdf {
namespace {

void AppendRows(std::span<const TextWord> words, std::span<size_t> order, std::string &text) {
	absl::c_stable_sort(order, [&](size_t a, size_t b) { return words[a].top < words[b].top; });
	bool continuation = false;
	size_t begin = 0;
	while (begin < order.size()) {
		double top = words[order[begin]].top;
		double bottom = words[order[begin]].bottom;
		size_t end = begin + 1;
		while (end < order.size()) {
			const auto &word = words[order[end]];
			const double overlap = std::min(bottom, word.bottom) - std::max(top, word.top);
			if (overlap < 0.5 * std::min(bottom - top, word.bottom - word.top)) {
				break;
			}
			top = std::min(top, word.top);
			bottom = std::max(bottom, word.bottom);
			++end;
		}
		auto row = order.subspan(begin, end - begin);
		absl::c_stable_sort(row, [&](size_t a, size_t b) { return words[a].left < words[b].left; });
		if (!text.empty() && !continuation) {
			text.push_back('\n');
		}
		for (size_t i = 0; i < row.size(); ++i) {
			if (i) {
				text.push_back(' ');
			}
			text.append(words[row[i]].text);
		}
		continuation = end < order.size() && text.ends_with('-');
		if (continuation) {
			text.pop_back();
		}
		begin = end;
	}
}

size_t FindSeparation(std::span<const TextWord> words, std::span<size_t> order, double minimum_gap,
                      double minimum_width, bool vertical) {
	auto start = [&](size_t i) {
		return vertical ? words[i].top : words[i].left;
	};
	auto finish = [&](size_t i) {
		return vertical ? words[i].bottom : words[i].right;
	};
	absl::c_stable_sort(order, [&](size_t a, size_t b) { return start(a) < start(b); });
	double limit = finish(order.front());
	for (size_t i : order) {
		limit = std::max(limit, finish(i));
	}
	double occupied = finish(order.front());
	double best_gap = minimum_gap;
	size_t split = 0;
	for (size_t i = 1; i < order.size(); ++i) {
		const double next = start(order[i]);
		const double gap = next - occupied;
		const bool wide_enough =
		    vertical || (occupied - start(order.front()) >= minimum_width && limit - next >= minimum_width);
		if (wide_enough && gap > best_gap) {
			best_gap = gap;
			split = i;
		}
		occupied = std::max(occupied, finish(order[i]));
	}
	return split;
}

void AppendRegion(std::span<const TextWord> words, std::span<size_t> order, double height, double page_width,
                  uint32_t depth, std::string &text) {
	if (depth < 32 && order.size() > 1) {
		size_t split = FindSeparation(words, order, 2 * height, 0, true);
		if (!split) {
			split = FindSeparation(words, order, 3 * height, 0.15 * page_width, false);
		}
		if (split) {
			AppendRegion(words, order.first(split), height, page_width, depth + 1, text);
			AppendRegion(words, order.subspan(split), height, page_width, depth + 1, text);
			return;
		}
	}
	AppendRows(words, order, text);
}

} // namespace

std::string GetReadingOrderText(std::span<const TextWord> words, double page_width) {
	std::string text;
	if (words.empty()) {
		return text;
	}
	std::vector<double> heights;
	heights.reserve(words.size());
	for (const auto &word : words) {
		if (!std::isfinite(word.left) || !std::isfinite(word.right) || !std::isfinite(word.top) ||
		    !std::isfinite(word.bottom) || word.right <= word.left || word.bottom <= word.top) {
			for (const auto &fragment : words) {
				if (!text.empty()) {
					text.push_back('\n');
				}
				text.append(fragment.text);
			}
			return text;
		}
		heights.push_back(word.bottom - word.top);
	}
	absl::c_sort(heights);
	std::vector<size_t> order(words.size());
	std::iota(order.begin(), order.end(), 0);
	AppendRegion(words, order, heights[heights.size() / 2], page_width, 0, text);
	return text;
}

} // namespace duckdb::pdf
