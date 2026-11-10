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

#include "podofo_tables.hpp"

#include <podofo/main/PdfError.h>
#include <podofo/main/PdfContentStreamReader.h>
#include <podofo/main/PdfPage.h>
#include <podofo/main/PdfMath.h>

#include <absl/algorithm/container.h>

#include <algorithm>
#include <cmath>
#include <stdexcept>

namespace duckdb::pdf {
namespace {

constexpr double kTolerance = 2;
constexpr size_t kMaxRules = 10000;
constexpr size_t kMaxWork = 4000000;

class Components {
public:
	explicit Components(size_t count) : _parents(count) {
		absl::c_iota(_parents, 0);
	}
	size_t Find(size_t i) {
		while (_parents[i] != i) {
			_parents[i] = _parents[_parents[i]];
			i = _parents[i];
		}
		return i;
	}
	void Join(size_t a, size_t b) {
		a = Find(a);
		b = Find(b);
		_parents[std::max(a, b)] = std::min(a, b);
	}

private:
	std::vector<size_t> _parents;
};

struct Edge {
	double position;
	double start;
	double end;
};

std::vector<double> Coordinates(const std::vector<Edge> &edges) {
	std::vector<double> result;
	for (const auto &edge : edges) {
		result.push_back(edge.position);
	}
	absl::c_sort(result);
	std::vector<double> distinct;
	for (double value : result) {
		if (distinct.empty() || value - distinct.back() > kTolerance) {
			distinct.push_back(value);
		}
	}
	return distinct;
}

bool Covers(const std::vector<Edge> &edges, double position, double start, double end) {
	for (const auto &edge : edges) {
		if (std::abs(edge.position - position) <= kTolerance && edge.start <= start + kTolerance) {
			start = std::max(start, edge.end);
		}
	}
	return start >= end - kTolerance;
}

} // namespace

std::vector<Rule> GetPageRules(const PoDoFo::PdfPage &page, absl::FunctionRef<void()> interrupt) {
	PoDoFo::PdfContentReaderArgs args;
	args.Flags = PoDoFo::PdfContentReaderFlags::SkipFetchInlineImages;
	PoDoFo::PdfContentStreamReader reader(page, args);
	PoDoFo::PdfContent content;
	std::vector<PoDoFo::Matrix> states(1);
	struct FormState {
		size_t depth;
		std::vector<Rule> path;
		PoDoFo::Vector2 current;
		PoDoFo::Vector2 origin;
		bool has_current;
	};
	std::vector<FormState> forms;
	std::vector<Rule> path;
	std::vector<Rule> result;
	PoDoFo::Vector2 current, origin;
	bool has_current = false;
	const auto rect = page.GetRectRaw().GetNormalized();
	auto point = [&](double x, double y) {
		return PoDoFo::Vector2(x, y) * states.back();
	};
	auto line = [&](const PoDoFo::Vector2 &to) {
		if (has_current) {
			path.push_back({current.X - rect.X, rect.GetTop() - current.Y, to.X - rect.X, rect.GetTop() - to.Y});
			if (path.size() > kMaxRules) {
				throw std::length_error("read_pdf_tables: page has too many path segments");
			}
		}
		current = to;
	};
	while (reader.TryReadNext(content)) {
		interrupt();
		if (content.HasErrors() || (content.GetWarnings() & PoDoFo::PdfContentWarnings::RecursiveXObject) !=
		                               PoDoFo::PdfContentWarnings::None) {
			throw PoDoFo::PdfError {PoDoFo::PdfErrorCode::InvalidContentStream, __FILE__, __LINE__};
		}
		if (content.GetType() == PoDoFo::PdfContentType::BeginFormXObject) {
			if (forms.size() >= 128) {
				throw std::length_error("read_pdf_tables: form nesting is too deep");
			}
			forms.push_back({states.size(), std::move(path), current, origin, has_current});
			path.clear();
			has_current = false;
			states.push_back(content->XObject->GetMatrix() * states.back());
			continue;
		}
		if (content.GetType() == PoDoFo::PdfContentType::EndFormXObject) {
			states.resize(forms.back().depth);
			path = std::move(forms.back().path);
			current = forms.back().current;
			origin = forms.back().origin;
			has_current = forms.back().has_current;
			forms.pop_back();
			continue;
		}
		if (content.GetType() != PoDoFo::PdfContentType::Operator) {
			continue;
		}
		const auto &s = content->Stack;
		switch (content->Operator) {
		case PoDoFo::PdfOperator::q:
			if (states.size() >= kMaxRules) {
				throw std::length_error("read_pdf_tables: graphics state nesting is too deep");
			}
			states.push_back(states.back());
			break;
		case PoDoFo::PdfOperator::Q:
			if (states.size() > (forms.empty() ? 1 : forms.back().depth + 1)) {
				states.pop_back();
			}
			break;
		case PoDoFo::PdfOperator::cm:
			states.back() = PoDoFo::Matrix(s[5].GetReal(), s[4].GetReal(), s[3].GetReal(), s[2].GetReal(),
			                               s[1].GetReal(), s[0].GetReal()) *
			                states.back();
			break;
		case PoDoFo::PdfOperator::m:
			current = origin = point(s[1].GetReal(), s[0].GetReal());
			has_current = true;
			break;
		case PoDoFo::PdfOperator::l:
			line(point(s[1].GetReal(), s[0].GetReal()));
			break;
		case PoDoFo::PdfOperator::c:
		case PoDoFo::PdfOperator::v:
		case PoDoFo::PdfOperator::y:
			current = point(s[1].GetReal(), s[0].GetReal());
			break;
		case PoDoFo::PdfOperator::h:
			line(origin);
			break;
		case PoDoFo::PdfOperator::re: {
			const double x = s[3].GetReal(), y = s[2].GetReal(), w = s[1].GetReal(), h = s[0].GetReal();
			current = origin = point(x, y);
			has_current = true;
			line(point(x + w, y));
			line(point(x + w, y + h));
			line(point(x, y + h));
			line(origin);
			break;
		}
		case PoDoFo::PdfOperator::s:
		case PoDoFo::PdfOperator::b:
		case PoDoFo::PdfOperator::b_Star:
			line(origin);
			[[fallthrough]];
		case PoDoFo::PdfOperator::S:
		case PoDoFo::PdfOperator::B:
		case PoDoFo::PdfOperator::B_Star:
		case PoDoFo::PdfOperator::f:
		case PoDoFo::PdfOperator::F:
		case PoDoFo::PdfOperator::f_Star:
			result.insert(result.end(), path.begin(), path.end());
			if (result.size() > kMaxRules) {
				throw std::length_error("read_pdf_tables: page has too many ruling lines");
			}
			[[fallthrough]];
		case PoDoFo::PdfOperator::n:
			path.clear();
			has_current = false;
			break;
		default:
			break;
		}
	}
	const auto rotation = PoDoFo::GetFrameRotationTransform(rect, page.GetRotationRadians());
	const auto displayed = rect * rotation;
	for (auto &rule : result) {
		auto a = PoDoFo::Vector2(rule.x1 + rect.X, rect.GetTop() - rule.y1) * rotation;
		auto b = PoDoFo::Vector2(rule.x2 + rect.X, rect.GetTop() - rule.y2) * rotation;
		rule = {a.X - displayed.X, displayed.GetTop() - a.Y, b.X - displayed.X, displayed.GetTop() - b.Y};
	}
	return result;
}

std::vector<Table> GetRuledTables(std::span<const Rule> rules, std::span<const TextWord> words,
                                  absl::FunctionRef<void()> interrupt) {
	std::vector<Edge> horizontal, vertical;
	for (const auto &r : rules) {
		if (!std::isfinite(r.x1) || !std::isfinite(r.x2) || !std::isfinite(r.y1) || !std::isfinite(r.y2)) {
			continue;
		}
		if (std::abs(r.y1 - r.y2) <= kTolerance && std::abs(r.x1 - r.x2) > 2 * kTolerance) {
			horizontal.push_back({(r.y1 + r.y2) / 2, std::min(r.x1, r.x2), std::max(r.x1, r.x2)});
		} else if (std::abs(r.x1 - r.x2) <= kTolerance && std::abs(r.y1 - r.y2) > 2 * kTolerance) {
			vertical.push_back({(r.x1 + r.x2) / 2, std::min(r.y1, r.y2), std::max(r.y1, r.y2)});
		}
	}
	if (horizontal.size() > kMaxRules || vertical.size() > kMaxRules ||
	    horizontal.size() * vertical.size() > kMaxWork) {
		throw std::length_error("read_pdf_tables: ruling grid is too complex");
	}
	Components connected(horizontal.size() + vertical.size());
	for (size_t h = 0; h < horizontal.size(); ++h) {
		interrupt();
		const auto &a = horizontal[h];
		for (size_t v = 0; v < vertical.size(); ++v) {
			const auto &b = vertical[v];
			if (b.position >= a.start - kTolerance && b.position <= a.end + kTolerance &&
			    a.position >= b.start - kTolerance && a.position <= b.end + kTolerance) {
				connected.Join(h, horizontal.size() + v);
			}
		}
	}
	struct RulingGroup {
		std::vector<Edge> horizontal;
		std::vector<Edge> vertical;
	};
	std::vector<RulingGroup> groups(horizontal.size() + vertical.size());
	for (size_t i = 0; i < horizontal.size(); ++i) {
		groups[connected.Find(i)].horizontal.push_back(horizontal[i]);
	}
	for (size_t i = 0; i < vertical.size(); ++i) {
		groups[connected.Find(horizontal.size() + i)].vertical.push_back(vertical[i]);
	}
	std::vector<Table> tables;
	for (auto &edges : groups) {
		interrupt();
		auto &[hs, vs] = edges;
		if (hs.size() < 2 || vs.size() < 2) {
			continue;
		}
		auto xs = Coordinates(vs), ys = Coordinates(hs);
		auto extend = [](std::vector<double> &coordinates, const std::vector<Edge> &perpendicular) {
			double start = coordinates.front(), end = coordinates.back();
			for (const auto &edge : perpendicular) {
				start = std::min(start, edge.start);
				end = std::max(end, edge.end);
			}
			if (coordinates.front() - start > kTolerance) {
				coordinates.insert(coordinates.begin(), start);
			}
			if (end - coordinates.back() > kTolerance) {
				coordinates.push_back(end);
			}
		};
		extend(xs, hs);
		extend(ys, vs);
		if (xs.size() < 3 || ys.size() < 3) {
			continue;
		}
		auto by_start = [](const Edge &a, const Edge &b) {
			return a.start < b.start;
		};
		absl::c_sort(hs, by_start);
		absl::c_sort(vs, by_start);
		if (absl::c_count_if(ys, [&](double y) { return Covers(hs, y, xs.front(), xs.back()); }) < 2) {
			continue;
		}
		const size_t columns = xs.size() - 1, rows = ys.size() - 1, count = rows * columns;
		if (count > kMaxWork / (hs.size() + vs.size())) {
			throw std::length_error("read_pdf_tables: ruling grid is too complex");
		}
		Components cells(count);
		for (size_t row = 0; row < rows; ++row) {
			interrupt();
			for (size_t col = 0; col < columns; ++col) {
				const size_t index = row * columns + col;
				if (col && !Covers(vs, xs[col], ys[row], ys[row + 1])) {
					cells.Join(index, index - 1);
				}
				if (row && !Covers(hs, ys[row], xs[col], xs[col + 1])) {
					cells.Join(index, index - columns);
				}
			}
		}
		std::vector<size_t> cell_counts(count), cell_last(count);
		for (size_t i = 0; i < count; ++i) {
			const auto root = cells.Find(i);
			++cell_counts[root];
			cell_last[root] = i;
		}
		bool rectangular = true;
		for (size_t i = 0; i < count; ++i) {
			if (cell_counts[i] && cell_counts[i] != (cell_last[i] / columns - i / columns + 1) *
			                                            (cell_last[i] % columns - i % columns + 1)) {
				rectangular = false;
				break;
			}
		}
		if (!rectangular) {
			continue;
		}
		std::vector<std::vector<TextWord>> content(count);
		bool found_text = false;
		for (const auto &word : words) {
			const double x = (word.left + word.right) / 2, y = (word.top + word.bottom) / 2;
			if (!(x > xs.front() && x < xs.back() && y > ys.front() && y < ys.back())) {
				continue;
			}
			const size_t col = absl::c_upper_bound(xs, x) - xs.begin() - 1;
			const size_t row = absl::c_upper_bound(ys, y) - ys.begin() - 1;
			content[cells.Find(row * columns + col)].push_back(word);
			found_text = true;
		}
		if (!found_text) {
			continue;
		}
		Table table {xs.front(), ys.front(), {}};
		for (size_t row = 0; row < rows; ++row) {
			interrupt();
			auto &output = table.rows.emplace_back();
			for (size_t col = 0; col < columns; ++col) {
				output.push_back(GetReadingOrderText(content[row * columns + col], xs.back() - xs.front()));
			}
		}
		tables.push_back(std::move(table));
	}
	absl::c_sort(tables,
	             [](const Table &a, const Table &b) { return a.top == b.top ? a.left < b.left : a.top < b.top; });
	return tables;
}

} // namespace duckdb::pdf
