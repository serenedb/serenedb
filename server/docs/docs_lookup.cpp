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

#include "docs/docs_lookup.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_split.h>

#include <algorithm>
#include <utility>

namespace sdb::docs {
namespace {

std::string Lower(std::string_view text) { return absl::AsciiStrToLower(text); }

bool IsTitleRowOf(const Doc& doc, std::string_view page) {
  return doc.path.size() > page.size() && doc.path.starts_with(page) &&
         doc.path[page.size()] == '#' &&
         doc.path.find('#', page.size() + 1) == std::string_view::npos;
}

size_t CountOccurrences(std::string_view haystack, std::string_view needle,
                        size_t cap) {
  if (needle.empty()) {
    return 0;
  }
  size_t count = 0;
  size_t pos = 0;
  while (count < cap) {
    pos = haystack.find(needle, pos);
    if (pos == std::string_view::npos) {
      break;
    }
    ++count;
    pos += needle.size();
  }
  return count;
}

}  // namespace

std::size_t HeadingDepth(std::string_view path) {
  std::size_t depth = 0;
  for (std::size_t i = 0; i < path.size(); ++i) {
    if (path[i] == '#' && (i == 0 || path[i - 1] != '\\')) {
      ++depth;
    }
  }
  return depth;
}

const Doc* FindByPath(std::string_view path) {
  if (path.empty()) {
    return nullptr;
  }
  const auto docs = GetDocs();
  const auto exact =
    absl::c_find_if(docs, [&](const Doc& doc) { return doc.path == path; });
  if (exact != docs.end()) {
    return &*exact;
  }
  const Doc* promoted = nullptr;
  for (const auto& doc : docs) {
    if (IsTitleRowOf(doc, path)) {
      if (promoted != nullptr) {
        return nullptr;
      }
      promoted = &doc;
    }
  }
  return promoted;
}

std::vector<const Doc*> Lookup(std::string_view name) {
  std::vector<const Doc*> hits;
  if (name.empty()) {
    return hits;
  }
  const auto wanted = Lower(name);
  const auto docs = GetDocs();

  for (const auto& doc : docs) {
    if (Lower(doc.title) == wanted) {
      hits.push_back(&doc);
    }
  }
  if (!hits.empty()) {
    return hits;
  }

  const auto call = absl::StrCat(wanted, "(");
  for (const auto& doc : docs) {
    if (absl::StartsWith(Lower(doc.title), call)) {
      hits.push_back(&doc);
    }
  }
  if (!hits.empty()) {
    return hits;
  }

  const auto word = absl::StrCat(wanted, " ");
  for (const auto& doc : docs) {
    if (absl::StartsWith(Lower(doc.title), word)) {
      hits.push_back(&doc);
    }
  }
  return hits;
}

std::vector<const Doc*> Similar(std::string_view name, size_t limit) {
  if (name.empty() || limit == 0) {
    return {};
  }
  std::vector<std::string> terms;
  for (const auto term : absl::StrSplit(name, ' ', absl::SkipEmpty())) {
    terms.push_back(Lower(term));
  }
  if (terms.empty()) {
    return {};
  }

  std::vector<std::pair<size_t, const Doc*>> scored;
  for (const auto& doc : GetDocs()) {
    const auto title = Lower(doc.title);
    const auto breadcrumb = Lower(doc.breadcrumb);
    const auto path = Lower(doc.path);
    const auto content = Lower(doc.content);
    size_t score = 0;
    bool all_present = true;
    for (const auto& term : terms) {
      const auto in_title = CountOccurrences(title, term, 2);
      const auto in_breadcrumb = CountOccurrences(breadcrumb, term, 2);
      const auto in_path = CountOccurrences(path, term, 2);
      const auto in_content = CountOccurrences(content, term, 4);
      if (in_title + in_breadcrumb + in_path + in_content == 0) {
        all_present = false;
        break;
      }
      const bool path_segment =
        absl::StartsWith(path, absl::StrCat(term, "/")) ||
        absl::StrContains(path, absl::StrCat("/", term, "/")) ||
        absl::StrContains(path, absl::StrCat("/", term, "."));
      score += 8 * in_title + 3 * in_breadcrumb + 2 * in_path + in_content +
               (path_segment ? 20 : 0);
    }
    if (all_present) {
      scored.emplace_back(score, &doc);
    }
  }

  absl::c_stable_sort(scored, [](const auto& lhs, const auto& rhs) {
    if (lhs.first != rhs.first) {
      return lhs.first > rhs.first;
    }
    if (lhs.second->title.size() != rhs.second->title.size()) {
      return lhs.second->title.size() < rhs.second->title.size();
    }
    return lhs.second->path < rhs.second->path;
  });
  std::vector<const Doc*> hits;
  for (const auto& [score, doc] : scored) {
    if (hits.size() >= limit) {
      break;
    }
    hits.push_back(doc);
  }
  return hits;
}

std::vector<const Doc*> ListPrefix(std::string_view prefix, bool pages_only) {
  std::vector<const Doc*> hits;
  for (const auto& doc : GetDocs()) {
    if (!doc.path.starts_with(prefix)) {
      continue;
    }
    if (pages_only && HeadingDepth(doc.path) > 1) {
      continue;
    }
    hits.push_back(&doc);
  }
  absl::c_sort(
    hits, [](const Doc* lhs, const Doc* rhs) { return lhs->path < rhs->path; });
  return hits;
}

std::vector<std::string> CompletePath(std::string_view prefix, size_t limit) {
  std::vector<std::string> out;
  if (!prefix.contains('/')) {
    std::string previous;
    for (const auto& doc : GetDocs()) {
      const auto slash = doc.path.find('/');
      const auto section = doc.path.substr(
        0, slash == std::string_view::npos ? doc.path.size() : slash);
      if (section != previous && section.starts_with(prefix)) {
        previous = section;
        if (absl::c_find(out, section) == out.end()) {
          out.emplace_back(section);
        }
      }
    }
  }
  for (const auto& doc : GetDocs()) {
    if (out.size() >= limit) {
      break;
    }
    const auto page = doc.path.substr(0, doc.path.find('#'));
    if (page.starts_with(prefix) && absl::c_find(out, page) == out.end()) {
      out.emplace_back(page);
    }
  }
  return out;
}

std::vector<const Doc*> Children(std::string_view path) {
  std::vector<const Doc*> hits;
  const auto depth = HeadingDepth(path);
  for (const auto& doc : GetDocs()) {
    if (doc.path.size() <= path.size() || !doc.path.starts_with(path) ||
        doc.path[path.size()] != '#') {
      continue;
    }
    if (HeadingDepth(doc.path) == depth + 1) {
      hits.push_back(&doc);
    }
  }
  absl::c_sort(
    hits, [](const Doc* lhs, const Doc* rhs) { return lhs->path < rhs->path; });
  return hits;
}

std::string ResolveDocLink(std::string_view base_path, std::string_view href) {
  auto target = href;
  if (const auto hash = target.find('#'); hash != std::string_view::npos) {
    target = target.substr(0, hash);
  }
  if (!target.ends_with(".md") && !target.ends_with(".mdx")) {
    return {};
  }
  auto page = base_path;
  if (const auto hash = page.find('#'); hash != std::string_view::npos) {
    page = page.substr(0, hash);
  }
  const auto slash = page.find_last_of('/');
  const auto combined =
    slash == std::string_view::npos || target.starts_with('/')
      ? std::string{target}
      : absl::StrCat(page.substr(0, slash), "/", target);

  std::vector<std::string_view> stack;
  for (const std::string_view part :
       absl::StrSplit(combined, '/', absl::SkipEmpty())) {
    if (part == "..") {
      if (!stack.empty()) {
        stack.pop_back();
      }
    } else if (part != ".") {
      stack.push_back(part);
    }
  }
  const auto resolved = absl::StrJoin(stack, "/");
  if (FindByPath(resolved) != nullptr) {
    return resolved;
  }
  return {};
}

}  // namespace sdb::docs
