////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2024 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Valery Mironov
////////////////////////////////////////////////////////////////////////////////

#include "iresearch/search/wildcard_ngram_filter.hpp"

#include <absl/base/internal/endian.h>

#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>

#include "basics/down_cast.h"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "iresearch/columnstore/column_reader.hpp"
#include "iresearch/columnstore/format.hpp"
#include "iresearch/columnstore/read_context.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {
namespace {

// Convert a SQL LIKE pattern to a RE2 regex pattern.
// '%' -> '.*', '_' -> '.', backslash escapes, all other regex chars escaped.
std::shared_ptr<RE2> BuildLikeMatcher(std::string_view pattern) {
  std::string regex;
  regex.reserve(pattern.size() * 2);
  regex += "\\A";  // anchor start
  bool escaped = false;
  for (char c : pattern) {
    if (escaped) {
      escaped = false;
      // Escape regex-special characters
      if (absl::StrContains("\\[](){}.*+?|^$", std::string_view{&c, 1})) {
        regex += '\\';
      }
      regex += c;
    } else if (c == '\\') {
      escaped = true;
    } else if (c == '%') {
      regex += ".*";
    } else if (c == '_') {
      regex += '.';
    } else {
      // Escape regex-special characters
      if (absl::StrContains("\\[](){}.*+?|^$", std::string_view{&c, 1})) {
        regex += '\\';
      }
      regex += c;
    }
  }
  regex += "\\z";  // anchor end
  RE2::Options opts;
  opts.set_dot_nl(true);  // '.' matches newline, equivalent to UREGEX_DOTALL
  auto re = std::make_shared<RE2>(regex, opts);
  if (!re->ok()) {
    return nullptr;
  }
  return re;
}

class WildcardIterator : public DocIterator {
 public:
  // Takes shared ownership of the RE2 matcher to guarantee it outlives the
  // iterator. RE2 is immutable and thread-safe for concurrent matching, so
  // no per-iterator clone is needed (unlike icu::RegexMatcher).
  //
  // Stored term list lives in the columnstore as a BLOB column; the
  // BlobPointReader owns its own scratch + ReadContext and returns a
  // bytes_view per doc that the varint-parsing loop below walks.
  WildcardIterator(std::shared_ptr<RE2> matcher, DocIterator::ptr&& approx,
                   const columnstore::ColumnReader& stored_field,
                   const columnstore::Reader& cs_reader)
    : _matcher{std::move(matcher)},
      _approx{std::move(approx)},
      _cursor{cs_reader, stored_field} {
    SDB_ASSERT(_approx);
    SDB_ASSERT(_matcher);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _approx->GetMutable(type);
  }

  doc_id_t advance() final {
    while (!doc_limits::eof(_approx->advance())) {
      if (Check(_approx->value())) {
        return _doc = _approx->value();
      }
    }
    return _doc = doc_limits::eof();
  }

  doc_id_t seek(doc_id_t target) final {
    target = _approx->seek(target);
    if (Check(target)) {
      return _doc = target;
    }
    return advance();
  }

 private:
  bool Check(doc_id_t doc) {
    // Per-doc point fetch via cached cursor; cursor reuses its open
    // ColumnSegment across consecutive docs in the same row group.
    // Empty span = null OR analyzer stored zero bytes -- either way skip.
    const auto value = _cursor.FetchDoc(doc);
    if (value.empty()) {
      return false;
    }
    auto* terms_begin = value.data();
    auto* terms_end = terms_begin + value.size();
    while (terms_begin != terms_end) {
      auto size = vread<uint32_t>(terms_begin);
      ++terms_begin;  // skip begin marker

      re2::StringPiece term{reinterpret_cast<const char*>(terms_begin),
                            static_cast<size_t>(size)};
      if (RE2::PartialMatch(term, *_matcher)) {
        return true;
      }

      terms_begin += size + 1;  // skip data and end marker
    }

    return false;
  }

  std::shared_ptr<RE2> _matcher;
  DocIterator::ptr _approx;
  columnstore::ColumnReader::BlobPointReader _cursor;
};

class WildcardQuery : public Filter::Query {
 public:
  WildcardQuery(std::shared_ptr<RE2> matcher, Query::ptr&& approx,
                field_id store_field_id)
    : _matcher{std::move(matcher)},
      _approx{std::move(approx)},
      _store_field_id{store_field_id} {
    SDB_ASSERT(_approx);
  }

  DocIterator::ptr execute(const ExecutionContext& ctx) const final {
    auto approx = _approx->execute(ctx);
    if (!_matcher || approx == DocIterator::empty()) {
      return approx;
    }
    SDB_ASSERT(_store_field_id != 0);
    const auto* cs_reader = ctx.segment.CsReader();
    if (!cs_reader) {
      return DocIterator::empty();
    }
    const auto* column = cs_reader->Column(_store_field_id);
    if (column == nullptr) {
      return DocIterator::empty();
    }
    return memory::make_managed<WildcardIterator>(_matcher, std::move(approx),
                                                  *column, *cs_reader);
  }

  void visit(const SubReader&, PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return kNoBoost; }

 private:
  std::shared_ptr<RE2> _matcher;
  Query::ptr _approx;
  field_id _store_field_id;
};

constexpr size_t kDefaultScoredTermsLimit = 1024;

}  // namespace

ByWildcardNgram::Buffer::Buffer(const PrepareContext& ctx,
                                std::string_view field,
                                const ByWildcardNgramOptions& opts,
                                score_t boost)
  : ScoredBuffer{ctx, boost},
    _field{field},
    _matcher{opts.matcher},
    _store_field_id{opts.store_field_id} {
  // Children inherit our cumulative boost via `child_ctx`; each child's
  // ScoredBuffer will fold this into its own `_boost` at construction.
  const auto child_ctx = ctx.Boost(boost);
  auto& parts = opts.parts;
  const auto size = parts.size();

  if (size == 0) {
    bytes_view token = opts.token;
    if (token.size() != 1 && token.back() == 0xFF) {
      _children.push_back(
        std::make_unique<ByTerm::Buffer>(child_ctx, field, token));
    } else {
      if (token.back() == 0xFF) {
        token = kEmptyStringView<byte_type>;
      }
      _children.push_back(std::make_unique<ByPrefix::Buffer>(
        child_ctx, field, token, kDefaultScoredTermsLimit));
    }
    _single = true;
    return;
  }

  if (size == 1 && opts.has_pos) {
    _children.push_back(
      ByPhrase::CreateFixedBuffer(child_ctx, field, parts[0]));
    _single = true;
    return;
  }

  _single = false;
  if (opts.has_pos) {
    _children.reserve(size);
    for (const auto& part : parts) {
      _children.push_back(ByPhrase::CreateFixedBuffer(child_ctx, field, part));
    }
  } else {
    for (const auto& part : parts) {
      for (const auto& info : part) {
        _children.push_back(std::make_unique<ByTerm::Buffer>(
          child_ctx, field, std::get<ByTermOptions>(info.part).term));
      }
    }
  }
}

void ByWildcardNgram::Buffer::PrepareSegment(const SubReader& segment) {
  for (auto& child : _children) {
    child->PrepareSegment(segment);
  }
}

void ByWildcardNgram::Buffer::Merge(PrepareBuffer&& other) {
  auto& rhs = sdb::basics::downCast<Buffer>(other);
  SDB_ASSERT(_children.size() == rhs._children.size());
  for (size_t i = 0; i < _children.size(); ++i) {
    _children[i]->Merge(std::move(*rhs._children[i]));
  }
}

bool ByWildcardNgram::Buffer::Empty() const noexcept {
  for (const auto& child : _children) {
    if (child->Empty()) {
      return true;
    }
  }
  return false;
}

Filter::Query::ptr ByWildcardNgram::Buffer::Compile(
  const PrepareContext& ctx) && {
  // `_boost` is the cumulative boost from root. Children already captured
  // it at construction; the AndQuery wrapping them needs it explicitly.
  auto sub_ctx = ctx;
  sub_ctx.boost = _boost;
  if (_single) {
    auto child = std::move(_children.front());
    auto inner = std::move(*child).Compile(sub_ctx);
    return memory::make_tracked<WildcardQuery>(
      ctx.memory, std::move(_matcher), std::move(inner), _store_field_id);
  }

  AndQuery::queries_t queries{{ctx.memory}};
  queries.reserve(_children.size());
  for (auto& cbuf : _children) {
    auto child = std::move(cbuf);
    queries.emplace_back(std::move(*child).Compile(sub_ctx));
  }
  const auto excl_start = queries.size();
  auto conjunction = memory::make_tracked<AndQuery>(ctx.memory);
  conjunction->prepare(sub_ctx, ScoreMergeType::Sum, std::move(queries),
                       excl_start);
  return memory::make_tracked<WildcardQuery>(
    ctx.memory, std::move(_matcher), std::move(conjunction), _store_field_id);
}

Filter::Query::ptr ByWildcardNgram::Prepare(
  const PrepareContext& ctx, std::string_view field,
  const ByWildcardNgramOptions& opts) {
  Buffer buf{ctx, field, opts};
  return Filter::PrepareWithBuffer(buf, ctx);
}

ByWildcardNgramOptions::ByWildcardNgramOptions(
  std::string_view pattern, analysis::WildcardAnalyzer& analyzer,
  bool has_positions) {
  auto& ngram = analyzer.ngram();
  const auto* term = irs::get<TermAttr>(ngram);

  auto make_parts_impl = [&](std::string_view v) {
    if (!ngram.reset(v)) {
      return false;
    }
    ByPhraseOptions part;
    while (ngram.next()) {
      part.push_back<ByTermOptions>(ByTermOptions{bstring{term->value}});
    }
    if (part.empty()) {
      return false;
    }
    parts.push_back(std::move(part));
    return true;
  };

  bytes_view best;
  auto make_parts = [&](const char* begin, const char* end) {
    SDB_ASSERT(begin <= end);
    std::string_view v{begin, end};
    if (!make_parts_impl(v) && best.size() <= v.size()) {
      best = ViewCast<byte_type>(v);
    }
  };

  std::string pattern_str;
  pattern_str.resize(2 + pattern.size());
  auto* pattern_first = pattern_str.data();
  auto* pattern_last = pattern_first;
  *pattern_last++ = '\xFF';
  auto* pattern_curr = pattern.data();
  auto* pattern_end = pattern_curr + pattern.size();
  bool needs_matcher = false;
  bool escaped = false;
  for (; pattern_curr != pattern_end; ++pattern_curr) {
    if (escaped) {
      escaped = false;
      *pattern_last++ = *pattern_curr;
    } else if (*pattern_curr == '\\') {
      escaped = true;
    } else if (*pattern_curr == '_' || *pattern_curr == '%') {
      if (*pattern_curr == '_' ||
          (pattern_curr != pattern.data() && pattern_curr != pattern_end - 1)) {
        needs_matcher = true;
      }
      make_parts(pattern_first, pattern_last);
      pattern_first = pattern_last;
    } else {
      *pattern_last++ = *pattern_curr;
    }
  }
  // We ignore escaped because post-filtering ignores it
  if (pattern_first != pattern_last) {
    *pattern_last++ = '\xFF';
    make_parts(pattern_first, pattern_last);
  }
  if (parts.empty()) {
    SDB_ASSERT(!best.empty());
    token = best;
  } else {
    has_pos = has_positions;
  }
  if (needs_matcher || !has_pos) {
    matcher = BuildLikeMatcher(pattern);
  }
}

}  // namespace irs
