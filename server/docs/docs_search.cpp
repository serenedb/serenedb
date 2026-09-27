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

#include "docs/docs_search.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>

#include <algorithm>
#include <array>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/object_cache.hpp>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/index_reader_options.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/parser/parser.hpp>
#include <iresearch/search/detail/doc_collector.hpp>
#include <iresearch/search/detail/lazy_bitset.hpp>
#include <iresearch/search/fill/docs_mask.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/search/filters/levenshtein_filter.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/prefix_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/span_directory.hpp>
#include <iresearch/utils/containers/flat_hash_map.hpp>
#include <iresearch/utils/containers/flat_hash_set.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <iresearch/utils/utf8_utils.hpp>
#include <markdown_utils.hpp>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <tuple>
#include <utility>

#include "connector/functions/markdown_render.h"
#include "docs/docs_index_data.h"

namespace sdb::docs {
namespace {

constexpr std::string_view kIndexKey = "sdb_docs_index";
constexpr size_t kFuzzyTerms = 50;
constexpr std::string_view kEllipsis = "\xE2\x80\xA6";

struct Column {
  irs::field_id stored = irs::field_limits::invalid();
  irs::field_id terms = irs::field_limits::invalid();
};

struct Layout {
  Column path;
  Column title;
  Column breadcrumb;
  Column content;
  Column content_text;

  std::array<irs::field_id, 3> Text() const noexcept {
    return {title.terms, breadcrumb.terms, content_text.terms};
  }
};

constexpr std::array<std::string_view, 9> kObjectColumns{
  "kind", "name", "signature", "summary",   "aliases",
  "path", "page", "category",  "breadcrumb"};

struct ObjectLayout {
  std::array<irs::field_id, kObjectColumns.size()> stored;
  irs::field_id names = irs::field_limits::invalid();
  irs::field_id kind = irs::field_limits::invalid();
};

class LayoutColumns {
 public:
  explicit LayoutColumns(std::span<const IndexFile> files) {
    const auto file = absl::c_find_if(
      files, [](const IndexFile& file) { return file.name == kLayoutFile; });
    if (file == files.end()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("the documentation index carries no ", kLayoutFile));
    }
    const std::string_view text{
      reinterpret_cast<const char*>(file->bytes.data()), file->bytes.size()};
    for (const std::string_view line :
         absl::StrSplit(text, '\n', absl::SkipEmpty())) {
      const std::vector<std::string_view> parts = absl::StrSplit(line, ' ');
      Column column;
      if (parts.size() < 2 || parts.size() > 3 ||
          (parts[1] != "-" && !absl::SimpleAtoi(parts[1], &column.stored)) ||
          (parts.size() == 3 && !absl::SimpleAtoi(parts[2], &column.terms))) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                        ERR_MSG("malformed ", kLayoutFile, " line: ", line));
      }
      _columns.emplace(parts[0], column);
    }
  }

  irs::field_id Stored(std::string_view name) const {
    const auto id = Find(name).stored;
    if (!irs::field_limits::valid(id)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG(kLayoutFile, " names no column '", name, "'"));
    }
    return id;
  }

  irs::field_id Terms(std::string_view name) const {
    const auto id = Find(name).terms;
    if (!irs::field_limits::valid(id)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG(kLayoutFile, " names no terms for '", name, "'"));
    }
    return id;
  }

 private:
  Column Find(std::string_view name) const {
    const auto it = _columns.find(name);
    return it == _columns.end() ? Column{} : it->second;
  }

  irs::containers::FlatHashMap<std::string_view, Column> _columns;
};

Layout ReadLayout(std::span<const IndexFile> files) {
  const LayoutColumns columns{files};
  const auto both = [&](std::string_view name) {
    return Column{.stored = columns.Stored(name), .terms = columns.Terms(name)};
  };
  return {.path = both("path"),
          .title = both("title"),
          .breadcrumb = both("breadcrumb"),
          .content = {.stored = columns.Stored("content")},
          .content_text = {.terms = columns.Terms("content_text")}};
}

ObjectLayout ReadObjectLayout(std::span<const IndexFile> files) {
  const LayoutColumns columns{files};
  ObjectLayout layout;
  absl::c_transform(
    kObjectColumns, layout.stored.begin(),
    [&](std::string_view name) { return columns.Stored(name); });
  layout.names = columns.Terms("names");
  layout.kind = columns.Terms("kind");
  return layout;
}

std::vector<std::string_view> Aliases(std::string_view aliases) {
  std::vector<std::string_view> names;
  for (std::string_view alias : absl::StrSplit(aliases, ',')) {
    alias = absl::StripAsciiWhitespace(alias);
    if (!alias.empty()) {
      names.push_back(alias);
    }
  }
  return names;
}

bool OfKind(const Object& object, std::string_view kind) {
  return kind.empty() || absl::EqualsIgnoreCase(object.kind, kind);
}

class DocsIndex final : public duckdb::ObjectCacheEntry {
 public:
  explicit DocsIndex(duckdb::DatabaseInstance& db)
    : _layout{ReadLayout(GetDocsIndex())},
      _object_layout{ReadObjectLayout(GetObjectsIndex())},
      _docs{Open(db, GetDocsIndex())},
      _objects{Open(db, GetObjectsIndex())} {}

  static std::string ObjectType() { return std::string{kIndexKey}; }
  std::string GetObjectType() final { return ObjectType(); }
  duckdb::optional_idx GetEstimatedCacheMemory() const final { return {}; }

  const irs::DirectoryReader& Reader() const noexcept { return _docs.reader; }
  const Layout& Fields() const noexcept { return _layout; }
  const irs::DirectoryReader& ObjectReader() const noexcept {
    return _objects.reader;
  }
  const ObjectLayout& ObjectFields() const noexcept { return _object_layout; }

 private:
  struct Opened {
    std::unique_ptr<irs::SpanDirectory> dir;
    irs::DirectoryReader reader;
  };

  static Opened Open(duckdb::DatabaseInstance& db,
                     std::span<const IndexFile> files) {
    irs::SpanDirectory::Files views;
    for (const auto& file : files) {
      views.emplace(file.name,
                    irs::bytes_view{file.bytes.data(), file.bytes.size()});
    }
    static const irs::ResourceManagementOptions kResourceManager;
    auto dir =
      std::make_unique<irs::SpanDirectory>(std::move(views), kResourceManager);
    irs::IndexReaderOptions options;
    options.db = &db;
    irs::DirectoryReader reader{*dir, irs::formats::Get("1_5simd"), options};
    return {.dir = std::move(dir), .reader = std::move(reader)};
  }

  Layout _layout;
  ObjectLayout _object_layout;
  Opened _docs;
  Opened _objects;
};

irs::analysis::Tokenizer::ptr DocsTokenizer() {
  return irs::analysis::TextTokenizer::Make({});
}

duckdb::shared_ptr<DocsIndex> AcquireIndex(duckdb::DatabaseInstance& db) {
  if (GetDocsIndex().empty()) {
    return nullptr;
  }
  return db.GetObjectCache().GetOrCreate<DocsIndex>(std::string{kIndexKey}, db);
}

template<typename Body>
auto WithIndex(duckdb::DatabaseInstance& db, Body&& body)
  -> decltype(body(std::declval<const DocsIndex&>())) {
  const auto index = AcquireIndex(db);
  if (!index) {
    return {};
  }
  return body(*index);
}

irs::Filter::ptr Term(irs::field_id field, std::string_view term) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field_id() = field;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return filter;
}

irs::Filter::ptr Prefix(irs::field_id field, std::string_view prefix) {
  auto filter = std::make_unique<irs::ByPrefix>();
  *filter->mutable_field_id() = field;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(prefix);
  return filter;
}

irs::Filter::ptr PathTerm(const Layout& layout, std::string_view path) {
  return Term(layout.path.terms, path);
}

irs::Filter::ptr PathPrefix(const Layout& layout, std::string_view prefix) {
  return Prefix(layout.path.terms, prefix);
}

class EntryFetcher {
 public:
  EntryFetcher(const irs::SubReader& segment, const Layout& layout,
               Content content) {
    const auto* col_reader = segment.GetColReader();
    if (!col_reader) {
      return;
    }
    const auto open = [&](const Column& column, Reader& reader) {
      if (const auto* stored = segment.Column(column.stored)) {
        reader.emplace(*col_reader, *stored);
        return true;
      }
      return false;
    };
    if (!open(layout.path, _path) || !open(layout.title, _title) ||
        !open(layout.breadcrumb, _breadcrumb) ||
        (content == Content::Include && !open(layout.content, _content))) {
      _path.reset();
    }
  }

  bool Valid() const noexcept { return _path.has_value(); }

  std::string_view Path(irs::doc_id_t doc) {
    return irs::ViewCast<char>(_path->FetchDoc(doc));
  }

  std::optional<Entry> Fetch(irs::doc_id_t doc) {
    const auto path = Path(doc);
    if (path.empty()) {
      return std::nullopt;
    }
    Entry entry{.path = std::string{path},
                .title = Read(_title, doc),
                .breadcrumb = Read(_breadcrumb, doc)};
    if (_content) {
      entry.content = Read(_content, doc);
    }
    return entry;
  }

 private:
  using Reader = std::optional<irs::ColumnReader::BlobPointReader>;

  static std::string Read(Reader& reader, irs::doc_id_t doc) {
    return std::string{irs::ViewCast<char>(reader->FetchDoc(doc))};
  }

  Reader _path;
  Reader _title;
  Reader _breadcrumb;
  Reader _content;
};

class ObjectFetcher {
 public:
  ObjectFetcher(const irs::SubReader& segment, const ObjectLayout& layout) {
    const auto* col_reader = segment.GetColReader();
    if (!col_reader) {
      return;
    }
    for (size_t i = 0; i < _columns.size(); ++i) {
      const auto* stored = segment.Column(layout.stored[i]);
      if (!stored) {
        return;
      }
      _columns[i].emplace(*col_reader, *stored);
    }
    _valid = true;
  }

  bool Valid() const noexcept { return _valid; }

  Object Fetch(irs::doc_id_t doc) {
    const auto text = [&](size_t i) {
      return std::string{irs::ViewCast<char>(_columns[i]->FetchDoc(doc))};
    };
    return {.kind = text(0),
            .name = text(1),
            .signature = text(2),
            .summary = text(3),
            .aliases = text(4),
            .path = text(5),
            .page = text(6),
            .category = text(7),
            .breadcrumb = text(8)};
  }

 private:
  std::array<std::optional<irs::ColumnReader::BlobPointReader>,
             kObjectColumns.size()>
    _columns;
  bool _valid = false;
};

template<typename MakeFetcher, typename Fn>
void ForEachMatchIn(const irs::DirectoryReader& reader,
                    const irs::Filter& filter, MakeFetcher&& make, Fn&& fn) {
  for (const auto& segment : reader) {
    auto query = filter.PrepareSegment(segment, {});
    if (!query || irs::QueryBuilder::IsEmpty(*query)) {
      continue;
    }
    auto node = query->PlanFill({}, irs::ScoreMergeType::Noop);
    if (!node) {
      continue;
    }
    const auto docs_count = static_cast<irs::doc_id_t>(segment.docs_count());
    std::optional<irs::detail::LazyBitset> live;
    if (auto* folded = node->Folded()) {
      live.emplace(std::move(*folded), irs::fill::DocsMask{segment});
    } else {
      live.emplace(std::move(node), docs_count, irs::fill::DocsMask{segment});
    }
    auto fetcher = make(segment);
    if (!fetcher.Valid()) {
      continue;
    }
    for (auto doc = live->Probe(irs::doc_limits::min());
         !irs::doc_limits::eof(doc); doc = live->Probe(doc + 1)) {
      fn(fetcher, doc);
    }
  }
}

template<typename Fn>
void ForEachMatch(const DocsIndex& index, const irs::Filter& filter,
                  Content content, Fn&& fn) {
  ForEachMatchIn(
    index.Reader(), filter,
    [&](const irs::SubReader& segment) {
      return EntryFetcher{segment, index.Fields(), content};
    },
    std::forward<Fn>(fn));
}

std::vector<Object> MatchingObjects(const DocsIndex& index,
                                    const irs::Filter& filter) {
  std::vector<Object> objects;
  ForEachMatchIn(
    index.ObjectReader(), filter,
    [&](const irs::SubReader& segment) {
      return ObjectFetcher{segment, index.ObjectFields()};
    },
    [&](ObjectFetcher& fetcher, irs::doc_id_t doc) {
      objects.push_back(fetcher.Fetch(doc));
    });
  std::ranges::sort(objects, {}, [](const Object& object) {
    return std::tie(object.kind, object.name, object.signature, object.path);
  });
  return objects;
}

template<typename Keep>
std::vector<Entry> CollectMatches(const DocsIndex& index,
                                  const irs::Filter& filter, Content content,
                                  Keep keep) {
  std::vector<Entry> out;
  ForEachMatch(index, filter, content,
               [&](EntryFetcher& fetcher, irs::doc_id_t doc) {
                 if (!keep(fetcher.Path(doc))) {
                   return;
                 }
                 if (auto entry = fetcher.Fetch(doc)) {
                   out.push_back(std::move(*entry));
                 }
               });
  return out;
}

std::vector<Entry> CollectMatches(const DocsIndex& index,
                                  const irs::Filter& filter, Content content) {
  return CollectMatches(index, filter, content,
                        [](std::string_view) { return true; });
}

std::vector<Entry> SortedByPath(std::vector<Entry> hits) {
  std::ranges::sort(hits, {}, &Entry::path);
  return hits;
}

std::vector<irs::bstring> Analyze(irs::analysis::Tokenizer& tokenizer,
                                  std::string_view text) {
  irs::ValueAnalyzer analyzer;
  irs::ValueTokens<> tokens;
  analyzer.Analyze(
    tokenizer,
    duckdb::string_t{text.data(), static_cast<uint32_t>(text.size())}, tokens);
  return tokens.terms() | std::views::transform([](const auto& token) {
           return irs::bstring{irs::AsBytesView(token)};
         }) |
         std::ranges::to<std::vector>();
}

void OptimizeScored(irs::Filter::ptr& filter,
                    std::span<const irs::field_id> fields) {
  irs::OptimizeContext optimize_ctx;
  optimize_ctx.scored = true;
  optimize_ctx.analyzed_fields.insert(fields.begin(), fields.end());
  irs::Optimize(filter, optimize_ctx);
}

irs::Filter::ptr AnyTerm(irs::field_id field, std::vector<irs::bstring> terms) {
  if (terms.empty()) {
    return nullptr;
  }
  auto any = std::make_unique<irs::BooleanFilter>();
  for (auto& term : terms) {
    any->Add(irs::TermClause{.field = field, .term = std::move(term)},
             irs::Occur::Should);
  }
  any->SetMinShouldMatch(1);
  return any;
}

irs::Filter::ptr Similar(std::string_view name,
                         irs::analysis::Tokenizer& tokenizer,
                         std::span<const irs::field_id> fields) {
  auto any = std::make_unique<irs::BooleanFilter>();
  for (const std::string_view word :
       absl::StrSplit(name, ' ', absl::SkipEmpty())) {
    auto tokens = Analyze(tokenizer, word);
    const auto term = tokens.size() == 1
                        ? std::move(tokens.front())
                        : irs::bstring{irs::ViewCast<irs::byte_type>(word)};
    for (const auto field : fields) {
      auto prefix = std::make_unique<irs::ByPrefix>();
      *prefix->mutable_field_id() = field;
      prefix->mutable_options()->term = term;
      any->Add(std::move(prefix), irs::Occur::Should);

      auto fuzzy = std::make_unique<irs::ByEditDistance>();
      *fuzzy->mutable_field_id() = field;
      auto& options = *fuzzy->mutable_options();
      options.term = term;
      options.max_distance = 1;
      options.max_terms = kFuzzyTerms;
      options.with_transpositions = true;
      any->Add(std::move(fuzzy), irs::Occur::Should);
    }
  }
  if (any->Size(irs::Occur::Should) == 0) {
    return nullptr;
  }
  any->SetMinShouldMatch(1);
  irs::Filter::ptr filter = std::move(any);
  OptimizeScored(filter, fields);
  return filter;
}

class DocsFields final : public irs::ParserContext::FieldProvider {
 public:
  DocsFields(irs::analysis::Tokenizer& tokenizer, const Layout& layout)
    : _tokenizer{&tokenizer}, _layout{&layout} {}

  bool Resolve(std::string_view name,
               irs::ParserContext::Field& out) const final {
    if (name == "path") {
      out = {.id = _layout->path.terms, .tokenizer = &_keyword};
      return true;
    }
    const auto* column = name == "title"        ? &_layout->title
                         : name == "breadcrumb" ? &_layout->breadcrumb
                         : name == "content"    ? &_layout->content_text
                                                : nullptr;
    if (!column) {
      return false;
    }
    out = {.id = column->terms, .tokenizer = _tokenizer};
    return true;
  }

 private:
  irs::analysis::Tokenizer* _tokenizer;
  const Layout* _layout;
  mutable irs::KeywordTokenizer _keyword;
};

constexpr std::string_view kStopwords[] = {
  "a",     "an",   "and",   "are",   "as",    "at",   "be",     "by",   "can",
  "could", "do",   "does",  "for",   "from",  "get",  "how",    "i",    "if",
  "in",    "into", "is",    "it",    "its",   "let",  "make",   "me",   "my",
  "no",    "not",  "of",    "on",    "or",    "our",  "should", "so",   "than",
  "that",  "the",  "their", "then",  "there", "this", "to",     "use",  "using",
  "we",    "what", "when",  "where", "which", "why",  "will",   "with", "would",
  "yes",   "you",  "your"};

constexpr irs::score_t kPhraseBoost = 2;
constexpr irs::score_t kStemBoost = 0.1F;

std::string_view Stem(std::string_view word) {
  constexpr std::string_view kSuffixes[] = {
    "ations", "ation", "ings", "ing", "ions", "ion", "ates",
    "ate",    "ies",   "es",   "ed",  "s",    "e"};
  for (const auto suffix : kSuffixes) {
    if (word.size() >= suffix.size() + 4 && word.ends_with(suffix)) {
      return word.substr(0, word.size() - suffix.size());
    }
  }
  return word;
}

bool LooksLikeLucene(std::string_view query) {
  if (query.find_first_of("\"*~^\\") != std::string_view::npos) {
    return true;
  }
  constexpr std::string_view kFieldPrefixes[] = {
    "title:", "breadcrumb:", "content:", "path:"};
  for (const std::string_view word :
       absl::StrSplit(query, absl::ByAnyChar(" \t\n"), absl::SkipEmpty())) {
    if (word == "AND" || word == "OR" || word == "NOT" ||
        word.starts_with('+') ||
        (word.size() > 1 && word.front() == '-' &&
         absl::ascii_isalpha(static_cast<unsigned char>(word[1]))) ||
        absl::c_any_of(kFieldPrefixes, [&](std::string_view prefix) {
          return word.starts_with(prefix);
        })) {
      return true;
    }
  }
  return false;
}

std::unique_ptr<irs::BooleanFilter> Parse(std::string_view query,
                                          const DocsIndex& index) {
  const auto tokenizer = DocsTokenizer();
  const DocsFields provider{*tokenizer, index.Fields()};
  auto root = std::make_unique<irs::BooleanFilter>();
  for (const auto field : index.Fields().Text()) {
    auto branch = std::make_unique<irs::BooleanFilter>();
    auto& branch_ref = *branch;
    root->Add(std::move(branch), irs::Occur::Should);

    irs::ParserContext parser_ctx{branch_ref, field, *tokenizer};
    parser_ctx.fields = &provider;
    try {
      if (!irs::ParseQuery(parser_ctx, query)) {
        return nullptr;
      }
    } catch (const std::exception&) {
      return nullptr;
    }
  }
  root->SetMinShouldMatch(1);
  return root;
}

std::unique_ptr<irs::BooleanFilter> Words(std::string_view text,
                                          const DocsIndex& index) {
  const auto ordered = Analyze(*DocsTokenizer(), text);
  auto terms = ordered;
  absl::c_sort(terms);
  terms.erase(std::unique(terms.begin(), terms.end()), terms.end());
  auto kept = terms;
  std::erase_if(kept, [](const irs::bstring& term) {
    const auto word = irs::ViewCast<char>(irs::bytes_view{term});
    return word.size() < 2 || absl::c_linear_search(kStopwords, word);
  });
  if (kept.empty()) {
    kept = std::move(terms);
  }
  if (kept.empty()) {
    return nullptr;
  }
  auto root = std::make_unique<irs::BooleanFilter>();
  for (const auto field : index.Fields().Text()) {
    root->Add(AnyTerm(field, kept), irs::Occur::Should);
    for (const auto& term : kept) {
      const auto stem = Stem(irs::ViewCast<char>(irs::bytes_view{term}));
      if (stem.size() < 4) {
        continue;
      }
      auto prefix = std::make_unique<irs::ByPrefix>();
      *prefix->mutable_field_id() = field;
      prefix->mutable_options()->term = irs::ViewCast<irs::byte_type>(stem);
      prefix->SetBoost(kStemBoost);
      root->Add(std::move(prefix), irs::Occur::Should);
    }
    if (ordered.size() < 2) {
      continue;
    }
    auto phrase = std::make_unique<irs::ByPhrase>();
    *phrase->mutable_field_id() = field;
    for (const auto& term : ordered) {
      phrase->mutable_options()->push_back<irs::ByTermOptions>().term = term;
    }
    phrase->SetBoost(kPhraseBoost);
    root->Add(std::move(phrase), irs::Occur::Should);
  }
  root->SetMinShouldMatch(1);
  return root;
}

irs::Filter::ptr Compile(std::string_view query, const DocsIndex& index,
                         std::string& error) {
  auto root = LooksLikeLucene(query) ? Parse(query, index) : nullptr;
  if (root && irs::ContainsNegation(*root)) {
    error =
      "exclusion (-term or NOT) is not supported here yet: the documentation "
      "is searched as three separate fields, so an exclusion would only apply "
      "to the field it matched in";
    return nullptr;
  }
  if (!root) {
    root = Words(query, index);
  }
  if (!root) {
    return nullptr;
  }
  irs::Filter::ptr filter = std::move(root);
  OptimizeScored(filter, index.Fields().Text());
  return filter;
}

std::vector<Entry> RunScored(const DocsIndex& index, const irs::Filter& filter,
                             size_t limit, Content content) {
  const auto& reader = index.Reader();
  const auto scorer = irs::BM25::Make({});
  const size_t capacity = std::max<size_t>(reader.live_docs_count(), 1);
  std::vector<irs::ScoreDoc> hits(capacity);
  const auto matched = irs::ExecuteTopK(reader, filter, *scorer, capacity,
                                        /*score_prune=*/false, std::span{hits});

  hits.resize(std::min(matched, capacity));
  std::ranges::sort(hits, {}, [](const irs::ScoreDoc& hit) {
    return std::tie(hit.segment_idx, hit.doc);
  });

  std::vector<std::optional<EntryFetcher>> fetchers(reader.size());
  const auto fetcher = [&](uint32_t segment) -> EntryFetcher& {
    auto& slot = fetchers[segment];
    if (!slot) {
      slot.emplace(reader[segment], index.Fields(), content);
    }
    return *slot;
  };

  struct Ranked {
    std::string path;
    const irs::ScoreDoc* hit;
  };
  std::vector<Ranked> ranked;
  ranked.reserve(hits.size());
  for (const auto& hit : hits) {
    auto& source = fetcher(hit.segment_idx);
    if (!source.Valid()) {
      continue;
    }
    if (const auto path = source.Path(hit.doc); !path.empty()) {
      ranked.push_back({.path = std::string{path}, .hit = &hit});
    }
  }
  absl::c_sort(ranked, [](const Ranked& lhs, const Ranked& rhs) {
    return std::tie(rhs.hit->score, lhs.path) <
           std::tie(lhs.hit->score, rhs.path);
  });
  if (ranked.size() > limit) {
    ranked.resize(limit);
  }

  std::vector<Entry> out;
  out.reserve(ranked.size());
  for (const auto& [path, hit] : ranked) {
    if (auto entry = fetcher(hit->segment_idx).Fetch(hit->doc)) {
      entry->score = hit->score;
      out.push_back(std::move(*entry));
    }
  }
  return out;
}

std::optional<Entry> ExactPath(const DocsIndex& index, std::string_view path,
                               Content content) {
  auto exact =
    CollectMatches(index, *PathTerm(index.Fields(), path), content,
                   [&](std::string_view found) { return found == path; });
  if (exact.empty()) {
    return std::nullopt;
  }
  return std::move(exact.front());
}

std::optional<Entry> PageEntry(const DocsIndex& index, std::string_view path,
                               Content content) {
  if (auto exact = ExactPath(index, path, content)) {
    return exact;
  }
  const auto page = absl::StrCat(path, "#");
  auto titles = CollectMatches(index, *PathPrefix(index.Fields(), page),
                               content, [&](std::string_view found) {
                                 return found.find('#', page.size()) ==
                                        std::string_view::npos;
                               });
  if (titles.size() != 1) {
    return std::nullopt;
  }
  return std::move(titles.front());
}

std::string Slug(std::string_view title) {
  std::string slug;
  for (const auto c : title) {
    const auto byte = static_cast<unsigned char>(c);
    if (absl::ascii_isalnum(byte) || c == '-' || c == '_' || byte >= 0x80) {
      slug.push_back(absl::ascii_tolower(byte));
    } else if (c == ' ') {
      slug.push_back('-');
    }
  }
  return slug;
}

std::optional<std::string> SectionForAnchor(const DocsIndex& index,
                                            std::string_view page,
                                            std::string_view anchor) {
  if (anchor.empty()) {
    return std::nullopt;
  }
  const auto wanted = absl::AsciiStrToLower(anchor);
  const auto call = absl::StrCat(wanted, "(");
  std::optional<std::string> best;
  int best_rank = 3;
  for (const auto& section : SortedByPath(CollectMatches(
         index, *PathPrefix(index.Fields(), absl::StrCat(page, "#")),
         Content::Omit))) {
    const auto slug = Slug(section.title);
    const auto title = absl::AsciiStrToLower(section.title);
    const int rank = slug == wanted                               ? 0
                     : title == wanted || title.starts_with(call) ? 1
                     : slug.starts_with(wanted)                   ? 2
                                                                  : 3;
    if (rank < best_rank) {
      best_rank = rank;
      best = section.path;
    }
  }
  return best;
}

std::optional<std::string> OnlyPageNamed(const DocsIndex& index,
                                         std::string_view page) {
  const auto file = absl::StrCat("/", page.substr(page.find_last_of('/') + 1));
  std::vector<std::string> pages;
  for (const auto& entry :
       CollectMatches(index, *PathPrefix(index.Fields(), ""), Content::Omit,
                      [&](std::string_view found) {
                        return found.substr(0, found.find('#')).ends_with(file);
                      })) {
    auto owner = entry.path.substr(0, entry.path.find('#'));
    if (!absl::c_linear_search(pages, owner)) {
      pages.push_back(std::move(owner));
    }
  }
  if (pages.size() != 1) {
    return std::nullopt;
  }
  return std::move(pages.front());
}

std::vector<Object> NamedObjects(const DocsIndex& index, std::string_view name,
                                 std::string_view kind) {
  const auto wanted = absl::StripAsciiWhitespace(kind);
  auto found = MatchingObjects(
    index,
    *Term(index.ObjectFields().names,
          absl::AsciiStrToLower(absl::StripAsciiWhitespace(CallName(name)))));
  std::erase_if(found,
                [&](const Object& object) { return !OfKind(object, wanted); });
  std::ranges::sort(found, {}, [](const Object& object) {
    return std::tie(object.kind, object.path, object.signature);
  });
  return found;
}

size_t Occurrences(std::string_view text, std::string_view needle) {
  size_t count = 0;
  for (auto at = text.find(needle); at != std::string_view::npos;
       at = text.find(needle, at + needle.size())) {
    ++count;
  }
  return count;
}

std::vector<Entry> Literal(const DocsIndex& index, std::string_view text,
                           size_t limit, Content content) {
  const auto needle = absl::StripAsciiWhitespace(text);
  std::vector<Entry> hits;
  if (needle.empty()) {
    return hits;
  }
  ForEachMatch(index, *PathPrefix(index.Fields(), ""), Content::Include,
               [&](EntryFetcher& fetcher, irs::doc_id_t doc) {
                 auto entry = fetcher.Fetch(doc);
                 if (!entry) {
                   return;
                 }
                 const auto in_title = Occurrences(entry->title, needle);
                 if (in_title == 0 && !entry->content.contains(needle)) {
                   return;
                 }
                 const auto in_body = Occurrences(
                   duckdb::markdown_utils::MarkdownToText(entry->content),
                   needle);
                 if (in_title + in_body > 0) {
                   entry->score = static_cast<double>(10 * in_title + in_body);
                   hits.push_back(std::move(*entry));
                 }
               });
  absl::c_sort(hits, [](const Entry& lhs, const Entry& rhs) {
    return lhs.score != rhs.score ? lhs.score > rhs.score : lhs.path < rhs.path;
  });
  if (hits.size() > limit) {
    hits.resize(limit);
  }
  if (content == Content::Omit) {
    for (auto& hit : hits) {
      hit.content.clear();
    }
  }
  return hits;
}

std::vector<Entry> KnownFirst(const DocsIndex& index, std::string_view query,
                              std::vector<Entry> hits, size_t limit,
                              Content content) {
  std::vector<Entry> known;
  const auto listed = [&](std::string_view path) {
    return absl::c_any_of(
      known, [&](const Entry& entry) { return entry.path == path; });
  };
  for (const auto& object : NamedObjects(index, query, {})) {
    if (listed(object.path)) {
      continue;
    }
    if (auto entry = ExactPath(index, object.path, content)) {
      known.push_back(std::move(*entry));
    }
  }
  if (known.empty()) {
    return hits;
  }
  const auto top = hits.empty() ? 1.0 : hits.front().score;
  for (auto& entry : known) {
    entry.score = top;
  }
  for (auto& hit : hits) {
    if (!listed(hit.path)) {
      known.push_back(std::move(hit));
    }
  }
  if (known.size() > limit) {
    known.resize(limit);
  }
  return known;
}

}  // namespace

void CheckLayout(std::span<const IndexFile> files) { ReadLayout(files); }

std::string Markdown(const Entry& entry) {
  if (absl::StartsWith(absl::StripLeadingAsciiWhitespace(entry.content),
                       "# ")) {
    return entry.content;
  }
  return absl::StrCat("# ", entry.title, "\n\n", entry.content);
}

std::string Snippet(std::string_view text, size_t limit) {
  auto flat = absl::StrJoin(
    absl::StrSplit(text, absl::ByAnyChar(" \t\n\v\f\r"), absl::SkipEmpty()),
    " ");
  const auto* begin = reinterpret_cast<const irs::byte_type*>(flat.data());
  const auto* end = begin + flat.size();
  const auto* cut = begin;
  for (size_t n = 0; n < limit && cut != end; ++n) {
    cut = irs::utf8_utils::Next(cut, end);
  }
  if (cut != end) {
    flat.resize(static_cast<size_t>(cut - begin));
    flat.append(kEllipsis);
  }
  return flat;
}

std::size_t HeadingDepth(std::string_view path) {
  std::size_t depth = 0;
  for (std::size_t i = 0; i < path.size(); ++i) {
    if (path[i] == '#' && (i == 0 || path[i - 1] != '\\')) {
      ++depth;
    }
  }
  return depth;
}

std::string_view CallName(std::string_view term) {
  const auto paren = term.find('(');
  if (paren == std::string_view::npos || paren == 0) {
    return term;
  }
  const auto name = absl::StripTrailingAsciiWhitespace(term.substr(0, paren));
  return absl::c_all_of(
           name,
           [](char c) {
             return absl::ascii_isalnum(static_cast<unsigned char>(c)) ||
                    c == '_' || c == '.';
           })
           ? name
           : term;
}

std::string SiteRoute(std::string_view link) {
  const auto scheme = link.find("://");
  if (scheme == std::string_view::npos) {
    return std::string{link};
  }
  auto rest = link.substr(scheme + 3);
  const auto end = std::min(rest.find_first_of("/?#"), rest.size());
  const auto host = absl::AsciiStrToLower(rest.substr(0, end));
  if (host != "serenedb.com" && !host.ends_with(".serenedb.com")) {
    return std::string{link};
  }
  rest = rest.substr(end);
  const auto hash = std::min(rest.find('#'), rest.size());
  std::vector<std::string_view> parts = absl::StrSplit(
    rest.substr(0, std::min(rest.find('?'), hash)), '/', absl::SkipEmpty());
  if (!parts.empty() && parts.front() == "docs") {
    parts.erase(parts.begin());
  }
  return absl::StrCat(absl::StrJoin(parts, "/"), rest.substr(hash));
}

std::optional<Entry> FindByPath(duckdb::DatabaseInstance& db,
                                std::string_view path) {
  if (path.empty()) {
    return std::nullopt;
  }
  return WithIndex(db, [&](const DocsIndex& index) {
    return PageEntry(index, path, Content::Include);
  });
}

std::optional<Entry> ResolveLink(duckdb::DatabaseInstance& db,
                                 std::string_view link, std::string_view base,
                                 Content content) {
  const auto route = SiteRoute(absl::StripAsciiWhitespace(link));
  link = route;
  if (link.empty() || connector::IsExternal(link)) {
    return std::nullopt;
  }
  return WithIndex(db, [&](const DocsIndex& index) -> std::optional<Entry> {
    if (auto exact = ExactPath(index, link, content)) {
      return exact;
    }
    for (auto parent = link.rfind('#');
         parent != std::string_view::npos && parent != link.find('#');
         parent = link.rfind('#', parent - 1)) {
      if (auto ancestor = ExactPath(index, link.substr(0, parent), content)) {
        return ancestor;
      }
    }
    const auto hash = link.find('#');
    const auto anchor = hash == std::string_view::npos ? std::string_view{}
                                                       : link.substr(hash + 1);
    std::string page{link.substr(0, hash)};
    if (page.empty()) {
      page = base.substr(0, base.find('#'));
    } else if (auto resolved = connector::ResolveHref(base, page);
               !resolved.empty()) {
      page = std::move(resolved);
    }
    if (page.empty()) {
      return std::nullopt;
    }
    const auto resolve = [&](std::string_view at) -> std::optional<Entry> {
      if (const auto section = SectionForAnchor(index, at, anchor)) {
        if (auto entry = ExactPath(index, *section, content)) {
          return entry;
        }
      }
      return PageEntry(index, at, content);
    };
    if (auto entry = resolve(page)) {
      return entry;
    }
    if (!page.ends_with(".md") && !page.ends_with(".mdx")) {
      const auto stem = absl::StripSuffix(page, "/");
      for (const std::string_view suffix :
           {".md", ".mdx", "/index.md", "/index.mdx"}) {
        if (auto entry = resolve(absl::StrCat(stem, suffix))) {
          return entry;
        }
      }
    }
    if (const auto only = OnlyPageNamed(index, page)) {
      return resolve(*only);
    }
    return std::nullopt;
  });
}

std::vector<Entry> Lookup(duckdb::DatabaseInstance& db, std::string_view name,
                          Content content) {
  if (name.empty()) {
    return {};
  }
  return WithIndex(db, [&](const DocsIndex& index) {
    const auto& layout = index.Fields();
    const auto filter =
      AnyTerm(layout.title.terms, Analyze(*DocsTokenizer(), name));
    if (!filter) {
      return std::vector<Entry>{};
    }
    const auto call = absl::StrCat(name, "(");
    const auto word = absl::StrCat(name, " ");
    const auto rank = [&](const Entry& entry) {
      if (absl::EqualsIgnoreCase(entry.title, name)) {
        return 0;
      }
      if (absl::StartsWithIgnoreCase(entry.title, call)) {
        return 1;
      }
      return absl::StartsWithIgnoreCase(entry.title, word) ? 2 : 3;
    };
    auto hits = CollectMatches(index, *filter, Content::Omit);
    if (hits.empty()) {
      return hits;
    }
    const auto best = std::ranges::min(hits | std::views::transform(rank));
    if (best == 3) {
      return std::vector<Entry>{};
    }
    std::erase_if(hits,
                  [&](const Entry& entry) { return rank(entry) != best; });
    if (content == Content::Include) {
      for (auto& hit : hits) {
        if (auto full = ExactPath(index, hit.path, content)) {
          hit = std::move(*full);
        }
      }
    }
    return SortedByPath(std::move(hits));
  });
}

std::vector<Object> Objects(duckdb::DatabaseInstance& db,
                            std::string_view kind) {
  return WithIndex(db, [&](const DocsIndex& index) {
    const auto field = index.ObjectFields().kind;
    const auto wanted = absl::AsciiStrToLower(absl::StripAsciiWhitespace(kind));
    return MatchingObjects(
      index, *(wanted.empty() ? Prefix(field, "") : Term(field, wanted)));
  });
}

std::vector<Object> FindObjects(duckdb::DatabaseInstance& db,
                                std::string_view name, std::string_view kind) {
  return WithIndex(db, [&](const DocsIndex& index) {
    return NamedObjects(index, name, kind);
  });
}

std::vector<std::string> CompleteName(duckdb::DatabaseInstance& db,
                                      std::string_view prefix,
                                      std::string_view kind, size_t limit,
                                      std::span<const std::string> extra) {
  return WithIndex(db, [&](const DocsIndex& index) {
    const auto wanted = absl::StripAsciiWhitespace(kind);
    std::vector<std::string> names;
    irs::containers::FlatHashSet<std::string> seen;
    const auto offer = [&](std::string_view name) {
      if (absl::StartsWithIgnoreCase(name, prefix) &&
          seen.insert(absl::AsciiStrToLower(name)).second) {
        names.emplace_back(name);
      }
    };
    for (const auto& object :
         MatchingObjects(index, *Prefix(index.ObjectFields().names,
                                        absl::AsciiStrToLower(prefix)))) {
      if (!OfKind(object, wanted)) {
        continue;
      }
      offer(object.name);
      absl::c_for_each(Aliases(object.aliases), offer);
    }
    absl::c_for_each(extra, offer);
    std::ranges::stable_sort(names, {}, [](const std::string& name) {
      return absl::AsciiStrToLower(name);
    });
    if (names.size() > limit) {
      names.resize(limit);
    }
    return names;
  });
}

std::vector<Entry> ListPrefix(duckdb::DatabaseInstance& db,
                              std::string_view prefix, bool pages_only,
                              Content content) {
  return WithIndex(db, [&](const DocsIndex& index) {
    return SortedByPath(
      CollectMatches(index, *PathPrefix(index.Fields(), prefix), content,
                     [&](std::string_view path) {
                       return !pages_only || HeadingDepth(path) <= 1;
                     }));
  });
}

std::vector<Entry> Children(duckdb::DatabaseInstance& db,
                            std::string_view path) {
  const auto depth = HeadingDepth(path);
  const auto page = absl::StrCat(path, "#");
  return WithIndex(db, [&](const DocsIndex& index) {
    return SortedByPath(
      CollectMatches(index, *PathPrefix(index.Fields(), page), Content::Omit,
                     [&](std::string_view found) {
                       return HeadingDepth(found) == depth + 1;
                     }));
  });
}

std::vector<std::string> CompletePath(duckdb::DatabaseInstance& db,
                                      std::string_view prefix, size_t limit) {
  const auto paths = ListPaths(db, prefix);
  std::vector<std::string> out;
  const auto offer = [&](std::string_view candidate) {
    if (out.size() < limit && candidate.starts_with(prefix) &&
        absl::c_find(out, candidate) == out.end()) {
      out.emplace_back(candidate);
    }
  };
  if (!prefix.contains('/')) {
    for (const auto& path : paths) {
      offer(std::string_view{path}.substr(0, path.find('/')));
    }
  }
  for (const auto& path : paths) {
    offer(std::string_view{path}.substr(0, path.find('#')));
  }
  return out;
}

std::vector<std::string> ListPaths(duckdb::DatabaseInstance& db,
                                   std::string_view prefix) {
  return WithIndex(db, [&](const DocsIndex& index) {
    std::vector<std::string> paths;
    ForEachMatch(index, *PathPrefix(index.Fields(), prefix), Content::Omit,
                 [&](EntryFetcher& fetcher, irs::doc_id_t doc) {
                   paths.emplace_back(fetcher.Path(doc));
                 });
    absl::c_sort(paths);
    return paths;
  });
}

std::optional<Entry> EntryAt(duckdb::DatabaseInstance& db,
                             std::string_view path, Content content) {
  return WithIndex(db, [&](const DocsIndex& index) {
    return ExactPath(index, path, content);
  });
}

std::vector<Entry> Search(duckdb::DatabaseInstance& db, std::string_view query,
                          size_t limit, Content content, std::string& error) {
  return WithIndex(db, [&](const DocsIndex& index) {
    std::vector<Entry> hits;
    if (const auto filter = Compile(query, index, error)) {
      hits = RunScored(index, *filter, limit, content);
    } else if (error.empty()) {
      hits = Literal(index, query, limit, content);
    }
    if (hits.empty() && error.empty() && query.contains('_')) {
      const auto words = absl::StrReplaceAll(query, {{"_", " "}});
      if (const auto filter = Compile(words, index, error)) {
        hits = RunScored(index, *filter, limit, content);
      }
    }
    if (!error.empty()) {
      return hits;
    }
    return KnownFirst(index, query, std::move(hits), limit, content);
  });
}

std::vector<Entry> Candidates(duckdb::DatabaseInstance& db,
                              std::string_view name, size_t limit) {
  return WithIndex(db, [&](const DocsIndex& index) {
    const auto tokenizer = DocsTokenizer();
    const auto text = index.Fields().Text();
    const std::span<const irs::field_id> all{text};
    for (const auto fields : {all.first(1), all}) {
      if (const auto filter = Similar(name, *tokenizer, fields)) {
        if (auto hits = RunScored(index, *filter, limit, Content::Include);
            !hits.empty()) {
          return hits;
        }
      }
    }
    return std::vector<Entry>{};
  });
}

}  // namespace sdb::docs
