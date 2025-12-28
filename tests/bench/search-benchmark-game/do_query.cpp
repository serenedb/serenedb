#include <absl/algorithm/container.h>
#include <absl/strings/str_split.h>
#include <iresearch/parser/parser.h>
#include <openssl/crypto.h>

#include <iostream>
#include <iresearch/analysis/analyzer.hpp>
#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/search/doc_collector.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/score.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/search/scorers.hpp>
#include <iresearch/store/directory.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/text_format.hpp>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <string>

#include "basics/assert.h"

enum class QueryType {
  Count,
  UnoptimizedCount,
  Top10,
  Top100,
  Top1000,
  Top10Count,
  Top100Count,
  Top1000Count,
  Unsupported,
};

namespace magic_enum {

template<>
constexpr customize::customize_t customize::enum_name<QueryType>(
  QueryType value) noexcept {
  switch (value) {
    case QueryType::Count:
    case QueryType::UnoptimizedCount:
      return "COUNT";
    case QueryType::Top10:
      return "TOP_10";
    case QueryType::Top100:
      return "TOP_100";
    case QueryType::Top1000:
      return "TOP_1000";
    case QueryType::Top10Count:
      return "TOP_10_COUNT";
    case QueryType::Top100Count:
      return "TOP_100_COUNT";
    case QueryType::Top1000Count:
      return "TOP_1000_COUNT";
    default:
      return "UNSUPPORTED";
  }
}

}  // namespace magic_enum

namespace {

constexpr std::string_view kFormatName = "1_5simd";
constexpr std::string_view kScorer = "bm25";
constexpr std::string_view kScorerOptions = R"({})";
constexpr std::string_view kTokenizer = "segmentation";
constexpr std::string_view kTokenizerOptions = R"({})";

struct Query {
  QueryType type = QueryType::Unsupported;
  std::string_view query;
};

Query ParseQuery(std::string_view str) {
  auto parts = absl::StrSplit(str, '\t');
  auto begin = parts.begin();

  auto type = magic_enum::enum_cast<QueryType>(*begin);
  if (!type) {
    return {};
  }

  ++begin;
  return {*type, *begin};
}

class Executor {
 public:
  Executor(std::string_view path)
    : _scorer{irs::scorers::Get(kScorer,
                                irs::Type<irs::text_format::Json>::get(),
                                kScorerOptions, false)},
      _tokenizer{irs::analysis::analyzers::Get(
        kTokenizer, irs::Type<irs::text_format::Json>::get(),
        kTokenizerOptions)},
      _format{irs::formats::Get(kFormatName, false)},
      _dir{path},
      _reader{
        irs::DirectoryReader(_dir, _format, {.scorers = {&_scorer_ptr, 1}})} {}

  size_t ExecuteTopK(size_t k, std::string_view query) {
    auto filter = ParseFilter(query);
    if (!filter) {
      return 0;
    }

    if (k == 10) {
      std::array<std::pair<float_t, uint32_t>, 20> results;
      return irs::ExecuteTopKOptimized<10>(_reader, *filter, _scorers, k,
                                           results);
    } else if (k == 100) {
      std::array<std::pair<float_t, uint32_t>, 200> results;
      return irs::ExecuteTopKOptimized<100>(_reader, *filter, _scorers, k,
                                            results);
    } else {
      return irs::ExecuteTopK(_reader, *filter, _scorers, k, _results);
    }
  }

  size_t ExecuteTopK1(size_t k, std::string_view query) {
    auto prepared = PrepareFilter(query);
    if (!prepared) {
      return 0;
    }

    _results.reserve(k);
    size_t count = 0;

    for (auto left = k; auto& segment : _reader) {
      auto docs = prepared->execute(irs::ExecutionContext{
        .segment = segment, .scorers = _scorers, .wand = _wand});
      const auto* doc = irs::get<irs::DocAttr>(*docs);
      const auto* score = irs::get<irs::ScoreAttr>(*docs);
      auto* threshold = irs::GetMutable<irs::ScoreAttr>(docs.get());

      if (!left && threshold) {
        threshold->Min(_results.front().first);
      }

      for (float_t score_value; docs->next();) {
        ++count;

        (*score)(&score_value);

        if (left) {
          _results.emplace_back(score_value, doc->value);

          if (0 == --left) {
            absl::c_make_heap(_results,
                              [](const auto& lhs, const auto& rhs) noexcept {
                                return lhs.first > rhs.first;
                              });

            threshold->Min(_results.front().first);
          }
        } else if (_results.front().first < score_value) {
          absl::c_pop_heap(_results,
                           [](const auto& lhs, const auto& rhs) noexcept {
                             return lhs.first > rhs.first;
                           });

          auto& [score, doc_id] = _results.back();
          score = score_value;
          doc_id = doc->value;

          absl::c_push_heap(
            _results,
            [](const std::pair<float_t, irs::doc_id_t>& lhs,
               const std::pair<float_t, irs::doc_id_t>& rhs) noexcept {
              return lhs.first > rhs.first;
            });

          threshold->Min(_results.front().first);
        }
      }
    }

    absl::c_sort(_results, [](const auto& lhs, const auto& rhs) noexcept {
      return lhs.first > rhs.first;
    });

    return count;
  }

  size_t ExecuteCount(std::string_view query) {
    size_t count = 0;
    auto prepared = PrepareFilter(query);
    for (auto& segment : _reader) {
      auto docs = prepared->execute(irs::ExecutionContext{.segment = segment});
      for (; docs->next(); ++count) {
      }
    }
    return count;
  }

  size_t ExecuteQuery(Query q) {
    auto [type, query] = q;
    switch (type) {
      case QueryType::UnoptimizedCount:
      case QueryType::Count:
        return ExecuteCount(query);
      case QueryType::Top10:
        return ExecuteTopK(10, query) > 0;
      case QueryType::Top100:
        return ExecuteTopK(100, query) > 0;
      case QueryType::Top1000:
        return ExecuteTopK(1000, query) > 0;
      case QueryType::Top10Count:
        return ExecuteTopK(10, query);
      case QueryType::Top100Count:
        return ExecuteTopK(100, query);
      case QueryType::Top1000Count:
        return ExecuteTopK(1000, query);
      default:
        return 0;
    }
  }

 private:
  irs::Filter::Prepared::ptr PrepareFilter(std::string_view query) {
    auto filter = ParseFilter(query);
    if (!filter) {
      return {};
    }
    return filter->prepare({
      .index = _reader,
      .scorers = _scorers,
    });
  }

  irs::Filter::ptr ParseFilter(std::string_view str) {
    auto root = std::make_unique<irs::Or>();
    sdb::ParserContext context{*root, "text", *_tokenizer};
    auto r = sdb::ParseQuery(context, str);
    if (!r.ok()) {
      return {};
    }
    return root;
  }

  std::vector<std::pair<float_t, irs::doc_id_t>> _results;
  irs::Scorer::ptr _scorer;
  irs::Scorer* _scorer_ptr{_scorer.get()};
  irs::Scorers _scorers{irs::Scorers::Prepare(std::span{&_scorer, 1})};
  irs::analysis::Analyzer::ptr _tokenizer;
  irs::Format::ptr _format;
  irs::MMapDirectory _dir;
  irs::DirectoryReader _reader;
  irs::WandContext _wand{.index = 0, .strict = true};
};

}  // namespace

int main(int argc, const char* argv[]) {
  irs::DefaultPDP(1, false);
  irs::DefaultPDP(1, true);
  irs::DefaultPDP(2, false);
  irs::DefaultPDP(2, true);
  irs::analysis::analyzers::Init();
  irs::formats::Init();
  irs::scorers::Init();
  irs::compression::Init();

  Executor executor{argv[1]};

  std::string data;
  while (std::getline(std::cin, data)) {
    const auto count = executor.ExecuteQuery(ParseQuery(data));
    if (!count) {
      std::cout << magic_enum::enum_name(QueryType::Unsupported) << "\n";
    }
    std::cout << count << "\n";
  }
  return 0;
}
