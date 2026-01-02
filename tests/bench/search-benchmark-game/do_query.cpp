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
#include <iresearch/index/index_reader_options.hpp>
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
#include <span>
#include <string>
#include <utility>

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
      _reader{irs::DirectoryReader(_dir, _format,
                                   {
                                     .scorers = {&_scorer_ptr, 1},
                                   })} {}

  size_t ExecuteTopK(size_t k, std::string_view query, bool full_count) {
    auto filter = ParseFilter(query);
    if (!filter) {
      return 0;
    }

    auto execute = [&]<size_t K> {
      irs::WandContext wand;
      if (!full_count) {
        wand.index = 0;
        wand.strict = true;
      }

      return irs::ExecuteTopK(
        _reader, *filter, _scorers, wand, k,
        std::span<std::pair<irs::score_t, irs::doc_id_t>, K>{_results});
    };

    _results.resize(k * 2);
    if (k == 10) {
      return execute.template operator()<20>();
    } else if (k == 100) {
      return execute.template operator()<200>();
    } else {
      return execute.template operator()<std::dynamic_extent>();
    }
  }

  size_t ExecuteCount(std::string_view query) {
    size_t count = 0;
    auto prepared = PrepareFilter(query);
    for (auto& segment : _reader) {
      auto docs = prepared->execute(irs::ExecutionContext{.segment = segment});
      count += docs->count();
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
        return ExecuteTopK(10, query, false);
      case QueryType::Top100:
        return ExecuteTopK(100, query, false);
      case QueryType::Top1000:
        return ExecuteTopK(1000, query, false);
      case QueryType::Top10Count:
        return ExecuteTopK(10, query, true);
      case QueryType::Top100Count:
        return ExecuteTopK(100, query, true);
      case QueryType::Top1000Count:
        return ExecuteTopK(1000, query, true);
      default:
        return 0;
    }
  }

 private:
  irs::Filter::Query::ptr PrepareFilter(std::string_view query) {
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
    if (root->size() == 1) {
      return root->PopBack();
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
