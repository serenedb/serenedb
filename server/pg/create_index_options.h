#pragma once

#include "pg/options_parser.h"
#include "search/inverted_index_shard.h"

namespace sdb::pg {

namespace sdb::pg {

inline constexpr OptionInfo kCommitInterval{"commit_interval", 1000,
                                            "Commit interval in milliseconds"};
inline constexpr OptionInfo kConsolidationInterval{
  "consolidation_interval", 1000, "Consolidation interval in milliseconds"};
inline constexpr OptionInfo kCleanupIntervalStep{"cleanup_interval_step", 1,
                                                 "Cleanup interval step"};
inline constexpr OptionInfo kIndexOptions[] = {
  kCommitInterval, kConsolidationInterval, kCleanupIntervalStep};
inline constexpr OptionGroup kIndexGroup{"Index", kIndexOptions, {}};

}  // namespace create_index_options

class CreateIndexOptionsParser : public OptionsParser {
 public:
  CreateIndexOptionsParser(const List* options,
                           explain_options::ExplainOptions& explain)
    : OptionsParser{options,
                    create_index_options::kIndexGroup,
                    {.operation = "CREATE INDEX", .explain = &explain}} {
    ParseOptions([&] { Parse(); });
  }

  search::InvertedIndexShardOptions GetOptions() && {
    return std::move(_shard_options);
  }

 private:
  void Parse() {
    using namespace create_index_options;
    _shard_options.base.commit_interval_ms =
      EraseOptionOrDefault<kCommitInterval>();
    _shard_options.base.consolidation_interval_ms =
      EraseOptionOrDefault<kConsolidationInterval>();
    _shard_options.base.cleanup_interval_step =
      EraseOptionOrDefault<kCleanupIntervalStep>();
  }

  search::InvertedIndexShardOptions _shard_options;
};

}  // namespace sdb::pg
