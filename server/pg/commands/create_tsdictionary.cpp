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

#include <absl/container/flat_hash_map.h>
#include <absl/strings/str_cat.h>
#include <unicode/locid.h>
#include <vpack/builder.h>
#include <vpack/value.h>
#include <vpack/value_type.h>

#include <iresearch/analysis/analyzers.hpp>
#include <iresearch/analysis/classification_tokenizer.hpp>
#include <iresearch/analysis/collation_tokenizer.hpp>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/analysis/geo_analyzer.hpp>
#include <iresearch/analysis/minhash_tokenizer.hpp>
#include <iresearch/analysis/multi_delimited_tokenizer.hpp>
#include <iresearch/analysis/nearest_neighbors_tokenizer.hpp>
#include <iresearch/analysis/ngram_tokenizer.hpp>
#include <iresearch/analysis/normalizing_tokenizer.hpp>
#include <iresearch/analysis/path_hierarchy_tokenizer.hpp>
#include <iresearch/analysis/pattern_tokenizer.hpp>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/stopwords_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/union_tokenizer.hpp>
#include <iresearch/analysis/wildcard_analyzer.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/utils/attribute_provider.hpp>
#include <type_traits>
#include <utility>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/search_analyzer_impl.h"
#include "catalog/tokenizer.h"
#include "pg/connection_context.h"
#include "pg/option_help.h"
#include "pg/options_parser.h"
#include "pg/sql_exception_macro.h"
#include "pg/sql_utils.h"
#include "pg/tokenizer_options.h"

namespace sdb::pg {
namespace {

inline constexpr std::string_view kTokenizerField = "tokenizer";
inline constexpr std::string_view kPropertiesField = "properties";
inline constexpr std::string_view kTypeField = "type";

using namespace std::string_view_literals;

// TODO: Remove this mapping
const containers::FlatHashMap<std::string_view, std::string_view>
  kNameMappings = {
    {tokenizer_options::kStopwordsPath.name, "stopwordsPath"},
    {tokenizer_options::kMinGram.name, "min"},
    {tokenizer_options::kMaxGram.name, "max"},
    {tokenizer_options::kEdgeNGramGroup.name, "edgeNGram"},
    {tokenizer_options::kPreserveOriginal.name, "preserveOriginal"},
    {tokenizer_options::kInputType.name, "streamType"},
    {tokenizer_options::kStartMarker.name, "startMarker"},
    {tokenizer_options::kEndMarker.name, "endMarker"},
    {tokenizer_options::kModelLocation.name, "model_location"},
    {tokenizer_options::kTopK.name, "top_k"},
    {tokenizer_options::kNumHashes.name, "numHashes"},
    {tokenizer_options::kNgramSize.name, "ngramSize"},
    {tokenizer_options::kGeoMaxCells.name, "max_cells"},
    {tokenizer_options::kGeoMinLevel.name, "min_level"},
    {tokenizer_options::kGeoMaxLevel.name, "max_level"},
    {tokenizer_options::kGeoLevelMod.name, "level_mod"},
    {tokenizer_options::kGeoOptimizeForSpace.name, "optimize_for_space"},
};

template<const auto& Array>
void VisitValues(auto&& callback) {
  [&]<std::size_t... Is>(std::index_sequence<Is...>) {
    (callback.template operator()<Array[Is]>(), ...);
  }(std::make_index_sequence<std::size(Array)>{});
}

void ParseCommaSeparated(std::string_view input,
                         std::invocable<std::string_view> auto&& callback) {
  while (!input.empty()) {
    auto pos = input.find(',');
    auto token = input.substr(0, pos);
    while (!token.empty() &&
           std::isspace(static_cast<unsigned char>(token.front()))) {
      token.remove_prefix(1);
    }
    while (!token.empty() &&
           std::isspace(static_cast<unsigned char>(token.back()))) {
      token.remove_suffix(1);
    }
    if (token.front() != '\"' || token.back() != '\"') {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("Invalid format of list of words(should be "
                              "comma-separated and quoted)"));
    }
    token.remove_suffix(1);
    token.remove_prefix(1);
    if (!token.empty()) {
      callback(token);
    }
    input = pos == std::string_view::npos ? "" : input.substr(pos + 1);
  }
}

std::string_view GetVPackName(std::string_view pg_name) {
  auto it = kNameMappings.find(pg_name);
  return it != kNameMappings.end() ? it->second : pg_name;
}

constexpr OptionInfo kTSDictionaryRootOptions[] = {
  tokenizer_options::kTemplate};
constexpr OptionGroup kTSDictionaryGroup = {
  "Text Search Dictionary", kTSDictionaryRootOptions,
  tokenizer_options::kTokenizerSubgroups};

class CreateTSDictionaryOptions : public OptionsParser {
 public:
  CreateTSDictionaryOptions(std::shared_ptr<const catalog::Snapshot> snapshot,
                            ObjectId db_id, std::string_view current_schema,
                            const duckdb::named_parameter_map_t& named_params)
    : OptionsParser{named_params,
                    kTSDictionaryGroup,
                    {.operation = "CREATE TEXT SEARCH DICTIONARY",
                     .help_hint = "Use WITH (HELP) to see available options"}},
      _snapshot{std::move(snapshot)},
      _db_id{db_id},
      _current_schema{current_schema} {
    ParseOptions([&] {
      _builder.openObject();
      _builder.add(kTokenizerField, vpack::Value{vpack::ValueType::Object});
      const auto type =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>();
      Parse<true>(type);
      _builder.close();  // close tokenizer
      _builder.close();  // close object
    });
  }

  auto Result() && {
    return std::make_tuple(std::move(_builder), _features,
                           _norm_row_group_size);
  }

 private:
  bool HasUnionChildOption(std::string_view prefix) const {
    auto child_prefix = OptionInfo::AdjustPrefix(prefix, "tokenizer");
    for (const auto& [name, _] : _options) {
      if (name.starts_with(child_prefix)) {
        return true;
      }
    }
    return false;
  }

  void ParseTemplateType(std::string_view type, std::string_view prefix) {
    bool found = false;
    VisitValues<kTSDictionaryGroup.subgroups>([&]<const OptionGroup & Group> {
      if (Group.name == type) {
        WriteTokenizerOptions<Group>(prefix);
        found = true;
        return;
      }
    });
    SDB_ASSERT(found);
  }

  static vpack::Slice GetFromPath(std::string_view name,
                                  std::string_view full_prefix,
                                  std::string_view name_prefix,
                                  vpack::Slice slice) {
    SDB_ASSERT(name_prefix == full_prefix.substr(0, name_prefix.size()));
    auto prefix = full_prefix.substr(name_prefix.size());
    while (!slice.isNone() && !prefix.empty()) {
      auto pos = prefix.find('_');
      auto next = prefix.substr(0, pos);
      SDB_ASSERT(!next.empty());
      slice = slice.get(next);
      prefix = pos == std::string_view::npos ? "" : prefix.substr(pos + 1);
    }
    if (slice.isNone()) {
      return slice;
    }
    return slice.get(name);
  }

  template<typename T>
  std::optional<T> GetFromCopy(std::string_view name, std::string_view prefix) {
    SDB_ASSERT(!_copy_from.empty());
    auto [name_prefix, slice] = _copy_from.back();
    auto field = GetFromPath(GetVPackName(name), prefix, name_prefix, slice);
    if (field.isNone()) {
      return std::nullopt;
    }
    if constexpr (std::is_same_v<T, std::string>) {
      return std::string{field.stringView()};
    } else if constexpr (std::is_same_v<T, bool>) {
      return field.getBool();
    } else if constexpr (std::is_same_v<T, int>) {
      return field.getNumber<int>();
    } else if constexpr (std::is_same_v<T, double>) {
      return field.getNumber<double>();
    } else if constexpr (std::is_same_v<T, char>) {
      auto sv = field.stringView();
      SDB_ASSERT(sv.size() == 1);
      return sv[0];
    } else if constexpr (std::is_enum_v<T>) {
      return magic_enum::enum_cast<T>(field.stringView(),
                                      magic_enum::case_insensitive);
    } else {
      static_assert(false, "Unsupported type T in GetFromCopy");
    }
  }

  template<const OptionInfo& Info>
  auto EraseOptionOrDefault(std::string_view prefix = "") {
    using R = decltype(OptionsParser::EraseOptionOrDefault<Info>(prefix));
    if (_copy_from.empty()) {
      return OptionsParser::EraseOptionOrDefault<Info>(prefix);
    }
    if (!OptionsParser::HasOption(Info.name, prefix)) {
      std::string_view name = Info.name;
      // tokenizer's properties vpack does not contains its type
      // tokenizer: {"tokenizer": {"type" : "some", "properties": {...}}}
      SDB_ASSERT(name != tokenizer_options::kTemplate.name);
      auto value = GetFromCopy<R>(name, prefix);
      if (value) {
        return *value;
      }
      return Info.GetDefaultValue<R>();
    } else {
      return OptionsParser::EraseOptionOrDefault<Info>(prefix);
    }
  }

  bool HasOption(const OptionInfo& info, std::string_view prefix) {
    bool has_option = OptionsParser::HasOption(info.name, prefix);
    if (has_option || _copy_from.empty()) {
      return has_option;
    }

    auto [name_prefix, slice] = _copy_from.back();
    auto field = GetFromPath(info.name, prefix, name_prefix, slice);
    return !field.isNone();
  }

  template<bool IsRoot>
  void Parse(std::string_view type, std::string_view prefix = "") {
    if (type == tokenizer_options::kCopyFromGroup.name) {
      ParseCopyFrom(prefix);
    } else {
      _builder.add(kPropertiesField, vpack::Value{vpack::ValueType::Object});

      ParseTemplateType(type, prefix);

      _builder.close();  // close properties
      _builder.add(kTypeField, type);
    }
    if constexpr (IsRoot) {
      ParseFeatures(type);
    }
  }

  template<const OptionGroup& Group>
  void ParseTokenizerGroup(std::string_view prefix) {
    VisitValues<Group.options>([&]<const OptionInfo & Option> {
      if constexpr (Option.name == tokenizer_options::kStopwords.name ||
                    Option.name == tokenizer_options::kDelimiters.name) {
        if (!OptionsParser::HasOption(Option.name, prefix) &&
            !_copy_from.empty()) {
          auto slice = GetFromPath(Option.name, prefix, _copy_from.back().first,
                                   _copy_from.back().second);
          if (!slice.isNone()) {
            _builder.add(GetVPackName(Option.name), slice);
            return;
          }
        }
        _builder.add(GetVPackName(Option.name),
                     vpack::Value{vpack::ValueType::Array});
        auto value = OptionsParser::EraseOptionOrDefault<Option>(prefix);
        ParseCommaSeparated(value,
                            [&](std::string_view word) { _builder.add(word); });
        _builder.close();
      } else {
        auto value = EraseOptionOrDefault<Option>(prefix);
        if constexpr (std::is_same_v<std::remove_cvref_t<decltype(value)>,
                                     std::string>) {
          if (value.empty()) {
            return;
          }
        }
        _builder.add(GetVPackName(Option.name), value);
      }
    });
  }

  template<const OptionGroup& Group>
  void WriteTokenizerOptions(std::string_view prefix) {
    if constexpr (Group.name == tokenizer_options::kMinHashGroup.name) {
      ParseMinHash(prefix);
      return;
    } else if constexpr (Group.name == tokenizer_options::kGeoPointGroup.name) {
      ParseGeoPoint(prefix);
      return;
    } else if constexpr (Group.name == tokenizer_options::kGeoJsonGroup.name) {
      ParseGeoJson(prefix);
      return;
    } else if constexpr (Group.name == tokenizer_options::kPipelineGroup.name) {
      ParsePipeline(prefix);
      return;
    } else if constexpr (Group.name == tokenizer_options::kUnionGroup.name) {
      ParseUnion(prefix);
      return;
    } else if constexpr (Group.name == tokenizer_options::kCopyFromGroup.name) {
      ParseCopyFrom(prefix);
      return;
    } else if constexpr (Group.name == tokenizer_options::kWildcardGroup.name) {
      ParseWildcard(prefix);
      return;
    } else {
      if constexpr (Group.name == tokenizer_options::kTextGroup.name) {
        bool has_ngram =
          HasOption(tokenizer_options::kMinGram, prefix) ||
          HasOption(tokenizer_options::kMaxGram, prefix) ||
          HasOption(tokenizer_options::kPreserveOriginal, prefix);
        if (has_ngram) {
          _builder.add(GetVPackName(tokenizer_options::kEdgeNGramGroup.name),
                       vpack::Value{vpack::ValueType::Object});
          WriteTokenizerOptions<Group.subgroups[0]>(prefix);
          _builder.close();
        }
      }
      ParseTokenizerGroup<Group>(prefix);
    }
  }

  void ParsePipeline(std::string_view prefix) {
    int step = 1;
    _builder.add(tokenizer_options::kPipelineGroup.name,
                 vpack::Value{vpack::ValueType::Array});
    auto slice = vpack::Slice::noneSlice();
    if (!_copy_from.empty() && _copy_from.back().first == prefix) {
      slice = GetFromPath(tokenizer_options::kPipelineGroup.name, prefix,
                          _copy_from.back().first, _copy_from.back().second);
      SDB_ASSERT(slice.isArray());
    }
    while (true) {
      auto step_prefix = OptionInfo::AdjustPrefix(prefix, "step", step);
      std::string type;
      bool type_from_copy = false;
      if (OptionsParser::HasOption(tokenizer_options::kTemplate, step_prefix)) {
        type =
          OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>(
            step_prefix);
      } else if (!slice.isNone()) {
        if (step > slice.length()) {
          break;
        }
        auto elem = slice.at(step - 1);
        if (elem.isNone()) {
          break;
        }
        type_from_copy = true;
        type = elem.get(kTypeField).stringView();
        _copy_from.emplace_back(step_prefix, elem.get(kPropertiesField));
      }
      if (type.empty()) {
        break;
      }
      _builder.openObject();
      Parse<false>(type, step_prefix);
      _builder.close();
      if (type_from_copy) {
        _copy_from.pop_back();
      }

      step++;
    }
    _builder.close();  // close array for pipeline
  }

  void ParseUnion(std::string_view prefix) {
    int tokenizer_num = 1;
    _builder.add(tokenizer_options::kUnionGroup.name,
                 vpack::Value{vpack::ValueType::Array});
    auto slice = vpack::Slice::noneSlice();
    if (!_copy_from.empty() && _copy_from.back().first == prefix) {
      slice = GetFromPath(tokenizer_options::kUnionGroup.name, prefix,
                          _copy_from.back().first, _copy_from.back().second);
      SDB_ASSERT(slice.isArray());
    }
    while (true) {
      auto tokenizer_prefix =
        OptionInfo::AdjustPrefix(prefix, "tokenizer", tokenizer_num);
      std::string type;
      bool type_from_copy = false;
      if (OptionsParser::HasOption(tokenizer_options::kTemplate,
                                   tokenizer_prefix)) {
        type =
          OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>(
            tokenizer_prefix);
      } else if (!slice.isNone()) {
        if (tokenizer_num > static_cast<int>(slice.length())) {
          break;
        }
        auto elem = slice.at(tokenizer_num - 1);
        if (elem.isNone()) {
          break;
        }
        type_from_copy = true;
        type = elem.get(kTypeField).stringView();
        _copy_from.emplace_back(tokenizer_prefix, elem.get(kPropertiesField));
      }
      if (type.empty()) {
        break;
      }
      _builder.openObject();
      Parse<false>(type, tokenizer_prefix);
      _builder.close();
      if (type_from_copy) {
        _copy_from.pop_back();
      }

      tokenizer_num++;
    }
    if (tokenizer_num == 1) {
      if (!slice.isNone() || !HasUnionChildOption(prefix)) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                        ERR_MSG("Union tokenizer requires at least one "
                                "tokenizer<N> child"));
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("Union tokenizer children must be numbered "
                              "densely starting from tokenizer1"));
    }
    _builder.close();  // close array for union
  }

  void ParseMinHash(std::string_view prefix) {
    auto tokenizer_prefix = OptionInfo::AdjustPrefix(prefix, kTokenizerField);
    std::string type;
    bool type_from_template = false;
    if (OptionsParser::HasOption(tokenizer_options::kTemplate,
                                 tokenizer_prefix) ||
        _copy_from.empty()) {
      type = OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>(
        tokenizer_prefix);
    } else {
      SDB_ASSERT(!_copy_from.empty());
      auto slice = GetFromPath(kTokenizerField, prefix, _copy_from.back().first,
                               _copy_from.back().second);
      type = slice.get(kTypeField).stringView();
      _copy_from.emplace_back(tokenizer_prefix, slice.get(kPropertiesField));
      type_from_template = true;
    }
    SDB_ASSERT(!type.empty());
    _builder.add(kTokenizerField, vpack::Value{vpack::ValueType::Object});
    Parse<false>(type, tokenizer_prefix);
    _builder.close();  // close tokenizer
    if (type_from_template) {
      _copy_from.pop_back();
    }
    int hashes = EraseOptionOrDefault<tokenizer_options::kNumHashes>(prefix);
    _builder.add(GetVPackName(tokenizer_options::kNumHashes.name), hashes);
  }

  void ParseWildcard(std::string_view prefix) {
    auto tokenizer_prefix = OptionInfo::AdjustPrefix(prefix, kTokenizerField);
    std::string type;
    bool type_from_template = false;
    if (OptionsParser::HasOption(tokenizer_options::kTemplate,
                                 tokenizer_prefix) ||
        _copy_from.empty()) {
      type = OptionsParser::EraseOptionOrDefault<tokenizer_options::kTemplate>(
        tokenizer_prefix);
    } else {
      SDB_ASSERT(!_copy_from.empty());
      auto slice = GetFromPath(kTokenizerField, prefix, _copy_from.back().first,
                               _copy_from.back().second);
      type = slice.get(kTypeField).stringView();
      _copy_from.emplace_back(tokenizer_prefix, slice.get(kPropertiesField));
      type_from_template = true;
    }
    SDB_ASSERT(!type.empty());
    _builder.add(kTokenizerField, vpack::Value{vpack::ValueType::Object});
    Parse<false>(type, tokenizer_prefix);
    _builder.close();
    if (type_from_template) {
      _copy_from.pop_back();
    }
    int ngram_size =
      EraseOptionOrDefault<tokenizer_options::kNgramSize>(prefix);
    _builder.add(GetVPackName(tokenizer_options::kNgramSize.name), ngram_size);
  }

  // Writes elements of a slash-separated path string as individual VPack array
  // strings. The builder must have an open array.
  void ParsePathString(std::string_view path) {
    while (!path.empty()) {
      auto pos = path.find('/');
      auto part = path.substr(0, pos);
      if (!part.empty()) {
        _builder.add(part);
      }
      path = pos == std::string_view::npos ? "" : path.substr(pos + 1);
    }
  }

  // Writes the nested "options" S2 sub-object, reading from copy-from if
  // needed.
  void ParseGeoS2Options(std::string_view prefix) {
    bool pop_copy = false;
    if (!_copy_from.empty()) {
      auto [name_prefix, slice] = _copy_from.back();
      auto opts_slice = GetFromPath("options", prefix, name_prefix, slice);
      if (!opts_slice.isNone()) {
        // Push the nested options slice so EraseOptionOrDefault finds fields
        // directly inside it without further traversal.
        _copy_from.emplace_back(prefix, opts_slice);
        pop_copy = true;
      }
    }
    _builder.add(GetVPackName(tokenizer_options::kGeoMaxCells.name),
                 EraseOptionOrDefault<tokenizer_options::kGeoMaxCells>(prefix));
    _builder.add(GetVPackName(tokenizer_options::kGeoMinLevel.name),
                 EraseOptionOrDefault<tokenizer_options::kGeoMinLevel>(prefix));
    _builder.add(GetVPackName(tokenizer_options::kGeoMaxLevel.name),
                 EraseOptionOrDefault<tokenizer_options::kGeoMaxLevel>(prefix));
    _builder.add(GetVPackName(tokenizer_options::kGeoLevelMod.name),
                 EraseOptionOrDefault<tokenizer_options::kGeoLevelMod>(prefix));
    _builder.add(
      GetVPackName(tokenizer_options::kGeoOptimizeForSpace.name),
      EraseOptionOrDefault<tokenizer_options::kGeoOptimizeForSpace>(prefix));
    if (pop_copy) {
      _copy_from.pop_back();
    }
  }

  void ParseGeoPoint(std::string_view prefix) {
    // Both latitude and longitude default to "" (empty path). When both
    // are empty the analyzer treats the indexed JSON value as a
    // [lat, lng] array directly (`_from_array` mode); when both are set
    // the analyzer walks the configured object paths. Half-set is
    // rejected here so the user gets a specific error at CREATE time
    // instead of a deferred "Failed to create analyzer" at first use.
    bool lat_set = false;
    if (!OptionsParser::HasOption(tokenizer_options::kGeoLatitude, prefix) &&
        !_copy_from.empty()) {
      auto [name_prefix, slice] = _copy_from.back();
      auto lat_slice = GetFromPath("latitude", prefix, name_prefix, slice);
      if (!lat_slice.isNone()) {
        _builder.add("latitude", lat_slice);
        // copy_from inherits a vpack array of path components; consider
        // it set if the array is non-empty (parent was in array mode if
        // empty, in path mode otherwise).
        lat_set = lat_slice.isArray() && lat_slice.length() > 0;
      } else {
        _builder.add("latitude", vpack::Slice::emptyArraySlice());
      }
    } else {
      auto lat_path =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kGeoLatitude>(
          prefix);
      if (lat_path.empty()) {
        _builder.add("latitude", vpack::Slice::emptyArraySlice());
      } else {
        _builder.add("latitude", vpack::Value{vpack::ValueType::Array});
        ParsePathString(lat_path);
        _builder.close();
        lat_set = true;
      }
    }

    bool lng_set = false;
    if (!OptionsParser::HasOption(tokenizer_options::kGeoLongitude, prefix) &&
        !_copy_from.empty()) {
      auto [name_prefix, slice] = _copy_from.back();
      auto lng_slice = GetFromPath("longitude", prefix, name_prefix, slice);
      if (!lng_slice.isNone()) {
        _builder.add("longitude", lng_slice);
        lng_set = lng_slice.isArray() && lng_slice.length() > 0;
      } else {
        _builder.add("longitude", vpack::Slice::emptyArraySlice());
      }
    } else {
      auto lng_path =
        OptionsParser::EraseOptionOrDefault<tokenizer_options::kGeoLongitude>(
          prefix);
      if (lng_path.empty()) {
        _builder.add("longitude", vpack::Slice::emptyArraySlice());
      } else {
        _builder.add("longitude", vpack::Value{vpack::ValueType::Array});
        ParsePathString(lng_path);
        _builder.close();
        lng_set = true;
      }
    }

    if (lat_set != lng_set) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("'latitude' and 'longitude' must be both set or both "
                "left empty for the geopoint tokenizer"));
    }

    // nested S2 options
    _builder.add("options", vpack::Value{vpack::ValueType::Object});
    ParseGeoS2Options(prefix);
    _builder.close();
  }

  void ParseGeoJson(std::string_view prefix) {
    auto type_str =
      EraseOptionOrDefault<tokenizer_options::kGeoJsonType>(prefix);
    if (!type_str.empty()) {
      _builder.add("type", type_str);
    }

    auto coding_str =
      EraseOptionOrDefault<tokenizer_options::kGeoJsonCoding>(prefix);
    if (!coding_str.empty()) {
      _builder.add("coding", coding_str);
    }

    // nested S2 options
    _builder.add("options", vpack::Value{vpack::ValueType::Object});
    ParseGeoS2Options(prefix);
    _builder.close();
  }

  void ParseCopyFrom(std::string_view prefix) {
    std::string from =
      OptionsParser::EraseOptionOrDefault<tokenizer_options::kFrom>(prefix);
    auto name = ParseObjectName(from, _current_schema);
    auto tokenizer =
      _snapshot->GetTokenizer(_db_id, name.schema, name.relation);
    if (!tokenizer) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("text search dictionary \"", from, "\" does not exist"));
    }
    auto slice = tokenizer->Slice().get(kTokenizerField);

    auto type = slice.get(kTypeField);
    _copy_from.emplace_back(prefix, slice.get(kPropertiesField));
    Parse<false>(type.stringView(), prefix);
    _copy_from.pop_back();
  }

  void ParseFeatures(std::string_view type) {
    VisitValues<tokenizer_options::kFeaturesOptions>(
      [&]<const OptionInfo & Feature> {
        bool use_feature = EraseOptionOrDefault<Feature>();
        if (use_feature) {
          bool added = _features.Add(Feature.name);
          SDB_ASSERT(added);
        }
      });
    if (OptionsParser::HasOption(tokenizer_options::kNormRowGroupSize)) {
      if (!_features.HasFeatures(irs::IndexFeatures::Norm)) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("norm_row_group_size requires norm = true"),
          ERR_HINT(tokenizer_options::kNormRowGroupSize.description));
      }
      _norm_row_group_size = static_cast<uint32_t>(
        EraseOptionOrDefault<tokenizer_options::kNormRowGroupSize>());
    }
    auto r = _features.Validate(type);
    if (!r.ok()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG(r.errorMessage()));
    }
  }

  vpack::Builder _builder;
  search::Features _features;
  uint32_t _norm_row_group_size = DEFAULT_ROW_GROUP_SIZE;
  std::vector<std::pair<std::string, vpack::Slice>> _copy_from;
  std::shared_ptr<const catalog::Snapshot> _snapshot;
  ObjectId _db_id;
  std::string_view _current_schema;
};

}  // namespace

void CreateTokenizer(ConnectionContext& conn_ctx, std::string_view name,
                     std::string_view schema, bool if_not_exists,
                     const duckdb::named_parameter_map_t& options) {
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto db_id = conn_ctx.GetDatabaseId();
  auto current_schema = conn_ctx.GetCurrentSchema();

  auto [b, features, norm_row_group_size] =
    std::move(
      CreateTSDictionaryOptions{snapshot, db_id, current_schema, options})
      .Result();

  // Validate tokenizer configuration
  auto tokenizer_slice = b.slice().get(kTokenizerField);
  if (!tokenizer_slice.isNone()) {
    auto type_slice = tokenizer_slice.get(kTypeField);
    auto properties_slice = tokenizer_slice.get(kPropertiesField);
    if (!type_slice.isNone() && !properties_slice.isNone()) {
      std::string dummy_output;
      if (!irs::analysis::analyzers::Normalize(
            dummy_output, type_slice.stringView(),
            irs::Type<irs::text_format::VPack>::get(),
            std::string{
              reinterpret_cast<const char*>(properties_slice.getDataPtr()),
              properties_slice.byteSize()},
            false)) {
        // If validation fails, the error should already be logged
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
          ERR_MSG("Failed to create text search dictionary \"", name, "\""));
      }
      if (features.HasFeatures(irs::IndexFeatures::Offs)) {
        auto test_analyzer = irs::analysis::analyzers::Get(
          type_slice.stringView(), irs::Type<irs::text_format::VPack>::get(),
          {reinterpret_cast<const char*>(properties_slice.getDataPtr()),
           properties_slice.byteSize()},
          false);
        if (test_analyzer && !irs::get<irs::OffsAttr>(*test_analyzer)) {
          THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                          ERR_MSG("Unsupported index features are specified"));
        }
      }
    }
  }

  auto tokenizer = std::make_shared<catalog::Tokenizer>(
    ObjectId{}, ObjectId{}, name, features, norm_row_group_size,
    std::string{reinterpret_cast<const char*>(b.slice().getDataPtr()),
                b.slice().byteSize()});

  auto& catalog = catalog::CatalogFeature::instance().Global();
  auto r = catalog.CreateTokenizer(db_id, schema, std::move(tokenizer));

  if (!r.ok() && !if_not_exists) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
      ERR_MSG("text search dictionary \"", name, "\" already exists"));
  }
}

}  // namespace sdb::pg
