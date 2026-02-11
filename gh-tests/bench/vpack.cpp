////////////////////////////////////////////////////////////////////////////////
/// @brief Library to build up VPack documents.
///
/// DISCLAIMER
///
/// Copyright 2015 ArangoDB GmbH, Cologne, Germany
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
/// @author Max Neunhoeffer
/// @author Jan Steemann
/// @author Copyright 2015, ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "vpack/vpack.h"

#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "simdjson.h"

namespace {
using namespace vpack;

enum ParserType {
  kUnknown,
  kVpack,
  kRapidjson,
  kSimdjsonDom,
  kSimdjsonOndemand,
  kSimdjsonDomToBuilder,
  kSimdjsonOndemandToBuilder,
};

void Usage(char* argv[]) {
  std::cout << "Usage: " << argv[0]
            << " FILENAME.json RUNTIME_IN_SECONDS COPIES TYPE" << std::endl;
  std::cout << "This program reads the file into a string, makes COPIES copies"
            << std::endl;
  std::cout << "and then parses the copies in a round-robin fashion to VPack."
            << std::endl;
  std::cout << "1 copy means its running in cache, more copies make it run"
            << std::endl;
  std::cout << "out of cache. The target areas are also in a different memory"
            << std::endl;
  std::cout << "area for each copy." << std::endl;
  std::cout << "TYPE must be: vpack/rapidjson/simdjson." << std::endl;
}

std::string TryReadFile(const std::string& filename) {
  std::string s;
  std::ifstream ifs(filename.c_str(), std::ifstream::in);

  if (!ifs.is_open()) {
    std::cerr << "Cannot open input file '" << filename << "'" << std::endl;
    ::exit(EXIT_FAILURE);
  }

  char buffer[4096];
  while (ifs.good()) {
    ifs.read(&buffer[0], sizeof(buffer));
    s.append(buffer, ifs.gcount());
  }
  ifs.close();

  return s;
}

std::string ReadFile(std::string filename) {
  std::filesystem::path path{__FILE__};
  path = path.parent_path();  // bench
  path = path.parent_path();  // tests
  path = path.parent_path();  // root
  path /= "resources";
  path /= "tests";
  path /= "vpack";
  path /= filename;
  return TryReadFile(path.string());
}

void DomToBuilder(simdjson::dom::element& doc, Builder* builder) {
  switch (doc.type()) {
    case simdjson::dom::element_type::OBJECT: {
      builder->add(Value(ValueType::Object));
      for (auto field : doc.get_object()) {
        builder->add(field.key);
        DomToBuilder(field.value, builder);
      }
      builder->close();
    } break;
    case simdjson::dom::element_type::ARRAY: {
      builder->add(Value(ValueType::Array));
      for (auto field : doc.get_array()) {
        DomToBuilder(field, builder);
      }
      builder->close();
    } break;
    case simdjson::dom::element_type::INT64:
      builder->add(doc.get_int64());
      break;
    case simdjson::dom::element_type::UINT64:
      builder->add(doc.get_uint64());
      break;
    case simdjson::dom::element_type::DOUBLE:
      builder->add(doc.get_double());
      break;
    case simdjson::dom::element_type::STRING:
      builder->add(doc.get_string());
      break;
    case simdjson::dom::element_type::BOOL:
      builder->add(doc.get_bool());
      break;
    case simdjson::dom::element_type::NULL_VALUE:
      builder->add(Value(ValueType::Null));
      break;
    default:
      std::cerr << "Unknown simdjson::dom::element_type!" << std::endl;
      exit(EXIT_FAILURE);
      break;
  }
}

// void OndemandToBuilder(simdjson::ondemand::document& doc, Builder* builder) {
//   switch (doc.type()) {
//     case simdjson::ondemand::json_type::object:
//       builder->add(Value(ValueType::Object));
//       for (auto field : doc.get_object()) {
//         builder->add(field.key().unescape());
//         OndemandToBuilder(field.value(), builder);
//       }
//       builder->close();
//       break;
//     case simdjson::ondemand::json_type::array:
//       builder->add(Value(ValueType::Array));
//       for (auto field : doc.get_array()) {
//         OndemandToBuilder(field, builder);
//       }
//       builder->close();
//       break;
//     case simdjson::ondemand::json_type::number:
//       break;
//     case simdjson::ondemand::json_type::string:
//       break;
//     case simdjson::ondemand::json_type::boolean:
//       break;
//     case simdjson::ondemand::json_type::null:
//       break;
//     default:
//       std::cerr << "Unknown simdjson::dom::element_type!" << std::endl;
//       exit(EXIT_FAILURE);
//       break;
//   }
// }

ParserType ParserTypeFromName(std::string_view name) noexcept {
  ParserType parser_type = ParserType::kVpack;  // default
  if (name == "vpack") {
    parser_type = ParserType::kVpack;
  } else if (name == "rapidjson") {
    parser_type = ParserType::kRapidjson;
  } else if (name == "simdjson_dom") {
    parser_type = ParserType::kSimdjsonDom;
  } else if (name == "simdjson_ondemand") {
    parser_type = ParserType::kSimdjsonOndemand;
  } else if (name == "simdjson_dom_to_builder") {
    parser_type = ParserType::kSimdjsonDomToBuilder;
  } else if (name == "simdjson_ondemand_to_builder") {
    parser_type = ParserType::kSimdjsonOndemandToBuilder;
  } else {
    parser_type = ParserType::kUnknown;
  }
  return parser_type;
}

const char* ParserTypeName(ParserType parser_type) noexcept {
  switch (parser_type) {
    case ParserType::kVpack:
      return "vpack";
    case ParserType::kRapidjson:
      return "rapidjson";
    case ParserType::kSimdjsonDom:
      return "simdjson_dom";
    case ParserType::kSimdjsonOndemand:
      return "simdjson_ondemand";
    case ParserType::kSimdjsonDomToBuilder:
      return "simdjson_dom_to_builder";
    case ParserType::kSimdjsonOndemandToBuilder:
      return "simdjson_ondemand_to_builder";
    case ParserType::kUnknown:
      return "unknown";
  }
}

void Run(std::string& data, int run_time, size_t copies, ParserType parser_type,
         bool full_output) {
  Options options;

  std::vector<std::string> inputs;
  std::vector<std::unique_ptr<Parser>> outputs;
  inputs.push_back(data);
  outputs.push_back(std::make_unique<Parser>(&options));

  for (size_t i = 1; i < copies; i++) {
    // Make an explicit copy:
    data.clear();
    data.insert(data.begin(), inputs[0].begin(), inputs[0].end());
    inputs.push_back(data);
    outputs.push_back(std::make_unique<Parser>(&options));
  }

  for (auto& input : inputs) {
    input.reserve(input.size() + simdjson::SIMDJSON_PADDING);
  }

  size_t count = 0;
  size_t total = 0;
  auto start = std::chrono::high_resolution_clock::now();
  decltype(start) now;

  simdjson::dom::parser dom_parser;
  simdjson::ondemand::parser ondemand_parser;
  Builder simd_builder;

  try {
    do {
      for (int i = 0; i < 2; i++) {
        switch (parser_type) {
          case kVpack: {
            outputs[count]->clear();
            outputs[count]->parse(inputs[count]);
          } break;
          case kRapidjson: {
            rapidjson::Document d;

            d.Parse(inputs[count]);
          } break;
          case kSimdjsonDom: {
            simdjson::dom::element doc;

            auto error = dom_parser.parse(inputs[count]).get(doc);
            if (error) {
              std::cerr << "simdjson parse failed" << error << std::endl;
              exit(EXIT_FAILURE);
            }
          } break;
          case kSimdjsonOndemand: {
            simdjson::ondemand::document doc;

            auto error = ondemand_parser.iterate(inputs[count]).get(doc);
            if (error) {
              std::cerr << "simdjson parse failed" << error << std::endl;
              exit(EXIT_FAILURE);
            }
          } break;
          case kSimdjsonDomToBuilder: {
            simdjson::dom::element doc;

            auto error = dom_parser.parse(inputs[count]).get(doc);
            if (error) {
              std::cerr << "simdjson parse failed" << error << std::endl;
              exit(EXIT_FAILURE);
            }
            simd_builder.clear();
            DomToBuilder(doc, &simd_builder);
          } break;
          // case SIMDJSON_ONDEMAND_TO_BUILDER: {
          //   simdjson::ondemand::document doc;

          //   auto error = ondemand_parser.iterate(inputs[count]).get(doc);
          //   if (error) {
          //     std::cerr << "simdjson parse failed" << error << std::endl;
          //     exit(EXIT_FAILURE);
          //   }
          //   simd_builder.clear();
          //   OndemandToBuilder(doc, &simd_builder);
          // } break;
          default: {
            std::cerr << "invalid parser type!";
            exit(EXIT_FAILURE);
          }
        }

        count++;
        if (count >= copies) {
          count = 0;
        }
        total++;
      }
      now = std::chrono::high_resolution_clock::now();
    } while (
      std::chrono::duration_cast<std::chrono::seconds>(now - start).count() <
      run_time);

    std::chrono::duration<double> total_time =
      std::chrono::duration_cast<std::chrono::duration<double>>(now - start);

    if (full_output) {
      std::cout << "Total runtime: " << std::setprecision(2) << std::fixed
                << total_time.count() << " s" << std::endl;
      std::cout << "Have parsed " << total << " times with "
                << ParserTypeName(parser_type) << " using " << copies
                << " copies of JSON data, each of size " << inputs[0].size()
                << "." << std::endl;
      std::cout << "Parsed " << inputs[0].size() * total << " bytes in total."
                << std::endl;
    }
    std::cout << std::setprecision(8) << std::fixed << " | " << std::setw(14)
              << static_cast<double>(inputs[0].size() * total) / (1 << 30) /
                   total_time.count()
              << " GB/s" << " | " << std::setw(14) << total / total_time.count()
              << " | " << std::endl;
  } catch (const Exception& ex) {
    std::cerr << "An exception occurred while running bench: " << ex.what()
              << std::endl;
    ::exit(EXIT_FAILURE);
  } catch (const std::exception& ex) {
    std::cerr << "An exception occurred while running bench: " << ex.what()
              << std::endl;
    ::exit(EXIT_FAILURE);
  } catch (...) {
    std::cerr << "An unknown exception occurred while running bench"
              << std::endl;
    ::exit(EXIT_FAILURE);
  }
}

void RunDefaultBench(bool all) {
  bool full_output = !all;
  int run_seconds = all ? 5 : 10;
  auto run_comparison = [&](const std::string& filename) {
    std::string data = ReadFile(filename);

    std::cout << std::endl;
    std::cout << "# " << filename << " ";
    for (size_t i = 0; i < 30 - filename.size(); ++i) {
      std::cout << "#";
    }
    std::cout << std::endl;

    std::cout << "|" << filename << " | " << "vpack                        ";
    Run(data, run_seconds, 1, ParserType::kVpack, full_output);
    std::cout << "|" << filename << " | " << "rapidjson                    ";
    Run(data, run_seconds, 1, ParserType::kRapidjson, full_output);
    std::cout << "|" << filename << " | " << "simdjson_dom                 ";
    Run(data, run_seconds, 1, ParserType::kSimdjsonDom, full_output);
    std::cout << "|" << filename << " | " << "simdjson_ondemand            ";
    Run(data, run_seconds, 1, ParserType::kSimdjsonOndemand, full_output);
    std::cout << "|" << filename << " | " << "simdjson_dom_to_builder      ";
    Run(data, run_seconds, 1, ParserType::kSimdjsonDomToBuilder, full_output);
    // std::cout << "|" << filename << " | " << "simdjson_ondemand_to_builder ";
    // run(data, runSeconds, 1, ParserType::SIMDJSON_ONDEMAND_TO_BUILDER,
    //     fullOutput);
  };

  std::vector<std::string> files = {
    // default
    "small.json", "sample.json", "sampleNoWhite.json", "commits.json",

    // all datasets
    "api-docs.json", "countries.json", "directory-tree.json",
    "doubles-small.json", "doubles.json", "file-list.json", "object.json",
    "pass1.json", "pass2.json", "pass3.json", "random1.json", "random2.json",
    "random3.json"};

  int data_set_size = all ? files.size() : 4;
  for (int i = 0; i < data_set_size; i++) {
    run_comparison(files[i]);
  }
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc == 1) {
    RunDefaultBench(false);
    return EXIT_FAILURE;
  }
  if (argc == 2 && ::strcmp(argv[1], "all") == 0) {
    RunDefaultBench(true);
    return EXIT_FAILURE;
  }

  if (argc != 5) {
    Usage(argv);
    return EXIT_FAILURE;
  }

  ParserType parser_type = ParserTypeFromName(argv[4]);
  if (parser_type == ParserType::kUnknown) {
    Usage(argv);
    return EXIT_FAILURE;
  }

  size_t copies = std::stoul(argv[3]);
  int run_time = std::stoi(argv[2]);

  // read input file
  std::string s = ReadFile(argv[1]);

  Run(s, run_time, copies, parser_type, true);

  return EXIT_SUCCESS;
}
