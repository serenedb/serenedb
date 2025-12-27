#pragma once

#include <cmath>
#include <iostream>
#include <string_view>
#include <vector>

namespace sdb {

enum class NodeType : uint8_t {
  Term,
  Phrase,
  Number,
  Field,
  And,
  Or,
  Not,
  Required,
  Prohibited,
  RangeInclusive,
  RangeExclusive,
  RangeIncExc,
  RangeExcInc,
  Fuzzy,
  Proximity,
  Boost
};

struct Node {
  NodeType type = NodeType::Term;
  union {
    std::string_view text{};
    double number;
    struct {
      int left;
      int right;
    } op;
    struct {
      int child;
      double param;
    } mod;
  };
};

class ParserContext {
 public:
  ParserContext() { _nodes.reserve(16); }

  int AddString(NodeType type, std::string_view val) {
    auto& node = _nodes.emplace_back();
    node.type = type;
    node.text = val;
    return static_cast<int>(_nodes.size() - 1);
  }

  int AddNumber(double val) {
    auto& node = _nodes.emplace_back();
    node.type = NodeType::Number;
    node.number = val;
    return static_cast<int>(_nodes.size() - 1);
  }

  int AddOp(NodeType type, int l, int r) {
    auto& node = _nodes.emplace_back();
    node.type = type;
    node.op.left = l;
    node.op.right = r;
    return static_cast<int>(_nodes.size() - 1);
  }

  int AddModifier(NodeType type, int child, double param) {
    auto& node = _nodes.emplace_back();
    node.type = type;
    node.mod.child = child;
    node.mod.param = param;
    return static_cast<int>(_nodes.size() - 1);
  }

  int GetNodeCount() const { return static_cast<int>(_nodes.size()); }

 private:
  std::vector<Node> _nodes;
};

}  // namespace sdb
