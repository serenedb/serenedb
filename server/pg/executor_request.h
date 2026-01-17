#pragma once

#include <cstdint>

struct Node;
struct CreateFunctionStmt;

namespace sdb::pg {

class ExecutorRequest {
 public:
  enum class Type : uint8_t { GenericDDL = 0, CreateFunction = 1 };

  ExecutorRequest(Type type, const Node& node) noexcept
    : type{type}, node{node} {}

  Type type;
  const Node& node;
};

class CreateFunctionRequest : public ExecutorRequest {
 public:
  CreateFunctionRequest(const Node& node);

  const CreateFunctionStmt& GetStmt();
};

}  // namespace sdb::pg
