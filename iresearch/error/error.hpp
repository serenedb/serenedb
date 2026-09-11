////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <exception>

#include "iresearch/utils/string.hpp"

namespace irs {

//////////////////////////////////////////////////////////////////////////////
/// @enum ErrorCode
//////////////////////////////////////////////////////////////////////////////
enum class ErrorCode : uint32_t {
  NoError = 0U,
  NotSupported,
  IoError,
  EofError,
  LockObtainFailed,
  FileNotFound,
  IndexNotFound,
  IndexError,
  NotImplError,
  IllegalArgument,
  IllegalState,
  UndefinedError,
};

#define DECLARE_ERROR_CODE(class_name)                          \
  ::irs::ErrorCode code() const noexcept final { return CODE; } \
  static constexpr ErrorCode CODE = ErrorCode::class_name

//////////////////////////////////////////////////////////////////////////////
/// @struct error_base
//////////////////////////////////////////////////////////////////////////////
struct ErrorBase : std::exception {
  virtual ErrorCode code() const noexcept { return ErrorCode::UndefinedError; }
  const char* what() const noexcept override;
};

//////////////////////////////////////////////////////////////////////////////
/// @class detailed_error_base
//////////////////////////////////////////////////////////////////////////////
class DetailedErrorBase : public ErrorBase {
 public:
  DetailedErrorBase() = default;

  explicit DetailedErrorBase(const char* error) : _error(error) {}

  explicit DetailedErrorBase(std::string&& error) noexcept
    : _error(std::move(error)) {}

  const char* what() const noexcept override { return _error.c_str(); }

 private:
  std::string _error;
};

//////////////////////////////////////////////////////////////////////////////
/// @struct not_supported
//////////////////////////////////////////////////////////////////////////////
struct NotSupported : ErrorBase {
  DECLARE_ERROR_CODE(NotSupported);

  const char* what() const noexcept final;
};

//////////////////////////////////////////////////////////////////////////////
/// @struct io_error
//////////////////////////////////////////////////////////////////////////////
struct IoError : DetailedErrorBase {
  DECLARE_ERROR_CODE(IoError);

  IoError() = default;

  template<typename T>
  explicit IoError(T&& error) : DetailedErrorBase(std::forward<T>(error)) {}
};

//////////////////////////////////////////////////////////////////////////////
/// @struct lock_obtain_failed
//////////////////////////////////////////////////////////////////////////////
class LockObtainFailed : public ErrorBase {
 public:
  DECLARE_ERROR_CODE(LockObtainFailed);

  explicit LockObtainFailed(std::string_view filename = "");
  const char* what() const noexcept final;

 private:
  std::string _error;
};

//////////////////////////////////////////////////////////////////////////////
/// @class file_not_found
//////////////////////////////////////////////////////////////////////////////
class FileNotFound : public ErrorBase {
 public:
  DECLARE_ERROR_CODE(FileNotFound);

  explicit FileNotFound(std::string_view filename = "");
  const char* what() const noexcept final;

 private:
  std::string _error;
};

//////////////////////////////////////////////////////////////////////////////
/// @struct file_not_found
//////////////////////////////////////////////////////////////////////////////
struct IndexNotFound : ErrorBase {
  DECLARE_ERROR_CODE(IndexNotFound);

  const char* what() const noexcept final;
};

//////////////////////////////////////////////////////////////////////////////
/// @struct index_error
//////////////////////////////////////////////////////////////////////////////
struct IndexError : DetailedErrorBase {
  DECLARE_ERROR_CODE(IndexError);

  template<typename T>
  explicit IndexError(T&& error) : DetailedErrorBase(std::forward<T>(error)) {}
};

//////////////////////////////////////////////////////////////////////////////
/// @struct not_impl_error
//////////////////////////////////////////////////////////////////////////////
struct NotImplError : ErrorBase {
  DECLARE_ERROR_CODE(NotImplError);

  const char* what() const noexcept final;
};

//////////////////////////////////////////////////////////////////////////////
/// @struct illegal_argument
//////////////////////////////////////////////////////////////////////////////
struct IllegalArgument : DetailedErrorBase {
  DECLARE_ERROR_CODE(IllegalArgument);

  template<typename T>
  explicit IllegalArgument(T&& error)
    : DetailedErrorBase(std::forward<T>(error)) {}
};

//////////////////////////////////////////////////////////////////////////////
/// @struct illegal_state
//////////////////////////////////////////////////////////////////////////////
struct IllegalState : DetailedErrorBase {
  DECLARE_ERROR_CODE(IllegalState);

  template<typename T>
  explicit IllegalState(T&& error)
    : DetailedErrorBase(std::forward<T>(error)) {}
};

}  // namespace irs
