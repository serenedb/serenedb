#pragma once

#include <absl/log/check.h>
#include <absl/log/log.h>

#include "raw_logging.h"

// A CHECK() macro that lets you assert the success of a function that
// returns -1 and sets errno in case of an error. E.g.
//
// CHECK_ERR(mkdir(path, 0700));
//
// or
//
// int fd = open(filename, flags); CHECK_ERR(fd) << ": open " << filename;
#define CHECK_ERR(invocation) \
  PLOG_IF(FATAL, ABSL_PREDICT_FALSE((invocation) == -1)) << #invocation
