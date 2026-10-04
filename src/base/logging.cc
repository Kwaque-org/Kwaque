#include "src/base/logging.h"

namespace kwaque::log {

namespace {

seastar::logger broker_logger("kwaque-broker");

} // namespace

seastar::logger& broker() { return broker_logger; }

} // namespace kwaque::log
