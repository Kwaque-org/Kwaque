#include "src/base/logging.h"

namespace kwaque::log {

seastar::logger& broker() {
    static seastar::logger logger("kwaque-broker");
    return logger;
}

} // namespace kwaque::log
