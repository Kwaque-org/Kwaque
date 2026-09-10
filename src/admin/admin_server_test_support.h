#pragma once

#include "src/admin/admin_server.h"

#include <seastar/http/httpd.hh>

namespace kwaque::admin::detail {

struct admin_server_test_access final {
    static seastar::httpd::http_server_control&
    native_server(admin_server& server) {
        return server.native_server_for_testing();
    }
};

} // namespace kwaque::admin::detail
