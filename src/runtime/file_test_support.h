#ifndef KWAQUE_SRC_RUNTIME_FILE_TEST_SUPPORT_H_
#define KWAQUE_SRC_RUNTIME_FILE_TEST_SUPPORT_H_

#include "src/runtime/file.h"

namespace kwaque::runtime {

class file_test_access final {
public:
    [[nodiscard]] static bool move_is_idle(const file& target) noexcept {
        target.owner_.assert_current();
        return file::move_is_idle(target);
    }
};

} // namespace kwaque::runtime

#endif // KWAQUE_SRC_RUNTIME_FILE_TEST_SUPPORT_H_
