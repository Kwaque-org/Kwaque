#pragma once

#include <string_view>

namespace kwaque::testing {

using invariant_observer = void (*)(std::string_view diagnostic);

class scoped_invariant_observer final {
public:
    explicit scoped_invariant_observer(invariant_observer observer) noexcept;
    ~scoped_invariant_observer();

    scoped_invariant_observer(const scoped_invariant_observer&) = delete;
    scoped_invariant_observer&
    operator=(const scoped_invariant_observer&) = delete;
    scoped_invariant_observer(scoped_invariant_observer&&) = delete;
    scoped_invariant_observer& operator=(scoped_invariant_observer&&) = delete;

private:
    invariant_observer previous_;
};

} // namespace kwaque::testing
