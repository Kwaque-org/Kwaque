#define _POSIX_C_SOURCE 200809L

#include "src/broker/cpu_features.h"

#include <errno.h>
#include <limits.h>
#include <stddef.h>
#include <string.h>
#include <unistd.h>

static int fail(const char* message) {
    size_t remaining = strlen(message);
    while (remaining != 0) {
        const ssize_t written = write(STDERR_FILENO, message, remaining);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            break;
        }
        message += written;
        remaining -= (size_t)written;
    }
    return 1;
}

int main(int argc, char** argv) {
    (void)argc;
    // This executable contains no broker dependencies or C++ initialization.
    // Check the CPU before loading the native broker and its shared libraries.
    if (!kwaque_cpu_supported()) {
#if defined(__x86_64__)
        return fail("kwaque: CPU lacks required westmere instructions\n");
#elif defined(__aarch64__)
        return fail(
          "kwaque: CPU lacks required armv8-a+crc+crypto instructions\n");
#endif
    }

    char path[PATH_MAX];
    const ssize_t length = readlink("/proc/self/exe", path, sizeof(path) - 1);
    if (length < 0 || (size_t)length >= sizeof(path) - 1) {
        return fail("kwaque: cannot resolve broker launcher location\n");
    }
    path[length] = '\0';
    char* basename = strrchr(path, '/');
    static const char native_name[] = "kwaque_native";
    if (
      basename == NULL
      || (size_t)(basename + 1 - path) + sizeof(native_name) > sizeof(path)) {
        return fail("kwaque: native broker path is too long\n");
    }
    for (size_t index = 0; index < sizeof(native_name); ++index) {
        basename[index + 1] = native_name[index];
    }
    execv(path, argv);
    return fail("kwaque: cannot execute native broker\n");
}
