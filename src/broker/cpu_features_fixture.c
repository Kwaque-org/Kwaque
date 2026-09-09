#include "src/broker/cpu_features.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

// Linked only into the fixture launcher; the runnable broker has no override.
bool kwaque_cpu_supported(void) {
    const char* architecture = getenv("KWAQUE_TEST_ARCH");
    const char* input = getenv("KWAQUE_TEST_FEATURES");
    if (architecture == NULL || input == NULL) {
        return false;
    }
    char* end = NULL;
    errno = 0;
    const unsigned long long features = strtoull(input, &end, 0);
    if (errno != 0 || end == input || *end != '\0') {
        return false;
    }
    if (strcmp(architecture, "x86_64") == 0) {
        return kwaque_x86_64_cpu_supported(
          (uint32_t)features, (uint32_t)(features >> 32));
    }
    if (strcmp(architecture, "aarch64") == 0) {
        return kwaque_aarch64_cpu_supported(features);
    }
    return false;
}
